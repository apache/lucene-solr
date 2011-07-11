package org.apache.lucene.util.fst;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.fst.Builder.UnCompiledNode;

// NOTE: while the FST is able to represent a non-final
// dead-end state (NON_FINAL_END_NODE=0), the layres above
// (FSTEnum, Util) have problems with this!!

/** Represents an FST using a compact byte[] format.
 *  <p> The format is similar to what's used by Morfologik
 *  (http://sourceforge.net/projects/morfologik).
 *
 *  <p><b>NOTE</b>: the FST cannot be larger than ~2.1 GB
 *  because it uses int to address the byte[].
 *
 * @lucene.experimental
 */
public class FST<T> {
  public static enum INPUT_TYPE {BYTE1, BYTE2, BYTE4};
  public final INPUT_TYPE inputType;

  private final static int BIT_FINAL_ARC = 1 << 0;
  private final static int BIT_LAST_ARC = 1 << 1;
  private final static int BIT_TARGET_NEXT = 1 << 2;
  private final static int BIT_STOP_NODE = 1 << 3;
  private final static int BIT_ARC_HAS_OUTPUT = 1 << 4;
  private final static int BIT_ARC_HAS_FINAL_OUTPUT = 1 << 5;

  // Arcs are stored as fixed-size (per entry) array, so
  // that we can find an arc using binary search.  We do
  // this when number of arcs is > NUM_ARCS_ARRAY:
  private final static int BIT_ARCS_AS_FIXED_ARRAY = 1 << 6;

  /**
   * @see #shouldExpand(UnCompiledNode)
   */
  final static int FIXED_ARRAY_SHALLOW_DISTANCE = 3; // 0 => only root node.

  /**
   * @see #shouldExpand(UnCompiledNode)
   */
  final static int FIXED_ARRAY_NUM_ARCS_SHALLOW = 5;

  /**
   * @see #shouldExpand(UnCompiledNode)
   */
  final static int FIXED_ARRAY_NUM_ARCS_DEEP = 10;

  private int[] bytesPerArc = new int[0];

  // Increment version to change it
  private final static String FILE_FORMAT_NAME = "FST";
  private final static int VERSION_START = 0;

  /** Changed numBytesPerArc for array'd case from byte to int. */
  private final static int VERSION_INT_NUM_BYTES_PER_ARC = 1;

  private final static int VERSION_CURRENT = VERSION_INT_NUM_BYTES_PER_ARC;

  // Never serialized; just used to represent the virtual
  // final node w/ no arcs:
  private final static int FINAL_END_NODE = -1;

  // Never serialized; just used to represent the virtual
  // non-final node w/ no arcs:
  private final static int NON_FINAL_END_NODE = 0;

  // if non-null, this FST accepts the empty string and
  // produces this output
  T emptyOutput;
  private byte[] emptyOutputBytes;

  private byte[] bytes;
  int byteUpto = 0;

  private int startNode = -1;

  public final Outputs<T> outputs;

  private int lastFrozenNode;

  private final T NO_OUTPUT;

  public int nodeCount;
  public int arcCount;
  public int arcWithOutputCount;

  // If arc has this label then that arc is final/accepted
  public static final int END_LABEL = -1;

  private boolean allowArrayArcs = true;

  private Arc<T> cachedRootArcs[];

  public final static class Arc<T> {
    public int label;
    public T output;

    int target;

    byte flags;
    public T nextFinalOutput;
    int nextArc;

    // This is non-zero if current arcs are fixed array:
    int posArcsStart;
    int bytesPerArc;
    int arcIdx;
    int numArcs;

    /** Returns this */
    public Arc<T> copyFrom(Arc<T> other) {
      label = other.label;
      target = other.target;
      flags = other.flags;
      output = other.output;
      nextFinalOutput = other.nextFinalOutput;
      nextArc = other.nextArc;
      if (other.bytesPerArc != 0) {
        bytesPerArc = other.bytesPerArc;
        posArcsStart = other.posArcsStart;
        arcIdx = other.arcIdx;
        numArcs = other.numArcs;
      } else {
        bytesPerArc = 0;
      }
      return this;
    }

    boolean flag(int flag) {
      return FST.flag(flags, flag);
    }

    public boolean isLast() {
      return flag(BIT_LAST_ARC);
    }

    public boolean isFinal() {
      return flag(BIT_FINAL_ARC);
    }
  };

  static boolean flag(int flags, int bit) {
    return (flags & bit) != 0;
  }

  private final BytesWriter writer;

  // make a new empty FST, for building
  public FST(INPUT_TYPE inputType, Outputs<T> outputs) {
    this.inputType = inputType;
    this.outputs = outputs;
    bytes = new byte[128];
    NO_OUTPUT = outputs.getNoOutput();
    
    writer = new BytesWriter();

    emptyOutput = null;
  }

  // create an existing FST
  public FST(DataInput in, Outputs<T> outputs) throws IOException {
    this.outputs = outputs;
    writer = null;
    CodecUtil.checkHeader(in, FILE_FORMAT_NAME, VERSION_INT_NUM_BYTES_PER_ARC, VERSION_INT_NUM_BYTES_PER_ARC);
    if (in.readByte() == 1) {
      // accepts empty string
      int numBytes = in.readVInt();
      // messy
      bytes = new byte[numBytes];
      in.readBytes(bytes, 0, numBytes);
      emptyOutput = outputs.read(getBytesReader(numBytes-1));
    } else {
      emptyOutput = null;
    }
    final byte t = in.readByte();
    switch(t) {
      case 0:
        inputType = INPUT_TYPE.BYTE1;
        break;
      case 1:
        inputType = INPUT_TYPE.BYTE2;
        break;
      case 2:
        inputType = INPUT_TYPE.BYTE4;
        break;
    default:
      throw new IllegalStateException("invalid input type " + t);
    }
    startNode = in.readVInt();
    nodeCount = in.readVInt();
    arcCount = in.readVInt();
    arcWithOutputCount = in.readVInt();

    bytes = new byte[in.readVInt()];
    in.readBytes(bytes, 0, bytes.length);
    NO_OUTPUT = outputs.getNoOutput();

    cacheRootArcs();
  }

  public INPUT_TYPE getInputType() {
    return inputType;
  }

  /** Returns bytes used to represent the FST */
  public int sizeInBytes() {
    return bytes.length;
  }

  void finish(int startNode) throws IOException {
    if (startNode == FINAL_END_NODE && emptyOutput != null) {
      startNode = 0;
    }
    if (this.startNode != -1) {
      throw new IllegalStateException("already finished");
    }
    byte[] finalBytes = new byte[writer.posWrite];
    System.arraycopy(bytes, 0, finalBytes, 0, writer.posWrite);
    bytes = finalBytes;
    this.startNode = startNode;

    cacheRootArcs();
  }

  // Caches first 128 labels
  @SuppressWarnings("unchecked")
  private void cacheRootArcs() throws IOException {
    cachedRootArcs = (FST.Arc<T>[]) new FST.Arc[0x80];
    final FST.Arc<T> arc = new FST.Arc<T>();
    getFirstArc(arc);
    final BytesReader in = getBytesReader(0);
    if (targetHasArcs(arc)) {
      readFirstRealArc(arc.target, arc);
      while(true) {
        assert arc.label != END_LABEL;
        if (arc.label < cachedRootArcs.length) {
          cachedRootArcs[arc.label] = new Arc<T>().copyFrom(arc);
        } else {
          break;
        }
        if (arc.isLast()) {
          break;
        }
        readNextRealArc(arc, in);
      }
    }
  }

  void setEmptyOutput(T v) throws IOException {
    if (emptyOutput != null) {
      emptyOutput = outputs.merge(emptyOutput, v);
    } else {
      emptyOutput = v;
    }

    // TODO: this is messy -- replace with sillyBytesWriter; maybe make
    // bytes private
    final int posSave = writer.posWrite;
    outputs.write(emptyOutput, writer);
    emptyOutputBytes = new byte[writer.posWrite-posSave];

    // reverse
    final int stopAt = (writer.posWrite - posSave)/2;
    int upto = 0;
    while(upto < stopAt) {
      final byte b = bytes[posSave + upto];
      bytes[posSave+upto] = bytes[writer.posWrite-upto-1];
      bytes[writer.posWrite-upto-1] = b;
      upto++;
    }
    System.arraycopy(bytes, posSave, emptyOutputBytes, 0, writer.posWrite-posSave);
    writer.posWrite = posSave;
  }

  public void save(DataOutput out) throws IOException {
    if (startNode == -1) {
      throw new IllegalStateException("call finish first");
    }
    CodecUtil.writeHeader(out, FILE_FORMAT_NAME, VERSION_CURRENT);
    // TODO: really we should encode this as an arc, arriving
    // to the root node, instead of special casing here:
    if (emptyOutput != null) {
      out.writeByte((byte) 1);
      out.writeVInt(emptyOutputBytes.length);
      out.writeBytes(emptyOutputBytes, 0, emptyOutputBytes.length);
    } else {
      out.writeByte((byte) 0);
    }
    final byte t;
    if (inputType == INPUT_TYPE.BYTE1) {
      t = 0;
    } else if (inputType == INPUT_TYPE.BYTE2) {
      t = 1;
    } else {
      t = 2;
    }
    out.writeByte(t);
    out.writeVInt(startNode);
    out.writeVInt(nodeCount);
    out.writeVInt(arcCount);
    out.writeVInt(arcWithOutputCount);
    out.writeVInt(bytes.length);
    out.writeBytes(bytes, 0, bytes.length);
  }

  private void writeLabel(int v) throws IOException {
    assert v >= 0: "v=" + v;
    if (inputType == INPUT_TYPE.BYTE1) {
      assert v <= 255: "v=" + v;
      writer.writeByte((byte) v);
    } else if (inputType == INPUT_TYPE.BYTE2) {
      assert v <= 65535: "v=" + v;
      writer.writeVInt(v);
    } else {
      //writeInt(v);
      writer.writeVInt(v);
    }
  }

  int readLabel(DataInput in) throws IOException {
    final int v;
    if (inputType == INPUT_TYPE.BYTE1) {
      v = in.readByte()&0xFF;
    } else { 
      v = in.readVInt();
    }
    return v;
  }

  // returns true if the node at this address has any
  // outgoing arcs
  public boolean targetHasArcs(Arc<T> arc) {
    return arc.target > 0;
  }

  // serializes new node by appending its bytes to the end
  // of the current byte[]
  int addNode(Builder.UnCompiledNode<T> node) throws IOException {
    //System.out.println("FST.addNode pos=" + posWrite + " numArcs=" + node.numArcs);
    if (node.numArcs == 0) {
      if (node.isFinal) {
        return FINAL_END_NODE;
      } else {
        return NON_FINAL_END_NODE;
      }
    }

    int startAddress = writer.posWrite;
    //System.out.println("  startAddr=" + startAddress);

    final boolean doFixedArray = shouldExpand(node);
    final int fixedArrayStart;
    if (doFixedArray) {
      if (bytesPerArc.length < node.numArcs) {
        bytesPerArc = new int[ArrayUtil.oversize(node.numArcs, 1)];
      }
      // write a "false" first arc:
      writer.writeByte((byte) BIT_ARCS_AS_FIXED_ARRAY);
      writer.writeVInt(node.numArcs);
      // placeholder -- we'll come back and write the number
      // of bytes per arc (int) here:
      // TODO: we could make this a vInt instead
      writer.writeInt(0);
      fixedArrayStart = writer.posWrite;
      //System.out.println("  do fixed arcs array arcsStart=" + fixedArrayStart);
    } else {
      fixedArrayStart = 0;
    }

    nodeCount++;
    arcCount += node.numArcs;
    
    final int lastArc = node.numArcs-1;

    int lastArcStart = writer.posWrite;
    int maxBytesPerArc = 0;
    for(int arcIdx=0;arcIdx<node.numArcs;arcIdx++) {
      final Builder.Arc<T> arc = node.arcs[arcIdx];
      final Builder.CompiledNode target = (Builder.CompiledNode) arc.target;
      int flags = 0;

      if (arcIdx == lastArc) {
        flags += BIT_LAST_ARC;
      }

      if (lastFrozenNode == target.address && !doFixedArray) {
        flags += BIT_TARGET_NEXT;
      }

      if (arc.isFinal) {
        flags += BIT_FINAL_ARC;
        if (arc.nextFinalOutput != NO_OUTPUT) {
          flags += BIT_ARC_HAS_FINAL_OUTPUT;
        }
      } else {
        assert arc.nextFinalOutput == NO_OUTPUT;
      }

      boolean targetHasArcs = target.address > 0;

      if (!targetHasArcs) {
        flags += BIT_STOP_NODE;
      }

      if (arc.output != NO_OUTPUT) {
        flags += BIT_ARC_HAS_OUTPUT;
      }

      writer.writeByte((byte) flags);
      writeLabel(arc.label);

      //System.out.println("  write arc: label=" + arc.label + " flags=" + flags);

      if (arc.output != NO_OUTPUT) {
        outputs.write(arc.output, writer);
        arcWithOutputCount++;
      }
      if (arc.nextFinalOutput != NO_OUTPUT) {
        outputs.write(arc.nextFinalOutput, writer);
      }

      if (targetHasArcs && (doFixedArray || lastFrozenNode != target.address)) {
        assert target.address > 0;
        writer.writeInt(target.address);
      }

      // just write the arcs "like normal" on first pass,
      // but record how many bytes each one took, and max
      // byte size:
      if (doFixedArray) {
        bytesPerArc[arcIdx] = writer.posWrite - lastArcStart;
        lastArcStart = writer.posWrite;
        maxBytesPerArc = Math.max(maxBytesPerArc, bytesPerArc[arcIdx]);
        //System.out.println("    bytes=" + bytesPerArc[arcIdx]);
      }
    }

    // TODO: if arc'd arrays will be "too wasteful" by some
    // measure, eg if arcs have vastly different sized
    // outputs, then we should selectively disable array for
    // such cases

    if (doFixedArray) {
      assert maxBytesPerArc > 0;
      // 2nd pass just "expands" all arcs to take up a fixed
      // byte size
      final int sizeNeeded = fixedArrayStart + node.numArcs * maxBytesPerArc;
      bytes = ArrayUtil.grow(bytes, sizeNeeded);
      // TODO: we could make this a vInt instead
      bytes[fixedArrayStart-4] = (byte) (maxBytesPerArc >> 24);
      bytes[fixedArrayStart-3] = (byte) (maxBytesPerArc >> 16);
      bytes[fixedArrayStart-2] = (byte) (maxBytesPerArc >> 8);
      bytes[fixedArrayStart-1] = (byte) maxBytesPerArc;

      // expand the arcs in place, backwards
      int srcPos = writer.posWrite;
      int destPos = fixedArrayStart + node.numArcs*maxBytesPerArc;
      writer.posWrite = destPos;
      for(int arcIdx=node.numArcs-1;arcIdx>=0;arcIdx--) {
        //System.out.println("  repack arcIdx=" + arcIdx + " srcPos=" + srcPos + " destPos=" + destPos);
        destPos -= maxBytesPerArc;
        srcPos -= bytesPerArc[arcIdx];
        if (srcPos != destPos) {
          assert destPos > srcPos;
          System.arraycopy(bytes, srcPos, bytes, destPos, bytesPerArc[arcIdx]);
        }
      }
    }

    // reverse bytes in-place; we do this so that the
    // "BIT_TARGET_NEXT" opto can work, ie, it reads the
    // node just before the current one
    final int endAddress = lastFrozenNode = writer.posWrite - 1;

    int left = startAddress;
    int right = endAddress;
    while (left < right) {
      final byte b = bytes[left];
      bytes[left++] = bytes[right];
      bytes[right--] = b;
    }

    return endAddress;
  }

  /** Fills virtual 'start' arc, ie, an empty incoming arc to
   *  the FST's start node */
  public Arc<T> getFirstArc(Arc<T> arc) {
    if (emptyOutput != null) {
      arc.flags = BIT_FINAL_ARC | BIT_LAST_ARC;
      arc.nextFinalOutput = emptyOutput;
    } else {
      arc.flags = BIT_LAST_ARC;
      arc.nextFinalOutput = NO_OUTPUT;
    }
    arc.output = NO_OUTPUT;

    // If there are no nodes, ie, the FST only accepts the
    // empty string, then startNode is 0, and then readFirstTargetArc
    arc.target = startNode;
    return arc;
  }

  /** Follows the <code>follow</code> arc and reads the last
   *  arc of its target; this changes the provided
   *  <code>arc</code> (2nd arg) in-place and returns it.
   * 
   * @return Returns the second argument
   * (<code>arc</code>). */
  public Arc<T> readLastTargetArc(Arc<T> follow, Arc<T> arc) throws IOException {
    //System.out.println("readLast");
    if (!targetHasArcs(follow)) {
      //System.out.println("  end node");
      assert follow.isFinal();
      arc.label = END_LABEL;
      arc.output = follow.nextFinalOutput;
      arc.flags = BIT_LAST_ARC;
      return arc;
    } else {
      final BytesReader in = getBytesReader(follow.target);
      arc.flags = in.readByte();
      if (arc.flag(BIT_ARCS_AS_FIXED_ARRAY)) {
        // array: jump straight to end
        arc.numArcs = in.readVInt();
        arc.bytesPerArc = in.readInt();
        //System.out.println("  array numArcs=" + arc.numArcs + " bpa=" + arc.bytesPerArc);
        arc.posArcsStart = in.pos;
        arc.arcIdx = arc.numArcs - 2;
      } else {
        // non-array: linear scan
        arc.bytesPerArc = 0;
        //System.out.println("  scan");
        while(!arc.isLast()) {
          // skip this arc:
          readLabel(in);
          if (arc.flag(BIT_ARC_HAS_OUTPUT)) {
            outputs.read(in);
          }
          if (arc.flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
            outputs.read(in);
          }
          if (arc.flag(BIT_STOP_NODE)) {
          } else if (arc.flag(BIT_TARGET_NEXT)) {
          } else {
            in.pos -= 4;
          }
          arc.flags = in.readByte();
        }
        arc.nextArc = in.pos+1;
      }
      readNextRealArc(arc, in);
      assert arc.isLast();
      return arc;
    }
  }

  /**
   * Follow the <code>follow</code> arc and read the first arc of its target;
   * this changes the provided <code>arc</code> (2nd arg) in-place and returns
   * it.
   * 
   * @return Returns the second argument (<code>arc</code>).
   */
  public Arc<T> readFirstTargetArc(Arc<T> follow, Arc<T> arc) throws IOException {
    //int pos = address;
    //System.out.println("    readFirstTarget follow.target=" + follow.target + " isFinal=" + follow.isFinal());
    if (follow.isFinal()) {
      // Insert "fake" final first arc:
      arc.label = END_LABEL;
      arc.output = follow.nextFinalOutput;
      if (follow.target <= 0) {
        arc.flags = BIT_LAST_ARC;
      } else {
        arc.flags = 0;
        arc.nextArc = follow.target;
      }
      //System.out.println("    insert isFinal; nextArc=" + follow.target + " isLast=" + arc.isLast() + " output=" + outputs.outputToString(arc.output));
      return arc;
    } else {
      return readFirstRealArc(follow.target, arc);
    }
  }

  // Not private because NodeHash needs access:
  Arc<T> readFirstRealArc(int address, Arc<T> arc) throws IOException {

    final BytesReader in = getBytesReader(address);

    arc.flags = in.readByte();

    if (arc.flag(BIT_ARCS_AS_FIXED_ARRAY)) {
      //System.out.println("  fixedArray");
      // this is first arc in a fixed-array
      arc.numArcs = in.readVInt();
      arc.bytesPerArc = in.readInt();
      arc.arcIdx = -1;
      arc.nextArc = arc.posArcsStart = in.pos;
      //System.out.println("  bytesPer=" + arc.bytesPerArc + " numArcs=" + arc.numArcs + " arcsStart=" + pos);
    } else {
      arc.nextArc = address;
      arc.bytesPerArc = 0;
    }
    return readNextRealArc(arc, in);
  }

  /**
   * Checks if <code>arc</code>'s target state is in expanded (or vector) format. 
   * 
   * @return Returns <code>true</code> if <code>arc</code> points to a state in an
   * expanded array format.
   */
  boolean isExpandedTarget(Arc<T> follow) throws IOException {
    if (!targetHasArcs(follow)) {
      return false;
    } else {
      final BytesReader in = getBytesReader(follow.target);
      final byte b = in.readByte();
      return (b & BIT_ARCS_AS_FIXED_ARRAY) != 0;
    }
  }

  /** In-place read; returns the arc. */
  public Arc<T> readNextArc(Arc<T> arc) throws IOException {
    if (arc.label == END_LABEL) {
      // This was a fake inserted "final" arc
      if (arc.nextArc <= 0) {
        // This arc went to virtual final node, ie has no outgoing arcs
        return null;
      }
      return readFirstRealArc(arc.nextArc, arc);
    } else {
      return readNextRealArc(arc, getBytesReader(0));
    }
  }

  /** Peeks at next arc's label; does not alter arc.  Do
   *  not call this if arc.isLast()! */
  public int readNextArcLabel(Arc<T> arc) throws IOException {
    assert !arc.isLast();

    final BytesReader in;
    if (arc.label == END_LABEL) {
      //System.out.println("    nextArc fake " + arc.nextArc);
      in = getBytesReader(arc.nextArc);
      byte flags = bytes[in.pos];
      if (flag(flags, BIT_ARCS_AS_FIXED_ARRAY)) {
        //System.out.println("    nextArc fake array");
        in.pos--;
        in.readVInt();
        in.readInt();
      }
    } else {
      if (arc.bytesPerArc != 0) {
        //System.out.println("    nextArc real array");
        // arcs are at fixed entries
        in = getBytesReader(arc.posArcsStart - (1+arc.arcIdx)*arc.bytesPerArc);
      } else {
        // arcs are packed
        //System.out.println("    nextArc real packed");
        in = getBytesReader(arc.nextArc);
      }
    }
    // skip flags
    in.readByte();
    return readLabel(in);
  }

  Arc<T> readNextRealArc(Arc<T> arc, final BytesReader in) throws IOException {
    // this is a continuing arc in a fixed array
    if (arc.bytesPerArc != 0) {
      // arcs are at fixed entries
      arc.arcIdx++;
      assert arc.arcIdx < arc.numArcs;
      in.pos = arc.posArcsStart - arc.arcIdx*arc.bytesPerArc;
    } else {
      // arcs are packed
      in.pos = arc.nextArc;
    }
    arc.flags = in.readByte();
    arc.label = readLabel(in);

    if (arc.flag(BIT_ARC_HAS_OUTPUT)) {
      arc.output = outputs.read(in);
    } else {
      arc.output = outputs.getNoOutput();
    }

    if (arc.flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
      arc.nextFinalOutput = outputs.read(in);
    } else {
      arc.nextFinalOutput = outputs.getNoOutput();
    }

    if (arc.flag(BIT_STOP_NODE)) {
      if (arc.flag(BIT_FINAL_ARC)) {
        arc.target = FINAL_END_NODE;
      } else {
        arc.target = NON_FINAL_END_NODE;
      }
      arc.nextArc = in.pos;
    } else if (arc.flag(BIT_TARGET_NEXT)) {
      arc.nextArc = in.pos;
      if (!arc.flag(BIT_LAST_ARC)) {
        if (arc.bytesPerArc == 0) {
          // must scan
          seekToNextNode(in);
        } else {
          in.pos = arc.posArcsStart - arc.bytesPerArc * arc.numArcs;
        }
      }
      arc.target = in.pos;
    } else {
      arc.target = in.readInt();
      arc.nextArc = in.pos;
    }

    return arc;
  }

  /** Finds an arc leaving the incoming arc, replacing the arc in place.
   *  This returns null if the arc was not found, else the incoming arc. */
  public Arc<T> findTargetArc(int labelToMatch, Arc<T> follow, Arc<T> arc) throws IOException {
    assert cachedRootArcs != null;
    // Short-circuit if this arc is in the root arc cache:
    if (follow.target == startNode && labelToMatch != END_LABEL && labelToMatch < cachedRootArcs.length) {
      final Arc<T> result = cachedRootArcs[labelToMatch];
      if (result == null) {
        return result;
      } else {
        arc.copyFrom(result);
        return arc;
      }
    }
 
    if (labelToMatch == END_LABEL) {
      if (follow.isFinal()) {
        if (follow.target <= 0) {
          arc.flags = BIT_LAST_ARC;
        } else {
          arc.flags = 0;
          arc.nextArc = follow.target;
        }
        arc.output = follow.nextFinalOutput;
        arc.label = END_LABEL;
        return arc;
      } else {
        return null;
      }
    }

    if (!targetHasArcs(follow)) {
      return null;
    }

    // TODO: maybe make an explicit thread state that holds
    // reusable stuff eg BytesReader:
    final BytesReader in = getBytesReader(follow.target);

    // System.out.println("fta label=" + (char) labelToMatch);

    if ((in.readByte() & BIT_ARCS_AS_FIXED_ARRAY) != 0) {
      // Arcs are full array; do binary search:
      arc.numArcs = in.readVInt();
      //System.out.println("  bs " + arc.numArcs);
      arc.bytesPerArc = in.readInt();
      arc.posArcsStart = in.pos;
      int low = 0;
      int high = arc.numArcs-1;
      while (low <= high) {
        //System.out.println("    cycle");
        int mid = (low + high) >>> 1;
        in.pos = arc.posArcsStart - arc.bytesPerArc*mid - 1;
        int midLabel = readLabel(in);
        final int cmp = midLabel - labelToMatch;
        if (cmp < 0)
          low = mid + 1;
        else if (cmp > 0)
          high = mid - 1;
        else {
          arc.arcIdx = mid-1;
          //System.out.println("    found!");
          return readNextRealArc(arc, in);
        }
      }

      return null;
    }

    // Linear scan
    readFirstTargetArc(follow, arc);
    while(true) {
      //System.out.println("  non-bs cycle");
      // TODO: we should fix this code to not have to create
      // object for the output of every arc we scan... only
      // for the matching arc, if found
      if (arc.label == labelToMatch) {
        //System.out.println("    found!");
        return arc;
      } else if (arc.label > labelToMatch) {
        return null;
      } else if (arc.isLast()) {
        return null;
      } else {
        readNextArc(arc);
      }
    }
  }

  private void seekToNextNode(BytesReader in) throws IOException {

    while(true) {

      final int flags = in.readByte();
      readLabel(in);

      if (flag(flags, BIT_ARC_HAS_OUTPUT)) {
        outputs.read(in);
      }

      if (flag(flags, BIT_ARC_HAS_FINAL_OUTPUT)) {
        outputs.read(in);
      }

      if (!flag(flags, BIT_STOP_NODE) && !flag(flags, BIT_TARGET_NEXT)) {
        in.readInt();
      }

      if (flag(flags, BIT_LAST_ARC)) {
        return;
      }
    }
  }

  public int getNodeCount() {
    // 1+ in order to count the -1 implicit final node
    return 1+nodeCount;
  }
  
  public int getArcCount() {
    return arcCount;
  }

  public int getArcWithOutputCount() {
    return arcWithOutputCount;
  }

  public void setAllowArrayArcs(boolean v) {
    allowArrayArcs = v;
  }
  
  /**
   * Nodes will be expanded if their depth (distance from the root node) is
   * &lt;= this value and their number of arcs is &gt;=
   * {@link #FIXED_ARRAY_NUM_ARCS_SHALLOW}.
   * 
   * <p>
   * Fixed array consumes more RAM but enables binary search on the arcs
   * (instead of a linear scan) on lookup by arc label.
   * 
   * @return <code>true</code> if <code>node</code> should be stored in an
   *         expanded (array) form.
   * 
   * @see #FIXED_ARRAY_NUM_ARCS_DEEP
   * @see Builder.UnCompiledNode#depth
   */
  private boolean shouldExpand(UnCompiledNode<T> node) {
    return allowArrayArcs &&
      ((node.depth <= FIXED_ARRAY_SHALLOW_DISTANCE && node.numArcs >= FIXED_ARRAY_NUM_ARCS_SHALLOW) || 
       node.numArcs >= FIXED_ARRAY_NUM_ARCS_DEEP);
  }

  // Non-static: writes to FST's byte[]
  class BytesWriter extends DataOutput {
    int posWrite;

    public BytesWriter() {
      // pad: ensure no node gets address 0 which is reserved to mean
      // the stop state w/ no arcs
      posWrite = 1;
    }

    @Override
    public void writeByte(byte b) {
      if (bytes.length == posWrite) {
        bytes = ArrayUtil.grow(bytes);
      }
      assert posWrite < bytes.length: "posWrite=" + posWrite + " bytes.length=" + bytes.length;
      bytes[posWrite++] = b;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
      final int size = posWrite + length;
      bytes = ArrayUtil.grow(bytes, size);
      System.arraycopy(b, offset, bytes, posWrite, length);
      posWrite += length;
    }
  }

  final BytesReader getBytesReader(int pos) {
    // TODO: maybe re-use via ThreadLocal?
    return new BytesReader(pos);
  }

  // Non-static: reads byte[] from FST
  final class BytesReader extends DataInput {
    int pos;

    public BytesReader(int pos) {
      this.pos = pos;
    }

    @Override
    public byte readByte() {
      return bytes[pos--];
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) {
      for(int i=0;i<len;i++) {
        b[offset+i] = bytes[pos--];
      }
    }
  }
}
