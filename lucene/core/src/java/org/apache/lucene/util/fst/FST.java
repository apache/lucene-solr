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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.Builder.UnCompiledNode;

// TODO: break this into WritableFST and ReadOnlyFST.. then
// we can have subclasses of ReadOnlyFST to handle the
// different byte[] level encodings (packed or
// not)... and things like nodeCount, arcCount are read only

// TODO: if FST is pure prefix trie we can do a more compact
// job, ie, once we are at a 'suffix only', just store the
// completion labels as a string not as a series of arcs.

// TODO: maybe make an explicit thread state that holds
// reusable stuff eg BytesReader, a scratch arc

// NOTE: while the FST is able to represent a non-final
// dead-end state (NON_FINAL_END_NODE=0), the layers above
// (FSTEnum, Util) have problems with this!!

/** Represents an finite state machine (FST), using a
 *  compact byte[] format.
 *  <p> The format is similar to what's used by Morfologik
 *  (http://sourceforge.net/projects/morfologik).
 *
 *  <p><b>NOTE</b>: the FST cannot be larger than ~2.1 GB
 *  because it uses int to address the byte[].
 *
 * @lucene.experimental
 */
public final class FST<T> {
  /** Specifies allowed range of each int input label for
   *  this FST. */
  public static enum INPUT_TYPE {BYTE1, BYTE2, BYTE4};
  public final INPUT_TYPE inputType;

  final static int BIT_FINAL_ARC = 1 << 0;
  final static int BIT_LAST_ARC = 1 << 1;
  final static int BIT_TARGET_NEXT = 1 << 2;

  // TODO: we can free up a bit if we can nuke this:
  final static int BIT_STOP_NODE = 1 << 3;
  final static int BIT_ARC_HAS_OUTPUT = 1 << 4;
  final static int BIT_ARC_HAS_FINAL_OUTPUT = 1 << 5;

  // Arcs are stored as fixed-size (per entry) array, so
  // that we can find an arc using binary search.  We do
  // this when number of arcs is > NUM_ARCS_ARRAY:

  // If set, the target node is delta coded vs current
  // position:
  private final static int BIT_TARGET_DELTA = 1 << 6;

  private final static byte ARCS_AS_FIXED_ARRAY = BIT_ARC_HAS_FINAL_OUTPUT;

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

  /** Write BYTE2 labels as 2-byte short, not vInt. */
  private final static int VERSION_SHORT_BYTE2_LABELS = 2;

  /** Added optional packed format. */
  private final static int VERSION_PACKED = 3;

  private final static int VERSION_CURRENT = VERSION_PACKED;

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

  // Not private to avoid synthetic access$NNN methods:
  byte[] bytes;
  int byteUpto = 0;

  private int startNode = -1;

  public final Outputs<T> outputs;

  private int lastFrozenNode;

  private final T NO_OUTPUT;

  public int nodeCount;
  public int arcCount;
  public int arcWithOutputCount;

  private final boolean packed;
  private final int[] nodeRefToAddress;

  // If arc has this label then that arc is final/accepted
  public static final int END_LABEL = -1;

  private boolean allowArrayArcs = true;

  private Arc<T> cachedRootArcs[];

  /** Represents a single arc. */
  public final static class Arc<T> {
    public int label;
    public T output;

    // From node (ord or address); currently only used when
    // building an FST w/ willPackFST=true:
    int node;

    // To node (ord or address):
    public int target;

    byte flags;
    public T nextFinalOutput;

    // address (into the byte[]), or ord/address if label == END_LABEL
    int nextArc;

    // This is non-zero if current arcs are fixed array:
    int posArcsStart;
    int bytesPerArc;
    int arcIdx;
    int numArcs;

    /** Returns this */
    public Arc<T> copyFrom(Arc<T> other) {
      node = other.node;
      label = other.label;
      target = other.target;
      flags = other.flags;
      output = other.output;
      nextFinalOutput = other.nextFinalOutput;
      nextArc = other.nextArc;
      bytesPerArc = other.bytesPerArc;
      if (bytesPerArc != 0) {
        posArcsStart = other.posArcsStart;
        arcIdx = other.arcIdx;
        numArcs = other.numArcs;
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

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("node=" + node);
      b.append(" target=" + target);
      b.append(" label=" + label);
      if (flag(BIT_LAST_ARC)) {
        b.append(" last");
      }
      if (flag(BIT_FINAL_ARC)) {
        b.append(" final");
      }
      if (flag(BIT_TARGET_NEXT)) {
        b.append(" targetNext");
      }
      if (flag(BIT_ARC_HAS_OUTPUT)) {
        b.append(" output=" + output);
      }
      if (flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
        b.append(" nextFinalOutput=" + nextFinalOutput);
      }
      if (bytesPerArc != 0) {
        b.append(" arcArray(idx=" + arcIdx + " of " + numArcs + ")");
      }
      return b.toString();
    }
  };

  private final static boolean flag(int flags, int bit) {
    return (flags & bit) != 0;
  }

  private final BytesWriter writer;

  // TODO: we can save RAM here by using growable packed
  // ints...:
  private int[] nodeAddress;

  // TODO: we could be smarter here, and prune periodically
  // as we go; high in-count nodes will "usually" become
  // clear early on:
  private int[] inCounts;

  // make a new empty FST, for building; Builder invokes
  // this ctor
  FST(INPUT_TYPE inputType, Outputs<T> outputs, boolean willPackFST) {
    this.inputType = inputType;
    this.outputs = outputs;
    bytes = new byte[128];
    NO_OUTPUT = outputs.getNoOutput();
    if (willPackFST) {
      nodeAddress = new int[8];
      inCounts = new int[8];
    } else {
      nodeAddress = null;
      inCounts = null;
    }
    
    writer = new BytesWriter();

    emptyOutput = null;
    packed = false;
    nodeRefToAddress = null;
  }

  /** Load a previously saved FST. */
  public FST(DataInput in, Outputs<T> outputs) throws IOException {
    this.outputs = outputs;
    writer = null;
    // NOTE: only reads most recent format; we don't have
    // back-compat promise for FSTs (they are experimental):
    CodecUtil.checkHeader(in, FILE_FORMAT_NAME, VERSION_PACKED, VERSION_PACKED);
    packed = in.readByte() == 1;
    if (in.readByte() == 1) {
      // accepts empty string
      int numBytes = in.readVInt();
      // messy
      bytes = new byte[numBytes];
      in.readBytes(bytes, 0, numBytes);
      if (packed) {
        emptyOutput = outputs.read(getBytesReader(0));
      } else {
        emptyOutput = outputs.read(getBytesReader(numBytes-1));
      }
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
    if (packed) {
      final int nodeRefCount = in.readVInt();
      nodeRefToAddress = new int[nodeRefCount];
      for(int idx=0;idx<nodeRefCount;idx++) {
        nodeRefToAddress[idx] = in.readVInt();
      }
    } else {
      nodeRefToAddress = null;
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
    int size = bytes.length;
    if (packed) {
      size += nodeRefToAddress.length * RamUsageEstimator.NUM_BYTES_INT;
    } else if (nodeAddress != null) {
      size += nodeAddress.length * RamUsageEstimator.NUM_BYTES_INT;
      size += inCounts.length * RamUsageEstimator.NUM_BYTES_INT;
    }
    return size;
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

  private int getNodeAddress(int node) {
    if (nodeAddress != null) {
      // Deref
      return nodeAddress[node];
    } else {
      // Straight
      return node;
    }
  }

  // Caches first 128 labels
  @SuppressWarnings({"rawtypes","unchecked"})
  private void cacheRootArcs() throws IOException {
    cachedRootArcs = (Arc<T>[]) new Arc[0x80];
    final Arc<T> arc = new Arc<T>();
    getFirstArc(arc);
    final BytesReader in = getBytesReader(0);
    if (targetHasArcs(arc)) {
      readFirstRealTargetArc(arc.target, arc, in);
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

  public T getEmptyOutput() {
    return emptyOutput;
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

    if (!packed) {
      // reverse
      final int stopAt = (writer.posWrite - posSave)/2;
      int upto = 0;
      while(upto < stopAt) {
        final byte b = bytes[posSave + upto];
        bytes[posSave+upto] = bytes[writer.posWrite-upto-1];
        bytes[writer.posWrite-upto-1] = b;
        upto++;
      }
    }
    System.arraycopy(bytes, posSave, emptyOutputBytes, 0, writer.posWrite-posSave);
    writer.posWrite = posSave;
  }

  public void save(DataOutput out) throws IOException {
    if (startNode == -1) {
      throw new IllegalStateException("call finish first");
    }
    if (nodeAddress != null) {
      throw new IllegalStateException("cannot save an FST pre-packed FST; it must first be packed");
    }
    CodecUtil.writeHeader(out, FILE_FORMAT_NAME, VERSION_CURRENT);
    if (packed) {
      out.writeByte((byte) 1);
    } else {
      out.writeByte((byte) 0);
    }
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
    if (packed) {
      assert nodeRefToAddress != null;
      out.writeVInt(nodeRefToAddress.length);
      for(int idx=0;idx<nodeRefToAddress.length;idx++) {
        out.writeVInt(nodeRefToAddress[idx]);
      }
    }
    out.writeVInt(startNode);
    out.writeVInt(nodeCount);
    out.writeVInt(arcCount);
    out.writeVInt(arcWithOutputCount);
    out.writeVInt(bytes.length);
    out.writeBytes(bytes, 0, bytes.length);
  }
  
  /**
   * Writes an automaton to a file. 
   */
  public void save(final File file) throws IOException {
    boolean success = false;
    OutputStream os = new BufferedOutputStream(new FileOutputStream(file));
    try {
      save(new OutputStreamDataOutput(os));
      success = true;
    } finally { 
      if (success) { 
        IOUtils.close(os);
      } else {
        IOUtils.closeWhileHandlingException(os); 
      }
    }
  }

  /**
   * Reads an automaton from a file. 
   */
  public static <T> FST<T> read(File file, Outputs<T> outputs) throws IOException {
    InputStream is = new BufferedInputStream(new FileInputStream(file));
    boolean success = false;
    try {
      FST<T> fst = new FST<T>(new InputStreamDataInput(is), outputs);
      success = true;
      return fst;
    } finally {
      if (success) { 
        IOUtils.close(is);
      } else {
        IOUtils.closeWhileHandlingException(is); 
      }
    }
  }

  private void writeLabel(int v) throws IOException {
    assert v >= 0: "v=" + v;
    if (inputType == INPUT_TYPE.BYTE1) {
      assert v <= 255: "v=" + v;
      writer.writeByte((byte) v);
    } else if (inputType == INPUT_TYPE.BYTE2) {
      assert v <= 65535: "v=" + v;
      writer.writeShort((short) v);
    } else {
      //writeInt(v);
      writer.writeVInt(v);
    }
  }

  int readLabel(DataInput in) throws IOException {
    final int v;
    if (inputType == INPUT_TYPE.BYTE1) {
      // Unsigned byte:
      v = in.readByte()&0xFF;
    } else if (inputType == INPUT_TYPE.BYTE2) {
      // Unsigned short:
      v = in.readShort()&0xFFFF;
    } else { 
      v = in.readVInt();
    }
    return v;
  }

  // returns true if the node at this address has any
  // outgoing arcs
  public static<T> boolean targetHasArcs(Arc<T> arc) {
    return arc.target > 0;
  }

  // serializes new node by appending its bytes to the end
  // of the current byte[]
  int addNode(Builder.UnCompiledNode<T> nodeIn) throws IOException {
    //System.out.println("FST.addNode pos=" + writer.posWrite + " numArcs=" + nodeIn.numArcs);
    if (nodeIn.numArcs == 0) {
      if (nodeIn.isFinal) {
        return FINAL_END_NODE;
      } else {
        return NON_FINAL_END_NODE;
      }
    }

    int startAddress = writer.posWrite;
    //System.out.println("  startAddr=" + startAddress);

    final boolean doFixedArray = shouldExpand(nodeIn);
    final int fixedArrayStart;
    if (doFixedArray) {
      if (bytesPerArc.length < nodeIn.numArcs) {
        bytesPerArc = new int[ArrayUtil.oversize(nodeIn.numArcs, 1)];
      }
      // write a "false" first arc:
      writer.writeByte(ARCS_AS_FIXED_ARRAY);
      writer.writeVInt(nodeIn.numArcs);
      // placeholder -- we'll come back and write the number
      // of bytes per arc (int) here:
      // TODO: we could make this a vInt instead
      writer.writeInt(0);
      fixedArrayStart = writer.posWrite;
      //System.out.println("  do fixed arcs array arcsStart=" + fixedArrayStart);
    } else {
      fixedArrayStart = 0;
    }

    arcCount += nodeIn.numArcs;
    
    final int lastArc = nodeIn.numArcs-1;

    int lastArcStart = writer.posWrite;
    int maxBytesPerArc = 0;
    for(int arcIdx=0;arcIdx<nodeIn.numArcs;arcIdx++) {
      final Builder.Arc<T> arc = nodeIn.arcs[arcIdx];
      final Builder.CompiledNode target = (Builder.CompiledNode) arc.target;
      int flags = 0;

      if (arcIdx == lastArc) {
        flags += BIT_LAST_ARC;
      }

      if (lastFrozenNode == target.node && !doFixedArray) {
        // TODO: for better perf (but more RAM used) we
        // could avoid this except when arc is "near" the
        // last arc:
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

      boolean targetHasArcs = target.node > 0;

      if (!targetHasArcs) {
        flags += BIT_STOP_NODE;
      } else if (inCounts != null) {
        inCounts[target.node]++;
      }

      if (arc.output != NO_OUTPUT) {
        flags += BIT_ARC_HAS_OUTPUT;
      }

      writer.writeByte((byte) flags);
      writeLabel(arc.label);

      // System.out.println("  write arc: label=" + (char) arc.label + " flags=" + flags + " target=" + target.node + " pos=" + writer.posWrite + " output=" + outputs.outputToString(arc.output));

      if (arc.output != NO_OUTPUT) {
        outputs.write(arc.output, writer);
        //System.out.println("    write output");
        arcWithOutputCount++;
      }

      if (arc.nextFinalOutput != NO_OUTPUT) {
        //System.out.println("    write final output");
        outputs.write(arc.nextFinalOutput, writer);
      }

      if (targetHasArcs && (flags & BIT_TARGET_NEXT) == 0) {
        assert target.node > 0;
        //System.out.println("    write target");
        writer.writeInt(target.node);
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
      //System.out.println("  doFixedArray");
      assert maxBytesPerArc > 0;
      // 2nd pass just "expands" all arcs to take up a fixed
      // byte size
      final int sizeNeeded = fixedArrayStart + nodeIn.numArcs * maxBytesPerArc;
      bytes = ArrayUtil.grow(bytes, sizeNeeded);
      // TODO: we could make this a vInt instead
      bytes[fixedArrayStart-4] = (byte) (maxBytesPerArc >> 24);
      bytes[fixedArrayStart-3] = (byte) (maxBytesPerArc >> 16);
      bytes[fixedArrayStart-2] = (byte) (maxBytesPerArc >> 8);
      bytes[fixedArrayStart-1] = (byte) maxBytesPerArc;

      // expand the arcs in place, backwards
      int srcPos = writer.posWrite;
      int destPos = fixedArrayStart + nodeIn.numArcs*maxBytesPerArc;
      writer.posWrite = destPos;
      for(int arcIdx=nodeIn.numArcs-1;arcIdx>=0;arcIdx--) {
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
    final int endAddress = writer.posWrite - 1;

    int left = startAddress;
    int right = endAddress;
    while (left < right) {
      final byte b = bytes[left];
      bytes[left++] = bytes[right];
      bytes[right--] = b;
    }
    //System.out.println("  endAddress=" + endAddress);

    nodeCount++;
    final int node;
    if (nodeAddress != null) {
      // Nodes are addressed by 1+ord:
      if (nodeCount == nodeAddress.length) {
        nodeAddress = ArrayUtil.grow(nodeAddress);
        inCounts = ArrayUtil.grow(inCounts);
      }
      nodeAddress[nodeCount] = endAddress;
      // System.out.println("  write nodeAddress[" + nodeCount + "] = " + endAddress);
      node = nodeCount;
    } else {
      node = endAddress;
    }
    lastFrozenNode = node;

    return node;
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
    // empty string, then startNode is 0
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
      arc.target = FINAL_END_NODE;
      arc.output = follow.nextFinalOutput;
      arc.flags = BIT_LAST_ARC;
      return arc;
    } else {
      final BytesReader in = getBytesReader(getNodeAddress(follow.target));
      arc.node = follow.target;
      final byte b = in.readByte();
      if (b == ARCS_AS_FIXED_ARRAY) {
        // array: jump straight to end
        arc.numArcs = in.readVInt();
        if (packed) {
          arc.bytesPerArc = in.readVInt();
        } else {
          arc.bytesPerArc = in.readInt();
        }
        //System.out.println("  array numArcs=" + arc.numArcs + " bpa=" + arc.bytesPerArc);
        arc.posArcsStart = in.pos;
        arc.arcIdx = arc.numArcs - 2;
      } else {
        arc.flags = b;
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
            if (packed) {
              in.readVInt();
            } else {
              in.skip(4);
            }
          }
          arc.flags = in.readByte();
        }
        // Undo the byte flags we read: 
        in.skip(-1);
        arc.nextArc = in.pos;
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
      arc.flags = BIT_FINAL_ARC;
      if (follow.target <= 0) {
        arc.flags |= BIT_LAST_ARC;
      } else {
        arc.node = follow.target;
        // NOTE: nextArc is a node (not an address!) in this case:
        arc.nextArc = follow.target;
      }
      arc.target = FINAL_END_NODE;
      //System.out.println("    insert isFinal; nextArc=" + follow.target + " isLast=" + arc.isLast() + " output=" + outputs.outputToString(arc.output));
      return arc;
    } else {
      return readFirstRealTargetArc(follow.target, arc, getBytesReader(0));
    }
  }

  public Arc<T> readFirstRealTargetArc(int node, Arc<T> arc, final BytesReader in) throws IOException {
    assert in.bytes == bytes;
    final int address = getNodeAddress(node);
    in.pos = address;
    //System.out.println("  readFirstRealTargtArc address="
    //+ address);
    //System.out.println("   flags=" + arc.flags);
    arc.node = node;

    if (in.readByte() == ARCS_AS_FIXED_ARRAY) {
      //System.out.println("  fixedArray");
      // this is first arc in a fixed-array
      arc.numArcs = in.readVInt();
      if (packed) {
        arc.bytesPerArc = in.readVInt();
      } else {
        arc.bytesPerArc = in.readInt();
      }
      arc.arcIdx = -1;
      arc.nextArc = arc.posArcsStart = in.pos;
      //System.out.println("  bytesPer=" + arc.bytesPerArc + " numArcs=" + arc.numArcs + " arcsStart=" + pos);
    } else {
      //arc.flags = b;
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
      final BytesReader in = getBytesReader(getNodeAddress(follow.target));
      return in.readByte() == ARCS_AS_FIXED_ARRAY;
    }
  }

  /** In-place read; returns the arc. */
  public Arc<T> readNextArc(Arc<T> arc) throws IOException {
    if (arc.label == END_LABEL) {
      // This was a fake inserted "final" arc
      if (arc.nextArc <= 0) {
        throw new IllegalArgumentException("cannot readNextArc when arc.isLast()=true");
      }
      return readFirstRealTargetArc(arc.nextArc, arc, getBytesReader(0));
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
      in = getBytesReader(getNodeAddress(arc.nextArc));
      final byte b = bytes[in.pos];
      if (b == ARCS_AS_FIXED_ARRAY) {
        //System.out.println("    nextArc fake array");
        in.skip(1);
        in.readVInt();
        if (packed) {
          in.readVInt();
        } else {
          in.readInt();
        }
      }
    } else {
      if (arc.bytesPerArc != 0) {
        //System.out.println("    nextArc real array");
        // arcs are at fixed entries
        in = getBytesReader(arc.posArcsStart);
        in.skip((1+arc.arcIdx)*arc.bytesPerArc);
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

  /** Never returns null, but you should never call this if
   *  arc.isLast() is true. */
  public Arc<T> readNextRealArc(Arc<T> arc, final BytesReader in) throws IOException {
    assert in.bytes == bytes;

    // TODO: can't assert this because we call from readFirstArc
    // assert !flag(arc.flags, BIT_LAST_ARC);

    // this is a continuing arc in a fixed array
    if (arc.bytesPerArc != 0) {
      // arcs are at fixed entries
      arc.arcIdx++;
      assert arc.arcIdx < arc.numArcs;
      in.skip(arc.posArcsStart, arc.arcIdx*arc.bytesPerArc);
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
      // TODO: would be nice to make this lazy -- maybe
      // caller doesn't need the target and is scanning arcs...
      if (nodeAddress == null) {
        if (!arc.flag(BIT_LAST_ARC)) {
          if (arc.bytesPerArc == 0) {
            // must scan
            seekToNextNode(in);
          } else {
            in.skip(arc.posArcsStart, arc.bytesPerArc * arc.numArcs);
          }
        }
        arc.target = in.pos;
      } else {
        arc.target = arc.node - 1;
        assert arc.target > 0;
      }
    } else {
      if (packed) {
        final int pos = in.pos;
        final int code = in.readVInt();
        if (arc.flag(BIT_TARGET_DELTA)) {
          // Address is delta-coded from current address:
          arc.target = pos + code;
          //System.out.println("    delta pos=" + pos + " delta=" + code + " target=" + arc.target);
        } else if (code < nodeRefToAddress.length) {
          // Deref
          arc.target = nodeRefToAddress[code];
          //System.out.println("    deref code=" + code + " target=" + arc.target);
        } else {
          // Absolute
          arc.target = code;
          //System.out.println("    abs code=" + code + " derefLen=" + nodeRefToAddress.length);
        }
      } else {
        arc.target = in.readInt();
      }
      arc.nextArc = in.pos;
    }
    return arc;
  }

  /** Finds an arc leaving the incoming arc, replacing the arc in place.
   *  This returns null if the arc was not found, else the incoming arc. */
  public Arc<T> findTargetArc(int labelToMatch, Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
    assert cachedRootArcs != null;
    assert in.bytes == bytes;

    if (labelToMatch == END_LABEL) {
      if (follow.isFinal()) {
        if (follow.target <= 0) {
          arc.flags = BIT_LAST_ARC;
        } else {
          arc.flags = 0;
          // NOTE: nextArc is a node (not an address!) in this case:
          arc.nextArc = follow.target;
          arc.node = follow.target;
        }
        arc.output = follow.nextFinalOutput;
        arc.label = END_LABEL;
        return arc;
      } else {
        return null;
      }
    }

    // Short-circuit if this arc is in the root arc cache:
    if (follow.target == startNode && labelToMatch < cachedRootArcs.length) {
      final Arc<T> result = cachedRootArcs[labelToMatch];
      if (result == null) {
        return result;
      } else {
        arc.copyFrom(result);
        return arc;
      }
    }

    if (!targetHasArcs(follow)) {
      return null;
    }

    in.pos = getNodeAddress(follow.target);

    arc.node = follow.target;

    // System.out.println("fta label=" + (char) labelToMatch);

    if (in.readByte() == ARCS_AS_FIXED_ARRAY) {
      // Arcs are full array; do binary search:
      arc.numArcs = in.readVInt();
      if (packed) {
        arc.bytesPerArc = in.readVInt();
      } else {
        arc.bytesPerArc = in.readInt();
      }
      arc.posArcsStart = in.pos;
      int low = 0;
      int high = arc.numArcs-1;
      while (low <= high) {
        //System.out.println("    cycle");
        int mid = (low + high) >>> 1;
        in.skip(arc.posArcsStart, arc.bytesPerArc*mid + 1);
        int midLabel = readLabel(in);
        final int cmp = midLabel - labelToMatch;
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          arc.arcIdx = mid-1;
          //System.out.println("    found!");
          return readNextRealArc(arc, in);
        }
      }

      return null;
    }

    // Linear scan
    readFirstRealTargetArc(follow.target, arc, in);

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
        readNextRealArc(arc, in);
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
        if (packed) {
          in.readVInt();
        } else {
          in.readInt();
        }
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
      assert posWrite <= bytes.length;
      if (bytes.length == posWrite) {
        bytes = ArrayUtil.grow(bytes);
      }
      assert posWrite < bytes.length: "posWrite=" + posWrite + " bytes.length=" + bytes.length;
      bytes[posWrite++] = b;
    }

    public void setPosWrite(int posWrite) {
      this.posWrite = posWrite;
      if (bytes.length < posWrite) {
        bytes = ArrayUtil.grow(bytes, posWrite);
      }
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
      final int size = posWrite + length;
      bytes = ArrayUtil.grow(bytes, size);
      System.arraycopy(b, offset, bytes, posWrite, length);
      posWrite += length;
    }
  }

  public final BytesReader getBytesReader(int pos) {
    // TODO: maybe re-use via ThreadLocal?
    if (packed) {
      return new ForwardBytesReader(bytes, pos);
    } else {
      return new ReverseBytesReader(bytes, pos);
    }
  }

  /** Reads the bytes from this FST.  Use {@link
   *  #getBytesReader(int)} to obtain an instance for this
   *  FST; re-use across calls (but only within a single
   *  thread) for better performance. */
  public static abstract class BytesReader extends DataInput {
    protected int pos;
    protected final byte[] bytes;
    protected BytesReader(byte[] bytes, int pos) {
      this.bytes = bytes;
      this.pos = pos;
    }
    abstract void skip(int byteCount);
    abstract void skip(int base, int byteCount);
  }

  final static class ReverseBytesReader extends BytesReader {

    public ReverseBytesReader(byte[] bytes, int pos) {
      super(bytes, pos);
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

    public void skip(int count) {
      pos -= count;
    }

    public void skip(int base, int count) {
      pos = base - count;
    }
  }

  // TODO: can we use just ByteArrayDataInput...?  need to
  // add a .skipBytes to DataInput.. hmm and .setPosition
  final static class ForwardBytesReader extends BytesReader {

    public ForwardBytesReader(byte[] bytes, int pos) {
      super(bytes, pos);
    }

    @Override
    public byte readByte() {
      return bytes[pos++];
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) {
      System.arraycopy(bytes, pos, b, offset, len);
      pos += len;
    }

    public void skip(int count) {
      pos += count;
    }

    public void skip(int base, int count) {
      pos = base + count;
    }
  }

  private static class ArcAndState<T> {
    final Arc<T> arc;
    final IntsRef chain;

    public ArcAndState(Arc<T> arc, IntsRef chain) {
      this.arc = arc;
      this.chain = chain;
    }
  }

  /*
  public void countSingleChains() throws IOException {
    // TODO: must assert this FST was built with
    // "willRewrite"

    final List<ArcAndState<T>> queue = new ArrayList<ArcAndState<T>>();

    // TODO: use bitset to not revisit nodes already
    // visited

    FixedBitSet seen = new FixedBitSet(1+nodeCount);
    int saved = 0;

    queue.add(new ArcAndState<T>(getFirstArc(new Arc<T>()), new IntsRef()));
    Arc<T> scratchArc = new Arc<T>();
    while(queue.size() > 0) {
      //System.out.println("cycle size=" + queue.size());
      //for(ArcAndState<T> ent : queue) {
      //  System.out.println("  " + Util.toBytesRef(ent.chain, new BytesRef()));
      //  }
      final ArcAndState<T> arcAndState = queue.get(queue.size()-1);
      seen.set(arcAndState.arc.node);
      final BytesRef br = Util.toBytesRef(arcAndState.chain, new BytesRef());
      if (br.length > 0 && br.bytes[br.length-1] == -1) {
        br.length--;
      }
      //System.out.println("  top node=" + arcAndState.arc.target + " chain=" + br.utf8ToString());
      if (targetHasArcs(arcAndState.arc) && !seen.get(arcAndState.arc.target)) {
        // push
        readFirstTargetArc(arcAndState.arc, scratchArc);
        //System.out.println("  push label=" + (char) scratchArc.label);
        //System.out.println("    tonode=" + scratchArc.target + " last?=" + scratchArc.isLast());
        
        final IntsRef chain = IntsRef.deepCopyOf(arcAndState.chain);
        chain.grow(1+chain.length);
        // TODO
        //assert scratchArc.label != END_LABEL;
        chain.ints[chain.length] = scratchArc.label;
        chain.length++;

        if (scratchArc.isLast()) {
          if (scratchArc.target != -1 && inCounts[scratchArc.target] == 1) {
            //System.out.println("    append");
          } else {
            if (arcAndState.chain.length > 1) {
              saved += chain.length-2;
              try {
                System.out.println("chain: " + Util.toBytesRef(chain, new BytesRef()).utf8ToString());
              } catch (AssertionError ae) {
                System.out.println("chain: " + Util.toBytesRef(chain, new BytesRef()));
              }
            }
            chain.length = 0;
          }
        } else {
          //System.out.println("    reset");
          if (arcAndState.chain.length > 1) {
            saved += arcAndState.chain.length-2;
            try {
              System.out.println("chain: " + Util.toBytesRef(arcAndState.chain, new BytesRef()).utf8ToString());
            } catch (AssertionError ae) {
              System.out.println("chain: " + Util.toBytesRef(arcAndState.chain, new BytesRef()));
            }
          }
          if (scratchArc.target != -1 && inCounts[scratchArc.target] != 1) {
            chain.length = 0;
          } else {
            chain.ints[0] = scratchArc.label;
            chain.length = 1;
          }
        }
        // TODO: instead of new Arc() we can re-use from
        // a by-depth array
        queue.add(new ArcAndState<T>(new Arc<T>().copyFrom(scratchArc), chain));
      } else if (!arcAndState.arc.isLast()) {
        // next
        readNextArc(arcAndState.arc);
        //System.out.println("  next label=" + (char) arcAndState.arc.label + " len=" + arcAndState.chain.length);
        if (arcAndState.chain.length != 0) {
          arcAndState.chain.ints[arcAndState.chain.length-1] = arcAndState.arc.label;
        }
      } else {
        if (arcAndState.chain.length > 1) {
          saved += arcAndState.chain.length-2;
          System.out.println("chain: " + Util.toBytesRef(arcAndState.chain, new BytesRef()).utf8ToString());
        }
        // pop
        //System.out.println("  pop");
        queue.remove(queue.size()-1);
        while(queue.size() > 0 && queue.get(queue.size()-1).arc.isLast()) {
          queue.remove(queue.size()-1);
        }
        if (queue.size() > 0) {
          final ArcAndState<T> arcAndState2 = queue.get(queue.size()-1);
          readNextArc(arcAndState2.arc);
          //System.out.println("  read next=" + (char) arcAndState2.arc.label + " queue=" + queue.size());
          assert arcAndState2.arc.label != END_LABEL;
          if (arcAndState2.chain.length != 0) {
            arcAndState2.chain.ints[arcAndState2.chain.length-1] = arcAndState2.arc.label;
          }
        }
      }
    }

    System.out.println("TOT saved " + saved);
  }
 */

  // Creates a packed FST
  private FST(INPUT_TYPE inputType, int[] nodeRefToAddress, Outputs<T> outputs) {
    packed = true;
    this.inputType = inputType;
    bytes = new byte[128];
    this.nodeRefToAddress = nodeRefToAddress;
    this.outputs = outputs;
    NO_OUTPUT = outputs.getNoOutput();
    writer = new BytesWriter();
  }

  /** Expert: creates an FST by packing this one.  This
   *  process requires substantial additional RAM (currently
   *  ~8 bytes per node), but then should produce a smaller FST. */
  public FST<T> pack(int minInCountDeref, int maxDerefNodes) throws IOException {

    // TODO: other things to try
    //   - renumber the nodes to get more next / better locality?
    //   - allow multiple input labels on an arc, so
    //     singular chain of inputs can take one arc (on
    //     wikipedia terms this could save another ~6%)
    //   - in the ord case, the output '1' is presumably
    //     very common (after NO_OUTPUT)... maybe use a bit
    //     for it..?
    //   - use spare bits in flags.... for top few labels /
    //     outputs / targets

    if (nodeAddress == null) {
      throw new IllegalArgumentException("this FST was not built with willPackFST=true");
    }

    Arc<T> arc = new Arc<T>();

    final BytesReader r = getBytesReader(0);

    final int topN = Math.min(maxDerefNodes, inCounts.length);

    // Find top nodes with highest number of incoming arcs:
    NodeQueue q = new NodeQueue(topN);

    // TODO: we could use more RAM efficient selection algo here...
    NodeAndInCount bottom = null;
    for(int node=0;node<inCounts.length;node++) {
      if (inCounts[node] >= minInCountDeref) {
        if (bottom == null) {
          q.add(new NodeAndInCount(node, inCounts[node]));
          if (q.size() == topN) {
            bottom = q.top();
          }
        } else if (inCounts[node] > bottom.count) {
          q.insertWithOverflow(new NodeAndInCount(node, inCounts[node]));
        }
      }
    }

    // Free up RAM:
    inCounts = null;

    final Map<Integer,Integer> topNodeMap = new HashMap<Integer,Integer>();
    for(int downTo=q.size()-1;downTo>=0;downTo--) {
      NodeAndInCount n = q.pop();
      topNodeMap.put(n.node, downTo);
      //System.out.println("map node=" + n.node + " inCount=" + n.count + " to newID=" + downTo);
    }

    // TODO: we can use packed ints:
    // +1 because node ords start at 1 (0 is reserved as
    // stop node):
    final int[] nodeRefToAddressIn = new int[topNodeMap.size()];

    final FST<T> fst = new FST<T>(inputType, nodeRefToAddressIn, outputs);

    final BytesWriter writer = fst.writer;
    
    final int[] newNodeAddress = new int[1+nodeCount];

    // Fill initial coarse guess:
    for(int node=1;node<=nodeCount;node++) {
      newNodeAddress[node] = 1 + bytes.length - nodeAddress[node];
    }

    int absCount;
    int deltaCount;
    int topCount;
    int nextCount;

    // Iterate until we converge:
    while(true) {

      //System.out.println("\nITER");
      boolean changed = false;

      // for assert:
      boolean negDelta = false;

      writer.posWrite = 0;
      // Skip 0 byte since 0 is reserved target:
      writer.writeByte((byte) 0);

      fst.arcWithOutputCount = 0;
      fst.nodeCount = 0;
      fst.arcCount = 0;

      absCount = deltaCount = topCount = nextCount = 0;

      int changedCount = 0;

      int addressError = 0;

      //int totWasted = 0;

      // Since we re-reverse the bytes, we now write the
      // nodes backwards, so that BIT_TARGET_NEXT is
      // unchanged:
      for(int node=nodeCount;node>=1;node--) {
        fst.nodeCount++;
        final int address = writer.posWrite;
        //System.out.println("  node: " + node + " address=" + address);
        if (address != newNodeAddress[node]) {
          addressError = address - newNodeAddress[node];
          //System.out.println("    change: " + (address - newNodeAddress[node]));
          changed = true;
          newNodeAddress[node] = address;
          changedCount++;
        }

        int nodeArcCount = 0;
        int bytesPerArc = 0;

        boolean retry = false;

        // for assert:
        boolean anyNegDelta = false;

        // Retry loop: possibly iterate more than once, if
        // this is an array'd node and bytesPerArc changes:
        writeNode:
        while(true) { // retry writing this node

          readFirstRealTargetArc(node, arc, r);

          final boolean useArcArray = arc.bytesPerArc != 0;
          if (useArcArray) {
            // Write false first arc:
            if (bytesPerArc == 0) {
              bytesPerArc = arc.bytesPerArc;
            }
            writer.writeByte(ARCS_AS_FIXED_ARRAY);
            writer.writeVInt(arc.numArcs);
            writer.writeVInt(bytesPerArc);
            //System.out.println("node " + node + ": " + arc.numArcs + " arcs");
          }

          int maxBytesPerArc = 0;
          //int wasted = 0;
          while(true) {  // iterate over all arcs for this node

            //System.out.println("    arc label=" + arc.label + " target=" + arc.target + " pos=" + writer.posWrite);
            final int arcStartPos = writer.posWrite;
            nodeArcCount++;

            byte flags = 0;

            if (arc.isLast()) {
              flags += BIT_LAST_ARC;
            }
            /*
            if (!useArcArray && nodeUpto < nodes.length-1 && arc.target == nodes[nodeUpto+1]) {
              flags += BIT_TARGET_NEXT;
            }
            */
            if (!useArcArray && node != 1 && arc.target == node-1) {
              flags += BIT_TARGET_NEXT;
              if (!retry) {
                nextCount++;
              }
            }
            if (arc.isFinal()) {
              flags += BIT_FINAL_ARC;
              if (arc.nextFinalOutput != NO_OUTPUT) {
                flags += BIT_ARC_HAS_FINAL_OUTPUT;
              }
            } else {
              assert arc.nextFinalOutput == NO_OUTPUT;
            }
            if (!targetHasArcs(arc)) {
              flags += BIT_STOP_NODE;
            }

            if (arc.output != NO_OUTPUT) {
              flags += BIT_ARC_HAS_OUTPUT;
            }

            final Integer ptr;
            final int absPtr;
            final boolean doWriteTarget = targetHasArcs(arc) && (flags & BIT_TARGET_NEXT) == 0;
            if (doWriteTarget) {

              ptr = topNodeMap.get(arc.target);
              if (ptr != null) {
                absPtr = ptr;
              } else {
                absPtr = topNodeMap.size() + newNodeAddress[arc.target] + addressError;
              }

              int delta = newNodeAddress[arc.target] + addressError - writer.posWrite - 2;
              if (delta < 0) {
                //System.out.println("neg: " + delta);
                anyNegDelta = true;
                delta = 0;
              }

              if (delta < absPtr) {
                flags |= BIT_TARGET_DELTA;
              }
            } else {
              ptr = null;
              absPtr = 0;
            }

            writer.writeByte(flags);
            fst.writeLabel(arc.label);

            if (arc.output != NO_OUTPUT) {
              outputs.write(arc.output, writer);
              if (!retry) {
                fst.arcWithOutputCount++;
              }
            }
            if (arc.nextFinalOutput != NO_OUTPUT) {
              outputs.write(arc.nextFinalOutput, writer);
            }

            if (doWriteTarget) {

              int delta = newNodeAddress[arc.target] + addressError - writer.posWrite;
              if (delta < 0) {
                anyNegDelta = true;
                //System.out.println("neg: " + delta);
                delta = 0;
              }

              if (flag(flags, BIT_TARGET_DELTA)) {
                //System.out.println("        delta");
                writer.writeVInt(delta);
                if (!retry) {
                  deltaCount++;
                }
              } else {
                /*
                if (ptr != null) {
                  System.out.println("        deref");
                } else {
                  System.out.println("        abs");
                }
                */
                writer.writeVInt(absPtr);
                if (!retry) {
                  if (absPtr >= topNodeMap.size()) {
                    absCount++;
                  } else {
                    topCount++;
                  }
                }
              }
            }

            if (useArcArray) {
              final int arcBytes = writer.posWrite - arcStartPos;
              //System.out.println("  " + arcBytes + " bytes");
              maxBytesPerArc = Math.max(maxBytesPerArc, arcBytes);
              // NOTE: this may in fact go "backwards", if
              // somehow (rarely, possibly never) we use
              // more bytesPerArc in this rewrite than the
              // incoming FST did... but in this case we
              // will retry (below) so it's OK to ovewrite
              // bytes:
              //wasted += bytesPerArc - arcBytes;
              writer.setPosWrite(arcStartPos + bytesPerArc);
            }

            if (arc.isLast()) {
              break;
            }

            readNextRealArc(arc, r);
          }

          if (useArcArray) {
            if (maxBytesPerArc == bytesPerArc || (retry && maxBytesPerArc <= bytesPerArc)) {
              // converged
              //System.out.println("  bba=" + bytesPerArc + " wasted=" + wasted);
              //totWasted += wasted;
              break;
            }
          } else {
            break;
          }

          //System.out.println("  retry this node maxBytesPerArc=" + maxBytesPerArc + " vs " + bytesPerArc);

          // Retry:
          bytesPerArc = maxBytesPerArc;
          writer.posWrite = address;
          nodeArcCount = 0;
          retry = true;
          anyNegDelta = false;
        }
        negDelta |= anyNegDelta;

        fst.arcCount += nodeArcCount;
      }

      if (!changed) {
        // We don't renumber the nodes (just reverse their
        // order) so nodes should only point forward to
        // other nodes because we only produce acyclic FSTs
        // w/ nodes only pointing "forwards":
        assert !negDelta;
        //System.out.println("TOT wasted=" + totWasted);
        // Converged!
        break;
      }
      //System.out.println("  " + changedCount + " of " + fst.nodeCount + " changed; retry");
    }

    for(Map.Entry<Integer,Integer> ent : topNodeMap.entrySet()) {
      nodeRefToAddressIn[ent.getValue()] = newNodeAddress[ent.getKey()];
    }

    fst.startNode = newNodeAddress[startNode];
    //System.out.println("new startNode=" + fst.startNode + " old startNode=" + startNode);

    if (emptyOutput != null) {
      fst.setEmptyOutput(emptyOutput);
    }

    assert fst.nodeCount == nodeCount: "fst.nodeCount=" + fst.nodeCount + " nodeCount=" + nodeCount;
    assert fst.arcCount == arcCount;
    assert fst.arcWithOutputCount == arcWithOutputCount: "fst.arcWithOutputCount=" + fst.arcWithOutputCount + " arcWithOutputCount=" + arcWithOutputCount;
    
    final byte[] finalBytes = new byte[writer.posWrite];
    //System.out.println("resize " + fst.bytes.length + " down to " + writer.posWrite);
    System.arraycopy(fst.bytes, 0, finalBytes, 0, writer.posWrite);
    fst.bytes = finalBytes;
    fst.cacheRootArcs();

    //final int size = fst.sizeInBytes();
    //System.out.println("nextCount=" + nextCount + " topCount=" + topCount + " deltaCount=" + deltaCount + " absCount=" + absCount);

    return fst;
  }

  private static class NodeAndInCount implements Comparable<NodeAndInCount> {
    final int node;
    final int count;

    public NodeAndInCount(int node, int count) {
      this.node = node;
      this.count = count;
    }
    
    @Override
    public int compareTo(NodeAndInCount other) {
      if (count > other.count) {
        return 1;
      } else if (count < other.count) {
        return -1;
      } else {
        // Tie-break: smaller node compares as greater than
        return other.node - node;
      }
    }
  }

  private static class NodeQueue extends PriorityQueue<NodeAndInCount> {
    public NodeQueue(int topN) {
      super(topN, false);
    }

    @Override
    public boolean lessThan(NodeAndInCount a, NodeAndInCount b) {
      final int cmp = a.compareTo(b);
      assert cmp != 0;
      return cmp < 0;
    }
  }
}
