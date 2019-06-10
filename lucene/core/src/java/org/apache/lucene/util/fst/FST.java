/*
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
package org.apache.lucene.util.fst;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.RamUsageEstimator;

// TODO: break this into WritableFST and ReadOnlyFST.. then
// we can have subclasses of ReadOnlyFST to handle the
// different byte[] level encodings (packed or
// not)... and things like nodeCount, arcCount are read only

// TODO: if FST is pure prefix trie we can do a more compact
// job, ie, once we are at a 'suffix only', just store the
// completion labels as a string not as a series of arcs.

// NOTE: while the FST is able to represent a non-final
// dead-end state (NON_FINAL_END_NODE=0), the layers above
// (FSTEnum, Util) have problems with this!!

/** Represents an finite state machine (FST), using a
 *  compact byte[] format.
 *  <p> The format is similar to what's used by Morfologik
 *  (https://github.com/morfologik/morfologik-stemming).
 *  
 *  <p> See the {@link org.apache.lucene.util.fst package
 *      documentation} for some simple examples.
 *
 * @lucene.experimental
 */
public final class FST<T> implements Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FST.class);
  private static final long ARC_SHALLOW_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Arc.class);

  /** Specifies allowed range of each int input label for
   *  this FST. */
  public static enum INPUT_TYPE {BYTE1, BYTE2, BYTE4};

  static final int BIT_FINAL_ARC = 1 << 0;
  static final int BIT_LAST_ARC = 1 << 1;
  static final int BIT_TARGET_NEXT = 1 << 2;

  // TODO: we can free up a bit if we can nuke this:
  static final int BIT_STOP_NODE = 1 << 3;

  /** This flag is set if the arc has an output. */
  public static final int BIT_ARC_HAS_OUTPUT = 1 << 4;

  static final int BIT_ARC_HAS_FINAL_OUTPUT = 1 << 5;

  // We use this as a marker (because this one flag is
  // illegal by itself ...):
  private static final byte ARCS_AS_ARRAY_PACKED = BIT_ARC_HAS_FINAL_OUTPUT;

  // this means either of these things in different contexts
  // in the midst of a direct array:
  private static final byte BIT_MISSING_ARC = 1 << 6;
  // at the start of a direct array:
  private static final byte ARCS_AS_ARRAY_WITH_GAPS = BIT_MISSING_ARC;

  /**
   * @see #shouldExpand(Builder, Builder.UnCompiledNode)
   */
  static final int FIXED_ARRAY_SHALLOW_DISTANCE = 3; // 0 => only root node.

  /**
   * @see #shouldExpand(Builder, Builder.UnCompiledNode)
   */
  static final int FIXED_ARRAY_NUM_ARCS_SHALLOW = 5;

  /**
   * @see #shouldExpand(Builder, Builder.UnCompiledNode)
   */
  static final int FIXED_ARRAY_NUM_ARCS_DEEP = 10;

  // Increment version to change it
  private static final String FILE_FORMAT_NAME = "FST";
  private static final int VERSION_START = 6;
  private static final int VERSION_CURRENT = 7;

  // Never serialized; just used to represent the virtual
  // final node w/ no arcs:
  private static final long FINAL_END_NODE = -1;

  // Never serialized; just used to represent the virtual
  // non-final node w/ no arcs:
  private static final long NON_FINAL_END_NODE = 0;

  /** If arc has this label then that arc is final/accepted */
  public static final int END_LABEL = -1;

  public final INPUT_TYPE inputType;

  // if non-null, this FST accepts the empty string and
  // produces this output
  T emptyOutput;

  /** A {@link BytesStore}, used during building, or during reading when
   *  the FST is very large (more than 1 GB).  If the FST is less than 1
   *  GB then bytesArray is set instead. */
  final BytesStore bytes;

  private final FSTStore fstStore;

  private long startNode = -1;

  public final Outputs<T> outputs;

  private Arc<T> cachedRootArcs[];

  /** Represents a single arc. */
  public static final class Arc<T> {
    public int label;
    public T output;

    /** To node (ord or address) */
    public long target;

    byte flags;
    public T nextFinalOutput;

    // address (into the byte[]), or ord/address if label == END_LABEL
    long nextArc;

    /** Where the first arc in the array starts; only valid if
     *  bytesPerArc != 0 */
    public long posArcsStart;
    
    /** Non-zero if this arc is part of an array, which means all
     *  arcs for the node are encoded with a fixed number of bytes so
     *  that we can random access by index.  We do when there are enough
     *  arcs leaving one node.  It wastes some bytes but gives faster
     *  lookups. */
    public int bytesPerArc;

    /** Where we are in the array; only valid if bytesPerArc != 0, and the array has no holes.
     * arcIdx = Integer.MIN_VALUE indicates that the arc is part of a direct array, addressed by
     * label.
     */
    public int arcIdx;

    /** How many arc, if bytesPerArc == 0. Otherwise, the size of the arc array. If the array is
     * direct, this may include holes. Otherwise it is also how many arcs are in the array */
    public int numArcs;

    /** Returns this */
    public Arc<T> copyFrom(Arc<T> other) {
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
      b.append(" target=").append(target);
      b.append(" label=0x").append(Integer.toHexString(label));
      if (flag(BIT_FINAL_ARC)) {
        b.append(" final");
      }
      if (flag(BIT_LAST_ARC)) {
        b.append(" last");
      }
      if (flag(BIT_TARGET_NEXT)) {
        b.append(" targetNext");
      }
      if (flag(BIT_STOP_NODE)) {
        b.append(" stop");
      }
      if (flag(BIT_ARC_HAS_OUTPUT)) {
        b.append(" output=").append(output);
      }
      if (flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
        b.append(" nextFinalOutput=").append(nextFinalOutput);
      }
      if (bytesPerArc != 0) {
        b.append(" arcArray(idx=").append(arcIdx).append(" of ").append(numArcs).append(")");
      }
      return b.toString();
    }
  };

  private static boolean flag(int flags, int bit) {
    return (flags & bit) != 0;
  }

  private final int version;

  // make a new empty FST, for building; Builder invokes
  // this ctor
  FST(INPUT_TYPE inputType, Outputs<T> outputs, int bytesPageBits) {
    this.inputType = inputType;
    this.outputs = outputs;
    version = VERSION_CURRENT;
    fstStore = null;
    bytes = new BytesStore(bytesPageBits);
    // pad: ensure no node gets address 0 which is reserved to mean
    // the stop state w/ no arcs
    bytes.writeByte((byte) 0);

    emptyOutput = null;
  }

  public static final int DEFAULT_MAX_BLOCK_BITS = Constants.JRE_IS_64BIT ? 30 : 28;

  /** Load a previously saved FST. */
  public FST(DataInput in, Outputs<T> outputs) throws IOException {
    this(in, outputs, new OnHeapFSTStore(DEFAULT_MAX_BLOCK_BITS));
  }

  /** Load a previously saved FST; maxBlockBits allows you to
   *  control the size of the byte[] pages used to hold the FST bytes. */
  public FST(DataInput in, Outputs<T> outputs, FSTStore fstStore) throws IOException {
    bytes = null;
    this.fstStore = fstStore;
    this.outputs = outputs;

    // NOTE: only reads most recent format; we don't have
    // back-compat promise for FSTs (they are experimental):
    version = CodecUtil.checkHeader(in, FILE_FORMAT_NAME, VERSION_START, VERSION_CURRENT);
    if (in.readByte() == 1) {
      // accepts empty string
      // 1 KB blocks:
      BytesStore emptyBytes = new BytesStore(10);
      int numBytes = in.readVInt();
      emptyBytes.copyBytes(in, numBytes);

      // De-serialize empty-string output:
      BytesReader reader = emptyBytes.getReverseReader();
      // NoOutputs uses 0 bytes when writing its output,
      // so we have to check here else BytesStore gets
      // angry:
      if (numBytes > 0) {
        reader.setPosition(numBytes-1);
      }
      emptyOutput = outputs.readFinalOutput(reader);
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
    startNode = in.readVLong();

    long numBytes = in.readVLong();
    this.fstStore.init(in, numBytes);
    cacheRootArcs();
  }

  public INPUT_TYPE getInputType() {
    return inputType;
  }

  private long ramBytesUsed(Arc<T>[] arcs) {
    long size = 0;
    if (arcs != null) {
      size += RamUsageEstimator.shallowSizeOf(arcs);
      for (Arc<T> arc : arcs) {
        if (arc != null) {
          size += ARC_SHALLOW_RAM_BYTES_USED;
          if (arc.output != null && arc.output != outputs.getNoOutput()) {
            size += outputs.ramBytesUsed(arc.output);
          }
          if (arc.nextFinalOutput != null && arc.nextFinalOutput != outputs.getNoOutput()) {
            size += outputs.ramBytesUsed(arc.nextFinalOutput);
          }
        }
      }
    }
    return size;
  }

  private int cachedArcsBytesUsed;

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES_USED;
    if (this.fstStore != null) {
      size += this.fstStore.ramBytesUsed();
    } else {
      size += bytes.ramBytesUsed();
    }

    size += cachedArcsBytesUsed;
    return size;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(input=" + inputType + ",output=" + outputs;
  }

  void finish(long newStartNode) throws IOException {
    assert newStartNode <= bytes.getPosition();
    if (startNode != -1) {
      throw new IllegalStateException("already finished");
    }
    if (newStartNode == FINAL_END_NODE && emptyOutput != null) {
      newStartNode = 0;
    }
    startNode = newStartNode;
    bytes.finish();
    cacheRootArcs();
  }
  
  // Optionally caches first 128 labels
  @SuppressWarnings({"rawtypes","unchecked"})
  private void cacheRootArcs() throws IOException {
    // We should only be called once per FST:
    assert cachedArcsBytesUsed == 0;

    final Arc<T> arc = new Arc<>();
    getFirstArc(arc);
    if (targetHasArcs(arc)) {
      final BytesReader in = getBytesReader();
      Arc<T>[] arcs = (Arc<T>[]) new Arc[0x80];
      readFirstRealTargetArc(arc.target, arc, in);
      int count = 0;
      while(true) {
        assert arc.label != END_LABEL;
        if (arc.label < arcs.length) {
          arcs[arc.label] = new Arc<T>().copyFrom(arc);
        } else {
          break;
        }
        if (arc.isLast()) {
          break;
        }
        readNextRealArc(arc, in);
        count++;
      }

      int cacheRAM = (int) ramBytesUsed(arcs);

      // Don't cache if there are only a few arcs or if the cache would use > 20% RAM of the FST itself:
      if (count >= FIXED_ARRAY_NUM_ARCS_SHALLOW && cacheRAM < ramBytesUsed()/5) {
        cachedRootArcs = arcs;
        cachedArcsBytesUsed = cacheRAM;
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
  }

  public void save(DataOutput out) throws IOException {
    if (startNode == -1) {
      throw new IllegalStateException("call finish first");
    }
    CodecUtil.writeHeader(out, FILE_FORMAT_NAME, VERSION_CURRENT);
    // TODO: really we should encode this as an arc, arriving
    // to the root node, instead of special casing here:
    if (emptyOutput != null) {
      // Accepts empty string
      out.writeByte((byte) 1);

      // Serialize empty-string output:
      ByteBuffersDataOutput ros = new ByteBuffersDataOutput();
      outputs.writeFinalOutput(emptyOutput, ros);
      byte[] emptyOutputBytes = ros.toArrayCopy();

      // reverse
      final int stopAt = emptyOutputBytes.length/2;
      int upto = 0;
      while (upto < stopAt) {
        final byte b = emptyOutputBytes[upto];
        emptyOutputBytes[upto] = emptyOutputBytes[emptyOutputBytes.length-upto-1];
        emptyOutputBytes[emptyOutputBytes.length-upto-1] = b;
        upto++;
      }
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
    out.writeVLong(startNode);
    if (bytes != null) {
      long numBytes = bytes.getPosition();
      out.writeVLong(numBytes);
      bytes.writeTo(out);
    } else {
      assert fstStore != null;
      fstStore.writeTo(out);
    }
  }
  
  /**
   * Writes an automaton to a file. 
   */
  public void save(final Path path) throws IOException {
    try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(path))) {
      save(new OutputStreamDataOutput(os));
    }
  }

  /**
   * Reads an automaton from a file. 
   */
  public static <T> FST<T> read(Path path, Outputs<T> outputs) throws IOException {
    try (InputStream is = Files.newInputStream(path)) {
      return new FST<>(new InputStreamDataInput(new BufferedInputStream(is)), outputs);
    }
  }

  private void writeLabel(DataOutput out, int v) throws IOException {
    assert v >= 0: "v=" + v;
    if (inputType == INPUT_TYPE.BYTE1) {
      assert v <= 255: "v=" + v;
      out.writeByte((byte) v);
    } else if (inputType == INPUT_TYPE.BYTE2) {
      assert v <= 65535: "v=" + v;
      out.writeShort((short) v);
    } else {
      out.writeVInt(v);
    }
  }

  /** Reads one BYTE1/2/4 label from the provided {@link DataInput}. */
  public int readLabel(DataInput in) throws IOException {
    final int v;
    if (inputType == INPUT_TYPE.BYTE1) {
      // Unsigned byte:
      v = in.readByte() & 0xFF;
    } else if (inputType == INPUT_TYPE.BYTE2) {
      // Unsigned short:
      v = in.readShort() & 0xFFFF;
    } else { 
      v = in.readVInt();
    }
    return v;
  }

  /** returns true if the node at this address has any
   *  outgoing arcs */
  public static<T> boolean targetHasArcs(Arc<T> arc) {
    return arc.target > 0;
  }

  // serializes new node by appending its bytes to the end
  // of the current byte[]
  long addNode(Builder<T> builder, Builder.UnCompiledNode<T> nodeIn) throws IOException {
    T NO_OUTPUT = outputs.getNoOutput();

    //System.out.println("FST.addNode pos=" + bytes.getPosition() + " numArcs=" + nodeIn.numArcs);
    if (nodeIn.numArcs == 0) {
      if (nodeIn.isFinal) {
        return FINAL_END_NODE;
      } else {
        return NON_FINAL_END_NODE;
      }
    }

    final long startAddress = builder.bytes.getPosition();
    //System.out.println("  startAddr=" + startAddress);

    final boolean doFixedArray = shouldExpand(builder, nodeIn);
    if (doFixedArray) {
      //System.out.println("  fixedArray");
      if (builder.reusedBytesPerArc.length < nodeIn.numArcs) {
        builder.reusedBytesPerArc = new int[ArrayUtil.oversize(nodeIn.numArcs, 1)];
      }
    }

    builder.arcCount += nodeIn.numArcs;
    
    final int lastArc = nodeIn.numArcs-1;

    long lastArcStart = builder.bytes.getPosition();
    int maxBytesPerArc = 0;
    for(int arcIdx=0; arcIdx < nodeIn.numArcs; arcIdx++) {
      final Builder.Arc<T> arc = nodeIn.arcs[arcIdx];
      final Builder.CompiledNode target = (Builder.CompiledNode) arc.target;
      int flags = 0;
      //System.out.println("  arc " + arcIdx + " label=" + arc.label + " -> target=" + target.node);

      if (arcIdx == lastArc) {
        flags += BIT_LAST_ARC;
      }

      if (builder.lastFrozenNode == target.node && !doFixedArray) {
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
      }

      if (arc.output != NO_OUTPUT) {
        flags += BIT_ARC_HAS_OUTPUT;
      }

      builder.bytes.writeByte((byte) flags);
      writeLabel(builder.bytes, arc.label);

      // System.out.println("  write arc: label=" + (char) arc.label + " flags=" + flags + " target=" + target.node + " pos=" + bytes.getPosition() + " output=" + outputs.outputToString(arc.output));

      if (arc.output != NO_OUTPUT) {
        outputs.write(arc.output, builder.bytes);
        //System.out.println("    write output");
      }

      if (arc.nextFinalOutput != NO_OUTPUT) {
        //System.out.println("    write final output");
        outputs.writeFinalOutput(arc.nextFinalOutput, builder.bytes);
      }

      if (targetHasArcs && (flags & BIT_TARGET_NEXT) == 0) {
        assert target.node > 0;
        //System.out.println("    write target");
        builder.bytes.writeVLong(target.node);
      }

      // just write the arcs "like normal" on first pass, but record how many bytes each one took
      // and max byte size:
      if (doFixedArray) {
        builder.reusedBytesPerArc[arcIdx] = (int) (builder.bytes.getPosition() - lastArcStart);
        lastArcStart = builder.bytes.getPosition();
        maxBytesPerArc = Math.max(maxBytesPerArc, builder.reusedBytesPerArc[arcIdx]);
        //System.out.println("    bytes=" + builder.reusedBytesPerArc[arcIdx]);
      }
    }

    // TODO: try to avoid wasteful cases: disable doFixedArray in that case
    /* 
     * 
     * LUCENE-4682: what is a fair heuristic here?
     * It could involve some of these:
     * 1. how "busy" the node is: nodeIn.inputCount relative to frontier[0].inputCount?
     * 2. how much binSearch saves over scan: nodeIn.numArcs
     * 3. waste: numBytes vs numBytesExpanded
     * 
     * the one below just looks at #3
    if (doFixedArray) {
      // rough heuristic: make this 1.25 "waste factor" a parameter to the phd ctor????
      int numBytes = lastArcStart - startAddress;
      int numBytesExpanded = maxBytesPerArc * nodeIn.numArcs;
      if (numBytesExpanded > numBytes*1.25) {
        doFixedArray = false;
      }
    }
    */

    if (doFixedArray) {
      final int MAX_HEADER_SIZE = 11; // header(byte) + numArcs(vint) + numBytes(vint)
      assert maxBytesPerArc > 0;
      // 2nd pass just "expands" all arcs to take up a fixed byte size

      // If more than (1 / DIRECT_ARC_LOAD_FACTOR) of the "slots" would be occupied, write an arc
      // array that may have holes in it so that we can address the arcs directly by label without
      // binary search
      int labelRange = nodeIn.arcs[nodeIn.numArcs - 1].label - nodeIn.arcs[0].label + 1;
      boolean writeDirectly = builder.useDirectArcAddressing && labelRange > 0
          && labelRange < Builder.DIRECT_ARC_LOAD_FACTOR * nodeIn.numArcs;

      //System.out.println("write int @pos=" + (fixedArrayStart-4) + " numArcs=" + nodeIn.numArcs);
      // create the header
      // TODO: clean this up: or just rewind+reuse and deal with it
      byte header[] = new byte[MAX_HEADER_SIZE]; 
      ByteArrayDataOutput bad = new ByteArrayDataOutput(header);
      // write a "false" first arc:
      if (writeDirectly) {
        bad.writeByte(ARCS_AS_ARRAY_WITH_GAPS);
        bad.writeVInt(labelRange);
      } else {
        bad.writeByte(ARCS_AS_ARRAY_PACKED);
        bad.writeVInt(nodeIn.numArcs);
      }
      bad.writeVInt(maxBytesPerArc);
      int headerLen = bad.getPosition();
      
      final long fixedArrayStart = startAddress + headerLen;

      if (writeDirectly) {
        writeArrayWithGaps(builder, nodeIn, fixedArrayStart, maxBytesPerArc, labelRange);
      } else {
        writeArrayPacked(builder, nodeIn, fixedArrayStart, maxBytesPerArc);
      }
      
      // now write the header
      builder.bytes.writeBytes(startAddress, header, 0, headerLen);
    }

    final long thisNodeAddress = builder.bytes.getPosition()-1;

    builder.bytes.reverse(startAddress, thisNodeAddress);

    builder.nodeCount++;
    return thisNodeAddress;
  }

  private void writeArrayPacked(Builder<T> builder, Builder.UnCompiledNode<T> nodeIn, long fixedArrayStart, int maxBytesPerArc) {
    // expand the arcs in place, backwards
    long srcPos = builder.bytes.getPosition();
    long destPos = fixedArrayStart + nodeIn.numArcs * maxBytesPerArc;
    assert destPos >= srcPos;
    if (destPos > srcPos) {
      builder.bytes.skipBytes((int) (destPos - srcPos));
      for(int arcIdx = nodeIn.numArcs - 1; arcIdx >= 0; arcIdx--) {
        destPos -= maxBytesPerArc;
        srcPos -= builder.reusedBytesPerArc[arcIdx];
        //System.out.println("  repack arcIdx=" + arcIdx + " srcPos=" + srcPos + " destPos=" + destPos);
        if (srcPos != destPos) {
          //System.out.println("  copy len=" + builder.reusedBytesPerArc[arcIdx]);
          assert destPos > srcPos: "destPos=" + destPos + " srcPos=" + srcPos + " arcIdx=" + arcIdx + " maxBytesPerArc=" + maxBytesPerArc + " reusedBytesPerArc[arcIdx]=" + builder.reusedBytesPerArc[arcIdx] + " nodeIn.numArcs=" + nodeIn.numArcs;
          builder.bytes.copyBytes(srcPos, destPos, builder.reusedBytesPerArc[arcIdx]);
        }
      }
    }
  }

  private void writeArrayWithGaps(Builder<T> builder, Builder.UnCompiledNode<T> nodeIn, long fixedArrayStart, int maxBytesPerArc, int labelRange) {
    // expand the arcs in place, backwards
    long srcPos = builder.bytes.getPosition();
    long destPos = fixedArrayStart + labelRange * maxBytesPerArc;
    // if destPos == srcPos it means all the arcs were the same length, and the array of them is *already* direct
    assert destPos >= srcPos;
    if (destPos > srcPos) {
      builder.bytes.skipBytes((int) (destPos - srcPos));
      int arcIdx = nodeIn.numArcs - 1;
      int firstLabel = nodeIn.arcs[0].label;
      int nextLabel = nodeIn.arcs[arcIdx].label;
      for (int directArcIdx = labelRange - 1; directArcIdx >= 0; directArcIdx--) {
        destPos -= maxBytesPerArc;
        if (directArcIdx == nextLabel - firstLabel) {
          int arcLen = builder.reusedBytesPerArc[arcIdx];
          srcPos -= arcLen;
          //System.out.println("  direct pack idx=" + directArcIdx + " arcIdx=" + arcIdx + " srcPos=" + srcPos + " destPos=" + destPos + " label=" + nextLabel);
          if (srcPos != destPos) {
            //System.out.println("  copy len=" + builder.reusedBytesPerArc[arcIdx]);
            assert destPos > srcPos: "destPos=" + destPos + " srcPos=" + srcPos + " arcIdx=" + arcIdx + " maxBytesPerArc=" + maxBytesPerArc + " reusedBytesPerArc[arcIdx]=" + builder.reusedBytesPerArc[arcIdx] + " nodeIn.numArcs=" + nodeIn.numArcs;
            builder.bytes.copyBytes(srcPos, destPos, arcLen);
            if (arcIdx == 0) {
              break;
            }
          }
          --arcIdx;
          nextLabel = nodeIn.arcs[arcIdx].label;
        } else {
          assert directArcIdx > arcIdx;
          // mark this as a missing arc
          //System.out.println("  direct pack idx=" + directArcIdx + " no arc");
          builder.bytes.writeByte(destPos, BIT_MISSING_ARC);
        }
      }
    }
  }

  /** Fills virtual 'start' arc, ie, an empty incoming arc to
   *  the FST's start node */
  public Arc<T> getFirstArc(Arc<T> arc) {
    T NO_OUTPUT = outputs.getNoOutput();

    if (emptyOutput != null) {
      arc.flags = BIT_FINAL_ARC | BIT_LAST_ARC;
      arc.nextFinalOutput = emptyOutput;
      if (emptyOutput != NO_OUTPUT) {
        arc.flags |= BIT_ARC_HAS_FINAL_OUTPUT;
      }
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
  public Arc<T> readLastTargetArc(Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
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
      in.setPosition(follow.target);
      final byte b = in.readByte();
      if (b == ARCS_AS_ARRAY_PACKED || b == ARCS_AS_ARRAY_WITH_GAPS) {
        // array: jump straight to end
        arc.numArcs = in.readVInt();
        arc.bytesPerArc = in.readVInt();
        //System.out.println("  array numArcs=" + arc.numArcs + " bpa=" + arc.bytesPerArc);
        arc.posArcsStart = in.getPosition();
        if (b == ARCS_AS_ARRAY_WITH_GAPS) {
          arc.arcIdx = Integer.MIN_VALUE;
          arc.nextArc = arc.posArcsStart - (arc.numArcs - 1) * arc.bytesPerArc;
        } else {
          arc.arcIdx = arc.numArcs - 2;
        }
      } else {
        arc.flags = b;
        // non-array: linear scan
        arc.bytesPerArc = 0;
        //System.out.println("  scan");
        while(!arc.isLast()) {
          // skip this arc:
          readLabel(in);
          if (arc.flag(BIT_ARC_HAS_OUTPUT)) {
            outputs.skipOutput(in);
          }
          if (arc.flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
            outputs.skipFinalOutput(in);
          }
          if (arc.flag(BIT_STOP_NODE)) {
          } else if (arc.flag(BIT_TARGET_NEXT)) {
          } else {
            readUnpackedNodeTarget(in);
          }
          arc.flags = in.readByte();
        }
        // Undo the byte flags we read:
        in.skipBytes(-1);
        arc.nextArc = in.getPosition();
      }
      readNextRealArc(arc, in);
      assert arc.isLast();
      return arc;
    }
  }

  private long readUnpackedNodeTarget(BytesReader in) throws IOException {
    return in.readVLong();
  }

  /**
   * Follow the <code>follow</code> arc and read the first arc of its target;
   * this changes the provided <code>arc</code> (2nd arg) in-place and returns
   * it.
   * 
   * @return Returns the second argument (<code>arc</code>).
   */
  public Arc<T> readFirstTargetArc(Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
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
        // NOTE: nextArc is a node (not an address!) in this case:
        arc.nextArc = follow.target;
      }
      arc.target = FINAL_END_NODE;
      //System.out.println("    insert isFinal; nextArc=" + follow.target + " isLast=" + arc.isLast() + " output=" + outputs.outputToString(arc.output));
      return arc;
    } else {
      return readFirstRealTargetArc(follow.target, arc, in);
    }
  }

  public Arc<T> readFirstRealTargetArc(long node, Arc<T> arc, final BytesReader in) throws IOException {
    final long address = node;
    in.setPosition(address);
    //System.out.println("   flags=" + arc.flags);

    byte flags = in.readByte();
    if (flags == ARCS_AS_ARRAY_PACKED || flags == ARCS_AS_ARRAY_WITH_GAPS) {
      //System.out.println("  fixedArray");
      // this is first arc in a fixed-array
      arc.numArcs = in.readVInt();
      arc.bytesPerArc = in.readVInt();
      if (flags == ARCS_AS_ARRAY_PACKED) {
        arc.arcIdx = -1;
      } else {
        arc.arcIdx = Integer.MIN_VALUE;
      }
      arc.nextArc = arc.posArcsStart = in.getPosition();
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
  boolean isExpandedTarget(Arc<T> follow, BytesReader in) throws IOException {
    if (!targetHasArcs(follow)) {
      return false;
    } else {
      in.setPosition(follow.target);
      byte flags = in.readByte();
      return flags == ARCS_AS_ARRAY_PACKED || flags == ARCS_AS_ARRAY_WITH_GAPS;
    }
  }

  /** In-place read; returns the arc. */
  public Arc<T> readNextArc(Arc<T> arc, BytesReader in) throws IOException {
    if (arc.label == END_LABEL) {
      // This was a fake inserted "final" arc
      if (arc.nextArc <= 0) {
        throw new IllegalArgumentException("cannot readNextArc when arc.isLast()=true");
      }
      return readFirstRealTargetArc(arc.nextArc, arc, in);
    } else {
      return readNextRealArc(arc, in);
    }
  }

  /** Peeks at next arc's label; does not alter arc.  Do
   *  not call this if arc.isLast()! */
  public int readNextArcLabel(Arc<T> arc, BytesReader in) throws IOException {
    assert !arc.isLast();

    if (arc.label == END_LABEL) {
      //System.out.println("    nextArc fake " +
      //arc.nextArc);
      
      long pos = arc.nextArc;
      in.setPosition(pos);

      final byte flags = in.readByte();
      if (flags == ARCS_AS_ARRAY_PACKED || flags == ARCS_AS_ARRAY_WITH_GAPS) {
        //System.out.println("    nextArc fixed array");
        in.readVInt();

        // Skip bytesPerArc:
        in.readVInt();
      } else {
        in.setPosition(pos);
      }
      // skip flags
      in.readByte();
    } else {
      if (arc.bytesPerArc != 0) {
        //System.out.println("    nextArc real array");
        // arcs are in an array
        if (arc.arcIdx >= 0) {
          in.setPosition(arc.posArcsStart);
          // point at next arc, -1 to skip flags
          in.skipBytes((1 + arc.arcIdx) * arc.bytesPerArc + 1);
        } else {
          in.setPosition(arc.nextArc);
          byte flags = in.readByte();
          // skip missing arcs
          while (flag(flags, BIT_MISSING_ARC)) {
            in.skipBytes(arc.bytesPerArc - 1);
            flags = in.readByte();
          }
        }
      } else {
        // arcs are packed
        //System.out.println("    nextArc real packed");
        // -1 to skip flags
        in.setPosition(arc.nextArc - 1);
      }
    }
    return readLabel(in);
  }

  /** Never returns null, but you should never call this if
   *  arc.isLast() is true. */
  public Arc<T> readNextRealArc(Arc<T> arc, final BytesReader in) throws IOException {

    // TODO: can't assert this because we call from readFirstArc
    // assert !flag(arc.flags, BIT_LAST_ARC);

    // this is a continuing arc in a fixed array
    if (arc.bytesPerArc != 0) {
      // arcs are in an array
      if (arc.arcIdx > Integer.MIN_VALUE) {
        arc.arcIdx++;
        assert arc.arcIdx < arc.numArcs;
        in.setPosition(arc.posArcsStart - arc.arcIdx * arc.bytesPerArc);
        arc.flags = in.readByte();
      } else {
        assert arc.nextArc <= arc.posArcsStart && arc.nextArc > arc.posArcsStart - arc.numArcs * arc.bytesPerArc;
        in.setPosition(arc.nextArc);
        arc.flags = in.readByte();
        while (flag(arc.flags, BIT_MISSING_ARC)) {
          // skip empty arcs
          arc.nextArc -= arc.bytesPerArc;
          in.skipBytes(arc.bytesPerArc - 1);
          arc.flags = in.readByte();
        }
      }
    } else {
      // arcs are packed
      in.setPosition(arc.nextArc);
      arc.flags = in.readByte();
    }
    arc.label = readLabel(in);

    if (arc.flag(BIT_ARC_HAS_OUTPUT)) {
      arc.output = outputs.read(in);
    } else {
      arc.output = outputs.getNoOutput();
    }

    if (arc.flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
      arc.nextFinalOutput = outputs.readFinalOutput(in);
    } else {
      arc.nextFinalOutput = outputs.getNoOutput();
    }

    if (arc.flag(BIT_STOP_NODE)) {
      if (arc.flag(BIT_FINAL_ARC)) {
        arc.target = FINAL_END_NODE;
      } else {
        arc.target = NON_FINAL_END_NODE;
      }
      if (arc.bytesPerArc == 0) {
        arc.nextArc = in.getPosition();
      } else {
        arc.nextArc -= arc.bytesPerArc;
      }
    } else if (arc.flag(BIT_TARGET_NEXT)) {
      arc.nextArc = in.getPosition();
      // TODO: would be nice to make this lazy -- maybe
      // caller doesn't need the target and is scanning arcs...
      if (!arc.flag(BIT_LAST_ARC)) {
        if (arc.bytesPerArc == 0) {
          // must scan
          seekToNextNode(in);
        } else {
          in.setPosition(arc.posArcsStart);
          in.skipBytes(arc.bytesPerArc * arc.numArcs);
        }
      }
      arc.target = in.getPosition();
    } else {
      arc.target = readUnpackedNodeTarget(in);
      if (arc.bytesPerArc > 0 && arc.arcIdx == Integer.MIN_VALUE) {
        // nextArc was pointing to *this* arc when we entered; advance to the next
        // if it is a missing arc, we will skip it later
        arc.nextArc -= arc.bytesPerArc;
      } else {
        // in list and fixed table encodings, the next arc always follows this one
        arc.nextArc = in.getPosition();
      }
    }
    return arc;
  }

  // LUCENE-5152: called only from asserts, to validate that the
  // non-cached arc lookup would produce the same result, to
  // catch callers that illegally modify shared structures with
  // the result (we shallow-clone the Arc itself, but e.g. a BytesRef
  // output is still shared):
  private boolean assertRootCachedArc(int label, Arc<T> cachedArc) throws IOException {
    Arc<T> arc = new Arc<>();
    getFirstArc(arc);
    BytesReader in = getBytesReader();
    Arc<T> result = findTargetArc(label, arc, arc, in, false);
    if (result == null) {
      assert cachedArc == null;
    } else {
      assert cachedArc != null;
      assert cachedArc.arcIdx == result.arcIdx;
      assert cachedArc.bytesPerArc == result.bytesPerArc;
      assert cachedArc.flags == result.flags;
      assert cachedArc.label == result.label;
      if (cachedArc.bytesPerArc == 0 || cachedArc.arcIdx == Integer.MIN_VALUE) {
        // in the sparse array case, this value is not valid, so don't assert it
        assert cachedArc.nextArc == result.nextArc;
      }
      assert cachedArc.nextFinalOutput.equals(result.nextFinalOutput);
      assert cachedArc.numArcs == result.numArcs;
      assert cachedArc.output.equals(result.output);
      assert cachedArc.posArcsStart == result.posArcsStart;
      assert cachedArc.target == result.target;
    }

    return true;
  }

  // TODO: could we somehow [partially] tableize arc lookups
  // like automaton?

  /** Finds an arc leaving the incoming arc, replacing the arc in place.
   *  This returns null if the arc was not found, else the incoming arc. */
  public Arc<T> findTargetArc(int labelToMatch, Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
    return findTargetArc(labelToMatch, follow, arc, in, true);
  }

  /** Finds an arc leaving the incoming arc, replacing the arc in place.
   *  This returns null if the arc was not found, else the incoming arc. */
  private Arc<T> findTargetArc(int labelToMatch, Arc<T> follow, Arc<T> arc, BytesReader in, boolean useRootArcCache) throws IOException {

    if (labelToMatch == END_LABEL) {
      if (follow.isFinal()) {
        if (follow.target <= 0) {
          arc.flags = BIT_LAST_ARC;
        } else {
          arc.flags = 0;
          // NOTE: nextArc is a node (not an address!) in this case:
          arc.nextArc = follow.target;
        }
        arc.output = follow.nextFinalOutput;
        arc.label = END_LABEL;
        return arc;
      } else {
        return null;
      }
    }

    // Short-circuit if this arc is in the root arc cache:
    if (useRootArcCache && cachedRootArcs != null && follow.target == startNode && labelToMatch < cachedRootArcs.length) {
      final Arc<T> result = cachedRootArcs[labelToMatch];

      // LUCENE-5152: detect tricky cases where caller
      // modified previously returned cached root-arcs:
      assert assertRootCachedArc(labelToMatch, result);

      if (result == null) {
        return null;
      } else {
        arc.copyFrom(result);
        return arc;
      }
    }

    if (!targetHasArcs(follow)) {
      return null;
    }

    in.setPosition(follow.target);

    // System.out.println("fta label=" + (char) labelToMatch);

    byte flags = in.readByte();
    if (flags == ARCS_AS_ARRAY_WITH_GAPS) {
      arc.numArcs = in.readVInt();
      arc.bytesPerArc = in.readVInt();
      arc.posArcsStart = in.getPosition();

      // Array is direct; address by label
      in.skipBytes(1);
      int firstLabel = readLabel(in);
      int arcPos = labelToMatch - firstLabel;
      if (arcPos == 0) {
        arc.nextArc = arc.posArcsStart;
      } else if (arcPos > 0) {
        if (arcPos >= arc.numArcs) {
          return null;
        }
        in.setPosition(arc.posArcsStart - arc.bytesPerArc * arcPos);
        flags = in.readByte();
        if (flag(flags, BIT_MISSING_ARC)) {
          return null;
        }
        // point to flags that we just read
        arc.nextArc = in.getPosition() + 1;
      } else {
        return null;
      }
      arc.arcIdx = Integer.MIN_VALUE;
      return readNextRealArc(arc, in);
    } else if (flags == ARCS_AS_ARRAY_PACKED) {
      arc.numArcs = in.readVInt();
      arc.bytesPerArc = in.readVInt();
      arc.posArcsStart = in.getPosition();

      // Array is sparse; do binary search:
      int low = 0;
      int high = arc.numArcs - 1;
      while (low <= high) {
        //System.out.println("    cycle");
        int mid = (low + high) >>> 1;
        // +1 to skip over flags
        in.setPosition(arc.posArcsStart - (arc.bytesPerArc * mid + 1));
        int midLabel = readLabel(in);
        final int cmp = midLabel - labelToMatch;
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          arc.arcIdx = mid - 1;
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
        outputs.skipOutput(in);
      }

      if (flag(flags, BIT_ARC_HAS_FINAL_OUTPUT)) {
        outputs.skipFinalOutput(in);
      }

      if (!flag(flags, BIT_STOP_NODE) && !flag(flags, BIT_TARGET_NEXT)) {
        readUnpackedNodeTarget(in);
      }

      if (flag(flags, BIT_LAST_ARC)) {
        return;
      }
    }
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
  private boolean shouldExpand(Builder<T> builder, Builder.UnCompiledNode<T> node) {
    return builder.allowArrayArcs &&
      ((node.depth <= FIXED_ARRAY_SHALLOW_DISTANCE && node.numArcs >= FIXED_ARRAY_NUM_ARCS_SHALLOW) || 
       node.numArcs >= FIXED_ARRAY_NUM_ARCS_DEEP);
  }
  
  /** Returns a {@link BytesReader} for this FST, positioned at
   *  position 0. */
  public BytesReader getBytesReader() {
    if (this.fstStore != null) {
      return this.fstStore.getReverseBytesReader();
    } else {
      return bytes.getReverseReader();
    }
  }

  /** Reads bytes stored in an FST. */
  public static abstract class BytesReader extends DataInput {
    /** Get current read position. */
    public abstract long getPosition();

    /** Set current read position. */
    public abstract void setPosition(long pos);

    /** Returns true if this reader uses reversed bytes
     *  under-the-hood. */
    public abstract boolean reversed();
  }

  /*
  public void countSingleChains() throws IOException {
    // TODO: must assert this FST was built with
    // "willRewrite"

    final List<ArcAndState<T>> queue = new ArrayList<>();

    // TODO: use bitset to not revisit nodes already
    // visited

    FixedBitSet seen = new FixedBitSet(1+nodeCount);
    int saved = 0;

    queue.add(new ArcAndState<T>(getFirstArc(new Arc<T>()), new IntsRef()));
    Arc<T> scratchArc = new Arc<>();
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

}
