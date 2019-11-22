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
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
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

  /** Specifies allowed range of each int input label for
   *  this FST. */
  public enum INPUT_TYPE {BYTE1, BYTE2, BYTE4}

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FST.class);
  private static final long ARC_SHALLOW_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Arc.class);

  private static final int BIT_FINAL_ARC = 1 << 0;
  static final int BIT_LAST_ARC = 1 << 1;
  static final int BIT_TARGET_NEXT = 1 << 2;

  // TODO: we can free up a bit if we can nuke this:
  private static final int BIT_STOP_NODE = 1 << 3;

  /** This flag is set if the arc has an output. */
  public static final int BIT_ARC_HAS_OUTPUT = 1 << 4;

  private static final int BIT_ARC_HAS_FINAL_OUTPUT = 1 << 5;

  /** Value of the arc flags to declare a node with fixed length arcs
   * designed for binary search. */
  // We use this as a marker because this one flag is illegal by itself.
  public static final byte ARCS_FOR_BINARY_SEARCH = BIT_ARC_HAS_FINAL_OUTPUT;

  /** Value of the arc flags to declare a node with fixed length arcs
   * and bit table designed for direct addressing. */
  static final byte ARCS_FOR_DIRECT_ADDRESSING = 1 << 6;

  /**
   * @see #shouldExpandNodeWithFixedLengthArcs
   */
  static final int FIXED_LENGTH_ARC_SHALLOW_DEPTH = 3; // 0 => only root node.

  /**
   * @see #shouldExpandNodeWithFixedLengthArcs
   */
  static final int FIXED_LENGTH_ARC_SHALLOW_NUM_ARCS = 5;

  /**
   * @see #shouldExpandNodeWithFixedLengthArcs
   */
  static final int FIXED_LENGTH_ARC_DEEP_NUM_ARCS = 10;

  /**
   * Maximum oversizing factor allowed for direct addressing compared to binary search when expansion
   * credits allow the oversizing. This factor prevents expansions that are obviously too costly even
   * if there are sufficient credits.
   *
   * @see #shouldExpandNodeWithDirectAddressing
   */
  private static final float DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR = 1.66f;

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

  /* Used for memory accounting */
  private int cachedArcsBytesUsed;

  /** If arc has this label then that arc is final/accepted */
  public static final int END_LABEL = -1;

  final INPUT_TYPE inputType;

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

  private Arc<T>[] cachedRootArcs;

  /** Represents a single arc. */
  public static final class Arc<T> {

    //*** Arc fields.

    private int label;

    private T output;

    private long target;

    private byte flags;

    private T nextFinalOutput;

    private long nextArc;

    private int arcIdx;

    //*** Fields for arcs belonging to a node with fixed length arcs.
    // So only valid when bytesPerArc != 0.

    private byte nodeFlags;

    private long posArcsStart;

    private int bytesPerArc;

    private int numArcs;

    private BitTable bitTable;

    private int firstLabel;

    /** Returns this */
    public Arc<T> copyFrom(Arc<T> other) {
      label = other.label();
      target = other.target();
      flags = other.flags();
      output = other.output();
      nextFinalOutput = other.nextFinalOutput();
      nextArc = other.nextArc();
      nodeFlags = other.nodeFlags();
      bytesPerArc = other.bytesPerArc();
      if (bytesPerArc() != 0) {
        posArcsStart = other.posArcsStart();
        arcIdx = other.arcIdx();
        numArcs = other.numArcs();
        if (nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING) {
          bitTable = other.bitTable() == null ? null : other.bitTable().copy();
          firstLabel = other.firstLabel();
        }
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
      b.append(" target=").append(target());
      b.append(" label=0x").append(Integer.toHexString(label()));
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
        b.append(" output=").append(output());
      }
      if (flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
        b.append(" nextFinalOutput=").append(nextFinalOutput());
      }
      if (bytesPerArc() != 0) {
        b.append(" arcArray(idx=").append(arcIdx()).append(" of ").append(numArcs()).append(")");
      }
      return b.toString();
    }

    public int label() {
      return label;
    }

    public T output() {
      return output;
    }

    /** Ord/address to target node. */
    public long target() {
      return target;
    }

    public byte flags() {
      return flags;
    }

    public T nextFinalOutput() {
      return nextFinalOutput;
    }

    /** Address (into the byte[]) of the next arc - only for list of variable length arc.
     * Or ord/address to the next node if label == {@link #END_LABEL}. */
     long nextArc() {
      return nextArc;
    }

    /** Where we are in the array; only valid if bytesPerArc != 0. */
    public int arcIdx() {
      return arcIdx;
    }

    /** Node header flags. Only meaningful to check if the value is either
     * {@link #ARCS_FOR_BINARY_SEARCH} or {@link #ARCS_FOR_DIRECT_ADDRESSING}
     * (other value when bytesPerArc == 0). */
    public byte nodeFlags() {
      return nodeFlags;
    }

    /** Where the first arc in the array starts; only valid if bytesPerArc != 0 */
    public long posArcsStart() {
      return posArcsStart;
    }

    /** Non-zero if this arc is part of a node with fixed length arcs, which means all
     *  arcs for the node are encoded with a fixed number of bytes so
     *  that we binary search or direct address. We do when there are enough
     *  arcs leaving one node. It wastes some bytes but gives faster lookups. */
    public int bytesPerArc() {
      return bytesPerArc;
    }

    /** How many arcs; only valid if bytesPerArc != 0 (fixed length arcs).
     * For a node designed for binary search this is the array size.
     * For a node designed for direct addressing, this is the label range. */
    public int numArcs() {
      return numArcs;
    }

    /** Table of bits of a direct addressing node.
     * Only valid if nodeFlags == {@link #ARCS_FOR_DIRECT_ADDRESSING};
     * may be null otherwise. */
    BitTable bitTable() {
      return bitTable;
    }

    /** The table of bits of a direct addressing node created lazily. */
    BitTable getOrCreateBitTable() {
      if (bitTable == null) {
        bitTable = new BitTable();
      }
      return bitTable;
    }

    /** First label of a direct addressing node.
     * Only valid if nodeFlags == {@link #ARCS_FOR_DIRECT_ADDRESSING}. */
    int firstLabel() {
      return firstLabel;
    }

    /**
     * Reusable table of bits using an array of long internally.
     */
    static class BitTable {

      private long[] bits;
      private int numLongs;

      /** Sets the number of longs in the internal long array.
       * Enlarges it if needed. Always clears the array. */
      BitTable setNumLongs(int numLongs) {
        assert numLongs >= 0;
        this.numLongs = numLongs;
        if (bits == null || bits.length < numLongs) {
          bits = new long[ArrayUtil.oversize(numLongs, Long.BYTES)];
        } else {
          for (int i = 0; i < numLongs; i++) {
            bits[i] = 0L;
          }
        }
        return this;
      }

      /** Creates a new {@link BitTable} by copying this one. */
      BitTable copy() {
        BitTable bitTable = new BitTable();
        bitTable.bits = ArrayUtil.copyOfSubArray(bits, 0, bits.length);
        bitTable.numLongs = numLongs;
        return bitTable;
      }

      boolean assertIsValid() {
        assert numLongs > 0 && numLongs <= bits.length;
        return true;
      }

      /** Forwards to {@link BitUtil#isBitSet(long[], int, int)}. */
      boolean isBitSet(int bitIndex) {
        return BitUtil.isBitSet(bits, numLongs, bitIndex);
      }

      /** Forwards to {@link BitUtil#countBits(long[], int)}. */
      int countBits() {
        return BitUtil.countBits(bits, numLongs);
      }

      /** Forwards to {@link BitUtil#countBitsUpTo(long[], int, int)}. */
      int countBitsUpTo(int bitIndex) {
        return BitUtil.countBitsUpTo(bits, numLongs, bitIndex);
      }

      /** Forwards to {@link BitUtil#nextBitSet(long[], int, int)}. */
      int nextBitSet(int bitIndex) {
        return BitUtil.nextBitSet(bits, numLongs, bitIndex);
      }

      /** Forwards to {@link BitUtil#previousBitSet(long[], int, int)}. */
      int previousBitSet(int bitIndex) {
        return BitUtil.previousBitSet(bits, numLongs, bitIndex);
      }
    }
  }

  private static boolean flag(int flags, int bit) {
    return (flags & bit) != 0;
  }

  // make a new empty FST, for building; Builder invokes this
  FST(INPUT_TYPE inputType, Outputs<T> outputs, int bytesPageBits) {
    this.inputType = inputType;
    this.outputs = outputs;
    fstStore = null;
    bytes = new BytesStore(bytesPageBits);
    // pad: ensure no node gets address 0 which is reserved to mean
    // the stop state w/ no arcs
    bytes.writeByte((byte) 0);
    emptyOutput = null;
  }

  private static final int DEFAULT_MAX_BLOCK_BITS = Constants.JRE_IS_64BIT ? 30 : 28;

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

    // NOTE: only reads formats VERSION_START up to VERSION_CURRENT; we don't have
    // back-compat promise for FSTs (they are experimental), but we are sometimes able to offer it
    CodecUtil.checkHeader(in, FILE_FORMAT_NAME, VERSION_START, VERSION_CURRENT);
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

  private long ramBytesUsed(Arc<T>[] arcs) {
    long size = 0;
    if (arcs != null) {
      size += RamUsageEstimator.shallowSizeOf(arcs);
      for (Arc<T> arc : arcs) {
        if (arc != null) {
          size += ARC_SHALLOW_RAM_BYTES_USED;
          if (arc.output() != null && arc.output() != outputs.getNoOutput()) {
            size += outputs.ramBytesUsed(arc.output());
          }
          if (arc.nextFinalOutput() != null && arc.nextFinalOutput() != outputs.getNoOutput()) {
            size += outputs.ramBytesUsed(arc.nextFinalOutput());
          }
        }
      }
    }
    return size;
  }

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
      readFirstRealTargetArc(arc.target(), arc, in);
      int count = 0;
      while(true) {
        assert arc.label() != END_LABEL;
        if (arc.label() < arcs.length) {
          arcs[arc.label()] = new Arc<T>().copyFrom(arc);
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
      if (count >= FIXED_LENGTH_ARC_SHALLOW_NUM_ARCS && cacheRAM < ramBytesUsed()/5) {
        cachedRootArcs = arcs;
        cachedArcsBytesUsed = cacheRAM;
      }
    }
  }
  
  public T getEmptyOutput() {
    return emptyOutput;
  }

  void setEmptyOutput(T v) {
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
      int emptyLen = emptyOutputBytes.length;

      // reverse
      final int stopAt = emptyLen / 2;
      int upto = 0;
      while (upto < stopAt) {
        final byte b = emptyOutputBytes[upto];
        emptyOutputBytes[upto] = emptyOutputBytes[emptyLen - upto - 1];
        emptyOutputBytes[emptyLen - upto - 1] = b;
        upto++;
      }
      out.writeVInt(emptyLen);
      out.writeBytes(emptyOutputBytes, 0, emptyLen);
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
    return arc.target() > 0;
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

    final boolean doFixedLengthArcs = shouldExpandNodeWithFixedLengthArcs(builder, nodeIn);
    if (doFixedLengthArcs) {
      //System.out.println("  fixed length arcs");
      if (builder.numBytesPerArc.length < nodeIn.numArcs) {
        builder.numBytesPerArc = new int[ArrayUtil.oversize(nodeIn.numArcs, Integer.BYTES)];
        builder.numLabelBytesPerArc = new int[builder.numBytesPerArc.length];
      }
    }

    builder.arcCount += nodeIn.numArcs;
    
    final int lastArc = nodeIn.numArcs-1;

    long lastArcStart = builder.bytes.getPosition();
    int maxBytesPerArc = 0;
    int maxBytesPerArcWithoutLabel = 0;
    for(int arcIdx=0; arcIdx < nodeIn.numArcs; arcIdx++) {
      final Builder.Arc<T> arc = nodeIn.arcs[arcIdx];
      final Builder.CompiledNode target = (Builder.CompiledNode) arc.target;
      int flags = 0;
      //System.out.println("  arc " + arcIdx + " label=" + arc.label + " -> target=" + target.node);

      if (arcIdx == lastArc) {
        flags += BIT_LAST_ARC;
      }

      if (builder.lastFrozenNode == target.node && !doFixedLengthArcs) {
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
      long labelStart = builder.bytes.getPosition();
      writeLabel(builder.bytes, arc.label);
      int numLabelBytes = (int) (builder.bytes.getPosition() - labelStart);

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
      if (doFixedLengthArcs) {
        int numArcBytes = (int) (builder.bytes.getPosition() - lastArcStart);
        builder.numBytesPerArc[arcIdx] = numArcBytes;
        builder.numLabelBytesPerArc[arcIdx] = numLabelBytes;
        lastArcStart = builder.bytes.getPosition();
        maxBytesPerArc = Math.max(maxBytesPerArc, numArcBytes);
        maxBytesPerArcWithoutLabel = Math.max(maxBytesPerArcWithoutLabel, numArcBytes - numLabelBytes);
        //System.out.println("    arcBytes=" + numArcBytes + " labelBytes=" + numLabelBytes);
      }
    }

    // TODO: try to avoid wasteful cases: disable doFixedLengthArcs in that case
    /* 
     * 
     * LUCENE-4682: what is a fair heuristic here?
     * It could involve some of these:
     * 1. how "busy" the node is: nodeIn.inputCount relative to frontier[0].inputCount?
     * 2. how much binSearch saves over scan: nodeIn.numArcs
     * 3. waste: numBytes vs numBytesExpanded
     * 
     * the one below just looks at #3
    if (doFixedLengthArcs) {
      // rough heuristic: make this 1.25 "waste factor" a parameter to the phd ctor????
      int numBytes = lastArcStart - startAddress;
      int numBytesExpanded = maxBytesPerArc * nodeIn.numArcs;
      if (numBytesExpanded > numBytes*1.25) {
        doFixedLengthArcs = false;
      }
    }
    */

    if (doFixedLengthArcs) {
      assert maxBytesPerArc > 0;
      // 2nd pass just "expands" all arcs to take up a fixed byte size

      int labelRange = nodeIn.arcs[nodeIn.numArcs - 1].label - nodeIn.arcs[0].label + 1;
      assert labelRange > 0;
      if (shouldExpandNodeWithDirectAddressing(builder, nodeIn, maxBytesPerArc, maxBytesPerArcWithoutLabel, labelRange)) {
        writeNodeForDirectAddressing(builder, nodeIn, startAddress, maxBytesPerArcWithoutLabel, labelRange);
        builder.directAddressingNodeCount++;
      } else {
        writeNodeForBinarySearch(builder, nodeIn, startAddress, maxBytesPerArc);
        builder.binarySearchNodeCount++;
      }
    }

    final long thisNodeAddress = builder.bytes.getPosition()-1;
    builder.bytes.reverse(startAddress, thisNodeAddress);
    builder.nodeCount++;
    return thisNodeAddress;
  }

  /**
   * Returns whether the given node should be expanded with fixed length arcs.
   * Nodes will be expanded depending on their depth (distance from the root node) and their number
   * of arcs.
   * <p>
   * Nodes with fixed length arcs use more space, because they encode all arcs with a fixed number
   * of bytes, but they allow either binary search or direct addressing on the arcs (instead of linear
   * scan) on lookup by arc label.
   */
  private boolean shouldExpandNodeWithFixedLengthArcs(Builder<T> builder, Builder.UnCompiledNode<T> node) {
    return builder.allowFixedLengthArcs &&
        ((node.depth <= FIXED_LENGTH_ARC_SHALLOW_DEPTH && node.numArcs >= FIXED_LENGTH_ARC_SHALLOW_NUM_ARCS) ||
            node.numArcs >= FIXED_LENGTH_ARC_DEEP_NUM_ARCS);
  }

  /**
   * Returns whether the given node should be expanded with direct addressing instead of binary search.
   * <p>
   * Prefer direct addressing for performance if it does not oversize binary search byte size too much,
   * so that the arcs can be directly addressed by label.
   *
   * @see Builder#getDirectAddressingMaxOversizingFactor()
   */
  private boolean shouldExpandNodeWithDirectAddressing(Builder<T> builder, Builder.UnCompiledNode<T> nodeIn,
                                                       int numBytesPerArc, int maxBytesPerArcWithoutLabel, int labelRange) {
    // Anticipate precisely the size of the encodings.
    int sizeForBinarySearch = numBytesPerArc * nodeIn.numArcs;
    int sizeForDirectAddressing = getNumPresenceBytes(labelRange) + builder.numLabelBytesPerArc[0]
        + maxBytesPerArcWithoutLabel * nodeIn.numArcs;

    // Determine the allowed oversize compared to binary search.
    // This is defined by a parameter of FST Builder (default 1: no oversize).
    int allowedOversize = (int) (sizeForBinarySearch * builder.getDirectAddressingMaxOversizingFactor());
    int expansionCost = sizeForDirectAddressing - allowedOversize;

    // Select direct addressing if either:
    // - Direct addressing size is smaller than binary search.
    //   In this case, increment the credit by the reduced size (to use it later).
    // - Direct addressing size is larger than binary search, but the positive credit allows the oversizing.
    //   In this case, decrement the credit by the oversize.
    // In addition, do not try to oversize to a clearly too large node size
    // (this is the DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR parameter).
    if (expansionCost <= 0 || (builder.directAddressingExpansionCredit >= expansionCost
        && sizeForDirectAddressing <= allowedOversize * DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR)) {
      builder.directAddressingExpansionCredit -= expansionCost;
      return true;
    }
    return false;
  }

  private void writeNodeForBinarySearch(Builder<T> builder, Builder.UnCompiledNode<T> nodeIn, long startAddress, int maxBytesPerArc) {
    // Build the header in a buffer.
    // It is a false/special arc which is in fact a node header with node flags followed by node metadata.
    builder.fixedLengthArcsBuffer
        .resetPosition()
        .writeByte(ARCS_FOR_BINARY_SEARCH)
        .writeVInt(nodeIn.numArcs)
        .writeVInt(maxBytesPerArc);
    int headerLen = builder.fixedLengthArcsBuffer.getPosition();

    // Expand the arcs in place, backwards.
    long srcPos = builder.bytes.getPosition();
    long destPos = startAddress + headerLen + nodeIn.numArcs * maxBytesPerArc;
    assert destPos >= srcPos;
    if (destPos > srcPos) {
      builder.bytes.skipBytes((int) (destPos - srcPos));
      for (int arcIdx = nodeIn.numArcs - 1; arcIdx >= 0; arcIdx--) {
        destPos -= maxBytesPerArc;
        int arcLen = builder.numBytesPerArc[arcIdx];
        srcPos -= arcLen;
        if (srcPos != destPos) {
          assert destPos > srcPos: "destPos=" + destPos + " srcPos=" + srcPos + " arcIdx=" + arcIdx + " maxBytesPerArc=" + maxBytesPerArc + " arcLen=" + arcLen + " nodeIn.numArcs=" + nodeIn.numArcs;
          builder.bytes.copyBytes(srcPos, destPos, arcLen);
        }
      }
    }

    // Write the header.
    builder.bytes.writeBytes(startAddress, builder.fixedLengthArcsBuffer.getBytes(), 0, headerLen);
  }

  private void writeNodeForDirectAddressing(Builder<T> builder, Builder.UnCompiledNode<T> nodeIn, long startAddress, int maxBytesPerArcWithoutLabel, int labelRange) {
    // Expand the arcs backwards in a buffer because we remove the labels.
    // So the obtained arcs might occupy less space. This is the reason why this
    // whole method is more complex.
    // Drop the label bytes since we can infer the label based on the arc index,
    // the presence bits, and the first label. Keep the first label.
    int headerMaxLen = 11;
    int numPresenceBytes = getNumPresenceBytes(labelRange);
    long srcPos = builder.bytes.getPosition();
    int totalArcBytes = builder.numLabelBytesPerArc[0] + nodeIn.numArcs * maxBytesPerArcWithoutLabel;
    int bufferOffset = headerMaxLen + numPresenceBytes + totalArcBytes;
    byte[] buffer = builder.fixedLengthArcsBuffer.ensureCapacity(bufferOffset).getBytes();
    // Copy the arcs to the buffer, dropping all labels except first one.
    for (int arcIdx = nodeIn.numArcs - 1; arcIdx >= 0; arcIdx--) {
      bufferOffset -= maxBytesPerArcWithoutLabel;
      int srcArcLen = builder.numBytesPerArc[arcIdx];
      srcPos -= srcArcLen;
      int labelLen = builder.numLabelBytesPerArc[arcIdx];
      // Copy the flags.
      builder.bytes.copyBytes(srcPos, buffer, bufferOffset, 1);
      // Skip the label, copy the remaining.
      int remainingArcLen = srcArcLen - 1 - labelLen;
      if (remainingArcLen != 0) {
        builder.bytes.copyBytes(srcPos + 1 + labelLen, buffer, bufferOffset + 1, remainingArcLen);
      }
      if (arcIdx == 0) {
        // Copy the label of the first arc only.
        bufferOffset -= labelLen;
        builder.bytes.copyBytes(srcPos + 1, buffer, bufferOffset, labelLen);
      }
    }
    assert bufferOffset == headerMaxLen + numPresenceBytes;

    // Build the header in the buffer.
    // It is a false/special arc which is in fact a node header with node flags followed by node metadata.
    builder.fixedLengthArcsBuffer
        .resetPosition()
        .writeByte(ARCS_FOR_DIRECT_ADDRESSING)
        .writeVInt(labelRange) // labelRange instead of numArcs.
        .writeVInt(maxBytesPerArcWithoutLabel); // maxBytesPerArcWithoutLabel instead of maxBytesPerArc.
    int headerLen = builder.fixedLengthArcsBuffer.getPosition();

    // Prepare the builder byte store. Enlarge or truncate if needed.
    long nodeEnd = startAddress + headerLen + numPresenceBytes + totalArcBytes;
    long currentPosition = builder.bytes.getPosition();
    if (nodeEnd >= currentPosition) {
      builder.bytes.skipBytes((int) (nodeEnd - currentPosition));
    } else {
      builder.bytes.truncate(nodeEnd);
    }
    assert builder.bytes.getPosition() == nodeEnd;

    // Write the header.
    long writeOffset = startAddress;
    builder.bytes.writeBytes(writeOffset, builder.fixedLengthArcsBuffer.getBytes(), 0, headerLen);
    writeOffset += headerLen;

    // Write the presence bits
    writePresenceBits(builder, nodeIn, writeOffset, numPresenceBytes);
    writeOffset += numPresenceBytes;

    // Write the first label and the arcs.
    builder.bytes.writeBytes(writeOffset, builder.fixedLengthArcsBuffer.getBytes(), bufferOffset, totalArcBytes);
  }

  private void writePresenceBits(Builder<T> builder, Builder.UnCompiledNode<T> nodeIn, long dest, int numPresenceBytes) {
    long bytePos = dest;
    byte presenceBits = 1; // The first arc is always present.
    int presenceIndex = 0;
    int previousLabel = nodeIn.arcs[0].label;
    for (int arcIdx = 1; arcIdx < nodeIn.numArcs; arcIdx++) {
      int label = nodeIn.arcs[arcIdx].label;
      assert label > previousLabel;
      presenceIndex += label - previousLabel;
      while (presenceIndex >= Byte.SIZE) {
        builder.bytes.writeByte(bytePos++, presenceBits);
        presenceBits = 0;
        presenceIndex -= Byte.SIZE;
      }
      // Set the bit at presenceIndex to flag that the corresponding arc is present.
      presenceBits |= 1 << presenceIndex;
      previousLabel = label;
    }
    assert presenceIndex == (nodeIn.arcs[nodeIn.numArcs - 1].label - nodeIn.arcs[0].label) % 8;
    assert presenceBits != 0; // The last byte is not 0.
    assert (presenceBits & (1 << presenceIndex)) != 0; // The last arc is always present.
    builder.bytes.writeByte(bytePos++, presenceBits);
    assert bytePos - dest == numPresenceBytes;
  }

  /** Gets the number of bytes required to flag the presence of each arc in the given label range, one bit per arc. */
  private static int getNumPresenceBytes(int labelRange) {
    return (labelRange + 7) / Byte.SIZE;
  }

  /**
   * Reads the presence bits of a direct-addressing node, store them in the provided arc {@link Arc#bitTable()}
   * and returns the number of presence bytes.
   */
  private int readPresenceBytes(Arc<T> arc, BytesReader in) throws IOException {
    int numPresenceBytes = getNumPresenceBytes(arc.numArcs());
    Arc.BitTable presenceBits = arc.getOrCreateBitTable().setNumLongs((numPresenceBytes + 7) / Long.BYTES);
    for (int i = 0; i < numPresenceBytes; i++) {
      // Read the next unsigned byte, shift it to the left, and appends it to the current long.
      presenceBits.bits[i / Long.BYTES] |= (in.readByte() & 0xFFL) << (i * Byte.SIZE);
    }
    assert assertPresenceBytesAreValid(arc);
    return numPresenceBytes;
  }

  private int getNumArcsDirectAddressing(Arc<T> arc) {
    assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
    return arc.bitTable().countBits();
  }

  private boolean assertPresenceBytesAreValid(Arc<T> arc) {
    assert arc.bitTable() != null;
    assert arc.bitTable().assertIsValid();
    // First bit must be set.
    assert arc.bitTable().isBitSet(0);
    // Last bit must be set.
    assert arc.bitTable().isBitSet(arc.numArcs() - 1);
    // No bit set after the last arc.
    assert arc.bitTable().nextBitSet(arc.numArcs() - 1) == -1;
    // Total bit set (real num arcs) must be <= label range (stored in arc.numArcs()).
    assert getNumArcsDirectAddressing(arc) <= arc.numArcs();
    return true;
  }

  /** Fills virtual 'start' arc, ie, an empty incoming arc to the FST's start node */
  public Arc<T> getFirstArc(Arc<T> arc) {
    T NO_OUTPUT = outputs.getNoOutput();

    if (emptyOutput != null) {
      arc.flags = BIT_FINAL_ARC | BIT_LAST_ARC;
      arc.nextFinalOutput = emptyOutput;
      if (emptyOutput != NO_OUTPUT) {
        arc.flags = (byte) (arc.flags() | BIT_ARC_HAS_FINAL_OUTPUT);
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
  Arc<T> readLastTargetArc(Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
    //System.out.println("readLast");
    if (!targetHasArcs(follow)) {
      //System.out.println("  end node");
      assert follow.isFinal();
      arc.label = END_LABEL;
      arc.target = FINAL_END_NODE;
      arc.output = follow.nextFinalOutput();
      arc.flags = BIT_LAST_ARC;
      arc.nodeFlags = arc.flags;
      return arc;
    } else {
      in.setPosition(follow.target());
      byte flags = arc.nodeFlags = in.readByte();
      if (flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING) {
        // Special arc which is actually a node header for fixed length arcs.
        // Jump straight to end to find the last arc.
        arc.numArcs = in.readVInt();
        arc.bytesPerArc = in.readVInt();
        //System.out.println("  array numArcs=" + arc.numArcs + " bpa=" + arc.bytesPerArc);
        if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
          readPresenceBytes(arc, in);
          arc.firstLabel = readLabel(in);
          arc.posArcsStart = in.getPosition();
          readArcByDirectAddressing(arc, in, arc.numArcs() - 1);
        } else {
          arc.arcIdx = arc.numArcs() - 2;
          arc.posArcsStart = in.getPosition();
          readNextRealArc(arc, in);
        }
      } else {
        arc.flags = flags;
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
        readNextRealArc(arc, in);
      }
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
      arc.output = follow.nextFinalOutput();
      arc.flags = BIT_FINAL_ARC;
      if (follow.target() <= 0) {
        arc.flags |= BIT_LAST_ARC;
      } else {
        // NOTE: nextArc is a node (not an address!) in this case:
        arc.nextArc = follow.target();
      }
      arc.target = FINAL_END_NODE;
      arc.nodeFlags = arc.flags;
      //System.out.println("    insert isFinal; nextArc=" + follow.target + " isLast=" + arc.isLast() + " output=" + outputs.outputToString(arc.output));
      return arc;
    } else {
      return readFirstRealTargetArc(follow.target(), arc, in);
    }
  }

  public Arc<T> readFirstRealTargetArc(long nodeAddress, Arc<T> arc, final BytesReader in) throws IOException {
    in.setPosition(nodeAddress);
    //System.out.println("   flags=" + arc.flags);

    byte flags = arc.nodeFlags = in.readByte();
    if (flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING) {
      //System.out.println("  fixed length arc");
      // Special arc which is actually a node header for fixed length arcs.
      arc.numArcs = in.readVInt();
      arc.bytesPerArc = in.readVInt();
      arc.arcIdx = -1;
      if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
        readPresenceBytes(arc, in);
        arc.firstLabel = readLabel(in);
      }
      arc.posArcsStart = in.getPosition();
      //System.out.println("  bytesPer=" + arc.bytesPerArc + " numArcs=" + arc.numArcs + " arcsStart=" + pos);
    } else {
      arc.nextArc = nodeAddress;
      arc.bytesPerArc = 0;
    }

    return readNextRealArc(arc, in);
  }

  /**
   * Returns whether <code>arc</code>'s target points to a node in expanded format (fixed length arcs).
   */
  boolean isExpandedTarget(Arc<T> follow, BytesReader in) throws IOException {
    if (!targetHasArcs(follow)) {
      return false;
    } else {
      in.setPosition(follow.target());
      byte flags = in.readByte();
      return flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING;
    }
  }

  /** In-place read; returns the arc. */
  public Arc<T> readNextArc(Arc<T> arc, BytesReader in) throws IOException {
    if (arc.label() == END_LABEL) {
      // This was a fake inserted "final" arc
      if (arc.nextArc() <= 0) {
        throw new IllegalArgumentException("cannot readNextArc when arc.isLast()=true");
      }
      return readFirstRealTargetArc(arc.nextArc(), arc, in);
    } else {
      return readNextRealArc(arc, in);
    }
  }

  /** Peeks at next arc's label; does not alter arc.  Do
   *  not call this if arc.isLast()! */
  int readNextArcLabel(Arc<T> arc, BytesReader in) throws IOException {
    assert !arc.isLast();

    if (arc.label() == END_LABEL) {
      //System.out.println("    nextArc fake " + arc.nextArc);
      // Next arc is the first arc of a node.
      // Position to read the first arc label.

      in.setPosition(arc.nextArc());
      byte flags = in.readByte();
      if (flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING) {
        //System.out.println("    nextArc fixed length arc");
        // Special arc which is actually a node header for fixed length arcs.
        int numArcs = in.readVInt();
        in.readVInt(); // Skip bytesPerArc.
        if (flags == ARCS_FOR_BINARY_SEARCH) {
          in.readByte(); // Skip arc flags.
        } else {
          in.skipBytes(getNumPresenceBytes(numArcs));
        }
      }
    } else {
      if (arc.bytesPerArc() != 0) {
        //System.out.println("    nextArc real array");
        // Arcs have fixed length.
        if (arc.nodeFlags() == ARCS_FOR_BINARY_SEARCH) {
          // Point to next arc, -1 to skip arc flags.
          in.setPosition(arc.posArcsStart() - (1 + arc.arcIdx()) * arc.bytesPerArc() - 1);
        } else {
          assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
          // Direct addressing node. The label is not stored but rather inferred
          // based on first label and arc index in the range.
          assert assertPresenceBytesAreValid(arc);
          assert arc.bitTable().isBitSet(arc.arcIdx());
          int nextIndex = arc.bitTable().nextBitSet(arc.arcIdx());
          assert nextIndex != -1;
          return arc.firstLabel() + nextIndex;
        }
      } else {
        // Arcs have variable length.
        //System.out.println("    nextArc real list");
        // Position to next arc, -1 to skip flags.
        in.setPosition(arc.nextArc() - 1);
      }
    }
    return readLabel(in);
  }

  public Arc<T> readArcByIndex(Arc<T> arc, final BytesReader in, int idx) throws IOException {
    assert idx >= 0 && idx < arc.numArcs();
    in.setPosition(arc.posArcsStart() - idx * arc.bytesPerArc());
    arc.arcIdx = idx;
    arc.flags = in.readByte();
    return readArc(arc, in);
  }

  /** Reads a present direct addressing node arc, with the provided index in the label range.
   *
   * @param rangeIndex The index of the arc in the label range. It must be present.
   *                   The real arc offset is computed based on the presence bits of
   *                   the direct addressing node.
   */
  public Arc<T> readArcByDirectAddressing(Arc<T> arc, final BytesReader in, int rangeIndex) throws IOException {
    assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
    assert arc.bytesPerArc() > 0;
    assert assertPresenceBytesAreValid(arc);
    assert rangeIndex >= 0 && rangeIndex < arc.numArcs();
    assert arc.bitTable().isBitSet(rangeIndex);
    int presenceIndex = arc.bitTable().countBitsUpTo(rangeIndex);
    in.setPosition(arc.posArcsStart() - presenceIndex * arc.bytesPerArc());
    arc.arcIdx = rangeIndex;
    arc.flags = in.readByte();
    return readArc(arc, in);
  }

  /** Never returns null, but you should never call this if
   *  arc.isLast() is true. */
  public Arc<T> readNextRealArc(Arc<T> arc, final BytesReader in) throws IOException {

    // TODO: can't assert this because we call from readFirstArc
    // assert !flag(arc.flags, BIT_LAST_ARC);

    switch (arc.nodeFlags()) {

      case ARCS_FOR_BINARY_SEARCH:
        assert arc.bytesPerArc() > 0;
        arc.arcIdx++;
        assert arc.arcIdx() >= 0 && arc.arcIdx() < arc.numArcs();
        in.setPosition(arc.posArcsStart() - arc.arcIdx() * arc.bytesPerArc());
        arc.flags = in.readByte();
        break;

      case ARCS_FOR_DIRECT_ADDRESSING:
        assert arc.bytesPerArc() > 0;
        assert assertPresenceBytesAreValid(arc);
        assert arc.arcIdx() == -1 || arc.bitTable().isBitSet(arc.arcIdx());
        int nextIndex = arc.bitTable().nextBitSet(arc.arcIdx());
        return readArcByDirectAddressing(arc, in, nextIndex);

      default:
        // Variable length arcs - linear search.
        assert arc.bytesPerArc() == 0;
        in.setPosition(arc.nextArc());
        arc.flags = in.readByte();
    }
    return readArc(arc, in);
  }

  /**
   * Reads an arc.
   * <br>Precondition: The arc flags byte has already been read and set;
   * the given BytesReader is positioned just after the arc flags byte.
   */
  private Arc<T> readArc(Arc<T> arc, BytesReader in) throws IOException {
    if (arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING) {
      arc.label = arc.firstLabel() + arc.arcIdx();
    } else {
      arc.label = readLabel(in);
    }

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
      arc.nextArc = in.getPosition(); // Only useful for list.
    } else if (arc.flag(BIT_TARGET_NEXT)) {
      arc.nextArc = in.getPosition(); // Only useful for list.
      // TODO: would be nice to make this lazy -- maybe
      // caller doesn't need the target and is scanning arcs...
      if (!arc.flag(BIT_LAST_ARC)) {
        if (arc.bytesPerArc() == 0) {
          // must scan
          seekToNextNode(in);
        } else {
          int numArcs = arc.nodeFlags == ARCS_FOR_DIRECT_ADDRESSING ? getNumArcsDirectAddressing(arc) : arc.numArcs();
          in.setPosition(arc.posArcsStart() - arc.bytesPerArc() * numArcs);
        }
      }
      arc.target = in.getPosition();
    } else {
      arc.target = readUnpackedNodeTarget(in);
      arc.nextArc = in.getPosition(); // Only useful for list.
    }
    return arc;
  }

  static <T> Arc<T> readEndArc(Arc<T> follow, Arc<T> arc) {
    if (follow.isFinal()) {
      if (follow.target() <= 0) {
        arc.flags = FST.BIT_LAST_ARC;
      } else {
        arc.flags = 0;
        // NOTE: nextArc is a node (not an address!) in this case:
        arc.nextArc = follow.target();
      }
      arc.output = follow.nextFinalOutput();
      arc.label = FST.END_LABEL;
      return arc;
    } else {
      return null;
    }
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
      assert cachedArc.arcIdx() == result.arcIdx();
      assert cachedArc.bytesPerArc() == result.bytesPerArc();
      assert cachedArc.flags() == result.flags();
      assert cachedArc.label() == result.label();
      assert cachedArc.bytesPerArc() != 0 || cachedArc.nextArc() == result.nextArc();
      assert cachedArc.nextFinalOutput().equals(result.nextFinalOutput());
      assert cachedArc.numArcs() == result.numArcs();
      assert cachedArc.output().equals(result.output());
      assert cachedArc.posArcsStart() == result.posArcsStart();
      assert cachedArc.target() == result.target();
      assert cachedArc.nodeFlags() == result.nodeFlags();
      assert cachedArc.nodeFlags() != ARCS_FOR_DIRECT_ADDRESSING || cachedArc.firstLabel() == result.firstLabel();
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
        if (follow.target() <= 0) {
          arc.flags = BIT_LAST_ARC;
        } else {
          arc.flags = 0;
          // NOTE: nextArc is a node (not an address!) in this case:
          arc.nextArc = follow.target();
        }
        arc.output = follow.nextFinalOutput();
        arc.label = END_LABEL;
        arc.nodeFlags = arc.flags;
        return arc;
      } else {
        return null;
      }
    }

    // Short-circuit if this arc is in the root arc cache:
    if (useRootArcCache && cachedRootArcs != null && follow.target() == startNode && labelToMatch < cachedRootArcs.length) {
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

    in.setPosition(follow.target());

    // System.out.println("fta label=" + (char) labelToMatch);

    byte flags = arc.nodeFlags = in.readByte();
    if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
      arc.numArcs = in.readVInt(); // This is in fact the label range.
      arc.bytesPerArc = in.readVInt();
      readPresenceBytes(arc, in);
      arc.firstLabel = readLabel(in);
      arc.posArcsStart = in.getPosition();

      int arcIndex = labelToMatch - arc.firstLabel();
      if (arcIndex < 0 || arcIndex >= arc.numArcs()) {
        return null; // Before or after label range.
      } else if (!arc.bitTable().isBitSet(arcIndex)) {
        return null; // Arc missing in the range.
      }
      return readArcByDirectAddressing(arc, in, arcIndex);
    } else if (flags == ARCS_FOR_BINARY_SEARCH) {
      arc.numArcs = in.readVInt();
      arc.bytesPerArc = in.readVInt();
      arc.posArcsStart = in.getPosition();

      // Array is sparse; do binary search:
      int low = 0;
      int high = arc.numArcs() - 1;
      while (low <= high) {
        //System.out.println("    cycle");
        int mid = (low + high) >>> 1;
        // +1 to skip over flags
        in.setPosition(arc.posArcsStart() - (arc.bytesPerArc() * mid + 1));
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
    readFirstRealTargetArc(follow.target(), arc, in);

    while(true) {
      //System.out.println("  non-bs cycle");
      // TODO: we should fix this code to not have to create
      // object for the output of every arc we scan... only
      // for the matching arc, if found
      if (arc.label() == labelToMatch) {
        //System.out.println("    found!");
        return arc;
      } else if (arc.label() > labelToMatch) {
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
