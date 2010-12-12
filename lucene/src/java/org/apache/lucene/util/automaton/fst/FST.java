package org.apache.lucene.util.automaton.fst;

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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.IntsRef;

/** Represents an FST using a compact byte[] format.
 *  <p> The format is similar to what's used by Morfologik
 *  (http://sourceforge.net/projects/morfologik).
 * @lucene.experimental
 */
public class FST<T> {
  public static enum INPUT_TYPE {BYTE1, BYTE2, BYTE4};
  private final INPUT_TYPE inputType;

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

  // If the node has >= this number of arcs, the arcs are
  // stored as a fixed array.  Fixed array consumes more RAM
  // but enables binary search on the arcs (instead of
  // linear scan) on lookup by arc label:
  private final static int NUM_ARCS_FIXED_ARRAY = 10;
  private int[] bytesPerArc = new int[0];

  // Increment version to change it
  private final static String FILE_FORMAT_NAME = "FST";
  private final static int VERSION_START = 0;
  private final static int VERSION_CURRENT = VERSION_START;

  // Never serialized; just used to represent the virtual
  // final node w/ no arcs:
  private final static int FINAL_END_NODE = -1;

  // Never serialized; just used to represent the virtual
  // non-final node w/ no arcs:
  private final static int NON_FINAL_END_NODE = 0;

  // if non-null, this FST accepts the empty string and
  // produces this output
  private T emptyOutput;
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

  public final static class Arc<T> {
    int label;  // really a "unsigned" byte
    int target;
    byte flags;
    T output;
    T nextFinalOutput;
    int nextArc;

    // This is non-zero if current arcs are fixed array:
    int posArcsStart;
    int bytesPerArc;
    int arcIdx;
    int numArcs;

    // Must call this before re-using an Arc instance on a
    // new node
    public void reset() {
      bytesPerArc = 0;
    }

    public boolean flag(int flag) {
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
  public FST(IndexInput in, Outputs<T> outputs) throws IOException {
    this.outputs = outputs;
    writer = null;
    CodecUtil.checkHeader(in, FILE_FORMAT_NAME, VERSION_START, VERSION_START);
    if (in.readByte() == 1) {
      // accepts empty string
      int numBytes = in.readVInt();
      // messy
      bytes = new byte[numBytes];
      in.readBytes(bytes, 0, numBytes);
      emptyOutput = outputs.read(new BytesReader(numBytes-1));
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
  }

  public INPUT_TYPE getInputType() {
    return inputType;
  }

  /** Returns bytes used to represent the FST */
  public int sizeInBytes() {
    return bytes.length;
  }

  void finish(int startNode) {
    if (this.startNode != -1) {
      throw new IllegalStateException("already finished");
    }
    byte[] finalBytes = new byte[writer.posWrite];
    System.arraycopy(bytes, 0, finalBytes, 0, writer.posWrite);
    bytes = finalBytes;
    this.startNode = startNode;
  }

  public void setEmptyOutput(T v) throws IOException {
    if (emptyOutput != null) {
      throw new IllegalStateException("empty output is already set");
    }
    emptyOutput = v;

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

  public void save(IndexOutput out) throws IOException {
    if (startNode == -1) {
      throw new IllegalStateException("call finish first");
    }
    CodecUtil.writeHeader(out, FILE_FORMAT_NAME, VERSION_CURRENT);
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

  private int readLabel(DataInput in) throws IOException {
    final int v;
    if (inputType == INPUT_TYPE.BYTE1) {
      v = in.readByte()&0xFF;
    } else if (inputType == INPUT_TYPE.BYTE2) {
      v = in.readVInt();
    } else {
      v = in.readVInt();
    }
    return v;
  }

  // returns true if the node at this address has any
  // outgoing arcs
  public boolean hasArcs(int address) {
    return address != FINAL_END_NODE && address != NON_FINAL_END_NODE;
  }

  public int getStartNode() {
    if (startNode == -1) {
      throw new IllegalStateException("call finish first");
    }
    return startNode;
  }

  // returns null if this FST does not accept the empty
  // string, else, the output for the empty string
  public T getEmptyOutput() {
    return emptyOutput;
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

    final boolean doFixedArray = node.numArcs >= NUM_ARCS_FIXED_ARRAY;
    final int fixedArrayStart;
    if (doFixedArray) {
      if (bytesPerArc.length < node.numArcs) {
        bytesPerArc = new int[ArrayUtil.oversize(node.numArcs, 1)];
      }
      // write a "false" first arc:
      writer.writeByte((byte) BIT_ARCS_AS_FIXED_ARRAY);
      writer.writeVInt(node.numArcs);
      // placeholder -- we'll come back and write the number
      // of bytes per arc here:
      writer.writeByte((byte) 0);
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

      boolean targetHasArcs = hasArcs(target.address);

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

    if (doFixedArray) {
      assert maxBytesPerArc > 0;
      // 2nd pass just "expands" all arcs to take up a fixed
      // byte size
      final int sizeNeeded = fixedArrayStart + node.numArcs * maxBytesPerArc;
      bytes = ArrayUtil.grow(bytes, sizeNeeded);
      if (maxBytesPerArc > 255) {
        throw new IllegalStateException("max arc size is too large (" + maxBytesPerArc + ")");
      }
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
    final int endAddress = writer.posWrite;
    final int stopAt = (endAddress - startAddress)/2;
    int upto = 0;
    while (upto < stopAt) {
      final byte b = bytes[startAddress+upto];
      bytes[startAddress+upto] = bytes[endAddress-upto-1];
      bytes[endAddress-upto-1] = b;
      upto++;
    }

    lastFrozenNode = endAddress - 1;
    /*
    System.out.println("  return node addr=" + (endAddress-1));
    for(int i=endAddress-1;i>=startAddress;i--) {
      System.out.println("    bytes[" + i + "]=" + bytes[i]);
    }
    */

    return endAddress-1;
  }

  public Arc<T> readFirstArc(int address, Arc<T> arc) throws IOException {
    //System.out.println("readFirstArc addr=" + address);
    //int pos = address;
    final BytesReader in = new BytesReader(address);

    arc.flags = in.readByte();

    if (arc.flag(BIT_ARCS_AS_FIXED_ARRAY)) {
      //System.out.println("  fixedArray");
      // this is first arc in a fixed-array
      arc.numArcs = in.readVInt();
      arc.bytesPerArc = in.readByte() & 0xFF;
      arc.arcIdx = -1;
      arc.posArcsStart = in.pos;
      //System.out.println("  bytesPer=" + arc.bytesPerArc + " numArcs=" + arc.numArcs + " arcsStart=" + pos);
    } else {
      in.pos++;
      arc.bytesPerArc = 0;
    }
    arc.nextArc = in.pos;
    return readNextArc(arc);
  }

  public Arc<T> readNextArc(Arc<T> arc) throws IOException {
    // this is a continuing arc in a fixed array
    final BytesReader in;
    if (arc.bytesPerArc != 0) {
      // arcs are at fixed entries
      arc.arcIdx++;
      in = new BytesReader(arc.posArcsStart - arc.arcIdx*arc.bytesPerArc);
    } else {
      // arcs are packed
      in = new BytesReader(arc.nextArc);
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
      arc.target = FINAL_END_NODE;
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

  public Arc<T> findArc(int address, int labelToMatch, Arc<T> arc) throws IOException {
    // TODO: maybe make an explicit thread state that holds
    // reusable stuff eg BytesReader:
    final BytesReader in = new BytesReader(address);

    if ((in.readByte() & BIT_ARCS_AS_FIXED_ARRAY) != 0) {
      // Arcs are full array; do binary search:
      //System.out.println("findArc: array label=" + labelToMatch);
      arc.numArcs = in.readVInt();
      arc.bytesPerArc = in.readByte() & 0xFF;
      arc.posArcsStart = in.pos;
      int low = 0;
      int high = arc.numArcs-1;
      while (low <= high) {
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
          return readNextArc(arc);
        }
      }

      return null;
    }
    //System.out.println("findArc: scan");

    readFirstArc(address, arc);

    while(true) {
      if (arc.label == labelToMatch) {
        return arc;
      } else if (arc.isLast()) {
        return null;
      } else {
        readNextArc(arc);
      }
    }
  }

  /** Looks up the output for this input, or null if the
   *  input is not accepted. FST must be
   *  INPUT_TYPE.BYTE4. */
  public T get(IntsRef input) throws IOException {
    assert inputType == INPUT_TYPE.BYTE4;

    if (input.length == 0) {
      return getEmptyOutput();
    }

    // TODO: would be nice not to alloc this on every lookup
    final FST.Arc<T> arc = new FST.Arc<T>();
    int node = getStartNode();
    T output = NO_OUTPUT;
    for(int i=0;i<input.length;i++) {
      if (!hasArcs(node)) {
        // hit end of FST before input end
        return null;
      }

      if (findArc(node, input.ints[input.offset + i], arc) != null) {
        node = arc.target;
        if (arc.output != NO_OUTPUT) {
          output = outputs.add(output, arc.output);
        }
      } else {
        return null;
      }
    }

    if (!arc.isFinal()) {
      // hit input's end before end node
      return null;
    }

    if (arc.nextFinalOutput != NO_OUTPUT) {
      output = outputs.add(output, arc.nextFinalOutput);
    }

    return output;
  }

  /** Logically casts input to UTF32 ints then looks up the output
   *  or null if the input is not accepted.  FST must be
   *  INPUT_TYPE.BYTE4.  */
  public T get(char[] input, int offset, int length) throws IOException {
    assert inputType == INPUT_TYPE.BYTE4;

    if (length == 0) {
      return getEmptyOutput();
    }

    // TODO: would be nice not to alloc this on every lookup
    final FST.Arc<T> arc = new FST.Arc<T>();
    int node = getStartNode();
    int charIdx = offset;
    final int charLimit = offset + length;
    T output = NO_OUTPUT;
    while(charIdx < charLimit) {
      if (!hasArcs(node)) {
        // hit end of FST before input end
        return null;
      }

      final int utf32 = Character.codePointAt(input, charIdx);
      charIdx += Character.charCount(utf32);

      if (findArc(node, utf32, arc) != null) {
        node = arc.target;
        if (arc.output != NO_OUTPUT) {
          output = outputs.add(output, arc.output);
        }
      } else {
        return null;
      }
    }

    if (!arc.isFinal()) {
      // hit input's end before end node
      return null;
    }

    if (arc.nextFinalOutput != NO_OUTPUT) {
      output = outputs.add(output, arc.nextFinalOutput);
    }

    return output;
  }


  /** Logically casts input to UTF32 ints then looks up the output
   *  or null if the input is not accepted.  FST must be
   *  INPUT_TYPE.BYTE4.  */
  public T get(CharSequence input) throws IOException {
    assert inputType == INPUT_TYPE.BYTE4;

    final int len = input.length();
    if (len == 0) {
      return getEmptyOutput();
    }

    // TODO: would be nice not to alloc this on every lookup
    final FST.Arc<T> arc = new FST.Arc<T>();
    int node = getStartNode();
    int charIdx = 0;
    final int charLimit = input.length();
    T output = NO_OUTPUT;
    while(charIdx < charLimit) {
      if (!hasArcs(node)) {
        // hit end of FST before input end
        return null;
      }

      final int utf32 = Character.codePointAt(input, charIdx);
      charIdx += Character.charCount(utf32);

      if (findArc(node, utf32, arc) != null) {
        node = arc.target;
        if (arc.output != NO_OUTPUT) {
          output = outputs.add(output, arc.output);
        }
      } else {
        return null;
      }
    }

    if (!arc.isFinal()) {
      // hit input's end before end node
      return null;
    }

    if (arc.nextFinalOutput != NO_OUTPUT) {
      output = outputs.add(output, arc.nextFinalOutput);
    }

    return output;
  }

  /** Looks up the output for this input, or null if the
   *  input is not accepted */
  public T get(BytesRef input) throws IOException {
    assert inputType == INPUT_TYPE.BYTE1;

    if (input.length == 0) {
      return getEmptyOutput();
    }

    // TODO: would be nice not to alloc this on every lookup
    final FST.Arc<T> arc = new FST.Arc<T>();
    int node = getStartNode();
    T output = NO_OUTPUT;
    for(int i=0;i<input.length;i++) {
      if (!hasArcs(node)) {
        // hit end of FST before input end
        return null;
      }

      if (findArc(node, input.bytes[i+input.offset], arc) != null) {
        node = arc.target;
        if (arc.output != NO_OUTPUT) {
          output = outputs.add(output, arc.output);
        }
      } else {
        return null;
      }
    }

    if (!arc.isFinal()) {
      // hit input's end before end node
      return null;
    }

    if (arc.nextFinalOutput != NO_OUTPUT) {
      output = outputs.add(output, arc.nextFinalOutput);
    }

    return output;
  }

  /** Returns true if this FST has no nodes */
  public boolean noNodes() {
    //System.out.println("isempty startNode=" + startNode);
    return startNode == 0;
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

  // NOTE: this consumes alot of RAM!
  // final arcs have a flat end (not arrow)
  // arcs w/ NEXT opto are in blue
  /*
    eg:
      PrintStream ps = new PrintStream("out.dot");
      fst.toDot(ps);
      ps.close();
      System.out.println("SAVED out.dot");
      
    then dot -Tpng out.dot > /x/tmp/out.png
  */
  public void toDot(PrintStream out) throws IOException {

    final List<Integer> queue = new ArrayList<Integer>();
    queue.add(startNode);

    final Set<Integer> seen = new HashSet<Integer>();
    seen.add(startNode);
    
    out.println("digraph FST {");
    out.println("  rankdir = LR;");
    //out.println("  " + startNode + " [shape=circle label=" + startNode + "];");
    out.println("  " + startNode + " [label=\"\" shape=circle];");
    out.println("  initial [shape=point color=white label=\"\"];");
    if (emptyOutput != null) {
      out.println("  initial -> " + startNode + " [arrowhead=tee label=\"(" + outputs.outputToString(emptyOutput) + ")\"];");
    } else {
      out.println("  initial -> " + startNode);
    }

    final Arc<T> arc = new Arc<T>();

    while(queue.size() != 0) {
      Integer node = queue.get(queue.size()-1);
      queue.remove(queue.size()-1);

      if (node == FINAL_END_NODE || node == NON_FINAL_END_NODE) {
        continue;
      }

      // scan all arcs
      readFirstArc(node, arc);
      while(true) {

        if (!seen.contains(arc.target)) {
          //out.println("  " + arc.target + " [label=" + arc.target + "];");
          out.println("  " + arc.target + " [label=\"\" shape=circle];");
          seen.add(arc.target);
          queue.add(arc.target);
        }
        String outs;
        if (arc.output != NO_OUTPUT) {
          outs = "/" + outputs.outputToString(arc.output);
        } else {
          outs = "";
        }
        if (arc.isFinal() && arc.nextFinalOutput != NO_OUTPUT) {
          outs += " (" + outputs.outputToString(arc.nextFinalOutput) + ")";
        }
        out.print("  " + node + " -> " + arc.target + " [label=\"" + arc.label + outs + "\"");
        if (arc.isFinal()) {
          out.print(" arrowhead=tee");
        }
        if (arc.flag(BIT_TARGET_NEXT)) {
          out.print(" color=blue");
        }
        out.println("];");
        
        if (arc.isLast()) {
          break;
        } else {
          readNextArc(arc);
        }
      }
    }
    out.println("}");
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

  // Non-static: writes to FST's byte[]
  private class BytesWriter extends DataOutput {
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

  // Non-static: reads byte[] from FST
  private class BytesReader extends DataInput {
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
