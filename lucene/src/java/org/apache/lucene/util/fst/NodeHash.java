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

// Used to dedup states (lookup already-frozen states)
final class NodeHash<T> {

  private int[] table;
  private int count;
  private int mask;
  private final FST<T> fst;
  private final FST.Arc<T> scratchArc = new FST.Arc<T>();

  public NodeHash(FST<T> fst) {
    table = new int[16];
    mask = 15;
    this.fst = fst;
  }

  private boolean nodesEqual(Builder.UnCompiledNode<T> node, int address, FST.BytesReader in) throws IOException {
    fst.readFirstRealTargetArc(address, scratchArc, in);
    if (scratchArc.bytesPerArc != 0 && node.numArcs != scratchArc.numArcs) {
      return false;
    }
    for(int arcUpto=0;arcUpto<node.numArcs;arcUpto++) {
      final Builder.Arc<T> arc = node.arcs[arcUpto];
      if (arc.label != scratchArc.label ||
          !arc.output.equals(scratchArc.output) ||
          ((Builder.CompiledNode) arc.target).node != scratchArc.target ||
          !arc.nextFinalOutput.equals(scratchArc.nextFinalOutput) ||
          arc.isFinal != scratchArc.isFinal()) {
        return false;
      }

      if (scratchArc.isLast()) {
        if (arcUpto == node.numArcs-1) {
          return true;
        } else {
          return false;
        }
      }
      fst.readNextRealArc(scratchArc, in);
    }

    return false;
  }

  // hash code for an unfrozen node.  This must be identical
  // to the un-frozen case (below)!!
  private int hash(Builder.UnCompiledNode<T> node) {
    final int PRIME = 31;
    //System.out.println("hash unfrozen");
    int h = 0;
    // TODO: maybe if number of arcs is high we can safely subsample?
    for(int arcIdx=0;arcIdx<node.numArcs;arcIdx++) {
      final Builder.Arc<T> arc = node.arcs[arcIdx];
      //System.out.println("  label=" + arc.label + " target=" + ((Builder.CompiledNode) arc.target).node + " h=" + h + " output=" + fst.outputs.outputToString(arc.output) + " isFinal?=" + arc.isFinal);
      h = PRIME * h + arc.label;
      h = PRIME * h + ((Builder.CompiledNode) arc.target).node;
      h = PRIME * h + arc.output.hashCode();
      h = PRIME * h + arc.nextFinalOutput.hashCode();
      if (arc.isFinal) {
        h += 17;
      }
    }
    //System.out.println("  ret " + (h&Integer.MAX_VALUE));
    return h & Integer.MAX_VALUE;
  }

  // hash code for a frozen node
  private int hash(int node) throws IOException {
    final int PRIME = 31;
    final FST.BytesReader in = fst.getBytesReader(0);
    //System.out.println("hash frozen node=" + node);
    int h = 0;
    fst.readFirstRealTargetArc(node, scratchArc, in);
    while(true) {
      //System.out.println("  label=" + scratchArc.label + " target=" + scratchArc.target + " h=" + h + " output=" + fst.outputs.outputToString(scratchArc.output) + " next?=" + scratchArc.flag(4) + " final?=" + scratchArc.isFinal());
      h = PRIME * h + scratchArc.label;
      h = PRIME * h + scratchArc.target;
      h = PRIME * h + scratchArc.output.hashCode();
      h = PRIME * h + scratchArc.nextFinalOutput.hashCode();
      if (scratchArc.isFinal()) {
        h += 17;
      }
      if (scratchArc.isLast()) {
        break;
      }
      fst.readNextRealArc(scratchArc, in);
    }
    //System.out.println("  ret " + (h&Integer.MAX_VALUE));
    return h & Integer.MAX_VALUE;
  }

  public int add(Builder.UnCompiledNode<T> nodeIn) throws IOException {
    // System.out.println("hash: add count=" + count + " vs " + table.length);
    final FST.BytesReader in = fst.getBytesReader(0);
    final int h = hash(nodeIn);
    int pos = h & mask;
    int c = 0;
    while(true) {
      final int v = table[pos];
      if (v == 0) {
        // freeze & add
        final int node = fst.addNode(nodeIn);
        //System.out.println("  now freeze node=" + node);
        assert hash(node) == h : "frozenHash=" + hash(node) + " vs h=" + h;
        count++;
        table[pos] = node;
        if (table.length < 2*count) {
          rehash();
        }
        return node;
      } else if (nodesEqual(nodeIn, v, in)) {
        // same node is already here
        return v;
      }

      // quadratic probe
      pos = (pos + (++c)) & mask;
    }
  }

  // called only by rehash
  private void addNew(int address) throws IOException {
    int pos = hash(address) & mask;
    int c = 0;
    while(true) {
      if (table[pos] == 0) {
        table[pos] = address;
        break;
      }

      // quadratic probe
      pos = (pos + (++c)) & mask;
    }
  }

  private void rehash() throws IOException {
    final int[] oldTable = table;
    table = new int[2*table.length];
    mask = table.length-1;
    for(int idx=0;idx<oldTable.length;idx++) {
      final int address = oldTable[idx];
      if (address != 0) {
        addNew(address);
      }
    }
  }

  public int count() {
    return count;
  }
}
