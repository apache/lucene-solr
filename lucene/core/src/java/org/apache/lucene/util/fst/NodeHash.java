package org.apache.lucene.util.fst;

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

import java.io.IOException;

import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;

// Used to dedup states (lookup already-frozen states)
final class NodeHash<T> {

  private GrowableWriter table;
  private int count;
  private int mask;
  private final FST<T> fst;
  private final FST.Arc<T> scratchArc = new FST.Arc<T>();
  private final FST.BytesReader in;

  public NodeHash(FST<T> fst, FST.BytesReader in) {
    table = new GrowableWriter(8, 16, PackedInts.COMPACT);
    mask = 15;
    this.fst = fst;
    this.in = in;
  }

  private boolean nodesEqual(Builder.UnCompiledNode<T> node, long address) throws IOException {
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
      long n = ((Builder.CompiledNode) arc.target).node;
      h = PRIME * h + (int) (n^(n>>32));
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
  private int hash(long node) throws IOException {
    final int PRIME = 31;
    //System.out.println("hash frozen node=" + node);
    int h = 0;
    fst.readFirstRealTargetArc(node, scratchArc, in);
    while(true) {
      //System.out.println("  label=" + scratchArc.label + " target=" + scratchArc.target + " h=" + h + " output=" + fst.outputs.outputToString(scratchArc.output) + " next?=" + scratchArc.flag(4) + " final?=" + scratchArc.isFinal() + " pos=" + in.getPosition());
      h = PRIME * h + scratchArc.label;
      h = PRIME * h + (int) (scratchArc.target^(scratchArc.target>>32));
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

  public long add(Builder.UnCompiledNode<T> nodeIn) throws IOException {
    // System.out.println("hash: add count=" + count + " vs " + table.size());
    final int h = hash(nodeIn);
    int pos = h & mask;
    int c = 0;
    while(true) {
      final long v = table.get(pos);
      if (v == 0) {
        // freeze & add
        final long node = fst.addNode(nodeIn);
        //System.out.println("  now freeze node=" + node);
        assert hash(node) == h : "frozenHash=" + hash(node) + " vs h=" + h;
        count++;
        table.set(pos, node);
        if (table.size() < 2*count) {
          rehash();
        }
        return node;
      } else if (nodesEqual(nodeIn, v)) {
        // same node is already here
        return v;
      }

      // quadratic probe
      pos = (pos + (++c)) & mask;
    }
  }

  // called only by rehash
  private void addNew(long address) throws IOException {
    int pos = hash(address) & mask;
    int c = 0;
    while(true) {
      if (table.get(pos) == 0) {
        table.set(pos, address);
        break;
      }

      // quadratic probe
      pos = (pos + (++c)) & mask;
    }
  }

  private void rehash() throws IOException {
    final GrowableWriter oldTable = table;

    if (oldTable.size() >= Integer.MAX_VALUE/2) {
      throw new IllegalStateException("FST too large (> 2.1 GB)");
    }

    table = new GrowableWriter(oldTable.getBitsPerValue(), 2*oldTable.size(), PackedInts.COMPACT);
    mask = table.size()-1;
    for(int idx=0;idx<oldTable.size();idx++) {
      final long address = oldTable.get(idx);
      if (address != 0) {
        addNew(address);
      }
    }
  }

  public int count() {
    return count;
  }
}
