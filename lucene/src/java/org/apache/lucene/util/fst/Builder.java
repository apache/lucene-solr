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

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

import java.io.IOException;

/**
 * Builds a compact FST (maps an IntsRef term to an arbitrary
 * output) from pre-sorted terms with outputs (the FST
 * becomes an FSA if you use NoOutputs).  The FST is written
 * on-the-fly into a compact serialized format byte array, which can
 * be saved to / loaded from a Directory or used directly
 * for traversal.  The FST is always finite (no cycles).
 *
 * <p>NOTE: The algorithm is described at
 * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.24.3698</p>
 *
 * If your outputs are ByteSequenceOutput then the final FST
 * will be minimal, but if you use PositiveIntOutput then
 * it's only "near minimal".  For example, aa/0, aab/1, bbb/2
 * will produce 6 states when a 5 state fst is also
 * possible.
 *
 * The parameterized type T is the output type.  See the
 * subclasses of {@link Outputs}.
 *
 * @lucene.experimental
 */

public class Builder<T> {
  private final NodeHash<T> dedupHash;
  private final FST<T> fst;
  private final T NO_OUTPUT;

  // simplistic pruning: we prune node (and all following
  // nodes) if less than this number of terms go through it:
  private final int minSuffixCount1;

  // better pruning: we prune node (and all following
  // nodes) if the prior node has less than this number of
  // terms go through it:
  private final int minSuffixCount2;

  private final IntsRef lastInput = new IntsRef();

  // NOTE: cutting this over to ArrayList instead loses ~6%
  // in build performance on 9.8M Wikipedia terms; so we
  // left this as an array:
  // current "frontier"
  private UnCompiledNode<T>[] frontier;

  public Builder(FST.INPUT_TYPE inputType, int minSuffixCount1, int minSuffixCount2, boolean doMinSuffix, Outputs<T> outputs) {
    this.minSuffixCount1 = minSuffixCount1;
    this.minSuffixCount2 = minSuffixCount2;
    fst = new FST<T>(inputType, outputs);
    if (doMinSuffix) {
      dedupHash = new NodeHash<T>(fst);
    } else {
      dedupHash = null;
    }
    NO_OUTPUT = outputs.getNoOutput();

    @SuppressWarnings("unchecked") final UnCompiledNode<T>[] f = (UnCompiledNode<T>[]) new UnCompiledNode[10];
    frontier = f;
    for(int idx=0;idx<frontier.length;idx++) {
      frontier[idx] = new UnCompiledNode<T>(this, idx);
    }
  }

  public int getTotStateCount() {
    return fst.nodeCount;
  }

  public long getTermCount() {
    return frontier[0].inputCount;
  }

  public int getMappedStateCount() {
    return dedupHash == null ? 0 : fst.nodeCount;
  }

  private CompiledNode compileNode(UnCompiledNode<T> n) throws IOException {

    final int address;
    if (dedupHash != null) {
      if (n.numArcs == 0) {
        address = fst.addNode(n);
      } else {
        address = dedupHash.add(n);
      }
    } else {
      address = fst.addNode(n);
    }
    assert address != -2;

    n.clear();

    final CompiledNode fn = new CompiledNode();
    fn.address = address;
    return fn;
  }

  private void compilePrevTail(int prefixLenPlus1) throws IOException {
    assert prefixLenPlus1 >= 1;
    //System.out.println("  compileTail " + prefixLenPlus1);
    for(int idx=lastInput.length; idx >= prefixLenPlus1; idx--) {
      boolean doPrune = false;
      boolean doCompile = false;

      final UnCompiledNode<T> node = frontier[idx];
      final UnCompiledNode<T> parent = frontier[idx-1];

      if (node.inputCount < minSuffixCount1) {
        doPrune = true;
        doCompile = true;
      } else if (idx > prefixLenPlus1) {
        // prune if parent's inputCount is less than suffixMinCount2
        if (parent.inputCount < minSuffixCount2 || minSuffixCount2 == 1 && parent.inputCount == 1) {
          // my parent, about to be compiled, doesn't make the cut, so
          // I'm definitely pruned 

          // if pruneCount2 is 1, we keep only up
          // until the 'distinguished edge', ie we keep only the
          // 'divergent' part of the FST. if my parent, about to be
          // compiled, has inputCount 1 then we are already past the
          // distinguished edge.  NOTE: this only works if
          // the FST outputs are not "compressible" (simple
          // ords ARE compressible).
          doPrune = true;
        } else {
          // my parent, about to be compiled, does make the cut, so
          // I'm definitely not pruned 
          doPrune = false;
        }
        doCompile = true;
      } else {
        // if pruning is disabled (count is 0) we can always
        // compile current node
        doCompile = minSuffixCount2 == 0;
      }

      //System.out.println("    label=" + ((char) lastInput.ints[lastInput.offset+idx-1]) + " idx=" + idx + " inputCount=" + frontier[idx].inputCount + " doCompile=" + doCompile + " doPrune=" + doPrune);

      if (node.inputCount < minSuffixCount2 || minSuffixCount2 == 1 && node.inputCount == 1) {
        // drop all arcs
        for(int arcIdx=0;arcIdx<node.numArcs;arcIdx++) {
          @SuppressWarnings("unchecked") final UnCompiledNode<T> target = (UnCompiledNode<T>) node.arcs[arcIdx].target;
          target.clear();
        }
        node.numArcs = 0;
      }

      if (doPrune) {
        // this node doesn't make it -- deref it
        node.clear();
        parent.deleteLast(lastInput.ints[lastInput.offset+idx-1], node);
      } else {

        if (minSuffixCount2 != 0) {
          compileAllTargets(node);
        }
        final T nextFinalOutput = node.output;

        // We "fake" the node as being final if it has no
        // outgoing arcs; in theory we could leave it
        // as non-final (the FST can represent this), but
        // FSTEnum, Util, etc., have trouble w/ non-final
        // dead-end states:
        final boolean isFinal = node.isFinal || node.numArcs == 0;

        if (doCompile) {
          // this node makes it and we now compile it.  first,
          // compile any targets that were previously
          // undecided:
          parent.replaceLast(lastInput.ints[lastInput.offset + idx-1],
                             compileNode(node),
                             nextFinalOutput,
                             isFinal);
        } else {
          // replaceLast just to install
          // nextFinalOutput/isFinal onto the arc
          parent.replaceLast(lastInput.ints[lastInput.offset + idx-1],
                             node,
                             nextFinalOutput,
                             isFinal);
          // this node will stay in play for now, since we are
          // undecided on whether to prune it.  later, it
          // will be either compiled or pruned, so we must
          // allocate a new node:
          frontier[idx] = new UnCompiledNode<T>(this, idx);
        }
      }
    }
  }

  private final IntsRef scratchIntsRef = new IntsRef(10);

  public void add(BytesRef input, T output) throws IOException {
    assert fst.getInputType() == FST.INPUT_TYPE.BYTE1;
    scratchIntsRef.grow(input.length);
    for(int i=0;i<input.length;i++) {
      scratchIntsRef.ints[i] = input.bytes[i+input.offset] & 0xFF;
    }
    scratchIntsRef.length = input.length;
    add(scratchIntsRef, output);
  }

  /** Sugar: adds the UTF32 codepoints from char[] slice.  FST
   *  must be FST.INPUT_TYPE.BYTE4! */
  public void add(char[] s, int offset, int length, T output) throws IOException {
    assert fst.getInputType() == FST.INPUT_TYPE.BYTE4;
    int charIdx = offset;
    int intIdx = 0;
    final int charLimit = offset + length;
    while(charIdx < charLimit) {
      scratchIntsRef.grow(intIdx+1);
      final int utf32 = Character.codePointAt(s, charIdx);
      scratchIntsRef.ints[intIdx] = utf32;
      charIdx += Character.charCount(utf32);
      intIdx++;
    }
    scratchIntsRef.length = intIdx;
    add(scratchIntsRef, output);
  }

  /** Sugar: adds the UTF32 codepoints from CharSequence.  FST
   *  must be FST.INPUT_TYPE.BYTE4! */
  public void add(CharSequence s, T output) throws IOException {
    assert fst.getInputType() == FST.INPUT_TYPE.BYTE4;
    int charIdx = 0;
    int intIdx = 0;
    final int charLimit = s.length();
    while(charIdx < charLimit) {
      scratchIntsRef.grow(intIdx+1);
      final int utf32 = Character.codePointAt(s, charIdx);
      scratchIntsRef.ints[intIdx] = utf32;
      charIdx += Character.charCount(utf32);
      intIdx++;
    }
    scratchIntsRef.length = intIdx;
    add(scratchIntsRef, output);
  }

  /** It's OK to add the same input twice in a row with
   *  different outputs, as long as outputs impls the merge
   *  method. */
  public void add(IntsRef input, T output) throws IOException {
    //System.out.println("\nFST ADD: input=" + input + " output=" + fst.outputs.outputToString(output));
    assert lastInput.length == 0 || input.compareTo(lastInput) >= 0: "inputs are added out of order lastInput=" + lastInput + " vs input=" + input;
    assert validOutput(output);

    //System.out.println("\nadd: " + input);
    if (input.length == 0) {
      // empty input: only allowed as first input.  we have
      // to special case this because the packed FST
      // format cannot represent the empty input since
      // 'finalness' is stored on the incoming arc, not on
      // the node
      frontier[0].inputCount++;
      frontier[0].isFinal = true;
      fst.setEmptyOutput(output);
      return;
    }

    // compare shared prefix length
    int pos1 = 0;
    int pos2 = input.offset;
    final int pos1Stop = Math.min(lastInput.length, input.length);
    while(true) {
      //System.out.println("  incr " + pos1);
      frontier[pos1].inputCount++;
      if (pos1 >= pos1Stop || lastInput.ints[pos1] != input.ints[pos2]) {
        break;
      }
      pos1++;
      pos2++;
    }
    final int prefixLenPlus1 = pos1+1;
      
    if (frontier.length < input.length+1) {
      @SuppressWarnings("unchecked") final UnCompiledNode<T>[] next =
        new UnCompiledNode[ArrayUtil.oversize(input.length+1, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      System.arraycopy(frontier, 0, next, 0, frontier.length);
      for(int idx=frontier.length;idx<next.length;idx++) {
        next[idx] = new UnCompiledNode<T>(this, idx);
      }
      frontier = next;
    }

    // minimize/compile states from previous input's
    // orphan'd suffix
    compilePrevTail(prefixLenPlus1);

    // init tail states for current input
    for(int idx=prefixLenPlus1;idx<=input.length;idx++) {
      frontier[idx-1].addArc(input.ints[input.offset + idx - 1],
                             frontier[idx]);
      //System.out.println("  incr tail " + idx);
      frontier[idx].inputCount++;
    }

    final UnCompiledNode<T> lastNode = frontier[input.length];
    lastNode.isFinal = true;
    lastNode.output = NO_OUTPUT;

    // push conflicting outputs forward, only as far as
    // needed
    for(int idx=1;idx<prefixLenPlus1;idx++) {
      final UnCompiledNode<T> node = frontier[idx];
      final UnCompiledNode<T> parentNode = frontier[idx-1];

      final T lastOutput = parentNode.getLastOutput(input.ints[input.offset + idx - 1]);
      assert validOutput(lastOutput);

      final T commonOutputPrefix;
      final T wordSuffix;

      if (lastOutput != NO_OUTPUT) {
        commonOutputPrefix = fst.outputs.common(output, lastOutput);
        assert validOutput(commonOutputPrefix);
        wordSuffix = fst.outputs.subtract(lastOutput, commonOutputPrefix);
        assert validOutput(wordSuffix);
        parentNode.setLastOutput(input.ints[input.offset + idx - 1], commonOutputPrefix);
        node.prependOutput(wordSuffix);
      } else {
        commonOutputPrefix = wordSuffix = NO_OUTPUT;
      }

      output = fst.outputs.subtract(output, commonOutputPrefix);
      assert validOutput(output);
    }

    if (lastInput.length == input.length && prefixLenPlus1 == 1+input.length) {
      // same input more than 1 time in a row, mapping to
      // multiple outputs
      lastNode.output = fst.outputs.merge(lastNode.output, output);
    } else {
      // this new arc is private to this new input; set its
      // arc output to the leftover output:
      frontier[prefixLenPlus1-1].setLastOutput(input.ints[input.offset + prefixLenPlus1-1], output);
    }

    // save last input
    lastInput.copy(input);

    //System.out.println("  count[0]=" + frontier[0].inputCount);
  }

  private boolean validOutput(T output) {
    return output == NO_OUTPUT || !output.equals(NO_OUTPUT);
  }

  /** Returns final FST.  NOTE: this will return null if
   *  nothing is accepted by the FST. */
  public FST<T> finish() throws IOException {

    // minimize nodes in the last word's suffix
    compilePrevTail(1);
    //System.out.println("finish: inputCount=" + frontier[0].inputCount);
    if (frontier[0].inputCount < minSuffixCount1 || frontier[0].inputCount < minSuffixCount2 || frontier[0].numArcs == 0) {
      if (fst.emptyOutput == null) {
        return null;
      } else if (minSuffixCount1 > 0 || minSuffixCount2 > 0) {
        // empty string got pruned
        return null;
      } else {
        fst.finish(compileNode(frontier[0]).address);
        //System.out.println("compile addr = " + fst.getStartNode());
        return fst;
      }
    } else {
      if (minSuffixCount2 != 0) {
        compileAllTargets(frontier[0]);
      }
      //System.out.println("NOW: " + frontier[0].numArcs);
      fst.finish(compileNode(frontier[0]).address);
    }
    
    return fst;
  }

  private void compileAllTargets(UnCompiledNode<T> node) throws IOException {
    for(int arcIdx=0;arcIdx<node.numArcs;arcIdx++) {
      final Arc<T> arc = node.arcs[arcIdx];
      if (!arc.target.isCompiled()) {
        // not yet compiled
        @SuppressWarnings("unchecked") final UnCompiledNode<T> n = (UnCompiledNode<T>) arc.target;
        if (n.numArcs == 0) {
          //System.out.println("seg=" + segment + "        FORCE final arc=" + (char) arc.label);
          arc.isFinal = n.isFinal = true;
        }
        arc.target = compileNode(n);
      }
    }
  }

  static class Arc<T> {
    public int label;                             // really an "unsigned" byte
    public Node target;
    public boolean isFinal;
    public T output;
    public T nextFinalOutput;
  }

  // NOTE: not many instances of Node or CompiledNode are in
  // memory while the FST is being built; it's only the
  // current "frontier":

  static interface Node {
    boolean isCompiled();
  }

  static final class CompiledNode implements Node {
    int address;
    public boolean isCompiled() {
      return true;
    }
  }

  static final class UnCompiledNode<T> implements Node {
    final Builder<T> owner;
    int numArcs;
    Arc<T>[] arcs;
    T output;
    boolean isFinal;
    long inputCount;

    /** This node's depth, starting from the automaton root. */
    final int depth;

    /**
     * @param depth
     *          The node's depth starting from the automaton root. Needed for
     *          LUCENE-2934 (node expansion based on conditions other than the
     *          fanout size).
     */
    @SuppressWarnings("unchecked")
    public UnCompiledNode(Builder<T> owner, int depth) {
      this.owner = owner;
      arcs = (Arc<T>[]) new Arc[1];
      arcs[0] = new Arc<T>();
      output = owner.NO_OUTPUT;
      this.depth = depth;
    }

    public boolean isCompiled() {
      return false;
    }

    public void clear() {
      numArcs = 0;
      isFinal = false;
      output = owner.NO_OUTPUT;
      inputCount = 0;

      // We don't clear the depth here because it never changes 
      // for nodes on the frontier (even when reused).
    }

    public T getLastOutput(int labelToMatch) {
      assert numArcs > 0;
      assert arcs[numArcs-1].label == labelToMatch;
      return arcs[numArcs-1].output;
    }

    public void addArc(int label, Node target) {
      assert label >= 0;
      assert numArcs == 0 || label > arcs[numArcs-1].label: "arc[-1].label=" + arcs[numArcs-1].label + " new label=" + label + " numArcs=" + numArcs;
      if (numArcs == arcs.length) {
        @SuppressWarnings("unchecked") final Arc<T>[] newArcs =
          new Arc[ArrayUtil.oversize(numArcs+1, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(arcs, 0, newArcs, 0, arcs.length);
        for(int arcIdx=numArcs;arcIdx<newArcs.length;arcIdx++) {
          newArcs[arcIdx] = new Arc<T>();
        }
        arcs = newArcs;
      }
      final Arc<T> arc = arcs[numArcs++];
      arc.label = label;
      arc.target = target;
      arc.output = arc.nextFinalOutput = owner.NO_OUTPUT;
      arc.isFinal = false;
    }

    public void replaceLast(int labelToMatch, Node target, T nextFinalOutput, boolean isFinal) {
      assert numArcs > 0;
      final Arc<T> arc = arcs[numArcs-1];
      assert arc.label == labelToMatch: "arc.label=" + arc.label + " vs " + labelToMatch;
      arc.target = target;
      //assert target.address != -2;
      arc.nextFinalOutput = nextFinalOutput;
      arc.isFinal = isFinal;
    }

    public void deleteLast(int label, Node target) {
      assert numArcs > 0;
      assert label == arcs[numArcs-1].label;
      assert target == arcs[numArcs-1].target;
      numArcs--;
    }

    public void setLastOutput(int labelToMatch, T newOutput) {
      assert owner.validOutput(newOutput);
      assert numArcs > 0;
      final Arc<T> arc = arcs[numArcs-1];
      assert arc.label == labelToMatch;
      arc.output = newOutput;
    }

    // pushes an output prefix forward onto all arcs
    public void prependOutput(T outputPrefix) {
      assert owner.validOutput(outputPrefix);

      for(int arcIdx=0;arcIdx<numArcs;arcIdx++) {
        arcs[arcIdx].output = owner.fst.outputs.add(outputPrefix, arcs[arcIdx].output);
        assert owner.validOutput(arcs[arcIdx].output);
      }

      if (isFinal) {
        output = owner.fst.outputs.add(outputPrefix, output);
        assert owner.validOutput(output);
      }
    }
  }
}
