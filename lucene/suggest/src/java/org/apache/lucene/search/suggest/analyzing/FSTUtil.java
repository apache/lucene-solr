package org.apache.lucene.search.suggest.analyzing;

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

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.State;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

// TODO: move to core?  nobody else uses it yet though...

/**
 * Exposes a utility method to enumerate all paths
 * intersecting an {@link Automaton} with an {@link FST}.
 */
public class FSTUtil {

  private FSTUtil() {
  }

  /** Holds a pair (automaton, fst) of states and accumulated output in the intersected machine. */
  public static final class Path<T> {

    /** Node in the automaton where path ends: */
    public final State state;

    /** Node in the FST where path ends: */
    public final FST.Arc<T> fstNode;

    /** Output of the path so far: */
    T output;

    /** Input of the path so far: */
    public final IntsRef input;

    /** Sole constructor. */
    public Path(State state, FST.Arc<T> fstNode, T output, IntsRef input) {
      this.state = state;
      this.fstNode = fstNode;
      this.output = output;
      this.input = input;
    }
  }

  /**
   * Enumerates all minimal prefix paths in the automaton that also intersect the FST,
   * accumulating the FST end node and output for each path.
   */
  public static <T> List<Path<T>> intersectPrefixPaths(Automaton a, FST<T> fst)
      throws IOException {
    assert a.isDeterministic();
    final List<Path<T>> queue = new ArrayList<Path<T>>();
    final List<Path<T>> endNodes = new ArrayList<Path<T>>();
    queue.add(new Path<T>(a.getInitialState(), fst
        .getFirstArc(new FST.Arc<T>()), fst.outputs.getNoOutput(),
        new IntsRef()));
    
    final FST.Arc<T> scratchArc = new FST.Arc<T>();
    final FST.BytesReader fstReader = fst.getBytesReader();
    
    while (queue.size() != 0) {
      final Path<T> path = queue.remove(queue.size() - 1);
      if (path.state.isAccept()) {
        endNodes.add(path);
        // we can stop here if we accept this path,
        // we accept all further paths too
        continue;
      }
      
      IntsRef currentInput = path.input;
      for (Transition t : path.state.getTransitions()) {
        final int min = t.getMin();
        final int max = t.getMax();
        if (min == max) {
          final FST.Arc<T> nextArc = fst.findTargetArc(t.getMin(),
              path.fstNode, scratchArc, fstReader);
          if (nextArc != null) {
            final IntsRef newInput = new IntsRef(currentInput.length + 1);
            newInput.copyInts(currentInput);
            newInput.ints[currentInput.length] = t.getMin();
            newInput.length = currentInput.length + 1;
            queue.add(new Path<T>(t.getDest(), new FST.Arc<T>()
                .copyFrom(nextArc), fst.outputs
                .add(path.output, nextArc.output), newInput));
          }
        } else {
          // TODO: if this transition's TO state is accepting, and
          // it accepts the entire range possible in the FST (ie. 0 to 255),
          // we can simply use the prefix as the accepted state instead of
          // looking up all the ranges and terminate early
          // here.  This just shifts the work from one queue
          // (this one) to another (the completion search
          // done in AnalyzingSuggester).
          FST.Arc<T> nextArc = Util.readCeilArc(min, fst, path.fstNode,
              scratchArc, fstReader);
          while (nextArc != null && nextArc.label <= max) {
            assert nextArc.label <=  max;
            assert nextArc.label >= min : nextArc.label + " "
                + min;
            final IntsRef newInput = new IntsRef(currentInput.length + 1);
            newInput.copyInts(currentInput);
            newInput.ints[currentInput.length] = nextArc.label;
            newInput.length = currentInput.length + 1;
            queue.add(new Path<T>(t.getDest(), new FST.Arc<T>()
                .copyFrom(nextArc), fst.outputs
                .add(path.output, nextArc.output), newInput));
            final int label = nextArc.label; // used in assert
            nextArc = nextArc.isLast() ? null : fst.readNextRealArc(nextArc,
                fstReader);
            assert nextArc == null || label < nextArc.label : "last: " + label
                + " next: " + nextArc.label;
          }
        }
      }
    }
    return endNodes;
  }
  
}
