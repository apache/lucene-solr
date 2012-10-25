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

  /** Enumerates all paths in the automaton that also
   *  intersect the FST, accumulating the FST end node and
   *  output for each path. */
  public static<T> List<Path<T>> intersectPrefixPaths(Automaton a, FST<T> fst) throws IOException {
    final List<Path<T>> queue = new ArrayList<Path<T>>();
    final List<Path<T>> endNodes = new ArrayList<Path<T>>();

    queue.add(new Path<T>(a.getInitialState(),
                          fst.getFirstArc(new FST.Arc<T>()),       
                          fst.outputs.getNoOutput(),
                          new IntsRef()));

    final FST.Arc<T> scratchArc = new FST.Arc<T>();
    final FST.BytesReader fstReader = fst.getBytesReader(0);

    //System.out.println("fst/a intersect");

    while (queue.size() != 0) {
      final Path<T> path = queue.remove(queue.size()-1);
      //System.out.println("  cycle path=" + path);
      if (path.state.isAccept()) {
        endNodes.add(path);
      }

      IntsRef currentInput = path.input;
      for(Transition t : path.state.getTransitions()) {
        
        // TODO: we can fix this if necessary:
        if (t.getMin() != t.getMax()) {
          throw new IllegalStateException("can only handle Transitions that match one character");
        }

        //System.out.println("    t=" + (char) t.getMin());

        final FST.Arc<T> nextArc = fst.findTargetArc(t.getMin(), path.fstNode, scratchArc, fstReader);
        if (nextArc != null) {
          //System.out.println("      fst matches");
          // Path continues:
          IntsRef newInput = new IntsRef(currentInput.length + 1);
          newInput.copyInts(currentInput);
          newInput.ints[currentInput.length] = t.getMin();
          newInput.length = currentInput.length + 1;

          queue.add(new Path<T>(t.getDest(),
                                new FST.Arc<T>().copyFrom(nextArc),
                                fst.outputs.add(path.output, nextArc.output),
                                newInput));
        }
      }
    }

    return endNodes;
  }
}
