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

package org.apache.lucene.queries.intervals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.lucene.search.IndexSearcher;

final class Disjunctions {

  // Given a list of sources that contain disjunctions, and a combiner function,
  // pulls the disjunctions to the top of the source tree

  // eg FUNC(a, b, OR(c, "d e")) => [FUNC(a, b, c), FUNC(a, b, "d e")]

  public static List<IntervalsSource> pullUp(
      List<IntervalsSource> sources, Function<List<IntervalsSource>, IntervalsSource> function) {

    List<List<IntervalsSource>> rewritten = new ArrayList<>();
    rewritten.add(new ArrayList<>());
    for (IntervalsSource source : sources) {
      List<IntervalsSource> disjuncts = splitDisjunctions(source);
      if (disjuncts.size() == 1) {
        rewritten.forEach(l -> l.add(disjuncts.get(0)));
      } else {
        if (rewritten.size() * disjuncts.size() > IndexSearcher.getMaxClauseCount()) {
          throw new IllegalArgumentException("Too many disjunctions to expand");
        }
        List<List<IntervalsSource>> toAdd = new ArrayList<>();
        for (IntervalsSource disj : disjuncts) {
          // clone the rewritten list, then append the disjunct
          for (List<IntervalsSource> subList : rewritten) {
            List<IntervalsSource> l = new ArrayList<>(subList);
            l.add(disj);
            toAdd.add(l);
          }
        }
        rewritten = toAdd;
      }
    }
    if (rewritten.size() == 1) {
      return Collections.singletonList(function.apply(rewritten.get(0)));
    }
    return rewritten.stream().map(function).collect(Collectors.toList());
  }

  // Given a source containing disjunctions, and a mapping function,
  // pulls the disjunctions to the top of the source tree
  public static List<IntervalsSource> pullUp(
      IntervalsSource source, Function<IntervalsSource, IntervalsSource> function) {
    List<IntervalsSource> disjuncts = splitDisjunctions(source);
    if (disjuncts.size() == 1) {
      return Collections.singletonList(function.apply(disjuncts.get(0)));
    }
    return disjuncts.stream().map(function).collect(Collectors.toList());
  }

  // Separate out disjunctions into individual sources
  // Clauses that have a minExtent of 1 are grouped together and treated as a single
  // source, as any overlapping intervals of length 1 can be treated as identical,
  // and we know that all combinatorial sources have a minExtent > 1
  private static List<IntervalsSource> splitDisjunctions(IntervalsSource source) {
    List<IntervalsSource> singletons = new ArrayList<>();
    List<IntervalsSource> nonSingletons = new ArrayList<>();
    for (IntervalsSource disj : source.pullUpDisjunctions()) {
      if (disj.minExtent() == 1) {
        singletons.add(disj);
      } else {
        nonSingletons.add(disj);
      }
    }
    List<IntervalsSource> split = new ArrayList<>();
    if (singletons.size() > 0) {
      split.add(Intervals.or(singletons.toArray(new IntervalsSource[0])));
    }
    split.addAll(nonSingletons);
    return split;
  }
}
