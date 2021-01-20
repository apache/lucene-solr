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
package org.apache.lucene.search.suggest.fst;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import org.apache.lucene.search.suggest.InMemorySorter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.*;

/**
 * Finite state automata based implementation of "autocomplete" functionality.
 *
 * <h2>Implementation details</h2>
 *
 * <p>The construction step in {@link #finalize()} works as follows:
 *
 * <ul>
 *   <li>A set of input terms and their buckets is given.
 *   <li>All terms in the input are prefixed with a synthetic pseudo-character (code) of the weight
 *       bucket the term fell into. For example a term <code>abc</code> with a discretized weight
 *       equal '1' would become <code>1abc</code>.
 *   <li>The terms are then sorted by their raw value of UTF-8 character values (including the
 *       synthetic bucket code in front).
 *   <li>A finite state automaton ({@link FST}) is constructed from the input. The root node has
 *       arcs labeled with all possible weights. We cache all these arcs, highest-weight first.
 * </ul>
 *
 * <p>At runtime, in {@link FSTCompletion#lookup(CharSequence, int)}, the automaton is utilized as
 * follows:
 *
 * <ul>
 *   <li>For each possible term weight encoded in the automaton (cached arcs from the root above),
 *       starting with the highest one, we descend along the path of the input key. If the key is
 *       not a prefix of a sequence in the automaton (path ends prematurely), we exit immediately --
 *       no completions.
 *   <li>Otherwise, we have found an internal automaton node that ends the key. <b>The entire
 *       subautomaton (all paths) starting from this node form the key's completions.</b> We start
 *       the traversal of this subautomaton. Every time we reach a final state (arc), we add a
 *       single suggestion to the list of results (the weight of this suggestion is constant and
 *       equal to the root path we started from). The tricky part is that because automaton edges
 *       are sorted and we scan depth-first, we can terminate the entire procedure as soon as we
 *       collect enough suggestions the user requested.
 *   <li>In case the number of suggestions collected in the step above is still insufficient, we
 *       proceed to the next (smaller) weight leaving the root node and repeat the same algorithm
 *       again.
 * </ul>
 *
 * <h2>Runtime behavior and performance characteristic</h2>
 *
 * The algorithm described above is optimized for finding suggestions to short prefixes in a
 * top-weights-first order. This is probably the most common use case: it allows presenting
 * suggestions early and sorts them by the global frequency (and then alphabetically).
 *
 * <p>If there is an exact match in the automaton, it is returned first on the results list (even
 * with by-weight sorting).
 *
 * <p>Note that the maximum lookup time for <b>any prefix</b> is the time of descending to the
 * subtree, plus traversal of the subtree up to the number of requested suggestions (because they
 * are already presorted by weight on the root level and alphabetically at any node level).
 *
 * <p>To order alphabetically only (no ordering by priorities), use identical term weights for all
 * terms. Alphabetical suggestions are returned even if non-constant weights are used, but the
 * algorithm for doing this is suboptimal.
 *
 * <p>"alphabetically" in any of the documentation above indicates UTF-8 representation order,
 * nothing else.
 *
 * <p><b>NOTE</b>: the FST file format is experimental and subject to suddenly change, requiring you
 * to rebuild the FST suggest index.
 *
 * @see FSTCompletion
 * @lucene.experimental
 */
public class FSTCompletionBuilder {
  /** Default number of buckets. */
  public static final int DEFAULT_BUCKETS = 10;

  /**
   * The number of separate buckets for weights (discretization). The more buckets, the more
   * fine-grained term weights (priorities) can be assigned. The speed of lookup will not decrease
   * for prefixes which have highly-weighted completions (because these are filled-in first), but
   * will decrease significantly for low-weighted terms (but these should be infrequent, so it is
   * all right).
   *
   * <p>The number of buckets must be within [1, 255] range.
   */
  private final int buckets;

  /** Finite state automaton encoding all the lookup terms. See class notes for details. */
  FST<Object> automaton;

  /**
   * FST construction require re-sorting the input. This is the class that collects all the input
   * entries, their weights and then provides sorted order.
   */
  private final BytesRefSorter sorter;

  /** Scratch buffer for {@link #add(BytesRef, int)}. */
  private final BytesRefBuilder scratch = new BytesRefBuilder();

  /** Max tail sharing length. */
  private final int shareMaxTailLength;

  /**
   * Creates an {@link FSTCompletion} with default options: 10 buckets, exact match promoted to
   * first position and {@link InMemorySorter} with a comparator obtained from {@link
   * Comparator#naturalOrder()}.
   */
  public FSTCompletionBuilder() {
    this(DEFAULT_BUCKETS, new InMemorySorter(Comparator.naturalOrder()), Integer.MAX_VALUE);
  }

  /**
   * Creates an FSTCompletion with the specified options.
   *
   * @param buckets The number of buckets for weight discretization. Buckets are used in {@link
   *     #add(BytesRef, int)} and must be smaller than the number given here.
   * @param sorter {@link BytesRefSorter} used for re-sorting input for the automaton. For large
   *     inputs, use on-disk sorting implementations. The sorter is closed automatically in {@link
   *     #build()} if it implements {@link Closeable}.
   * @param shareMaxTailLength Max shared suffix sharing length.
   *     <p>See the description of this parameter in {@link
   *     org.apache.lucene.util.fst.FSTCompiler.Builder}. In general, for very large inputs you'll
   *     want to construct a non-minimal automaton which will be larger, but the construction will
   *     take far less ram. For minimal automata, set it to {@link Integer#MAX_VALUE}.
   */
  public FSTCompletionBuilder(int buckets, BytesRefSorter sorter, int shareMaxTailLength) {
    if (buckets < 1 || buckets > 255) {
      throw new IllegalArgumentException("Buckets must be >= 1 and <= 255: " + buckets);
    }

    if (sorter == null) {
      throw new IllegalArgumentException("BytesRefSorter must not be null.");
    }

    this.sorter = sorter;
    this.buckets = buckets;
    this.shareMaxTailLength = shareMaxTailLength;
  }

  /**
   * Appends a single suggestion and its weight to the internal buffers.
   *
   * @param utf8 The suggestion (utf8 representation) to be added. The content is copied and the
   *     object can be reused.
   * @param bucket The bucket to place this suggestion in. Must be non-negative and smaller than the
   *     number of buckets passed in the constructor. Higher numbers indicate suggestions that
   *     should be presented before suggestions placed in smaller buckets.
   */
  public void add(BytesRef utf8, int bucket) throws IOException {
    if (bucket < 0 || bucket >= buckets) {
      throw new IllegalArgumentException(
          "Bucket outside of the allowed range [0, " + buckets + "): " + bucket);
    }

    scratch.grow(utf8.length + 10);
    scratch.clear();
    scratch.append((byte) bucket);
    scratch.append(utf8);
    sorter.add(scratch.get());
  }

  /**
   * Builds the final automaton from a list of added entries. This method may take a longer while as
   * it needs to build the automaton.
   */
  public FSTCompletion build() throws IOException {
    this.automaton = buildAutomaton(sorter);

    if (sorter instanceof Closeable) {
      ((Closeable) sorter).close();
    }

    return new FSTCompletion(automaton);
  }

  /** Builds the final automaton from a list of entries. */
  private FST<Object> buildAutomaton(BytesRefSorter sorter) throws IOException {
    // Build the automaton.
    final Outputs<Object> outputs = NoOutputs.getSingleton();
    final Object empty = outputs.getNoOutput();
    final FSTCompiler<Object> fstCompiler =
        new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, outputs)
            .shareMaxTailLength(shareMaxTailLength)
            .build();

    BytesRefBuilder scratch = new BytesRefBuilder();
    BytesRef entry;
    final IntsRefBuilder scratchIntsRef = new IntsRefBuilder();
    int count = 0;
    BytesRefIterator iter = sorter.iterator();
    while ((entry = iter.next()) != null) {
      count++;
      if (scratch.get().compareTo(entry) != 0) {
        fstCompiler.add(Util.toIntsRef(entry, scratchIntsRef), empty);
        scratch.copyBytes(entry);
      }
    }

    return count == 0 ? null : fstCompiler.compile();
  }
}
