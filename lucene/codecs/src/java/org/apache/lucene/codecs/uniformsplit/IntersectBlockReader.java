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

package org.apache.lucene.codecs.uniformsplit;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Transition;

/**
 * The "intersect" {@link TermsEnum} response to {@link
 * UniformSplitTerms#intersect(CompiledAutomaton, BytesRef)}, intersecting the terms with an
 * automaton.
 *
 * <p>By design of the UniformSplit block keys, it is less efficient than {@code
 * org.apache.lucene.backward_codecs.lucene40.blocktree.IntersectTermsEnum} for {@link
 * org.apache.lucene.search.FuzzyQuery} (-37%). It is slightly slower for {@link
 * org.apache.lucene.search.WildcardQuery} (-5%) and slightly faster for {@link
 * org.apache.lucene.search.PrefixQuery} (+5%).
 *
 * @lucene.experimental
 */
public class IntersectBlockReader extends BlockReader {

  /**
   * Block iteration order. Whether to move next block, jump to a block away, or end the iteration.
   */
  protected enum BlockIteration {
    NEXT,
    SEEK,
    END
  }

  /**
   * Threshold that controls when to attempt to jump to a block away.
   *
   * <p>This counter is 0 when entering a block. It is incremented each time a term is rejected by
   * the automaton. When the counter is greater than or equal to this threshold, then we compute the
   * next term accepted by the automaton, with {@link AutomatonNextTermCalculator}, and we jump to a
   * block away if the next term accepted is greater than the immediate next term in the block.
   *
   * <p>A low value, for example 1, improves the performance of automatons requiring many jumps, for
   * example {@link org.apache.lucene.search.FuzzyQuery} and most {@link
   * org.apache.lucene.search.WildcardQuery}. A higher value improves the performance of automatons
   * with less or no jump, for example {@link org.apache.lucene.search.PrefixQuery}. A threshold of
   * 4 seems to be a good balance.
   */
  protected final int NUM_CONSECUTIVELY_REJECTED_TERMS_THRESHOLD = 4;

  protected final Automaton automaton;
  protected final ByteRunAutomaton runAutomaton;
  protected final boolean finite;
  protected final BytesRef commonSuffix; // maybe null
  protected final int minTermLength;
  protected final AutomatonNextTermCalculator nextStringCalculator;

  /** Set this when our current mode is seeking to this term. Set to null after. */
  protected BytesRef seekTerm;
  /** Number of bytes accepted by the automaton when validating the current term. */
  protected int numMatchedBytes;
  /**
   * Automaton states reached when validating the current term, from 0 to {@link #numMatchedBytes} -
   * 1.
   */
  protected int[] states;
  /** Block iteration order determined when scanning the terms in the current block. */
  protected BlockIteration blockIteration;
  /**
   * Counter of the number of consecutively rejected terms. Depending on {@link
   * #NUM_CONSECUTIVELY_REJECTED_TERMS_THRESHOLD}, this may trigger a jump to a block away.
   */
  protected int numConsecutivelyRejectedTerms;

  protected IntersectBlockReader(
      CompiledAutomaton compiled,
      BytesRef startTerm,
      IndexDictionary.BrowserSupplier dictionaryBrowserSupplier,
      IndexInput blockInput,
      PostingsReaderBase postingsReader,
      FieldMetadata fieldMetadata,
      BlockDecoder blockDecoder)
      throws IOException {
    super(dictionaryBrowserSupplier, blockInput, postingsReader, fieldMetadata, blockDecoder);
    automaton = compiled.automaton;
    runAutomaton = compiled.runAutomaton;
    finite = compiled.finite;
    commonSuffix = compiled.commonSuffixRef;
    minTermLength = getMinTermLength();
    nextStringCalculator = new AutomatonNextTermCalculator(compiled);
    seekTerm = startTerm;
  }

  /**
   * Computes the minimal length of the terms accepted by the automaton. This speeds up the term
   * scanning for automatons accepting a finite language.
   */
  protected int getMinTermLength() {
    // Automatons accepting infinite language (e.g. PrefixQuery and WildcardQuery) do not benefit
    // much from
    // min term length while it takes time to compute it. More precisely, by skipping this
    // computation PrefixQuery
    // is significantly boosted while WildcardQuery might be slightly degraded on average. This min
    // term length
    // mainly boosts FuzzyQuery.
    int commonSuffixLength = commonSuffix == null ? 0 : commonSuffix.length;
    if (!finite) {
      return commonSuffixLength;
    }
    // Since we are only dealing with finite language, there is no loop to detect.
    int commonPrefixLength = 0;
    int state = 0;
    Transition t = null;
    while (true) {
      if (runAutomaton.isAccept(state)) {
        // The common prefix reaches a final state. So common prefix and common suffix overlap.
        // Min term length is the max between common prefix and common suffix lengths.
        return Math.max(commonPrefixLength, commonSuffixLength);
      }
      if (automaton.getNumTransitions(state) == 1) {
        if (t == null) {
          t = new Transition();
        }
        automaton.getTransition(state, 0, t);
        if (t.min == t.max) {
          state = t.dest;
          commonPrefixLength++;
          continue;
        }
      }
      break;
    }
    // Min term length is the sum of common prefix and common suffix lengths.
    return commonPrefixLength + commonSuffixLength;
  }

  @Override
  public BytesRef next() throws IOException {
    if (blockHeader == null) {
      if (!seekFirstBlock()) {
        return null;
      }
      states = new int[32];
      blockIteration = BlockIteration.NEXT;
    }
    termState = null;
    do {
      BytesRef term = nextTermInBlockMatching();
      if (term != null) {
        return term;
      }
    } while (nextBlock());
    return null;
  }

  protected boolean seekFirstBlock() throws IOException {
    seekTerm = nextStringCalculator.nextSeekTerm(seekTerm);
    if (seekTerm == null) {
      return false;
    }
    long blockStartFP = getOrCreateDictionaryBrowser().seekBlock(seekTerm);
    if (blockStartFP == -1) {
      blockStartFP = fieldMetadata.getFirstBlockStartFP();
    } else if (isBeyondLastTerm(seekTerm, blockStartFP)) {
      return false;
    }
    initializeHeader(seekTerm, blockStartFP);
    return blockHeader != null;
  }

  /**
   * Finds the next block line that matches (accepted by the automaton), or null when at end of
   * block.
   *
   * @return The next term in the current block that is accepted by the automaton; or null if none.
   */
  protected BytesRef nextTermInBlockMatching() throws IOException {
    if (seekTerm == null) {
      if (readLineInBlock() == null) {
        return null;
      }
    } else {
      SeekStatus seekStatus = seekInBlock(seekTerm);
      seekTerm = null;
      if (seekStatus == SeekStatus.END) {
        return null;
      }
      assert numMatchedBytes == 0;
      assert numConsecutivelyRejectedTerms == 0;
    }
    while (true) {
      TermBytes lineTermBytes = blockLine.getTermBytes();
      BytesRef lineTerm = lineTermBytes.getTerm();
      assert lineTerm.offset == 0;
      if (states.length <= lineTerm.length) {
        states = ArrayUtil.growExact(states, ArrayUtil.oversize(lineTerm.length + 1, Byte.BYTES));
      }
      // Since terms are delta encoded, we may start the automaton steps from the last state reached
      // by the previous term.
      int index = Math.min(lineTermBytes.getSuffixOffset(), numMatchedBytes);
      // Skip this term early if it is shorter than the min term length, or if it does not end with
      // the common suffix
      // accepted by the automaton.
      if (lineTerm.length >= minTermLength
          && (commonSuffix == null || endsWithCommonSuffix(lineTerm.bytes, lineTerm.length))) {
        int state = states[index];
        while (true) {
          if (index == lineTerm.length) {
            if (runAutomaton.isAccept(state)) {
              // The automaton accepts the current term. Record the number of matched bytes and
              // return the term.
              assert runAutomaton.run(lineTerm.bytes, 0, lineTerm.length);
              numMatchedBytes = index;
              if (numConsecutivelyRejectedTerms > 0) {
                numConsecutivelyRejectedTerms = 0;
              }
              assert blockIteration == BlockIteration.NEXT;
              return lineTerm;
            }
            break;
          }
          state = runAutomaton.step(state, lineTerm.bytes[index] & 0xff);
          if (state == -1) {
            // The automaton rejects the current term.
            break;
          }
          // Record the reached automaton state.
          states[++index] = state;
        }
      }
      // The current term is not accepted by the automaton.
      // Still record the reached automaton state to start the next term steps from there.
      assert !runAutomaton.run(lineTerm.bytes, 0, lineTerm.length);
      numMatchedBytes = index;
      // If the number of consecutively rejected terms reaches the threshold,
      // then determine whether it is worthwhile to jump to a block away.
      if (++numConsecutivelyRejectedTerms >= NUM_CONSECUTIVELY_REJECTED_TERMS_THRESHOLD
          && lineIndexInBlock < blockHeader.getLinesCount() - 1
          && !nextStringCalculator.isLinearState(lineTerm)) {
        // Compute the next term accepted by the automaton after the current term.
        if ((seekTerm = nextStringCalculator.nextSeekTerm(lineTerm)) == null) {
          blockIteration = BlockIteration.END;
          return null;
        }
        // It is worthwhile to jump to a block away if the next term accepted is after the next term
        // in the block.
        // Actually the block away may be the current block, but this is a good heuristic.
        readLineInBlock();
        if (seekTerm.compareTo(blockLine.getTermBytes().getTerm()) > 0) {
          // Stop scanning this block terms and set the iteration order to jump to a block away by
          // seeking seekTerm.
          blockIteration = BlockIteration.SEEK;
          return null;
        }
        seekTerm = null;
        // If it is not worthwhile to jump to a block away, do not attempt anymore for the current
        // block.
        numConsecutivelyRejectedTerms = Integer.MIN_VALUE;
      } else if (readLineInBlock() == null) {
        // No more terms in the block. The iteration order is to open the very next block.
        assert blockIteration == BlockIteration.NEXT;
        return null;
      }
    }
  }

  /**
   * Indicates whether the given term ends with the automaton common suffix. This allows to quickly
   * skip terms that the automaton would reject eventually.
   */
  protected boolean endsWithCommonSuffix(byte[] termBytes, int termLength) {
    byte[] suffixBytes = commonSuffix.bytes;
    int suffixLength = commonSuffix.length;
    int offset = termLength - suffixLength;
    assert offset >= 0; // We already checked minTermLength.
    for (int i = 0; i < suffixLength; i++) {
      if (termBytes[offset + i] != suffixBytes[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Opens the next block. Depending on the {@link #blockIteration} order, it may be the very next
   * block, or a block away that may contain {@link #seekTerm}.
   *
   * @return true if the next block is opened; false if there is no blocks anymore and the iteration
   *     is over.
   */
  protected boolean nextBlock() throws IOException {
    long blockStartFP;
    switch (blockIteration) {
      case NEXT:
        assert seekTerm == null;
        blockStartFP = blockInput.getFilePointer();
        break;
      case SEEK:
        assert seekTerm != null;
        blockStartFP = getOrCreateDictionaryBrowser().seekBlock(seekTerm);
        if (isBeyondLastTerm(seekTerm, blockStartFP)) {
          return false;
        }
        blockIteration = BlockIteration.NEXT;
        break;
      case END:
        return false;
      default:
        throw new UnsupportedOperationException(
            "Unsupported " + BlockIteration.class.getSimpleName());
    }
    numMatchedBytes = 0;
    numConsecutivelyRejectedTerms = 0;
    initializeHeader(seekTerm, blockStartFP);
    return blockHeader != null;
  }

  @Override
  public boolean seekExact(BytesRef text) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seekExact(long ord) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seekExact(BytesRef term, TermState state) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SeekStatus seekCeil(BytesRef text) {
    throw new UnsupportedOperationException();
  }

  /**
   * This is mostly a copy of AutomatonTermsEnum. Since it's an inner class, the outer class can
   * call methods that ATE does not expose. It'd be nice if ATE's logic could be more extensible.
   */
  protected class AutomatonNextTermCalculator {
    // for path tracking: each short records gen when we last
    // visited the state; we use gens to avoid having to clear
    protected final short[] visited;
    protected short curGen;
    // the reference used for seeking forwards through the term dictionary
    protected final BytesRefBuilder seekBytesRef = new BytesRefBuilder();
    // true if we are enumerating an infinite portion of the DFA.
    // in this case it is faster to drive the query based on the terms dictionary.
    // when this is true, linearUpperBound indicate the end of range
    // of terms where we should simply do sequential reads instead.
    protected boolean linear;
    protected final BytesRef linearUpperBound = new BytesRef();
    protected final Transition transition = new Transition();
    protected final IntsRefBuilder savedStates = new IntsRefBuilder();

    protected AutomatonNextTermCalculator(CompiledAutomaton compiled) {
      visited = compiled.finite ? null : new short[runAutomaton.getSize()];
    }

    /** Records the given state has been visited. */
    protected void setVisited(int state) {
      if (!finite) {
        visited[state] = curGen;
      }
    }

    /** Indicates whether the given state has been visited. */
    protected boolean isVisited(int state) {
      return !finite && visited[state] == curGen;
    }

    /** True if the current state of the automata is best iterated linearly (without seeking). */
    protected boolean isLinearState(BytesRef term) {
      return linear && term.compareTo(linearUpperBound) < 0;
    }

    /** @see org.apache.lucene.index.FilteredTermsEnum#nextSeekTerm(BytesRef) */
    protected BytesRef nextSeekTerm(final BytesRef term) {
      // System.out.println("ATE.nextSeekTerm term=" + term);
      if (term == null) {
        assert seekBytesRef.length() == 0;
        // return the empty term, as it's valid
        if (runAutomaton.isAccept(0)) {
          return seekBytesRef.get();
        }
      } else {
        seekBytesRef.copyBytes(term);
      }

      // seek to the next possible string;
      if (nextString()) {
        return seekBytesRef.get(); // reposition
      } else {
        return null; // no more possible strings can match
      }
    }

    /**
     * Sets the enum to operate in linear fashion, as we have found a looping transition at
     * position: we set an upper bound and act like a TermRangeQuery for this portion of the term
     * space.
     */
    protected void setLinear(int position) {
      assert linear == false;

      int state = 0;
      int maxInterval = 0xff;
      // System.out.println("setLinear pos=" + position + " seekbytesRef=" + seekBytesRef);
      for (int i = 0; i < position; i++) {
        state = runAutomaton.step(state, seekBytesRef.byteAt(i) & 0xff);
        assert state >= 0 : "state=" + state;
      }
      final int numTransitions = automaton.getNumTransitions(state);
      automaton.initTransition(state, transition);
      for (int i = 0; i < numTransitions; i++) {
        automaton.getNextTransition(transition);
        if (transition.min <= (seekBytesRef.byteAt(position) & 0xff)
            && (seekBytesRef.byteAt(position) & 0xff) <= transition.max) {
          maxInterval = transition.max;
          break;
        }
      }
      // 0xff terms don't get the optimization... not worth the trouble.
      if (maxInterval != 0xff) maxInterval++;
      int length = position + 1; /* position + maxTransition */
      if (linearUpperBound.bytes.length < length) {
        linearUpperBound.bytes = new byte[ArrayUtil.oversize(length, Byte.BYTES)];
      }
      System.arraycopy(seekBytesRef.bytes(), 0, linearUpperBound.bytes, 0, position);
      linearUpperBound.bytes[position] = (byte) maxInterval;
      linearUpperBound.length = length;

      linear = true;
    }

    /**
     * Increments the byte buffer to the next String in binary order after s that will not put the
     * machine into a reject state. If such a string does not exist, returns false.
     *
     * <p>The correctness of this method depends upon the automaton being deterministic, and having
     * no transitions to dead states.
     *
     * @return true if more possible solutions exist for the DFA
     */
    protected boolean nextString() {
      int state;
      int pos = 0;
      savedStates.grow(seekBytesRef.length() + 1);
      savedStates.setIntAt(0, 0);

      while (true) {
        if (!finite && ++curGen == 0) {
          // Clear the visited states every time curGen wraps (so very infrequently to not impact
          // average perf).
          Arrays.fill(visited, (short) -1);
        }
        linear = false;
        // walk the automaton until a character is rejected.
        for (state = savedStates.intAt(pos); pos < seekBytesRef.length(); pos++) {
          setVisited(state);
          int nextState = runAutomaton.step(state, seekBytesRef.byteAt(pos) & 0xff);
          if (nextState == -1) break;
          savedStates.setIntAt(pos + 1, nextState);
          // we found a loop, record it for faster enumeration
          if (!linear && isVisited(nextState)) {
            setLinear(pos);
          }
          state = nextState;
        }

        // take the useful portion, and the last non-reject state, and attempt to
        // append characters that will match.
        if (nextString(state, pos)) {
          return true;
        } else {
          /* no more solutions exist from this useful portion, backtrack */
          if ((pos = backtrack(pos)) < 0) /* no more solutions at all */ return false;
          final int newState =
              runAutomaton.step(savedStates.intAt(pos), seekBytesRef.byteAt(pos) & 0xff);
          if (newState >= 0 && runAutomaton.isAccept(newState))
            /* String is good to go as-is */
            return true;
          /* else advance further */
          // paranoia? if we backtrack thru an infinite DFA, the loop detection is important!
          // for now, restart from scratch for all infinite DFAs
          if (!finite) pos = 0;
        }
      }
    }

    /**
     * Returns the next String in lexicographic order that will not put the machine into a reject
     * state.
     *
     * <p>This method traverses the DFA from the given position in the String, starting at the given
     * state.
     *
     * <p>If this cannot satisfy the machine, returns false. This method will walk the minimal path,
     * in lexicographic order, as long as possible.
     *
     * <p>If this method returns false, then there might still be more solutions, it is necessary to
     * backtrack to find out.
     *
     * @param state current non-reject state
     * @param position useful portion of the string
     * @return true if more possible solutions exist for the DFA from this position
     */
    protected boolean nextString(int state, int position) {
      /*
       * the next lexicographic character must be greater than the existing
       * character, if it exists.
       */
      int c = 0;
      if (position < seekBytesRef.length()) {
        c = seekBytesRef.byteAt(position) & 0xff;
        // if the next byte is 0xff and is not part of the useful portion,
        // then by definition it puts us in a reject state, and therefore this
        // path is dead. there cannot be any higher transitions. backtrack.
        if (c++ == 0xff) return false;
      }

      seekBytesRef.setLength(position);
      setVisited(state);

      final int numTransitions = automaton.getNumTransitions(state);
      automaton.initTransition(state, transition);
      // find the minimal path (lexicographic order) that is >= c

      for (int i = 0; i < numTransitions; i++) {
        automaton.getNextTransition(transition);
        if (transition.max >= c) {
          int nextChar = Math.max(c, transition.min);
          // append either the next sequential char, or the minimum transition
          seekBytesRef.grow(seekBytesRef.length() + 1);
          seekBytesRef.append((byte) nextChar);
          state = transition.dest;
          /*
           * as long as is possible, continue down the minimal path in
           * lexicographic order. if a loop or accept state is encountered, stop.
           */
          while (!isVisited(state) && !runAutomaton.isAccept(state)) {
            setVisited(state);
            /*
             * Note: we work with a DFA with no transitions to dead states.
             * so the below is ok, if it is not an accept state,
             * then there MUST be at least one transition.
             */
            automaton.initTransition(state, transition);
            automaton.getNextTransition(transition);
            state = transition.dest;

            // append the minimum transition
            seekBytesRef.grow(seekBytesRef.length() + 1);
            seekBytesRef.append((byte) transition.min);

            // we found a loop, record it for faster enumeration
            if (!linear && isVisited(state)) {
              setLinear(seekBytesRef.length() - 1);
            }
          }
          return true;
        }
      }
      return false;
    }

    /**
     * Attempts to backtrack thru the string after encountering a dead end at some given position.
     * Returns false if no more possible strings can match.
     *
     * @param position current position in the input String
     * @return {@code position >= 0} if more possible solutions exist for the DFA
     */
    protected int backtrack(int position) {
      while (position-- > 0) {
        int nextChar = seekBytesRef.byteAt(position) & 0xff;
        // if a character is 0xff it's a dead-end too,
        // because there is no higher character in binary sort order.
        if (nextChar++ != 0xff) {
          seekBytesRef.setByteAt(position, (byte) nextChar);
          seekBytesRef.setLength(position + 1);
          return position;
        }
      }
      return -1; /* all solutions exhausted */
    }
  }
}
