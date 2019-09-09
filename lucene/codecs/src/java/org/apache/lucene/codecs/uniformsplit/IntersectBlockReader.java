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
import java.util.Objects;

import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.Transition;

/**
 * The "intersect" {@link TermsEnum} response to {@link UniformSplitTerms#intersect(CompiledAutomaton, BytesRef)},
 * intersecting the terms with an automaton.
 */
public class IntersectBlockReader extends BlockReader {

  protected final AutomatonNextTermCalculator nextStringCalculator;
  protected final ByteRunAutomaton runAutomaton;
  protected final BytesRef commonSuffixRef; // maybe null
  protected final BytesRef commonPrefixRef;
  protected final BytesRef startTerm; // maybe null

  /** Set this when our current mode is seeking to this term.  Set to null after. */
  protected BytesRef seekTerm;

  protected int blockPrefixRunAutomatonState;
  protected int blockPrefixLen;

  /**
   * Number of bytes accepted by the last call to {@link #runAutomatonForState}.
   */
  protected int numBytesAccepted;
  /**
   * Whether the current term is beyond the automaton common prefix.
   * If true this means the enumeration should stop immediately.
   */
  protected boolean beyondCommonPrefix;

  public IntersectBlockReader(CompiledAutomaton compiled, BytesRef startTerm,
                              DictionaryBrowserSupplier dictionaryBrowserSupplier, IndexInput blockInput, PostingsReaderBase postingsReader,
                              FieldMetadata fieldMetadata, BlockDecoder blockDecoder) throws IOException {
    super(dictionaryBrowserSupplier, blockInput, postingsReader, fieldMetadata, blockDecoder);
    this.nextStringCalculator = new AutomatonNextTermCalculator(compiled);
    Automaton automaton = Objects.requireNonNull(compiled.automaton);
    this.runAutomaton = Objects.requireNonNull(compiled.runAutomaton);
    this.commonSuffixRef = compiled.commonSuffixRef; // maybe null
    this.commonPrefixRef = Operations.getCommonPrefixBytesRef(automaton); // never null

    this.startTerm = startTerm;
    assert startTerm == null || StringHelper.startsWith(startTerm, commonPrefixRef);
    // it is thus also true that startTerm >= commonPrefixRef

    this.seekTerm = startTerm != null ? startTerm : commonPrefixRef;
  }

  @Override
  public BytesRef next() throws IOException {
    clearTermState();

    if (blockHeader == null) { // initial state
      // note: don't call super.seekCeil here; we have our own logic

      // Set the browser position to the block having the seek term.
      // Even if -1, it's okay since we'll soon call nextKey().
      long blockStartFP = getOrCreateDictionaryBrowser().seekBlock(seekTerm);
      if (isBeyondLastTerm(seekTerm, blockStartFP)) {
        return null; // EOF
      }

      // Starting at this block find and load the next matching block.
      //   note: Since seekBlock was just called, we actually consider the current block as "next".
      if (nextBlockMatchingPrefix() == false) { // note: starts at seek'ed block, which may have a match
        return null; // EOF
      }
    }

    do {

      // look in the rest of this block.
      BytesRef term = nextTermInBlockMatching();
      if (term != null) {
        return term;
      }

      // next term dict matching prefix
    } while (nextBlockMatchingPrefix());

    return null; // EOF
  }

  /**
   * Find the next block that appears to contain terms that could match the automata.
   * The prefix is the primary clue.  Returns true if at one, or false for no more (EOF).
   */
  protected boolean nextBlockMatchingPrefix() throws IOException {
    if (beyondCommonPrefix) {
      return false; // EOF
    }

    IndexDictionary.Browser browser = getOrCreateDictionaryBrowser();

    do {

      // Get next block key (becomes in effect the current blockKey)
      BytesRef blockKey = browser.nextKey();
      if (blockKey == null) {
        return false; // EOF
      }

      blockPrefixLen = browser.getBlockPrefixLen();
      blockPrefixRunAutomatonState = runAutomatonForState(blockKey.bytes, blockKey.offset, blockPrefixLen, 0);

      // We may have passed commonPrefix  (a short-circuit optimization).
      if (isBeyondCommonPrefix(blockKey)) {
        return false; // EOF
      }

      if (blockPrefixRunAutomatonState >= 0) {
        break; // a match
      }

      //
      // This block doesn't match.
      //

      seekTerm = null; // we're moving on to another block, and seekTerm is before it.

      // Should we simply get the next key (linear mode) or try to seek?
      if (nextStringCalculator.isLinearState(blockKey)) {
        continue;
      }

      // Maybe the next block's key matches?  We have to check this before calling nextStringCalculator.
      BytesRef peekKey = browser.peekKey();
      if (peekKey == null) {
        return false; // EOF
      }
      if (runAutomatonForState(peekKey.bytes, peekKey.offset, peekKey.length, 0) >= 0) {
        continue; // yay; it matched.  Continue to actually advance to it.  This is rare?
      }

      // Seek to a block by calculating the next term to match the automata *following* peekKey.
      this.seekTerm = nextStringCalculator.nextSeekTerm(browser.peekKey());
      if (seekTerm == null) {
        return false; // EOF
      }
      browser.seekBlock(seekTerm);
      //continue

    } while (true); // while not a match

    // A match!

    //NOTE: we could determine if this automata has a prefix for this specific block (longer than the commonPrefix).
    //  If we see it, we could set it as the seekTerm and we could also exit the block early if we get past this prefix
    //  and runAutomatonFromPrefix would start from this prefix.  Smiley tried but benchmarks were not favorable to it.

    initializeHeader(null, browser.getBlockFilePointer());

    return true;
  }

  /**
   * Find the next block line that matches, or null when at end of block.
   */
  protected BytesRef nextTermInBlockMatching() throws IOException {
    do {
      // if seekTerm is set, then we seek into this block instead of starting with the first blindly.
      if (seekTerm != null) {
        assert blockLine == null;
        boolean moveBeyondIfFound = seekTerm == startTerm; // for startTerm, we want to get the following term
        SeekStatus seekStatus = seekInBlock(seekTerm);
        seekTerm = null;// reset.
        if (seekStatus == SeekStatus.END) {
          return null;
        } else if (seekStatus == SeekStatus.FOUND && moveBeyondIfFound) {
          if (readLineInBlock() == null) {
            return null;
          }
        }
        assert blockLine != null;
      } else {
        if (readLineInBlock() == null) {
          return null;
        }
      }

      TermBytes lineTermBytes = blockLine.getTermBytes();
      BytesRef lineTerm = lineTermBytes.getTerm();

      if (commonSuffixRef == null || StringHelper.endsWith(lineTerm, commonSuffixRef)) {
        if (runAutomatonFromPrefix(lineTerm)) {
          return lineTerm;
        } else if (beyondCommonPrefix) {
          return null;
        }
      }

    } while (true);
  }

  protected boolean runAutomatonFromPrefix(BytesRef term) {
    int state = runAutomatonForState(term.bytes, term.offset + blockPrefixLen, term.length - blockPrefixLen, blockPrefixRunAutomatonState);
    if (state >= 0 && runAutomaton.isAccept(state)) {
      return true;
    }
    if (isBeyondCommonPrefix(term)) {
      // If the automaton rejects early the term, before the common prefix length,
      // and if the term rejected byte is lexicographically after the same byte in the common prefix,
      // then it means the current term is beyond the common prefix.
      // Exit immediately the enumeration.
      beyondCommonPrefix = true;
    }
    return false;
  }

  /**
   * Run the automaton and return the final state (not necessary accepted). -1 signifies no state / no match.
   * Sets {@link #numBytesAccepted} with the offset of the first byte rejected by the automaton;
   * or (offset + length) if no byte is rejected.
   */
  protected int runAutomatonForState(byte[] s, int offset, int length, int initialState) {
    //see ByteRunAutomaton.run(); similar
    int state = initialState;
    int index = 0;
    while (index < length) {
      state = runAutomaton.step(state, s[index + offset] & 0xFF);
      if (state == -1) {
        break;
      }
      index++;
    }
    numBytesAccepted = index;
    return state;
  }

  /**
   * Determines if the provided {@link BytesRef} is beyond the automaton common prefix.
   * This method must be called after a call to {@link #runAutomatonForState} because
   * it uses {@link #numBytesAccepted} value.
   */
  protected boolean isBeyondCommonPrefix(BytesRef bytesRef) {
    // If the automaton rejects early the bytes, before the common prefix length,
    // and if the rejected byte is lexicographically after the same byte in the common prefix,
    // then it means the bytes are beyond the common prefix.
    return numBytesAccepted < commonPrefixRef.length
        && numBytesAccepted < bytesRef.length
        && (bytesRef.bytes[numBytesAccepted + bytesRef.offset] & 0xFF) > (commonPrefixRef.bytes[numBytesAccepted + commonPrefixRef.offset] & 0xFF);
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
  public SeekStatus seekCeil(BytesRef text) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seekExact(BytesRef term, TermState state) {
    throw new UnsupportedOperationException();
  }

  /**
   * This is a copy of AutomatonTermsEnum.  Since it's an inner class, the outer class can
   * call methods that ATE does not expose.  It'd be nice if ATE's logic could be more extensible.
   */
  protected static class AutomatonNextTermCalculator {
    // a tableized array-based form of the DFA
    protected final ByteRunAutomaton runAutomaton;
    // common suffix of the automaton
    protected final BytesRef commonSuffixRef;
    // true if the automaton accepts a finite language
    protected final boolean finite;
    // array of sorted transitions for each state, indexed by state number
    protected final Automaton automaton;
    // for path tracking: each long records gen when we last
    // visited the state; we use gens to avoid having to clear
    protected final long[] visited;
    protected long curGen;
    // the reference used for seeking forwards through the term dictionary
    protected final BytesRefBuilder seekBytesRef = new BytesRefBuilder();
    // true if we are enumerating an infinite portion of the DFA.
    // in this case it is faster to drive the query based on the terms dictionary.
    // when this is true, linearUpperBound indicate the end of range
    // of terms where we should simply do sequential reads instead.
    protected boolean linear = false;
    protected final BytesRef linearUpperBound = new BytesRef(10);
    protected Transition transition = new Transition();
    protected final IntsRefBuilder savedStates = new IntsRefBuilder();

    protected AutomatonNextTermCalculator(CompiledAutomaton compiled) {
      if (compiled.type != CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
        throw new IllegalArgumentException("please use CompiledAutomaton.getTermsEnum instead");
      }
      this.finite = compiled.finite;
      this.runAutomaton = compiled.runAutomaton;
      assert this.runAutomaton != null;
      this.commonSuffixRef = compiled.commonSuffixRef;
      this.automaton = compiled.automaton;

      // used for path tracking, where each bit is a numbered state.
      visited = new long[runAutomaton.getSize()];
    }

    /** True if the current state of the automata is best iterated linearly (without seeking). */
    protected boolean isLinearState(BytesRef term) {
      return linear && term.compareTo(linearUpperBound) < 0;
    }

    /** @see org.apache.lucene.index.FilteredTermsEnum#nextSeekTerm(BytesRef) */
    protected BytesRef nextSeekTerm(final BytesRef term) throws IOException {
      //System.out.println("ATE.nextSeekTerm term=" + term);
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
        return seekBytesRef.get();  // reposition
      } else {
        return null;          // no more possible strings can match
      }
    }

    /**
     * Sets the enum to operate in linear fashion, as we have found
     * a looping transition at position: we set an upper bound and
     * act like a TermRangeQuery for this portion of the term space.
     */
    protected void setLinear(int position) {
      assert linear == false;

      int state = 0;
      assert state == 0;
      int maxInterval = 0xff;
      //System.out.println("setLinear pos=" + position + " seekbytesRef=" + seekBytesRef);
      for (int i = 0; i < position; i++) {
        state = runAutomaton.step(state, seekBytesRef.byteAt(i) & 0xff);
        assert state >= 0: "state=" + state;
      }
      final int numTransitions = automaton.getNumTransitions(state);
      automaton.initTransition(state, transition);
      for (int i = 0; i < numTransitions; i++) {
        automaton.getNextTransition(transition);
        if (transition.min <= (seekBytesRef.byteAt(position) & 0xff) &&
            (seekBytesRef.byteAt(position) & 0xff) <= transition.max) {
          maxInterval = transition.max;
          break;
        }
      }
      // 0xff terms don't get the optimization... not worth the trouble.
      if (maxInterval != 0xff)
        maxInterval++;
      int length = position + 1; /* position + maxTransition */
      if (linearUpperBound.bytes.length < length)
        linearUpperBound.bytes = new byte[length];
      System.arraycopy(seekBytesRef.bytes(), 0, linearUpperBound.bytes, 0, position);
      linearUpperBound.bytes[position] = (byte) maxInterval;
      linearUpperBound.length = length;

      linear = true;
    }

    /**
     * Increments the byte buffer to the next String in binary order after s that will not put
     * the machine into a reject state. If such a string does not exist, returns
     * false.
     *
     * The correctness of this method depends upon the automaton being deterministic,
     * and having no transitions to dead states.
     *
     * @return true if more possible solutions exist for the DFA
     */
    protected boolean nextString() {
      int state;
      int pos = 0;
      savedStates.grow(seekBytesRef.length()+1);
      savedStates.setIntAt(0, 0);

      while (true) {
        curGen++;
        linear = false;
        // walk the automaton until a character is rejected.
        for (state = savedStates.intAt(pos); pos < seekBytesRef.length(); pos++) {
          visited[state] = curGen;
          int nextState = runAutomaton.step(state, seekBytesRef.byteAt(pos) & 0xff);
          if (nextState == -1)
            break;
          savedStates.setIntAt(pos+1, nextState);
          // we found a loop, record it for faster enumeration
          if (!finite && !linear && visited[nextState] == curGen) {
            setLinear(pos);
          }
          state = nextState;
        }

        // take the useful portion, and the last non-reject state, and attempt to
        // append characters that will match.
        if (nextString(state, pos)) {
          return true;
        } else { /* no more solutions exist from this useful portion, backtrack */
          if ((pos = backtrack(pos)) < 0) /* no more solutions at all */
            return false;
          final int newState = runAutomaton.step(savedStates.intAt(pos), seekBytesRef.byteAt(pos) & 0xff);
          if (newState >= 0 && runAutomaton.isAccept(newState))
            /* String is good to go as-is */
            return true;
          /* else advance further */
          // TODO: paranoia? if we backtrack thru an infinite DFA, the loop detection is important!
          // for now, restart from scratch for all infinite DFAs
          if (!finite) pos = 0;
        }
      }
    }

    /**
     * Returns the next String in lexicographic order that will not put
     * the machine into a reject state.
     *
     * This method traverses the DFA from the given position in the String,
     * starting at the given state.
     *
     * If this cannot satisfy the machine, returns false. This method will
     * walk the minimal path, in lexicographic order, as long as possible.
     *
     * If this method returns false, then there might still be more solutions,
     * it is necessary to backtrack to find out.
     *
     * @param state current non-reject state
     * @param position useful portion of the string
     * @return true if more possible solutions exist for the DFA from this
     *         position
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
        if (c++ == 0xff)
          return false;
      }

      seekBytesRef.setLength(position);
      visited[state] = curGen;

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
          while (visited[state] != curGen && !runAutomaton.isAccept(state)) {
            visited[state] = curGen;
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
            if (!finite && !linear && visited[state] == curGen) {
              setLinear(seekBytesRef.length()-1);
            }
          }
          return true;
        }
      }
      return false;
    }

    /**
     * Attempts to backtrack thru the string after encountering a dead end
     * at some given position. Returns false if no more possible strings
     * can match.
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
          seekBytesRef.setLength(position+1);
          return position;
        }
      }
      return -1; /* all solutions exhausted */
    }
  }

}

