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
import org.apache.lucene.index.AutomatonTermsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;

/**
 * The "intersect" {@link TermsEnum} response to {@link UniformSplitTerms#intersect(CompiledAutomaton, BytesRef)},
 * intersecting the terms with an automaton.
 */
public class IntersectBlockReader extends BlockReader {

  protected final AutomatonNextTermCalculator nextStringCalculator; // we reuse some non-trivial logic there
  protected final ByteRunAutomaton runAutomaton;
  protected final BytesRef commonSuffixRef; // maybe null
  protected final BytesRef commonPrefixRef;
  protected final BytesRef startTerm; // maybe null

  protected BytesRef seekTerm; // set this when our current mode is seeking to this term.  Set to null after.

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

  //TODO this approach is a hack to reuse complex code in AutomatonTermsEnum.  How to make ATE's logic more reusable?
  protected static class AutomatonNextTermCalculator extends AutomatonTermsEnum {

    protected AutomatonNextTermCalculator(CompiledAutomaton compiled) {
      super(TermsEnum.EMPTY, compiled);
    }

    // expose this method that is protected in ATE
    @Override
    protected BytesRef nextSeekTerm(BytesRef term) throws IOException {
      return super.nextSeekTerm(term);
    }

    // expose this method that is protected in ATE
    @Override
    protected boolean isLinearState(BytesRef term) {
      return super.isLinearState(term);
    }
  }

}

