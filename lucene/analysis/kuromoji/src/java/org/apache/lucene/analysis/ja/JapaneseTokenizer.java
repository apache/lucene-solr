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
package org.apache.lucene.analysis.ja;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ja.dict.CharacterDefinition;
import org.apache.lucene.analysis.ja.dict.ConnectionCosts;
import org.apache.lucene.analysis.ja.dict.Dictionary;
import org.apache.lucene.analysis.ja.dict.TokenInfoDictionary;
import org.apache.lucene.analysis.ja.dict.TokenInfoFST;
import org.apache.lucene.analysis.ja.dict.UnknownDictionary;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.apache.lucene.analysis.ja.tokenattributes.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.util.RollingCharBuffer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.FST;

// TODO: somehow factor out a reusable viterbi search here,
// so other decompounders/tokenizers can reuse...

/**
 * Tokenizer for Japanese that uses morphological analysis.
 * <p>
 * This tokenizer sets a number of additional attributes:
 * <ul>
 *   <li>{@link BaseFormAttribute} containing base form for inflected
 *       adjectives and verbs.
 *   <li>{@link PartOfSpeechAttribute} containing part-of-speech.
 *   <li>{@link ReadingAttribute} containing reading and pronunciation.
 *   <li>{@link InflectionAttribute} containing additional part-of-speech
 *       information for inflected forms.
 * </ul>
 * <p>
 * This tokenizer uses a rolling Viterbi search to find the 
 * least cost segmentation (path) of the incoming characters.  
 * For tokens that appear to be compound (&gt; length 2 for all
 * Kanji, or &gt; length 7 for non-Kanji), we see if there is a
 * 2nd best segmentation of that token after applying
 * penalties to the long tokens.  If so, and the Mode is
 * {@link Mode#SEARCH}, we output the alternate segmentation 
 * as well.
 */
public final class JapaneseTokenizer extends Tokenizer {

  /**
   * Tokenization mode: this determines how the tokenizer handles
   * compound and unknown words.
   */
  public static enum Mode {
    /**
     * Ordinary segmentation: no decomposition for compounds,
     */
    NORMAL, 

    /**
     * Segmentation geared towards search: this includes a 
     * decompounding process for long nouns, also including
     * the full compound token as a synonym.
     */
    SEARCH, 

    /**
     * Extended mode outputs unigrams for unknown words.
     * @lucene.experimental
     */
    EXTENDED
  }

  /**
   * Default tokenization mode. Currently this is {@link Mode#SEARCH}.
   */
  public static final Mode DEFAULT_MODE = Mode.SEARCH;

  /**
   * Token type reflecting the original source of this token
   */
  public enum Type {
    /**
     * Known words from the system dictionary.
     */
    KNOWN,
    /**
     * Unknown words (heuristically segmented).
     */
    UNKNOWN,
    /**
     * Known words from the user dictionary.
     */
    USER
  }

  private static final boolean VERBOSE = false;

  private static final int SEARCH_MODE_KANJI_LENGTH = 2;

  private static final int SEARCH_MODE_OTHER_LENGTH = 7; // Must be >= SEARCH_MODE_KANJI_LENGTH

  private static final int SEARCH_MODE_KANJI_PENALTY = 3000;

  private static final int SEARCH_MODE_OTHER_PENALTY = 1700;

  // For safety:
  private static final int MAX_UNKNOWN_WORD_LENGTH = 1024;
  private static final int MAX_BACKTRACE_GAP = 1024;

  private final EnumMap<Type, Dictionary> dictionaryMap = new EnumMap<>(Type.class);

  private final TokenInfoFST fst;
  private final TokenInfoDictionary dictionary;
  private final UnknownDictionary unkDictionary;
  private final ConnectionCosts costs;
  private final UserDictionary userDictionary;
  private final CharacterDefinition characterDefinition;

  private final FST.Arc<Long> arc = new FST.Arc<>();
  private final FST.BytesReader fstReader;
  private final IntsRef wordIdRef = new IntsRef();

  private final FST.BytesReader userFSTReader;
  private final TokenInfoFST userFST;

  private final RollingCharBuffer buffer = new RollingCharBuffer();

  private final WrappedPositionArray positions = new WrappedPositionArray();

  private final boolean discardPunctuation;
  private final boolean searchMode;
  private final boolean extendedMode;
  private final boolean outputCompounds;

  // Index of the last character of unknown word:
  private int unknownWordEndIndex = -1;

  // True once we've hit the EOF from the input reader:
  private boolean end;

  // Last absolute position we backtraced from:
  private int lastBackTracePos;

  // Position of last token we returned; we use this to
  // figure out whether to set posIncr to 0 or 1:
  private int lastTokenPos;

  // Next absolute position to process:
  private int pos;

  // Already parsed, but not yet passed to caller, tokens:
  private final List<Token> pending = new ArrayList<>();

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLengthAtt = addAttribute(PositionLengthAttribute.class);
  private final BaseFormAttribute basicFormAtt = addAttribute(BaseFormAttribute.class);
  private final PartOfSpeechAttribute posAtt = addAttribute(PartOfSpeechAttribute.class);
  private final ReadingAttribute readingAtt = addAttribute(ReadingAttribute.class);
  private final InflectionAttribute inflectionAtt = addAttribute(InflectionAttribute.class);

  /**
   * Create a new JapaneseTokenizer.
   * <p>
   * Uses the default AttributeFactory.
   * 
   * @param userDictionary Optional: if non-null, user dictionary.
   * @param discardPunctuation true if punctuation tokens should be dropped from the output.
   * @param mode tokenization mode.
   */
  public JapaneseTokenizer(UserDictionary userDictionary, boolean discardPunctuation, Mode mode) {
    this(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, userDictionary, discardPunctuation, mode);
  }

  /**
   * Create a new JapaneseTokenizer.
   *
   * @param factory the AttributeFactory to use
   * @param userDictionary Optional: if non-null, user dictionary.
   * @param discardPunctuation true if punctuation tokens should be dropped from the output.
   * @param mode tokenization mode.
   */
  public JapaneseTokenizer
      (AttributeFactory factory, UserDictionary userDictionary, boolean discardPunctuation, Mode mode) {
    super(factory);
    dictionary = TokenInfoDictionary.getInstance();
    fst = dictionary.getFST();
    unkDictionary = UnknownDictionary.getInstance();
    characterDefinition = unkDictionary.getCharacterDefinition();
    this.userDictionary = userDictionary;
    costs = ConnectionCosts.getInstance();
    fstReader = fst.getBytesReader();
    if (userDictionary != null) {
      userFST = userDictionary.getFST();
      userFSTReader = userFST.getBytesReader();
    } else {
      userFST = null;
      userFSTReader = null;
    }
    this.discardPunctuation = discardPunctuation;
    switch(mode){
      case SEARCH:
        searchMode = true;
        extendedMode = false;
        outputCompounds = true;
        break;
      case EXTENDED:
        searchMode = true;
        extendedMode = true;
        outputCompounds = false;
        break;
      default:
        searchMode = false;
        extendedMode = false;
        outputCompounds = false;
        break;
    }
    buffer.reset(this.input);

    resetState();

    dictionaryMap.put(Type.KNOWN, dictionary);
    dictionaryMap.put(Type.UNKNOWN, unkDictionary);
    dictionaryMap.put(Type.USER, userDictionary);
  }

  private GraphvizFormatter dotOut;

  /** Expert: set this to produce graphviz (dot) output of
   *  the Viterbi lattice */
  public void setGraphvizFormatter(GraphvizFormatter dotOut) {
    this.dotOut = dotOut;
  }

  @Override
  public void close() throws IOException {
    super.close();
    buffer.reset(input);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    buffer.reset(input);
    resetState();
  }

  private void resetState() {
    positions.reset();
    unknownWordEndIndex = -1;
    pos = 0;
    end = false;
    lastBackTracePos = 0;
    lastTokenPos = -1;
    pending.clear();

    // Add BOS:
    positions.get(0).add(0, 0, -1, -1, -1, Type.KNOWN);
  }

  @Override
  public void end() throws IOException {
    super.end();
    // Set final offset
    int finalOffset = correctOffset(pos);
    offsetAtt.setOffset(finalOffset, finalOffset);
  }

  // Returns the added cost that a 2nd best segmentation is
  // allowed to have.  Ie, if we see path with cost X,
  // ending in a compound word, and this method returns
  // threshold > 0, then we will also find the 2nd best
  // segmentation and if its path score is within this
  // threshold of X, we'll include it in the output:
  private int computeSecondBestThreshold(int pos, int length) throws IOException {
    // TODO: maybe we do something else here, instead of just
    // using the penalty...?  EG we can be more aggressive on
    // when to also test for 2nd best path
    return computePenalty(pos, length);
  }

  private int computePenalty(int pos, int length) throws IOException {
    if (length > SEARCH_MODE_KANJI_LENGTH) {
      boolean allKanji = true;
      // check if node consists of only kanji
      final int endPos = pos + length;
      for (int pos2 = pos; pos2 < endPos; pos2++) {
        if (!characterDefinition.isKanji((char) buffer.get(pos2))) {
          allKanji = false;
          break;
        }
      }
      if (allKanji) {  // Process only Kanji keywords
        return (length - SEARCH_MODE_KANJI_LENGTH) * SEARCH_MODE_KANJI_PENALTY;
      } else if (length > SEARCH_MODE_OTHER_LENGTH) {
        return (length - SEARCH_MODE_OTHER_LENGTH) * SEARCH_MODE_OTHER_PENALTY;
      }
    }
    return 0;
  }

  // Holds all back pointers arriving to this position:
  final static class Position {

    int pos;

    int count;

    // maybe single int array * 5?
    int[] costs = new int[8];
    int[] lastRightID = new int[8];
    int[] backPos = new int[8];
    int[] backIndex = new int[8];
    int[] backID = new int[8];
    Type[] backType = new Type[8];

    // Only used when finding 2nd best segmentation under a
    // too-long token:
    int forwardCount;
    int[] forwardPos = new int[8];
    int[] forwardID = new int[8];
    int[] forwardIndex = new int[8];
    Type[] forwardType = new Type[8];

    public void grow() {
      costs = ArrayUtil.grow(costs, 1+count);
      lastRightID = ArrayUtil.grow(lastRightID, 1+count);
      backPos = ArrayUtil.grow(backPos, 1+count);
      backIndex = ArrayUtil.grow(backIndex, 1+count);
      backID = ArrayUtil.grow(backID, 1+count);

      // NOTE: sneaky: grow separately because
      // ArrayUtil.grow will otherwise pick a different
      // length than the int[]s we just grew:
      final Type[] newBackType = new Type[backID.length];
      System.arraycopy(backType, 0, newBackType, 0, backType.length);
      backType = newBackType;
    }

    public void growForward() {
      forwardPos = ArrayUtil.grow(forwardPos, 1+forwardCount);
      forwardID = ArrayUtil.grow(forwardID, 1+forwardCount);
      forwardIndex = ArrayUtil.grow(forwardIndex, 1+forwardCount);

      // NOTE: sneaky: grow separately because
      // ArrayUtil.grow will otherwise pick a different
      // length than the int[]s we just grew:
      final Type[] newForwardType = new Type[forwardPos.length];
      System.arraycopy(forwardType, 0, newForwardType, 0, forwardType.length);
      forwardType = newForwardType;
    }

    public void add(int cost, int lastRightID, int backPos, int backIndex, int backID, Type backType) {
      // NOTE: this isn't quite a true Viterbi search,
      // because we should check if lastRightID is
      // already present here, and only update if the new
      // cost is less than the current cost, instead of
      // simply appending.  However, that will likely hurt
      // performance (usually we add a lastRightID only once),
      // and it means we actually create the full graph
      // intersection instead of a "normal" Viterbi lattice:
      if (count == costs.length) {
        grow();
      }
      this.costs[count] = cost;
      this.lastRightID[count] = lastRightID;
      this.backPos[count] = backPos;
      this.backIndex[count] = backIndex;
      this.backID[count] = backID;
      this.backType[count] = backType;
      count++;
    }

    public void addForward(int forwardPos, int forwardIndex, int forwardID, Type forwardType) {
      if (forwardCount == this.forwardID.length) {
        growForward();
      }
      this.forwardPos[forwardCount] = forwardPos;
      this.forwardIndex[forwardCount] = forwardIndex;
      this.forwardID[forwardCount] = forwardID;
      this.forwardType[forwardCount] = forwardType;
      forwardCount++;
    }

    public void reset() {
      count = 0;
      // forwardCount naturally resets after it runs:
      assert forwardCount == 0: "pos=" + pos + " forwardCount=" + forwardCount;
    }
  }

  private void add(Dictionary dict, Position fromPosData, int endPos, int wordID, Type type, boolean addPenalty) throws IOException {
    final int wordCost = dict.getWordCost(wordID);
    final int leftID = dict.getLeftId(wordID);
    int leastCost = Integer.MAX_VALUE;
    int leastIDX = -1;
    assert fromPosData.count > 0;
    for(int idx=0;idx<fromPosData.count;idx++) {
      // Cost is path cost so far, plus word cost (added at
      // end of loop), plus bigram cost:
      final int cost = fromPosData.costs[idx] + costs.get(fromPosData.lastRightID[idx], leftID);
      if (VERBOSE) {
        System.out.println("      fromIDX=" + idx + ": cost=" + cost + " (prevCost=" + fromPosData.costs[idx] + " wordCost=" + wordCost + " bgCost=" + costs.get(fromPosData.lastRightID[idx], leftID) + " leftID=" + leftID);
      }
      if (cost < leastCost) {
        leastCost = cost;
        leastIDX = idx;
        if (VERBOSE) {
          System.out.println("        **");
        }
      }
    }

    leastCost += wordCost;

    if (VERBOSE) {
      System.out.println("      + cost=" + leastCost + " wordID=" + wordID + " leftID=" + leftID + " leastIDX=" + leastIDX + " toPos=" + endPos + " toPos.idx=" + positions.get(endPos).count);
    }

    if ((addPenalty || (!outputCompounds && searchMode)) && type != Type.USER) {
      final int penalty = computePenalty(fromPosData.pos, endPos - fromPosData.pos);
      if (VERBOSE) {
        if (penalty > 0) {
          System.out.println("        + penalty=" + penalty + " cost=" + (leastCost+penalty));
        }
      }
      leastCost += penalty;
    }

    //positions.get(endPos).add(leastCost, dict.getRightId(wordID), fromPosData.pos, leastIDX, wordID, type);
    assert leftID == dict.getRightId(wordID);
    positions.get(endPos).add(leastCost, leftID, fromPosData.pos, leastIDX, wordID, type);
  }

  @Override
  public boolean incrementToken() throws IOException {

    // parse() is able to return w/o producing any new
    // tokens, when the tokens it had produced were entirely
    // punctuation.  So we loop here until we get a real
    // token or we end:
    while (pending.size() == 0) {
      if (end) {
        return false;
      }

      // Push Viterbi forward some more:
      parse();
    }

    final Token token = pending.remove(pending.size()-1);

    int position = token.getPosition();
    int length = token.getLength();
    clearAttributes();
    assert length > 0;
    //System.out.println("off=" + token.getOffset() + " len=" + length + " vs " + token.getSurfaceForm().length);
    termAtt.copyBuffer(token.getSurfaceForm(), token.getOffset(), length);
    offsetAtt.setOffset(correctOffset(position), correctOffset(position+length));
    basicFormAtt.setToken(token);
    posAtt.setToken(token);
    readingAtt.setToken(token);
    inflectionAtt.setToken(token);
    if (token.getPosition() == lastTokenPos) {
      posIncAtt.setPositionIncrement(0);
      posLengthAtt.setPositionLength(token.getPositionLength());
    } else {
      assert token.getPosition() > lastTokenPos;
      posIncAtt.setPositionIncrement(1);
      posLengthAtt.setPositionLength(1);
    }
    if (VERBOSE) {
      System.out.println(Thread.currentThread().getName() + ":    incToken: return token=" + token);
    }
    lastTokenPos = token.getPosition();
    return true;
  }

  // TODO: make generic'd version of this "circular array"?
  // It's a bit tricky because we do things to the Position
  // (eg, set .pos = N on reuse)...
  static final class WrappedPositionArray {
    private Position[] positions = new Position[8];

    public WrappedPositionArray() {
      for(int i=0;i<positions.length;i++) {
        positions[i] = new Position();
      }
    }

    // Next array index to write to in positions:
    private int nextWrite;

    // Next position to write:
    private int nextPos;
    
    // How many valid Position instances are held in the
    // positions array:
    private int count;

    public void reset() {
      nextWrite--;
      while(count > 0) {
        if (nextWrite == -1) {
          nextWrite = positions.length - 1;
        }
        positions[nextWrite--].reset();
        count--;
      }
      nextWrite = 0;
      nextPos = 0;
      count = 0;
    }

    /** Get Position instance for this absolute position;
     *  this is allowed to be arbitrarily far "in the
     *  future" but cannot be before the last freeBefore. */
    public Position get(int pos) {
      while(pos >= nextPos) {
        //System.out.println("count=" + count + " vs len=" + positions.length);
        if (count == positions.length) {
          Position[] newPositions = new Position[ArrayUtil.oversize(1+count, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
          //System.out.println("grow positions " + newPositions.length);
          System.arraycopy(positions, nextWrite, newPositions, 0, positions.length-nextWrite);
          System.arraycopy(positions, 0, newPositions, positions.length-nextWrite, nextWrite);
          for(int i=positions.length;i<newPositions.length;i++) {
            newPositions[i] = new Position();
          }
          nextWrite = positions.length;
          positions = newPositions;
        }
        if (nextWrite == positions.length) {
          nextWrite = 0;
        }
        // Should have already been reset:
        assert positions[nextWrite].count == 0;
        positions[nextWrite++].pos = nextPos++;
        count++;
      }
      assert inBounds(pos);
      final int index = getIndex(pos);
      assert positions[index].pos == pos;
      return positions[index];
    }

    public int getNextPos() {
      return nextPos;
    }

    // For assert:
    private boolean inBounds(int pos) {
      return pos < nextPos && pos >= nextPos - count;
    }

    private int getIndex(int pos) {
      int index = nextWrite - (nextPos - pos);
      if (index < 0) {
        index += positions.length;
      }
      return index;
    }

    public void freeBefore(int pos) {
      final int toFree = count - (nextPos - pos);
      assert toFree >= 0;
      assert toFree <= count;
      int index = nextWrite - count;
      if (index < 0) {
        index += positions.length;
      }
      for(int i=0;i<toFree;i++) {
        if (index == positions.length) {
          index = 0;
        }
        //System.out.println("  fb idx=" + index);
        positions[index].reset();
        index++;
      }
      count -= toFree;
    }
  }

  /* Incrementally parse some more characters.  This runs
   * the viterbi search forwards "enough" so that we
   * generate some more tokens.  How much forward depends on
   * the chars coming in, since some chars could cause
   * longer-lasting ambiguity in the parsing.  Once the
   * ambiguity is resolved, then we back trace, produce
   * the pending tokens, and return. */
  private void parse() throws IOException {
    if (VERBOSE) {
      System.out.println("\nPARSE");
    }

    // Advances over each position (character):
    while (true) {

      if (buffer.get(pos) == -1) {
        // End
        break;
      }

      final Position posData = positions.get(pos);
      final boolean isFrontier = positions.getNextPos() == pos+1;

      if (posData.count == 0) {
        // No arcs arrive here; move to next position:
        if (VERBOSE) {
          System.out.println("    no arcs in; skip pos=" + pos);
        }
        pos++;
        continue;
      }

      if (pos > lastBackTracePos && posData.count == 1 && isFrontier) {
        //  if (pos > lastBackTracePos && posData.count == 1 && isFrontier) {
        // We are at a "frontier", and only one node is
        // alive, so whatever the eventual best path is must
        // come through this node.  So we can safely commit
        // to the prefix of the best path at this point:
        backtrace(posData, 0);

        // Re-base cost so we don't risk int overflow:
        posData.costs[0] = 0;

        if (pending.size() != 0) {
          return;
        } else {
          // This means the backtrace only produced
          // punctuation tokens, so we must keep parsing.
        }
      }

      if (pos - lastBackTracePos >= MAX_BACKTRACE_GAP) {
        // Safety: if we've buffered too much, force a
        // backtrace now.  We find the least-cost partial
        // path, across all paths, backtrace from it, and
        // then prune all others.  Note that this, in
        // general, can produce the wrong result, if the
        // total best path did not in fact back trace
        // through this partial best path.  But it's the
        // best we can do... (short of not having a
        // safety!).

        // First pass: find least cost partial path so far,
        // including ending at future positions:
        int leastIDX = -1;
        int leastCost = Integer.MAX_VALUE;
        Position leastPosData = null;
        for(int pos2=pos;pos2<positions.getNextPos();pos2++) {
          final Position posData2 = positions.get(pos2);
          for(int idx=0;idx<posData2.count;idx++) {
            //System.out.println("    idx=" + idx + " cost=" + cost);
            final int cost = posData2.costs[idx];
            if (cost < leastCost) {
              leastCost = cost;
              leastIDX = idx;
              leastPosData = posData2;
            }
          }
        }

        // We will always have at least one live path:
        assert leastIDX != -1;

        // Second pass: prune all but the best path:
        for(int pos2=pos;pos2<positions.getNextPos();pos2++) {
          final Position posData2 = positions.get(pos2);
          if (posData2 != leastPosData) {
            posData2.reset();
          } else {
            if (leastIDX != 0) {
              posData2.costs[0] = posData2.costs[leastIDX];
              posData2.lastRightID[0] = posData2.lastRightID[leastIDX];
              posData2.backPos[0] = posData2.backPos[leastIDX];
              posData2.backIndex[0] = posData2.backIndex[leastIDX];
              posData2.backID[0] = posData2.backID[leastIDX];
              posData2.backType[0] = posData2.backType[leastIDX];
            }
            posData2.count = 1;
          }
        }

        backtrace(leastPosData, 0);

        // Re-base cost so we don't risk int overflow:
        Arrays.fill(leastPosData.costs, 0, leastPosData.count, 0);

        if (pos != leastPosData.pos) {
          // We jumped into a future position:
          assert pos < leastPosData.pos;
          pos = leastPosData.pos;
        }

        if (pending.size() != 0) {
          return;
        } else {
          // This means the backtrace only produced
          // punctuation tokens, so we must keep parsing.
          continue;
        }
      }

      if (VERBOSE) {
        System.out.println("\n  extend @ pos=" + pos + " char=" + (char) buffer.get(pos));
      }

      if (VERBOSE) {
        System.out.println("    " + posData.count + " arcs in");
      }

      boolean anyMatches = false;

      // First try user dict:
      if (userFST != null) {
        userFST.getFirstArc(arc);
        int output = 0;
        for(int posAhead=posData.pos;;posAhead++) {
          final int ch = buffer.get(posAhead);
          if (ch == -1) {
            break;
          }
          if (userFST.findTargetArc(ch, arc, arc, posAhead == posData.pos, userFSTReader) == null) {
            break;
          }
          output += arc.output.intValue();
          if (arc.isFinal()) {
            if (VERBOSE) {
              System.out.println("    USER word " + new String(buffer.get(pos, posAhead - pos + 1)) + " toPos=" + (posAhead + 1));
            }
            add(userDictionary, posData, posAhead+1, output + arc.nextFinalOutput.intValue(), Type.USER, false);
            anyMatches = true;
          }
        }
      }

      // TODO: we can be more aggressive about user
      // matches?  if we are "under" a user match then don't
      // extend KNOWN/UNKNOWN paths?

      if (!anyMatches) {
        // Next, try known dictionary matches
        fst.getFirstArc(arc);
        int output = 0;

        for(int posAhead=posData.pos;;posAhead++) {
          final int ch = buffer.get(posAhead);
          if (ch == -1) {
            break;
          }
          //System.out.println("    match " + (char) ch + " posAhead=" + posAhead);
          
          if (fst.findTargetArc(ch, arc, arc, posAhead == posData.pos, fstReader) == null) {
            break;
          }

          output += arc.output.intValue();

          // Optimization: for known words that are too-long
          // (compound), we should pre-compute the 2nd
          // best segmentation and store it in the
          // dictionary instead of recomputing it each time a
          // match is found.

          if (arc.isFinal()) {
            dictionary.lookupWordIds(output + arc.nextFinalOutput.intValue(), wordIdRef);
            if (VERBOSE) {
              System.out.println("    KNOWN word " + new String(buffer.get(pos, posAhead - pos + 1)) + " toPos=" + (posAhead + 1) + " " + wordIdRef.length + " wordIDs");
            }
            for (int ofs = 0; ofs < wordIdRef.length; ofs++) {
              add(dictionary, posData, posAhead+1, wordIdRef.ints[wordIdRef.offset + ofs], Type.KNOWN, false);
              anyMatches = true;
            }
          }
        }
      }

      // In the case of normal mode, it doesn't process unknown word greedily.

      if (!searchMode && unknownWordEndIndex > posData.pos) {
        pos++;
        continue;
      }

      final char firstCharacter = (char) buffer.get(pos);
      if (!anyMatches || characterDefinition.isInvoke(firstCharacter)) {

        // Find unknown match:
        final int characterId = characterDefinition.getCharacterClass(firstCharacter);
        final boolean isPunct = isPunctuation(firstCharacter);

        // NOTE: copied from UnknownDictionary.lookup:
        int unknownWordLength;
        if (!characterDefinition.isGroup(firstCharacter)) {
          unknownWordLength = 1;
        } else {
          // Extract unknown word. Characters with the same character class are considered to be part of unknown word
          unknownWordLength = 1;
          for (int posAhead=pos+1;unknownWordLength<MAX_UNKNOWN_WORD_LENGTH;posAhead++) {
            final int ch = buffer.get(posAhead);
            if (ch == -1) {
              break;
            }
            if (characterId == characterDefinition.getCharacterClass((char) ch) &&
                isPunctuation((char) ch) == isPunct) {
              unknownWordLength++;
            } else {
              break;
            }
          }
        }

        unkDictionary.lookupWordIds(characterId, wordIdRef); // characters in input text are supposed to be the same
        if (VERBOSE) {
          System.out.println("    UNKNOWN word len=" + unknownWordLength + " " + wordIdRef.length + " wordIDs");
        }
        for (int ofs = 0; ofs < wordIdRef.length; ofs++) {
          add(unkDictionary, posData, posData.pos + unknownWordLength, wordIdRef.ints[wordIdRef.offset + ofs], Type.UNKNOWN, false);
        }

        unknownWordEndIndex = posData.pos + unknownWordLength;
      }

      pos++;
    }

    end = true;

    if (pos > 0) {

      final Position endPosData = positions.get(pos);
      int leastCost = Integer.MAX_VALUE;
      int leastIDX = -1;
      if (VERBOSE) {
        System.out.println("  end: " + endPosData.count + " nodes");
      }
      for(int idx=0;idx<endPosData.count;idx++) {
        // Add EOS cost:
        final int cost = endPosData.costs[idx] + costs.get(endPosData.lastRightID[idx], 0);
        //System.out.println("    idx=" + idx + " cost=" + cost + " (pathCost=" + endPosData.costs[idx] + " bgCost=" + costs.get(endPosData.lastRightID[idx], 0) + ") backPos=" + endPosData.backPos[idx]);
        if (cost < leastCost) {
          leastCost = cost;
          leastIDX = idx;
        }
      }

      backtrace(endPosData, leastIDX);
    } else {
      // No characters in the input string; return no tokens!
    }
  }

  // Eliminates arcs from the lattice that are compound
  // tokens (have a penalty) or are not congruent with the
  // compound token we've matched (ie, span across the
  // startPos).  This should be fairly efficient, because we
  // just keep the already intersected structure of the
  // graph, eg we don't have to consult the FSTs again:

  private void pruneAndRescore(int startPos, int endPos, int bestStartIDX) throws IOException {
    if (VERBOSE) {
      System.out.println("  pruneAndRescore startPos=" + startPos + " endPos=" + endPos + " bestStartIDX=" + bestStartIDX);
    }

    // First pass: walk backwards, building up the forward
    // arcs and pruning inadmissible arcs:
    for(int pos=endPos; pos > startPos; pos--) {
      final Position posData = positions.get(pos);
      if (VERBOSE) {
        System.out.println("    back pos=" + pos);
      }
      for(int arcIDX=0;arcIDX<posData.count;arcIDX++) {
        final int backPos = posData.backPos[arcIDX];
        if (backPos >= startPos) {
          // Keep this arc:
          //System.out.println("      keep backPos=" + backPos);
          positions.get(backPos).addForward(pos,
                                            arcIDX,
                                            posData.backID[arcIDX],
                                            posData.backType[arcIDX]);
        } else {
          if (VERBOSE) {
            System.out.println("      prune");
          }
        }
      }
      if (pos != startPos) {
        posData.count = 0;
      }
    }

    // Second pass: walk forward, re-scoring:
    for(int pos=startPos; pos < endPos; pos++) {
      final Position posData = positions.get(pos);
      if (VERBOSE) {
        System.out.println("    forward pos=" + pos + " count=" + posData.forwardCount);
      }
      if (posData.count == 0) {
        // No arcs arrive here...
        if (VERBOSE) {
          System.out.println("      skip");
        }
        posData.forwardCount = 0;
        continue;
      }

      if (pos == startPos) {
        // On the initial position, only consider the best
        // path so we "force congruence":  the
        // sub-segmentation is "in context" of what the best
        // path (compound token) had matched:
        final int rightID;
        if (startPos == 0) {
          rightID = 0;
        } else {
          rightID = getDict(posData.backType[bestStartIDX]).getRightId(posData.backID[bestStartIDX]);
        }
        final int pathCost = posData.costs[bestStartIDX];
        for(int forwardArcIDX=0;forwardArcIDX<posData.forwardCount;forwardArcIDX++) {
          final Type forwardType = posData.forwardType[forwardArcIDX];
          final Dictionary dict2 = getDict(forwardType);
          final int wordID = posData.forwardID[forwardArcIDX];
          final int toPos = posData.forwardPos[forwardArcIDX];
          final int newCost = pathCost + dict2.getWordCost(wordID) + 
            costs.get(rightID, dict2.getLeftId(wordID)) +
            computePenalty(pos, toPos-pos);
          if (VERBOSE) {
            System.out.println("      + " + forwardType + " word " + new String(buffer.get(pos, toPos-pos)) + " toPos=" + toPos + " cost=" + newCost + " penalty=" + computePenalty(pos, toPos-pos) + " toPos.idx=" + positions.get(toPos).count);
          }
          positions.get(toPos).add(newCost,
                                   dict2.getRightId(wordID),
                                   pos,
                                   bestStartIDX,
                                   wordID,
                                   forwardType);
        }
      } else {
        // On non-initial positions, we maximize score
        // across all arriving lastRightIDs:
        for(int forwardArcIDX=0;forwardArcIDX<posData.forwardCount;forwardArcIDX++) {
          final Type forwardType = posData.forwardType[forwardArcIDX];
          final int toPos = posData.forwardPos[forwardArcIDX];
          if (VERBOSE) {
            System.out.println("      + " + forwardType + " word " + new String(buffer.get(pos, toPos-pos)) + " toPos=" + toPos);
          }
          add(getDict(forwardType),
              posData,
              toPos,
              posData.forwardID[forwardArcIDX],
              forwardType,
              true);
        }
      }
      posData.forwardCount = 0;
    }
  }

  // Backtrace from the provided position, back to the last
  // time we back-traced, accumulating the resulting tokens to
  // the pending list.  The pending list is then in-reverse
  // (last token should be returned first).
  private void backtrace(final Position endPosData, final int fromIDX) throws IOException {
    final int endPos = endPosData.pos;

    if (VERBOSE) {
      System.out.println("\n  backtrace: endPos=" + endPos + " pos=" + pos + "; " + (pos - lastBackTracePos) + " characters; last=" + lastBackTracePos + " cost=" + endPosData.costs[fromIDX]);
    }

    final char[] fragment = buffer.get(lastBackTracePos, endPos-lastBackTracePos);

    if (dotOut != null) {
      dotOut.onBacktrace(this, positions, lastBackTracePos, endPosData, fromIDX, fragment, end);
    }

    int pos = endPos;
    int bestIDX = fromIDX;
    Token altToken = null;

    // We trace backwards, so this will be the leftWordID of
    // the token after the one we are now on:
    int lastLeftWordID = -1;

    int backCount = 0;

    // TODO: sort of silly to make Token instances here; the
    // back trace has all info needed to generate the
    // token.  So, we could just directly set the attrs,
    // from the backtrace, in incrementToken w/o ever
    // creating Token; we'd have to defer calling freeBefore
    // until after the backtrace was fully "consumed" by
    // incrementToken.

    while (pos > lastBackTracePos) {
      //System.out.println("BT: back pos=" + pos + " bestIDX=" + bestIDX);
      final Position posData = positions.get(pos);
      assert bestIDX < posData.count;

      int backPos = posData.backPos[bestIDX];
      assert backPos >= lastBackTracePos: "backPos=" + backPos + " vs lastBackTracePos=" + lastBackTracePos;
      int length = pos - backPos;
      Type backType = posData.backType[bestIDX];
      int backID = posData.backID[bestIDX];
      int nextBestIDX = posData.backIndex[bestIDX];

      if (outputCompounds && searchMode && altToken == null && backType != Type.USER) {
        
        // In searchMode, if best path had picked a too-long
        // token, we use the "penalty" to compute the allowed
        // max cost of an alternate back-trace.  If we find an
        // alternate back trace with cost below that
        // threshold, we pursue it instead (but also output
        // the long token).
        //System.out.println("    2nd best backPos=" + backPos + " pos=" + pos);

        final int penalty = computeSecondBestThreshold(backPos, pos-backPos);
        
        if (penalty > 0) {
          if (VERBOSE) {
            System.out.println("  compound=" + new String(buffer.get(backPos, pos-backPos)) + " backPos=" + backPos + " pos=" + pos + " penalty=" + penalty + " cost=" + posData.costs[bestIDX] + " bestIDX=" + bestIDX + " lastLeftID=" + lastLeftWordID);
          }

          // Use the penalty to set maxCost on the 2nd best
          // segmentation:
          int maxCost = posData.costs[bestIDX] + penalty;
          if (lastLeftWordID != -1) {
            maxCost += costs.get(getDict(backType).getRightId(backID), lastLeftWordID);
          }

          // Now, prune all too-long tokens from the graph:
          pruneAndRescore(backPos, pos,
                          posData.backIndex[bestIDX]);

          // Finally, find 2nd best back-trace and resume
          // backtrace there:
          int leastCost = Integer.MAX_VALUE;
          int leastIDX = -1;
          for(int idx=0;idx<posData.count;idx++) {
            int cost = posData.costs[idx];
            //System.out.println("    idx=" + idx + " prevCost=" + cost);
            
            if (lastLeftWordID != -1) {
              cost += costs.get(getDict(posData.backType[idx]).getRightId(posData.backID[idx]),
                                lastLeftWordID);
              //System.out.println("      += bgCost=" + costs.get(getDict(posData.backType[idx]).getRightId(posData.backID[idx]),
              //lastLeftWordID) + " -> " + cost);
            }
            //System.out.println("penalty " + posData.backPos[idx] + " to " + pos);
            //cost += computePenalty(posData.backPos[idx], pos - posData.backPos[idx]);
            if (cost < leastCost) {
              //System.out.println("      ** ");
              leastCost = cost;
              leastIDX = idx;
            }
          }
          //System.out.println("  leastIDX=" + leastIDX);

          if (VERBOSE) {
            System.out.println("  afterPrune: " + posData.count + " arcs arriving; leastCost=" + leastCost + " vs threshold=" + maxCost + " lastLeftWordID=" + lastLeftWordID);
          }

          if (leastIDX != -1 && leastCost <= maxCost && posData.backPos[leastIDX] != backPos) {
            // We should have pruned the altToken from the graph:
            assert posData.backPos[leastIDX] != backPos;

            // Save the current compound token, to output when
            // this alternate path joins back:
            altToken = new Token(backID,
                                 fragment,
                                 backPos - lastBackTracePos,
                                 length,
                                 backType,
                                 backPos,
                                 getDict(backType));

            // Redirect our backtrace to 2nd best:
            bestIDX = leastIDX;
            nextBestIDX = posData.backIndex[bestIDX];

            backPos = posData.backPos[bestIDX];
            length = pos - backPos;
            backType = posData.backType[bestIDX];
            backID = posData.backID[bestIDX];
            backCount = 0;
            //System.out.println("  do alt token!");
            
          } else {
            // I think in theory it's possible there is no
            // 2nd best path, which is fine; in this case we
            // only output the compound token:
            //System.out.println("  no alt token! bestIDX=" + bestIDX);
          }
        }
      }

      final int offset = backPos - lastBackTracePos;
      assert offset >= 0;

      if (altToken != null && altToken.getPosition() >= backPos) {

        // We've backtraced to the position where the
        // compound token starts; add it now:

        // The pruning we did when we created the altToken
        // ensures that the back trace will align back with
        // the start of the altToken:
        assert altToken.getPosition() == backPos: altToken.getPosition() + " vs " + backPos;

        // NOTE: not quite right: the compound token may
        // have had all punctuation back traced so far, but
        // then the decompounded token at this position is
        // not punctuation.  In this case backCount is 0,
        // but we should maybe add the altToken anyway...?

        if (backCount > 0) {
          backCount++;
          altToken.setPositionLength(backCount);
          if (VERBOSE) {
            System.out.println("    add altToken=" + altToken);
          }
          pending.add(altToken);
        } else {
          // This means alt token was all punct tokens:
          if (VERBOSE) {
            System.out.println("    discard all-punctuation altToken=" + altToken);
          }
          assert discardPunctuation;
        }
        altToken = null;
      }

      final Dictionary dict = getDict(backType);

      if (backType == Type.USER) {

        // Expand the phraseID we recorded into the actual
        // segmentation:
        final int[] wordIDAndLength = userDictionary.lookupSegmentation(backID);
        int wordID = wordIDAndLength[0];
        int current = 0;
        for(int j=1; j < wordIDAndLength.length; j++) {
          final int len = wordIDAndLength[j];
          //System.out.println("    add user: len=" + len);
          pending.add(new Token(wordID+j-1,
                                fragment,
                                current + offset,
                                len,
                                Type.USER,
                                current + backPos,
                                dict));
          if (VERBOSE) {
            System.out.println("    add USER token=" + pending.get(pending.size()-1));
          }
          current += len;
        }

        // Reverse the tokens we just added, because when we
        // serve them up from incrementToken we serve in
        // reverse:
        Collections.reverse(pending.subList(pending.size() - (wordIDAndLength.length - 1),
                                            pending.size()));

        backCount += wordIDAndLength.length-1;
      } else {

        if (extendedMode && backType == Type.UNKNOWN) {
          // In EXTENDED mode we convert unknown word into
          // unigrams:
          int unigramTokenCount = 0;
          for(int i=length-1;i>=0;i--) {
            int charLen = 1;
            if (i > 0 && Character.isLowSurrogate(fragment[offset+i])) {
              i--;
              charLen = 2;
            }
            //System.out.println("    extended tok offset="
            //+ (offset + i));
            if (!discardPunctuation || !isPunctuation(fragment[offset+i])) {
              pending.add(new Token(CharacterDefinition.NGRAM,
                                    fragment,
                                    offset + i,
                                    charLen,
                                    Type.UNKNOWN,
                                    backPos + i,
                                    unkDictionary));
              unigramTokenCount++;
            }
          }
          backCount += unigramTokenCount;
          
        } else if (!discardPunctuation || length == 0 || !isPunctuation(fragment[offset])) {
          pending.add(new Token(backID,
                                fragment,
                                offset,
                                length,
                                backType,
                                backPos,
                                dict));
          if (VERBOSE) {
            System.out.println("    add token=" + pending.get(pending.size()-1));
          }
          backCount++;
        } else {
          if (VERBOSE) {
            System.out.println("    skip punctuation token=" + new String(fragment, offset, length));
          }
        }
      }

      lastLeftWordID = dict.getLeftId(backID);
      pos = backPos;
      bestIDX = nextBestIDX;
    }

    lastBackTracePos = endPos;

    if (VERBOSE) {
      System.out.println("  freeBefore pos=" + endPos);
    }
    // Notify the circular buffers that we are done with
    // these positions:
    buffer.freeBefore(endPos);
    positions.freeBefore(endPos);
  }

  Dictionary getDict(Type type) {
    return dictionaryMap.get(type);
  }

  private static boolean isPunctuation(char ch) {
    switch(Character.getType(ch)) {
      case Character.SPACE_SEPARATOR:
      case Character.LINE_SEPARATOR:
      case Character.PARAGRAPH_SEPARATOR:
      case Character.CONTROL:
      case Character.FORMAT:
      case Character.DASH_PUNCTUATION:
      case Character.START_PUNCTUATION:
      case Character.END_PUNCTUATION:
      case Character.CONNECTOR_PUNCTUATION:
      case Character.OTHER_PUNCTUATION:
      case Character.MATH_SYMBOL:
      case Character.CURRENCY_SYMBOL:
      case Character.MODIFIER_SYMBOL:
      case Character.OTHER_SYMBOL:
      case Character.INITIAL_QUOTE_PUNCTUATION:
      case Character.FINAL_QUOTE_PUNCTUATION:
        return true;
      default:
        return false;
    }
  }
}
