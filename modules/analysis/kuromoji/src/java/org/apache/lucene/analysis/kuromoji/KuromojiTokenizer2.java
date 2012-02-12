package org.apache.lucene.analysis.kuromoji;

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

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.CharStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.kuromoji.Segmenter.Mode;
import org.apache.lucene.analysis.kuromoji.dict.CharacterDefinition;
import org.apache.lucene.analysis.kuromoji.dict.ConnectionCosts;
import org.apache.lucene.analysis.kuromoji.dict.Dictionary;
import org.apache.lucene.analysis.kuromoji.dict.TokenInfoDictionary;
import org.apache.lucene.analysis.kuromoji.dict.TokenInfoFST;
import org.apache.lucene.analysis.kuromoji.dict.UnknownDictionary;
import org.apache.lucene.analysis.kuromoji.dict.UserDictionary;
import org.apache.lucene.analysis.kuromoji.tokenattributes.*;
import org.apache.lucene.analysis.kuromoji.viterbi.ViterbiNode.Type;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.FST;

// TODO: somehow factor out a reusable viterbi search here,
// so other decompounders/tokenizers can reuse...

// nocommit add toDot and look at 1st pass intersection

// nocommit need better testing of compound output... need
// assertAnalyzesTo to accept posLength too

// nocommit add comment explaining this isn't quite a real viterbi

// nocomit explain how the 2nd best tokenization is
// "contextual"...

// nocommit -- need a test that doesn't pre-split by
// sentence... ie, we don't BOS/EOS on each sentence
// break any more... so this can change the results
// depending on whether ipadic was "trained" with sentence
// breaks?

// nocommit -- should we use the sentence breakiterator
// too..?  we can simply use it to slip an EOS/BOS token
// in...

/* Uses a rolling Viterbi search to find the least cost
 * segmentation (path) of the incoming characters.
 *
 * @lucene.experimental */
public final class KuromojiTokenizer2 extends Tokenizer {

  private static final boolean VERBOSE = false;

  private static final int SEARCH_MODE_KANJI_LENGTH = 2;

  private static final int SEARCH_MODE_OTHER_LENGTH = 7; // Must be >= SEARCH_MODE_KANJI_LENGTH

  private static final int SEARCH_MODE_KANJI_PENALTY = 3000;

  private static final int SEARCH_MODE_OTHER_PENALTY = 1700;

  // nocommit
  public static boolean DO_OUTPUT_COMPOUND = false;

  private final EnumMap<Type, Dictionary> dictionaryMap = new EnumMap<Type, Dictionary>(Type.class);

  private final TokenInfoFST fst;
  private final TokenInfoDictionary dictionary;
  private final UnknownDictionary unkDictionary;
  private final ConnectionCosts costs;
  private final UserDictionary userDictionary;
  private final CharacterDefinition characterDefinition;

  private final FST.Arc<Long> arc = new FST.Arc<Long>();
  private final FST.BytesReader fstReader;
  private final IntsRef wordIdRef = new IntsRef();

  private final FST.BytesReader userFSTReader;
  private final TokenInfoFST userFST;

  private Reader reader;

  // Next absolute position to process:
  private int pos;

  private WrappedCharArray buffer = new WrappedCharArray();

  // index of the last character of unknown word:
  // nocommit put back:
  // int unknownWordEndIndex = -1;

  private WrappedPositionArray positions = new WrappedPositionArray();

  private boolean end;
  private final boolean discardPunctuation;
  private final boolean searchMode;
  private final boolean extendedMode;
  private int sameLeastIndex;

  private int lastBackTracePos;
  private int lastTokenPos;

  // Already parsed but not yet passed to caller:
  private final List<Token> pending = new ArrayList<Token>();

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  private final BaseFormAttribute basicFormAtt = addAttribute(BaseFormAttribute.class);
  private final PartOfSpeechAttribute posAtt = addAttribute(PartOfSpeechAttribute.class);
  private final ReadingAttribute readingAtt = addAttribute(ReadingAttribute.class);
  private final InflectionAttribute inflectionAtt = addAttribute(InflectionAttribute.class);

  public KuromojiTokenizer2(Reader input, UserDictionary userDictionary, boolean discardPunctuation, Mode mode) {
    super(input);
    dictionary = TokenInfoDictionary.getInstance();
    fst = dictionary.getFST();
    unkDictionary = UnknownDictionary.getInstance();
    characterDefinition = unkDictionary.getCharacterDefinition();
    this.userDictionary = userDictionary;
    costs = ConnectionCosts.getInstance();
    fstReader = fst.getBytesReader(0);
    if (userDictionary != null) {
      userFST = userDictionary.getFST();
      userFSTReader = userFST.getBytesReader(0);
    } else {
      userFST = null;
      userFSTReader = null;
    }
    this.discardPunctuation = discardPunctuation;
    switch(mode){
      case SEARCH:
        searchMode = true;
        extendedMode = false;
        break;
      case EXTENDED:
        searchMode = true;
        extendedMode = true;
        break;
      default:
        searchMode = false;
        extendedMode = false;
        break;
    }
    buffer.reset(input);
    resetState();

    dictionaryMap.put(Type.KNOWN, dictionary);
    dictionaryMap.put(Type.UNKNOWN, unkDictionary);
    dictionaryMap.put(Type.USER, userDictionary);
  }

  @Override
  public void reset(Reader input) throws IOException {
    super.reset(input);
    buffer.reset(input);
    resetState();
  }

  private void resetState() {
    positions.reset();
    // nocommit put back
    //unknownWordEndIndex = -1;
    pos = 0;
    end = false;
    lastBackTracePos = 0;
    lastTokenPos = -1;
    pending.clear();

    // Add BOS:
    positions.get(0).add(0, 0, -1, -1, -1, Type.KNOWN);
  }

  @Override
  public final void end() {
    // set final offset
    offsetAtt.setOffset(correctOffset(pos), correctOffset(pos));
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
      final int penalty;
      if (allKanji) {	// Process only Kanji keywords
        return (length - SEARCH_MODE_KANJI_LENGTH) * SEARCH_MODE_KANJI_PENALTY;
      } else if (length > SEARCH_MODE_OTHER_LENGTH) {
        return (length - SEARCH_MODE_OTHER_LENGTH) * SEARCH_MODE_OTHER_PENALTY;								
      }
    }
    return 0;
  }

  // Holds all back pointers arriving to this position:
  private final static class Position {

    int pos;

    int count;

    // maybe single int array * 5?
    int[] costs = new int[4];
    // nommit rename to lastRightID or pathRightID or something:
    int[] nodeID = new int[4];
    int[] backPos = new int[4];
    int[] backIndex = new int[4];
    int[] backID = new int[4];
    Type[] backType = new Type[4];

    // Only used when finding 2nd best segmentation under a
    // too-long token:
    int forwardCount;
    int[] forwardPos = new int[4];
    int[] forwardID = new int[4];
    int[] forwardIndex = new int[4];
    Type[] forwardType = new Type[4];

    public void grow() {
      costs = ArrayUtil.grow(costs, 1+count);
      nodeID = ArrayUtil.grow(nodeID, 1+count);
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

    public void add(int cost, int nodeID, int backPos, int backIndex, int backID, Type backType) {
      // nocommit in theory, we should check if nodeID is
      // already present here, and update it if
      // so... instead of just always adding:
      if (count == costs.length) {
        grow();
      }
      this.costs[count] = cost;
      this.nodeID[count] = nodeID;
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

    // nocommit maybe?
    // public void update(int cost, int nodeID, int backPos, int backIndex, int backID)
  }

  // nocommit absorb into add...?
  private void add(Dictionary dict, Position posData, int endPos, int wordID, Type type) throws IOException {
    final int wordCost = dict.getWordCost(wordID);
    final int leftID = dict.getLeftId(wordID);
    int leastCost = Integer.MAX_VALUE;
    int leastIDX = -1;
    assert posData.count > 0;
    for(int idx=0;idx<posData.count;idx++) {
      // Cost is path cost so far, plus word cost, plus
      // bigram cost:
      final int cost = posData.costs[idx] + wordCost + costs.get(posData.nodeID[idx], leftID);
      if (cost < leastCost) {
        leastCost = cost;
        leastIDX = idx;
      }
    }

    if (VERBOSE) {
      System.out.println("      + cost=" + leastCost + " wordID=" + wordID + " leftID=" + leftID + " tok=" + new String(buffer.get(posData.pos, endPos-posData.pos)) + " leastIDX=" + leastIDX + " toPos.idx=" + positions.get(endPos).count);
    }

    // nocommit ideally we don't have to do this ... just
    // putting it here to confirm same results as current
    // segmenter:
    if (!DO_OUTPUT_COMPOUND && searchMode && type != Type.USER) {
      final int penalty = computePenalty(posData.pos, endPos - posData.pos);
      if (VERBOSE) {
        if (penalty > 0) {
          System.out.println("        + penalty=" + penalty + " cost=" + (leastCost+penalty));
        }
      }
      leastCost += penalty;
    }

    positions.get(endPos).add(leastCost, dict.getRightId(wordID), posData.pos, leastIDX, wordID, type);
    /*
    if (sameLeastIndex == -1) {
      sameLeastIndex = leastIDX;
    } else if (sameLeastIndex != leastIDX) {
      sameLeastIndex = Integer.MAX_VALUE;
    }
    */
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
    termAtt.copyBuffer(token.getSurfaceForm(), token.getOffset(), length);
    offsetAtt.setOffset(correctOffset(position), correctOffset(position+length));
    basicFormAtt.setToken(token);
    posAtt.setToken(token);
    readingAtt.setToken(token);
    inflectionAtt.setToken(token);
    if (token.getPosition() == lastTokenPos) {
      posIncAtt.setPositionIncrement(0);
      posIncAtt.setPositionLength(token.getPositionLength());
    } else {
      assert token.getPosition() > lastTokenPos;
      posIncAtt.setPositionIncrement(1);
      posIncAtt.setPositionLength(1);
    }
    if (VERBOSE) {
      System.out.println("    incToken: return token=" + token);
    }
    lastTokenPos = token.getPosition();
    return true;
  }

  // Acts like a forever growing char[] as you read
  // characters into it from the provided reader, but
  // internally it uses a circular buffer to only hold the
  // characters that haven't been freed yet:
  private static final class WrappedCharArray {

    // TODO: pull out as standalone oal.util class?

    private Reader reader;

    private char[] buffer = new char[32];

    // Next array index to write to in buffer:
    private int nextWrite;

    // Next absolute position to read from reader:
    private int nextPos;

    // How many valid chars (wrapped) are in the buffer:
    private int count;

    // True if we hit EOF
    private boolean end;
    
    /** Clear array and switch to new reader. */
    public void reset(Reader reader) {
      this.reader = reader;
      nextPos = 0;
      nextWrite = 0;
      count = 0;
      end = false;
    }

    /* Absolute position read.  NOTE: pos must not jump
     * ahead by more than 1!  Ie, it's OK to read arbitarily
     * far back (just not prior to the last {@link
     * #freeBefore}), but NOT ok to read arbitrarily far
     * ahead.  Returns -1 if you hit EOF. */
    public int get(int pos) throws IOException {
      //System.out.println("    get pos=" + pos + " nextPos=" + nextPos + " count=" + count);
      if (pos == nextPos) {
        if (end) {
          return -1;
        }
        final int ch = reader.read();
        if (ch == -1) {
          end = true;
          return -1;
        }
        if (count == buffer.length) {
          // Grow
          final char[] newBuffer = new char[ArrayUtil.oversize(1+count, RamUsageEstimator.NUM_BYTES_CHAR)];
          System.arraycopy(buffer, nextWrite, newBuffer, 0, buffer.length - nextWrite);
          System.arraycopy(buffer, 0, newBuffer, buffer.length - nextWrite, nextWrite);
          nextWrite = buffer.length;
          //System.out.println("buffer: grow from " + buffer.length + " to " + newBuffer.length);
          buffer = newBuffer;
        }
        if (nextWrite == buffer.length) {
          nextWrite = 0;
        }
        buffer[nextWrite++] = (char) ch;
        count++;
        nextPos++;
        return ch;
      } else {
        // Cannot read from future (except by 1):
        assert pos < nextPos;

        // Cannot read from already freed past:
        assert nextPos - pos <= count;

        final int index = getIndex(pos);
        return buffer[index];
      }
    }

    // For assert:
    private boolean inBounds(int pos) {
      return pos < nextPos && pos >= nextPos - count;
    }

    private int getIndex(int pos) {
      int index = nextWrite - (nextPos - pos);
      if (index < 0) {
        // Wrap:
        index += buffer.length;
        assert index >= 0;
      }
      return index;
    }

    public char[] get(int posStart, int length) {
      assert length > 0;
      assert inBounds(posStart): "posStart=" + posStart + " length=" + length;
      //System.out.println("    buffer.get posStart=" + posStart + " len=" + length);
      
      final int startIndex = getIndex(posStart);
      final int endIndex = getIndex(posStart + length);
      //System.out.println("      startIndex=" + startIndex + " endIndex=" + endIndex);

      final char[] result = new char[length];
      // nocommit what if entire buffer is requested...?
      if (endIndex >= startIndex) {
        System.arraycopy(buffer, startIndex, result, 0, endIndex-startIndex);
      } else {
        // Wrapped:
        final int part1 = buffer.length-startIndex;
        System.arraycopy(buffer, startIndex, result, 0, part1);
        System.arraycopy(buffer, 0, result, buffer.length-startIndex, length-part1);
      }
      return result;
    }

    /** Call this to notify us that no chars before this
     *  absolute position are needed anymore. */
    public void freeBefore(int pos) {
      assert pos <= nextPos;
      count = nextPos - pos;
      assert count < buffer.length;
    }
  }

  // TODO: make generic'd version of this "circular array"?
  private static final class WrappedPositionArray {
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

      // nocommit if I change 40 below it changes number of
      // tokens in TestQuality!
      if ((pos - lastBackTracePos >= 100) && posData.count == 1 && isFrontier) {
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
          // nocommit: make punctuation-only testcase
          // This means the backtrace only produced
          // punctuation tokens, so we must keep parsing.
        }
      }

      if (VERBOSE) {
        System.out.println("\n  extend @ pos=" + pos + " char=" + (char) buffer.get(pos));
      }

      sameLeastIndex = -1;

      if (posData.count == 0) {
        // No arcs arrive here; move to next position:
        pos++;
        if (VERBOSE) {
          System.out.println("    no arcs in; skip");
        }
        continue;
      }

      if (VERBOSE) {
        System.out.println("    " + posData.count + " arcs in");
      }

      // nocommit must also 1) detect a
      // less-obvious-yet-still-committable backtrace op,
      // when, even though N > 1 states are/were alive, they
      // all back through a single state, and also 2) maybe
      // need to "force" a "when all else fails" backtrace

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
            add(userDictionary, posData, posAhead+1, output + arc.nextFinalOutput.intValue(), Type.USER);
            anyMatches = true;
          }
        }
      }

      // nocommit we can be more aggressive about user
      // matches?  if we are "under" a user match then don't
      // extend these paths?

      if (!anyMatches) {
        // Next, try known dictionary matches
        fst.getFirstArc(arc);
        int output = 0;

        boolean allKanji = true;
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

          // NOTE: for known words that are too-long
          // (compound), we should pre-compute the 2nd
          // best segmentation and store it in the
          // dictionary.  This is just a time/space opto...

          if (arc.isFinal()) {
            dictionary.lookupWordIds(output + arc.nextFinalOutput.intValue(), wordIdRef);
            if (VERBOSE) {
              System.out.println("    KNOWN word " + new String(buffer.get(pos, posAhead - pos + 1)) + " toPos=" + (posAhead + 1) + " " + wordIdRef.length + " wordIDs");
            }
            for (int ofs = 0; ofs < wordIdRef.length; ofs++) {
              add(dictionary, posData, posAhead+1, wordIdRef.ints[wordIdRef.offset + ofs], Type.KNOWN);
              anyMatches = true;
            }
          }
        }
      }

      // In the case of normal mode, it doesn't process unknown word greedily.

      // nocommit: fix
      /*
      if (!searchMode && unknownWordEndIndex > posData.pos) {
        continue;
      }
      */
      final char firstCharacter = (char) buffer.get(pos);
      // nocommit -- can't we NOT pursue unk if a known
      // token "covers" us...?
      if (!anyMatches || characterDefinition.isInvoke(firstCharacter)) {

        // Find unknown match:
        final int characterId = characterDefinition.getCharacterClass(firstCharacter);

        // NOTE: copied from UnknownDictionary.lookup:
        int unknownWordLength;
        if (!characterDefinition.isGroup(firstCharacter)) {
          unknownWordLength = 1;
        } else {
          // Extract unknown word. Characters with the same character class are considered to be part of unknown word
          unknownWordLength = 1;
          for (int posAhead=pos+1;;posAhead++) {
            final int ch = buffer.get(posAhead);
            if (ch == -1) {
              break;
            }
            if (characterId == characterDefinition.getCharacterClass((char) ch)) {
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
          add(unkDictionary, posData, posData.pos + unknownWordLength, wordIdRef.ints[wordIdRef.offset + ofs], Type.UNKNOWN);
        }

        // nocommit fixme
        //unknownWordEndIndex = posData.pos + unknownWordLength;
      }

      // nocommit explainme:
      if (false && (pos - lastBackTracePos >= 100) && sameLeastIndex != -1 && sameLeastIndex != Integer.MAX_VALUE &&
          pos != lastBackTracePos && isFrontier) {

        final int sav = sameLeastIndex;

        //System.out.println("**SAME: " + sameLeastIndex);
        backtrace(posData, sameLeastIndex);

        // Re-base cost so we don't risk int overflow:
        // nocommit: this does nothing: arcs were already extended
        posData.costs[sav] = 0;

        if (pending.size() != 0) {
          return;
        } else {
          // nocommit: make punctuation-only testcase
          // This means the backtrace only produced
          // punctuation tokens, so we must keep parsing.
        }
      }

      pos++;
    }

    end = true;

    if (pos > 0) {

      final Position endPosData = positions.get(pos);
      int leastCost = Integer.MAX_VALUE;
      int leastIDX = 0;
      if (VERBOSE) {
        System.out.println("  end: " + endPosData.count + " nodes");
      }
      for(int idx=0;idx<endPosData.count;idx++) {
        // Add EOS cost:
        final int cost = endPosData.costs[idx] + costs.get(endPosData.nodeID[idx], 0);
        //System.out.println("    idx=" + idx + " cost=" + cost);
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

    int minBackPos = Integer.MAX_VALUE;

    final int compoundLength = endPos - startPos;
    // First pass: walk backwards, building up the forward
    // arcs and pruning inadmissible arcs:
    for(int pos=endPos; pos >= startPos; pos--) {
      final Position posData = positions.get(pos);
      if (VERBOSE) {
        System.out.println("    back pos=" + pos);
      }
      for(int arcIDX=0;arcIDX<posData.count;arcIDX++) {
        final int backPos = posData.backPos[arcIDX];
        // nocommit: O(N^2) cost here!!  adversary could
        // exploit this i think...?  how to protect?  hmm
        // maybe i need to track lastKanjiCount at each
        // position..
        // nocommit hacky:
        // if (backPos >= startPos && computePenalty(backPos, pos-backPos) == 0) {
        if (backPos >= startPos && (pos-backPos) <= 2*compoundLength/3) {
          //if (backPos != -1 && (pos-backPos) <= 2*compoundLength/3) {
          // Keep this arc:
          //System.out.println("      keep backPos=" + backPos);
          minBackPos = Math.min(backPos, minBackPos);
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

    // nocommit what if minBackPos not set...?

    // Second pass: walk forward, re-scoring:
    //for(int pos=startPos; pos < endPos; pos++) {
    for(int pos=minBackPos; pos < endPos; pos++) {
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
        final Dictionary dict = getDict(posData.backType[bestStartIDX]);
        final int rightID = startPos == 0 ? 0 : dict.getRightId(posData.backID[bestStartIDX]);
        final int pathCost = posData.backID[bestStartIDX];
        for(int forwardArcIDX=0;forwardArcIDX<posData.forwardCount;forwardArcIDX++) {
          final Type forwardType = posData.forwardType[forwardArcIDX];
          final Dictionary dict2 = getDict(forwardType);
          final int wordID = posData.forwardID[forwardArcIDX];
          final int newCost = pathCost + dict2.getWordCost(wordID) + 
            costs.get(rightID, dict2.getLeftId(wordID));
          final int toPos = posData.forwardPos[forwardArcIDX];
          if (VERBOSE) {
            System.out.println("      + " + forwardType + " word " + new String(buffer.get(pos, toPos-pos)) + " toPos=" + toPos);
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
        // across all arriving nodeIDs:
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
              forwardType);
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
    if (VERBOSE) {
      System.out.println("\n  backtrace: " + (pos - lastBackTracePos) + " characters; last=" + lastBackTracePos + " cost=" + endPosData.costs[fromIDX]);
    }
    final int endPos = endPosData.pos;

    final char[] fragment = buffer.get(lastBackTracePos, endPos-lastBackTracePos);

    int pos = endPos;
    int bestIDX = fromIDX;
    Token altToken = null;

    // We trace backwards, so this will be the leftWordID of
    // the token after the one we are now on:
    int lastLeftWordID = -1;

    int backCount = 0;

    // nocommit: don't use intermediate Token instance
    // here... change this to just hold raw back trace info,
    // then in incrementToken we pull the necessary char[],
    // and only call freeBefore once we're done iterating
    // these tokens:
    while (pos > lastBackTracePos) {
      //System.out.println("back pos=" + pos);
      final Position posData = positions.get(pos);

      int backPos = posData.backPos[bestIDX];
      int length = pos - backPos;
      Type backType = posData.backType[bestIDX];

      if (DO_OUTPUT_COMPOUND && searchMode && altToken == null && backType != Type.USER) {
        
        // In searchMode, if best path had picked a too-long
        // token, we use the "penalty" to compute the allowed
        // max cost of an alternate back-trace.  If we find an
        // alternate back trace with cost below that
        // threshold, we pursue it instead (but also output
        // the long token).

        // nocommit -- maybe re-tune this "penalty", now
        // that it means something very different ("output
        // smaller segmentation"):
        final int penalty = computePenalty(backPos, pos-backPos);
        
        if (penalty > 0) {
          if (VERBOSE) {
            System.out.println("  compound=" + new String(buffer.get(backPos, pos-backPos)) + " backPos=" + backPos + " pos=" + pos + " penalty=" + penalty);
          }

          // Use the penalty to set maxCost on the 2nd best
          // segmentation:
          final int maxCost = posData.costs[bestIDX] + penalty;

          final int backID = posData.backID[bestIDX];

          // Now, prune all too-long tokens from the graph:
          pruneAndRescore(backPos, pos,
                          posData.backIndex[bestIDX]);

          if (VERBOSE) {
            System.out.println("  afterPrune: " + posData.count + " arcs arriving");
          }

          // Finally, find 2nd best back-trace and resume
          // backtrace there:
          int leastCost = Integer.MAX_VALUE;
          int leastIDX = -1;
          for(int idx=0;idx<posData.count;idx++) {
            int cost = posData.costs[idx];
            if (lastLeftWordID != -1) {
              cost += costs.get(getDict(posData.backType[idx]).getRightId(posData.backID[idx]),
                                lastLeftWordID);
            }
            if (cost < leastCost) {
              leastCost = cost;
              leastIDX = idx;
            }
          }
          //System.out.println("  leastIDX=" + leastIDX);

          // nocommit -- must also check whether 2nd best score falls w/in threshold?

          if (leastIDX != -1 && posData.backPos[leastIDX] <= maxCost) {
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

            backPos = posData.backPos[bestIDX];
            length = pos - backPos;
            backType = posData.backType[bestIDX];
            backCount = 0;
            
          } else {
            // I think in theory it's possible there is no
            // 2nd best path, which is fine; in this case we
            // only output the compound token:
          }
        }
      }

      final int offset = backPos - lastBackTracePos;
      assert offset >= 0;

      // nocommit
      if (altToken != null && altToken.getPosition() >= backPos) {

        // We've backtraced to the position where the
        // compound token starts; add it now:

        // The pruning we did when we created the altToken
        // ensures that the back trace will align back with
        // the start of the altToken:
        // cannot assert...
        //assert altToken.getPosition() == backPos: altToken.getPosition() + " vs " + backPos;

        if (VERBOSE) {
          System.out.println("    add altToken=" + altToken);
        }
        assert backCount >= 1;
        backCount++;
        altToken.setPositionLength(backCount);
        pending.add(altToken);
        altToken = null;
      }

      final Dictionary dict = getDict(backType);

      if (backType == Type.USER) {

        // Expand the phraseID we recorded into the actual
        // segmentation:
        final int[] wordIDAndLength = userDictionary.lookupSegmentation(posData.backID[bestIDX]);
        int wordID = wordIDAndLength[0];
        int current = backPos;
        for(int j=1; j < wordIDAndLength.length; j++) {
          final int len = wordIDAndLength[j];
          //System.out.println("    add user: len=" + len);
          pending.add(new Token(wordID+j-1,
                                fragment,
                                backPos + current - lastBackTracePos,
                                len,
                                Type.USER,
                                backPos + current,
                                dict));
          current += len;
        }

        // Reverse the tokens we just added, because when we
        // serve them up from incrementToken we serve in
        // reverse:
        Collections.reverse(pending.subList(pending.size() - (wordIDAndLength.length - 1),
                                            pending.size()));

        backCount += wordIDAndLength.length-1;
      } else {

        if (extendedMode && posData.backType[bestIDX] == Type.UNKNOWN) {
          // nocommit what if the altToken is unknonwn?  
          // In EXTENDED mode we convert unknown word into
          // unigrams:
          int unigramTokenCount = 0;
          for(int i=length-1;i>=0;i--) {
            int charLen = 1;
            if (i > 0 && Character.isLowSurrogate(fragment[offset+i])) {
              i--;
              charLen = 2;
            }
            //System.out.println("    extended tok offset=" + (offset + i));
            pending.add(new Token(CharacterDefinition.NGRAM,
                                  fragment,
                                  offset + i,
                                  charLen,
                                  Type.UNKNOWN,
                                  backPos + i,
                                  unkDictionary));
            unigramTokenCount++;
          }
          backCount += unigramTokenCount;
          
        } else if (!discardPunctuation || length == 0 || !isPunctuation(fragment[offset])) {
          //System.out.println("backPos=" + backPos);
          pending.add(new Token(posData.backID[bestIDX],
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

      lastLeftWordID = dict.getLeftId(posData.backID[bestIDX]);
      pos = backPos;
      bestIDX = posData.backIndex[bestIDX];
    }

    lastBackTracePos = endPos;
    // nocommit explain & justify...:
    if (endPosData.count == 0) {
      endPosData.count = 1;
    }

    if (VERBOSE) {
      System.out.println("  freeBefore pos=" + endPos);
    }
    // Notify the circular buffers that we are done with
    // these positions:
    buffer.freeBefore(endPos);
    positions.freeBefore(endPos);
  }

  private Dictionary getDict(Type type) {
    return dictionaryMap.get(type);
  }

  private static final boolean isPunctuation(char ch) {
    // TODO: somehow this is slowish.. takes ~5% off
    // chars/msec from Perf.java; maybe we
    // can spend RAM...

    // nocommit can we call this only when token is len
    // 1... or it's unknown...?
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
