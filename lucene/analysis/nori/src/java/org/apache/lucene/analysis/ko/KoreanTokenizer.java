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
package org.apache.lucene.analysis.ko;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ko.dict.CharacterDefinition;
import org.apache.lucene.analysis.ko.dict.ConnectionCosts;
import org.apache.lucene.analysis.ko.dict.Dictionary;
import org.apache.lucene.analysis.ko.dict.TokenInfoDictionary;
import org.apache.lucene.analysis.ko.dict.TokenInfoFST;
import org.apache.lucene.analysis.ko.dict.UnknownDictionary;
import org.apache.lucene.analysis.ko.dict.UserDictionary;
import org.apache.lucene.analysis.ko.tokenattributes.PartOfSpeechAttribute;
import org.apache.lucene.analysis.ko.tokenattributes.ReadingAttribute;
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

/**
 * Tokenizer for Korean that uses morphological analysis.
 * <p>
 * This tokenizer sets a number of additional attributes:
 * <ul>
 *   <li>{@link PartOfSpeechAttribute} containing part-of-speech.
 *   <li>{@link ReadingAttribute} containing reading.
 * </ul>
 * <p>
 * This tokenizer uses a rolling Viterbi search to find the
 * least cost segmentation (path) of the incoming characters.
 * @lucene.experimental
 */
public final class KoreanTokenizer extends Tokenizer {

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

  /**
   * Decompound mode: this determines how the tokenizer handles
   * {@link POS.Type#COMPOUND}, {@link POS.Type#INFLECT} and {@link POS.Type#PREANALYSIS} tokens.
   */
  public enum DecompoundMode {
    /**
     * No decomposition for compound.
     */
    NONE,

    /**
     * Decompose compounds and discards the original form (default).
     */
    DISCARD,

    /**
     * Decompose compounds and keeps the original form.
     */
    MIXED
  }

  /**
   * Default mode for the decompound of tokens ({@link DecompoundMode#DISCARD}.
   */
  public static final DecompoundMode DEFAULT_DECOMPOUND = DecompoundMode.DISCARD;

  private static final boolean VERBOSE = false;

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

  private final DecompoundMode mode;
  private final boolean outputUnknownUnigrams;

  private final RollingCharBuffer buffer = new RollingCharBuffer();

  private final WrappedPositionArray positions = new WrappedPositionArray();

  // True once we've hit the EOF from the input reader:
  private boolean end;

  // Last absolute position we backtraced from:
  private int lastBackTracePos;

  // Next absolute position to process:
  private int pos;

  // Already parsed, but not yet passed to caller, tokens:
  private final List<Token> pending = new ArrayList<>();

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLengthAtt = addAttribute(PositionLengthAttribute.class);
  private final PartOfSpeechAttribute posAtt = addAttribute(PartOfSpeechAttribute.class);
  private final ReadingAttribute readingAtt = addAttribute(ReadingAttribute.class);


  /**
   * Creates a new KoreanTokenizer with default parameters.
   * <p>
   * Uses the default AttributeFactory.
   */
  public KoreanTokenizer() {
    this(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, null, DEFAULT_DECOMPOUND, false);
  }

  /**
   * Create a new KoreanTokenizer.
   *
   * @param factory the AttributeFactory to use
   * @param userDictionary Optional: if non-null, user dictionary.
   * @param mode Decompound mode.
   * @param outputUnknownUnigrams If true outputs unigrams for unknown words.
   */
  public KoreanTokenizer(AttributeFactory factory, UserDictionary userDictionary, DecompoundMode mode, boolean outputUnknownUnigrams) {
    super(factory);
    this.mode = mode;
    this.outputUnknownUnigrams = outputUnknownUnigrams;
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
    pos = 0;
    end = false;
    lastBackTracePos = 0;
    pending.clear();

    // Add BOS:
    positions.get(0).add(0, 0, -1, -1, -1, -1, Type.KNOWN);
  }

  @Override
  public void end() throws IOException {
    super.end();
    // Set final offset
    int finalOffset = correctOffset(pos);
    offsetAtt.setOffset(finalOffset, finalOffset);
  }

  // Holds all back pointers arriving to this position:
  final static class Position {

    int pos;

    int count;

    // maybe single int array * 5?
    int[] costs = new int[8];
    int[] lastRightID = new int[8];
    int[] backPos = new int[8];
    int[] backWordPos = new int[8];
    int[] backIndex = new int[8];
    int[] backID = new int[8];
    Type[] backType = new Type[8];

    public void grow() {
      costs = ArrayUtil.grow(costs, 1+count);
      lastRightID = ArrayUtil.grow(lastRightID, 1+count);
      backPos = ArrayUtil.grow(backPos, 1+count);
      backWordPos = ArrayUtil.grow(backWordPos, 1+count);
      backIndex = ArrayUtil.grow(backIndex, 1+count);
      backID = ArrayUtil.grow(backID, 1+count);

      // NOTE: sneaky: grow separately because
      // ArrayUtil.grow will otherwise pick a different
      // length than the int[]s we just grew:
      final Type[] newBackType = new Type[backID.length];
      System.arraycopy(backType, 0, newBackType, 0, backType.length);
      backType = newBackType;
    }

    public void add(int cost, int lastRightID, int backPos, int backRPos, int backIndex, int backID, Type backType) {
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
      this.backWordPos[count] = backRPos;
      this.backIndex[count] = backIndex;
      this.backID[count] = backID;
      this.backType[count] = backType;
      count++;
    }

    public void reset() {
      count = 0;
    }
  }

  /**
   * Returns the space penalty associated with the provided {@link POS.Tag}.
   *
   * @param leftPOS the left part of speech of the current token.
   * @param numSpaces the number of spaces before the current token.
   */
  private int computeSpacePenalty(POS.Tag leftPOS, int numSpaces) {
    int spacePenalty = 0;
    if (numSpaces > 0) {
      // TODO we should extract the penalty (left-space-penalty-factor) from the dicrc file.
      switch (leftPOS) {
        case E:
        case J:
        case VCP:
        case XSA:
        case XSN:
        case XSV:
          spacePenalty = 3000;
          break;

        default:
          break;
      }
    }
    return spacePenalty;

  }

  private void add(Dictionary dict, Position fromPosData, int wordPos, int endPos, int wordID, Type type) throws IOException {
    final POS.Tag leftPOS = dict.getLeftPOS(wordID);
    final int wordCost = dict.getWordCost(wordID);
    final int leftID = dict.getLeftId(wordID);
    int leastCost = Integer.MAX_VALUE;
    int leastIDX = -1;
    assert fromPosData.count > 0;
    for(int idx=0;idx<fromPosData.count;idx++) {
      // The number of spaces before the term
      int numSpaces = wordPos - fromPosData.pos;

      // Cost is path cost so far, plus word cost (added at
      // end of loop), plus bigram cost and space penalty cost.
      final int cost = fromPosData.costs[idx] + costs.get(fromPosData.lastRightID[idx], leftID) + computeSpacePenalty(leftPOS, numSpaces);
      if (VERBOSE) {
        System.out.println("      fromIDX=" + idx + ": cost=" + cost + " (prevCost=" + fromPosData.costs[idx] + " wordCost=" + wordCost + " bgCost=" + costs.get(fromPosData.lastRightID[idx], leftID) +
            " spacePenalty=" + computeSpacePenalty(leftPOS, numSpaces) + ") leftID=" + leftID + " leftPOS=" + leftPOS.name() + ")");
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

    positions.get(endPos).add(leastCost, dict.getRightId(wordID), fromPosData.pos, wordPos, leastIDX, wordID, type);
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

    int length = token.getLength();
    clearAttributes();
    assert length > 0;
    //System.out.println("off=" + token.getOffset() + " len=" + length + " vs " + token.getSurfaceForm().length);
    termAtt.copyBuffer(token.getSurfaceForm(), token.getOffset(), length);
    offsetAtt.setOffset(correctOffset(token.getStartOffset()), correctOffset(token.getEndOffset()));
    posAtt.setToken(token);
    readingAtt.setToken(token);
    posIncAtt.setPositionIncrement(token.getPositionIncrement());
    posLengthAtt.setPositionLength(token.getPositionLength());
    if (VERBOSE) {
      System.out.println(Thread.currentThread().getName() + ":    incToken: return token=" + token);
    }
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

    // Index of the last character of unknown word:
    int unknownWordEndIndex = -1;

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
        if (pending.size() > 0) {
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
              posData2.backWordPos[0] = posData2.backWordPos[leastIDX];
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
        if (pending.size() > 0) {
          return;
        } else {
          // This means the backtrace only produced
          // punctuation tokens, so we must keep parsing.
        }
      }

      if (VERBOSE) {
        System.out.println("\n  extend @ pos=" + pos + " char=" + (char) buffer.get(pos) + " hex=" + Integer.toHexString(buffer.get(pos)));
      }

      if (VERBOSE) {
        System.out.println("    " + posData.count + " arcs in");
      }

      // Move to the first character that is not a whitespace.
      // The whitespaces are added as a prefix for the term that we extract,
      // this information is then used when computing the cost for the term using
      // the space penalty factor.
      // They are removed when the final tokens are generated.
      if (Character.getType(buffer.get(pos)) == Character.SPACE_SEPARATOR) {
        int nextChar = buffer.get(++pos);
        while (nextChar != -1 && Character.getType(nextChar) == Character.SPACE_SEPARATOR) {
          pos ++;
          nextChar = buffer.get(pos);
        }
      }
      if (buffer.get(pos) == -1) {
        pos = posData.pos;
      }

      boolean anyMatches = false;

      // First try user dict:
      if (userFST != null) {
        userFST.getFirstArc(arc);
        int output = 0;
        for(int posAhead=pos;;posAhead++) {
          final int ch = buffer.get(posAhead);
          if (ch == -1) {
            break;
          }
          if (userFST.findTargetArc(ch, arc, arc, posAhead == pos, userFSTReader) == null) {
            break;
          }
          output += arc.output.intValue();
          if (arc.isFinal()) {
            if (VERBOSE) {
              System.out.println("    USER word " + new String(buffer.get(pos, posAhead - pos + 1)) + " toPos=" + (posAhead + 1));
            }
            add(userDictionary, posData, pos, posAhead+1, output + arc.nextFinalOutput.intValue(), Type.USER);
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

        for(int posAhead=pos;;posAhead++) {
          final int ch = buffer.get(posAhead);
          if (ch == -1) {
            break;
          }
          //System.out.println("    match " + (char) ch + " posAhead=" + posAhead);

          if (fst.findTargetArc(ch, arc, arc, posAhead == pos, fstReader) == null) {
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
              add(dictionary, posData, pos, posAhead+1, wordIdRef.ints[wordIdRef.offset + ofs], Type.KNOWN);
              anyMatches = true;
            }
          }
        }
      }

      if (unknownWordEndIndex > posData.pos) {
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
          for (int posAhead = pos + 1; unknownWordLength < MAX_UNKNOWN_WORD_LENGTH; posAhead++) {
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
          add(unkDictionary, posData, pos, pos + unknownWordLength, wordIdRef.ints[wordIdRef.offset + ofs], Type.UNKNOWN);
        }
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

  // the pending list.  The pending list is then in-reverse
  // (last token should be returned first).
  private void backtrace(final Position endPosData, final int fromIDX) {
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
      int backWordPos = posData.backWordPos[bestIDX];
      assert backPos >= lastBackTracePos: "backPos=" + backPos + " vs lastBackTracePos=" + lastBackTracePos;
      // the length of the word without the whitespaces at the beginning.
      int length = pos - backWordPos;
      Type backType = posData.backType[bestIDX];
      int backID = posData.backID[bestIDX];
      int nextBestIDX = posData.backIndex[bestIDX];
      // the start of the word after the whitespace at the beginning.
      final int fragmentOffset = backWordPos - lastBackTracePos;
      assert fragmentOffset >= 0;

      final Dictionary dict = getDict(backType);

      if (outputUnknownUnigrams && backType == Type.UNKNOWN) {
        // outputUnknownUnigrams converts unknown word into unigrams:
        for (int i = length - 1; i >= 0; i--) {
          int charLen = 1;
          if (i > 0 && Character.isLowSurrogate(fragment[fragmentOffset + i])) {
            i--;
            charLen = 2;
          }
          final DictionaryToken token = new DictionaryToken(Type.UNKNOWN,
              unkDictionary,
              CharacterDefinition.NGRAM,
              fragment,
              fragmentOffset+i,
              charLen,
              backWordPos+i,
              backWordPos+i+charLen
          );
          if (shouldFilterToken(token) == false) {
            pending.add(token);
            if (VERBOSE) {
              System.out.println("    add token=" + pending.get(pending.size() - 1));
            }
          }
        }
      } else {
        final DictionaryToken token = new DictionaryToken(backType,
            dict,
            backID,
            fragment,
            fragmentOffset,
            length,
            backWordPos,
            backWordPos + length
        );
        if (token.getPOSType() == POS.Type.MORPHEME || mode == DecompoundMode.NONE) {
          if (shouldFilterToken(token) == false) {
            pending.add(token);
            if (VERBOSE) {
              System.out.println("    add token=" + pending.get(pending.size() - 1));
            }
          }
        } else {
          Dictionary.Morpheme[] morphemes = token.getMorphemes();
          if (morphemes == null) {
            pending.add(token);
            if (VERBOSE) {
              System.out.println("    add token=" + pending.get(pending.size() - 1));
            }
          } else {
            int endOffset = backWordPos + length;
            int posLen = 0;
            // decompose the compound
            for (int i = morphemes.length - 1; i >= 0; i--) {
              final Dictionary.Morpheme morpheme = morphemes[i];
              final Token compoundToken;
              if (token.getPOSType() == POS.Type.COMPOUND) {
                assert endOffset - morpheme.surfaceForm.length() >= 0;
                compoundToken = new DecompoundToken(morpheme.posTag, morpheme.surfaceForm,
                    endOffset - morpheme.surfaceForm.length(), endOffset);
              } else {
                compoundToken = new DecompoundToken(morpheme.posTag, morpheme.surfaceForm, token.getStartOffset(), token.getEndOffset());
              }
              if (i == 0 && mode == DecompoundMode.MIXED) {
                compoundToken.setPositionIncrement(0);
              }
              ++ posLen;
              endOffset -= morpheme.surfaceForm.length();
              pending.add(compoundToken);
              if (VERBOSE) {
                System.out.println("    add token=" + pending.get(pending.size() - 1));
              }
            }
            if (mode == DecompoundMode.MIXED) {
              token.setPositionLength(Math.max(1, posLen));
              pending.add(token);
              if (VERBOSE) {
                System.out.println("    add token=" + pending.get(pending.size() - 1));
              }
            }
          }
        }
      }

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

  private boolean shouldFilterToken(Token token) {
    return isPunctuation(token.getSurfaceForm()[token.getOffset()]);
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
