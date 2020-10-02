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
package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Splits words into subwords and performs optional transformations on subword
 * groups, producing a correct token graph so that e.g. {@link PhraseQuery} can
 * work correctly when this filter is used in the search-time analyzer.  Unlike
 * the deprecated {@link WordDelimiterFilter}, this token filter produces a
 * correct token graph as output.  However, it cannot consume an input token
 * graph correctly. Processing is suppressed by {@link KeywordAttribute#isKeyword()}=true.
 *
 * <p>
 * Words are split into subwords with the following rules:
 * <ul>
 * <li>split on intra-word delimiters (by default, all non alpha-numeric
 * characters): <code>"Wi-Fi"</code> &#8594; <code>"Wi", "Fi"</code></li>
 * <li>split on case transitions: <code>"PowerShot"</code> &#8594;
 * <code>"Power", "Shot"</code></li>
 * <li>split on letter-number transitions: <code>"SD500"</code> &#8594;
 * <code>"SD", "500"</code></li>
 * <li>leading and trailing intra-word delimiters on each subword are ignored:
 * <code>"//hello---there, 'dude'"</code> &#8594;
 * <code>"hello", "there", "dude"</code></li>
 * <li>trailing "'s" are removed for each subword: <code>"O'Neil's"</code>
 * &#8594; <code>"O", "Neil"</code>
 * <ul>
 * <li>Note: this step isn't performed in a separate filter because of possible
 * subword combinations.</li>
 * </ul>
 * </li>
 * </ul>
 * 
 * The <b>GENERATE...</b> options affect how incoming tokens are broken into parts, and the
 * various <b>CATENATE_...</b> parameters affect how those parts are combined.
 *
 * <ul>
 * <li>If no CATENATE option is set, then no subword combinations are generated:
 * <code>"PowerShot"</code> &#8594; <code>0:"Power", 1:"Shot"</code> (0 and 1 are the token
 * positions)</li>
 * <li>CATENATE_WORDS means that in addition to the subwords, maximum runs of
 * non-numeric subwords are catenated and produced at the same position of the
 * last subword in the run:
 * <ul>
 * <li><code>"PowerShot"</code> &#8594;
 * <code>0:"Power", 1:"Shot" 1:"PowerShot"</code></li>
 * <li><code>"A's+B's&amp;C's"</code> &gt; <code>0:"A", 1:"B", 2:"C", 2:"ABC"</code>
 * </li>
 * <li><code>"Super-Duper-XL500-42-AutoCoder!"</code> &#8594;
 * <code>0:"Super", 1:"Duper", 2:"XL", 2:"SuperDuperXL", 3:"500" 4:"42", 5:"Auto", 6:"Coder", 6:"AutoCoder"</code>
 * </li>
 * </ul>
 * </li>
 * <li>CATENATE_NUMBERS works like CATENATE_WORDS, but for adjacent digit sequences.</li>
 * <li>CATENATE_ALL smushes together all the token parts without distinguishing numbers and words.</li>
 * </ul>
 * One use for {@link WordDelimiterGraphFilter} is to help match words with different
 * subword delimiters. For example, if the source text contained "wi-fi" one may
 * want "wifi" "WiFi" "wi-fi" "wi+fi" queries to all match. One way of doing so
 * is to specify CATENATE options in the analyzer used for indexing, and not
 * in the analyzer used for querying. Given that
 * the current {@link StandardTokenizer} immediately removes many intra-word
 * delimiters, it is recommended that this filter be used after a tokenizer that
 * does not do this (such as {@link WhitespaceTokenizer}).
 */

public final class WordDelimiterGraphFilter extends TokenFilter {
  
  /**
   * Causes parts of words to be generated:
   * <p>
   * "PowerShot" =&gt; "Power" "Shot"
   */
  public static final int GENERATE_WORD_PARTS = 1;

  /**
   * Causes number subwords to be generated:
   * <p>
   * "500-42" =&gt; "500" "42"
   */
  public static final int GENERATE_NUMBER_PARTS = 2;

  /**
   * Causes maximum runs of word parts to be catenated:
   * <p>
   * "wi-fi" =&gt; "wifi"
   */
  public static final int CATENATE_WORDS = 4;

  /**
   * Causes maximum runs of number parts to be catenated:
   * <p>
   * "500-42" =&gt; "50042"
   */
  public static final int CATENATE_NUMBERS = 8;

  /**
   * Causes all subword parts to be catenated:
   * <p>
   * "wi-fi-4000" =&gt; "wifi4000"
   */
  public static final int CATENATE_ALL = 16;

  /**
   * Causes original words are preserved and added to the subword list (Defaults to false)
   * <p>
   * "500-42" =&gt; "500" "42" "500-42"
   */
  public static final int PRESERVE_ORIGINAL = 32;

  /**
   * Causes lowercase -&gt; uppercase transition to start a new subword.
   */
  public static final int SPLIT_ON_CASE_CHANGE = 64;

  /**
   * If not set, causes numeric changes to be ignored (subwords will only be generated
   * given SUBWORD_DELIM tokens).
   */
  public static final int SPLIT_ON_NUMERICS = 128;

  /**
   * Causes trailing "'s" to be removed for each subword
   * <p>
   * "O'Neil's" =&gt; "O", "Neil"
   */
  public static final int STEM_ENGLISH_POSSESSIVE = 256;

  /**
   * Suppresses processing terms with {@link KeywordAttribute#isKeyword()}=true.
   */
  public static final int IGNORE_KEYWORDS = 512;

  /**
   * If not null is the set of tokens to protect from being delimited
   *
   */
  final CharArraySet protWords;

  private final int flags;

  // packs start pos, end pos, start part, end part (= slice of the term text) for each buffered part:
  private int[] bufferedParts = new int[16];
  private int bufferedLen;
  private int bufferedPos;

  // holds text for each buffered part, or null if it's a simple slice of the original term
  private char[][] bufferedTermParts = new char[4][];
  
  private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAttribute = addAttribute(KeywordAttribute.class);;
  private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncAttribute = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLenAttribute = addAttribute(PositionLengthAttribute.class);

  // used for iterating word delimiter breaks
  private final WordDelimiterIterator iterator;

  // used for concatenating runs of similar typed subwords (word,number)
  private final WordDelimiterConcatenation concat = new WordDelimiterConcatenation();

  private final boolean adjustInternalOffsets;

  // number of subwords last output by concat.
  private int lastConcatCount;

  // used for catenate all
  private final WordDelimiterConcatenation concatAll = new WordDelimiterConcatenation();

  // used for accumulating position increment gaps so that we preserve incoming holes:
  private int accumPosInc;

  private char[] savedTermBuffer = new char[16];
  private int savedTermLength;
  private int savedStartOffset;
  private int savedEndOffset;
  private AttributeSource.State savedState;
  private int lastStartOffset;
  private boolean adjustingOffsets;

  private int wordPos;

  /**
   * Creates a new WordDelimiterGraphFilter
   *
   * @param in TokenStream to be filtered
   * @param adjustInternalOffsets if the offsets of partial terms should be adjusted
   * @param charTypeTable table containing character types
   * @param configurationFlags Flags configuring the filter
   * @param protWords If not null is the set of tokens to protect from being delimited
   */
  public WordDelimiterGraphFilter(TokenStream in, boolean adjustInternalOffsets, byte[] charTypeTable, int configurationFlags, CharArraySet protWords) {
    super(in);
    if ((configurationFlags &
        ~(GENERATE_WORD_PARTS |
          GENERATE_NUMBER_PARTS |
          CATENATE_WORDS |
          CATENATE_NUMBERS |
          CATENATE_ALL |
          PRESERVE_ORIGINAL |
          SPLIT_ON_CASE_CHANGE |
          SPLIT_ON_NUMERICS |
          STEM_ENGLISH_POSSESSIVE |
          IGNORE_KEYWORDS)) != 0) {
      throw new IllegalArgumentException("flags contains unrecognized flag: " + configurationFlags);
    }
    this.flags = configurationFlags;
    this.protWords = protWords;
    this.iterator = new WordDelimiterIterator(
        charTypeTable, has(SPLIT_ON_CASE_CHANGE), has(SPLIT_ON_NUMERICS), has(STEM_ENGLISH_POSSESSIVE));
    this.adjustInternalOffsets = adjustInternalOffsets;
  }

  /**
   * Creates a new WordDelimiterGraphFilter using {@link WordDelimiterIterator#DEFAULT_WORD_DELIM_TABLE}
   * as its charTypeTable
   *
   * @param in TokenStream to be filtered
   * @param configurationFlags Flags configuring the filter
   * @param protWords If not null is the set of tokens to protect from being delimited
   */
  public WordDelimiterGraphFilter(TokenStream in, int configurationFlags, CharArraySet protWords) {
    this(in, false, WordDelimiterIterator.DEFAULT_WORD_DELIM_TABLE, configurationFlags, protWords);
  }

  /** Iterates all words parts and concatenations, buffering up the term parts we should return. */
  private void bufferWordParts() throws IOException {

    saveState();

    // if length by start + end offsets doesn't match the term's text then set offsets for all our word parts/concats to the incoming
    // offsets.  this can happen if WDGF is applied to an injected synonym, or to a stem'd form, etc:
    adjustingOffsets = adjustInternalOffsets && savedEndOffset - savedStartOffset == savedTermLength;

    bufferedLen = 0;
    lastConcatCount = 0;
    wordPos = 0;

    if (has(PRESERVE_ORIGINAL)) {
      // add the original token now so that it is always emitted first
      // we will edit the term length after all other parts have been buffered
      buffer(0, 1, 0, savedTermLength);
    }

    if (iterator.isSingleWord()) {
      buffer(wordPos, wordPos+1, iterator.current, iterator.end);
      wordPos++;
      iterator.next();
    } else {

      // iterate all words parts, possibly buffering them, building up concatenations and possibly buffering them too:
      while (iterator.end != WordDelimiterIterator.DONE) {
        int wordType = iterator.type();
      
        // do we already have queued up incompatible concatenations?
        if (concat.isNotEmpty() && (concat.type & wordType) == 0) {
          flushConcatenation(concat);
        }

        // add subwords depending upon options
        if (shouldConcatenate(wordType)) {
          concatenate(concat);
        }
      
        // add all subwords (catenateAll)
        if (has(CATENATE_ALL)) {
          concatenate(concatAll);
        }
      
        // if we should output the word or number part
        if (shouldGenerateParts(wordType)) {
          buffer(wordPos, wordPos+1, iterator.current, iterator.end);
          wordPos++;
        }
        iterator.next();
      }

      if (concat.isNotEmpty()) {
        // flush final concatenation
        flushConcatenation(concat);
      }
        
      if (concatAll.isNotEmpty()) {
        // only if we haven't output this same combo above, e.g. PowerShot with CATENATE_WORDS:
        if (concatAll.subwordCount > lastConcatCount) {
          if (wordPos == concatAll.startPos) {
            // we are not generating parts, so we must advance wordPos now
            wordPos++;
          }
          concatAll.write();
        }
        concatAll.clear();
      }
    }

    if (has(PRESERVE_ORIGINAL)) {
      // we now know how many tokens need to be injected, so we can set the original
      // token's position length
      if (wordPos == 0) {
        // can happen w/ strange flag combos and inputs :)
        wordPos++;
      }
      bufferedParts[1] = wordPos;
    }
            
    sorter.sort(has(PRESERVE_ORIGINAL) ? 1 : 0, bufferedLen);
    wordPos = 0;

    // set back to 0 for iterating from the buffer
    bufferedPos = 0;
  }

  @Override
  public boolean incrementToken() throws IOException {
    while (true) {
      if (savedState == null) {

        // process a new input token
        if (input.incrementToken() == false) {
          return false;
        }
        if (has(IGNORE_KEYWORDS) && keywordAttribute.isKeyword()) {
            return true;
        }
        int termLength = termAttribute.length();
        char[] termBuffer = termAttribute.buffer();

        accumPosInc += posIncAttribute.getPositionIncrement();

        // iterate & cache all word parts up front:
        iterator.setText(termBuffer, termLength);
        iterator.next();
        
        // word of no delimiters, or protected word: just return it
        if ((iterator.current == 0 && iterator.end == termLength) ||
            (protWords != null && protWords.contains(termBuffer, 0, termLength))) {
          posIncAttribute.setPositionIncrement(accumPosInc);
          accumPosInc = 0;
          return true;
        }
        
        // word of simply delimiters: swallow this token, creating a hole, and move on to next token
        if (iterator.end == WordDelimiterIterator.DONE) {
          if (has(PRESERVE_ORIGINAL) == false) {
            continue;
          } else {
            accumPosInc = 0;
            return true;
          }
        }

        // otherwise, we have delimiters, process & buffer all parts:
        bufferWordParts();
      }

      if (bufferedPos < bufferedLen) {
        clearAttributes();
        restoreState(savedState);

        char[] termPart = bufferedTermParts[bufferedPos];
        int startPos = bufferedParts[4*bufferedPos];
        int endPos = bufferedParts[4*bufferedPos+1];
        int startPart = bufferedParts[4*bufferedPos+2];
        int endPart = bufferedParts[4*bufferedPos+3];
        bufferedPos++;

        int startOffset;
        int endOffset;

        if (adjustingOffsets == false) {
          startOffset = savedStartOffset;
          endOffset = savedEndOffset;
        } else {
          startOffset = savedStartOffset + startPart;
          endOffset = savedStartOffset + endPart;
        }

        // never let offsets go backwards:
        startOffset = Math.max(startOffset, lastStartOffset);
        endOffset = Math.max(endOffset, lastStartOffset);

        offsetAttribute.setOffset(startOffset, endOffset);
        lastStartOffset = startOffset;

        if (termPart == null) {
          termAttribute.copyBuffer(savedTermBuffer, startPart, endPart - startPart);
        } else {
          termAttribute.copyBuffer(termPart, 0, termPart.length);
        }

        posIncAttribute.setPositionIncrement(accumPosInc + startPos - wordPos);
        accumPosInc = 0;
        posLenAttribute.setPositionLength(endPos - startPos);
        wordPos = startPos;
        return true;
      }
        
      // no saved concatenations, on to the next input word
      savedState = null;
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    accumPosInc = 0;
    savedState = null;
    lastStartOffset = 0;
    concat.clear();
    concatAll.clear();
  }

  // ================================================= Helper Methods ================================================

  private class PositionSorter extends InPlaceMergeSorter {
    @Override
    protected int compare(int i, int j) {
      // smaller start offset
      int iOff = bufferedParts[4 * i + 2];
      int jOff = bufferedParts[4 * j + 2];
      int cmp = Integer.compare(iOff, jOff);
      if (cmp != 0) {
        return cmp;
      }

      // longer end offset
      iOff = bufferedParts[4 * i + 3];
      jOff = bufferedParts[4 * j + 3];
      return Integer.compare(jOff, iOff);
    }

    @Override
    protected void swap(int i, int j) {
      int iOffset = 4*i;
      int jOffset = 4*j;
      for(int x=0;x<4;x++) {
        int tmp = bufferedParts[iOffset+x];
        bufferedParts[iOffset+x] = bufferedParts[jOffset+x];
        bufferedParts[jOffset+x] = tmp;
      }

      char[] tmp2 = bufferedTermParts[i];
      bufferedTermParts[i] = bufferedTermParts[j];
      bufferedTermParts[j] = tmp2;
    }
  }
  
  final PositionSorter sorter = new PositionSorter();

  /** 
   * startPos, endPos -> graph start/end position
   * startPart, endPart -> slice of the original term for this part
   */

  void buffer(int startPos, int endPos, int startPart, int endPart) {
    buffer(null, startPos, endPos, startPart, endPart);
  }

  /** 
   * a null termPart means it's a simple slice of the original term
   */
  void buffer(char[] termPart, int startPos, int endPos, int startPart, int endPart) {
    /*
    System.out.println("buffer: pos=" + startPos + "-" + endPos + " part=" + startPart + "-" + endPart);
    if (termPart != null) {
      System.out.println("  termIn=" + new String(termPart));
    } else {
      System.out.println("  term=" + new String(savedTermBuffer, startPart, endPart-startPart));
    }
    */
    assert endPos > startPos: "startPos=" + startPos + " endPos=" + endPos;
    assert endPart > startPart || (endPart == 0 && startPart == 0 && savedTermLength == 0): "startPart=" + startPart + " endPart=" + endPart;
    if ((bufferedLen+1)*4 > bufferedParts.length) {
      bufferedParts = ArrayUtil.grow(bufferedParts, (bufferedLen+1)*4);
    }
    if (bufferedTermParts.length == bufferedLen) {
      int newSize = ArrayUtil.oversize(bufferedLen+1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
      char[][] newArray = new char[newSize][];
      System.arraycopy(bufferedTermParts, 0, newArray, 0, bufferedTermParts.length);
      bufferedTermParts = newArray;
    }
    bufferedTermParts[bufferedLen] = termPart;
    bufferedParts[bufferedLen*4] = startPos;
    bufferedParts[bufferedLen*4+1] = endPos;
    bufferedParts[bufferedLen*4+2] = startPart;
    bufferedParts[bufferedLen*4+3] = endPart;
    bufferedLen++;
  }
  
  /**
   * Saves the existing attribute states
   */
  private void saveState() {
    savedTermLength = termAttribute.length();
    savedStartOffset = offsetAttribute.startOffset();
    savedEndOffset = offsetAttribute.endOffset();
    savedState = captureState();

    if (savedTermBuffer.length < savedTermLength) {
      savedTermBuffer = new char[ArrayUtil.oversize(savedTermLength, Character.BYTES)];
    }

    System.arraycopy(termAttribute.buffer(), 0, savedTermBuffer, 0, savedTermLength);
  }

  /**
   * Flushes the given WordDelimiterConcatenation by either writing its concat and then clearing, or just clearing.
   *
   * @param concat WordDelimiterConcatenation that will be flushed
   */
  private void flushConcatenation(WordDelimiterConcatenation concat) {
    if (wordPos == concat.startPos) {
      // we are not generating parts, so we must advance wordPos now
      wordPos++;
    }
    lastConcatCount = concat.subwordCount;
    if (concat.subwordCount != 1 || shouldGenerateParts(concat.type) == false) {
      concat.write();
    }
    concat.clear();
  }

  /**
   * Determines whether to concatenate a word or number if the current word is the given type
   *
   * @param wordType Type of the current word used to determine if it should be concatenated
   * @return {@code true} if concatenation should occur, {@code false} otherwise
   */
  private boolean shouldConcatenate(int wordType) {
    return (has(CATENATE_WORDS) && WordDelimiterIterator.isAlpha(wordType)) || (has(CATENATE_NUMBERS) && WordDelimiterIterator.isDigit(wordType));
  }

  /**
   * Determines whether a word/number part should be generated for a word of the given type
   *
   * @param wordType Type of the word used to determine if a word/number part should be generated
   * @return {@code true} if a word/number part should be generated, {@code false} otherwise
   */
  private boolean shouldGenerateParts(int wordType) {
    return (has(GENERATE_WORD_PARTS) && WordDelimiterIterator.isAlpha(wordType)) || (has(GENERATE_NUMBER_PARTS) && WordDelimiterIterator.isDigit(wordType));
  }

  /**
   * Concatenates the saved buffer to the given WordDelimiterConcatenation
   *
   * @param concatenation WordDelimiterConcatenation to concatenate the buffer to
   */
  private void concatenate(WordDelimiterConcatenation concatenation) {
    if (concatenation.isEmpty()) {
      concatenation.type = iterator.type();
      concatenation.startPart = iterator.current;
      concatenation.startPos = wordPos;
    }
    concatenation.append(savedTermBuffer, iterator.current, iterator.end - iterator.current);
    concatenation.endPart = iterator.end;
  }

  /**
   * Determines whether the given flag is set
   *
   * @param flag Flag to see if set
   * @return {@code true} if flag is set
   */
  private boolean has(int flag) {
    return (flags & flag) != 0;
  }

  // ================================================= Inner Classes =================================================

  /**
   * A WDF concatenated 'run'
   */
  final class WordDelimiterConcatenation {
    final StringBuilder buffer = new StringBuilder();
    int startPart;
    int endPart;
    int startPos;
    int type;
    int subwordCount;

    /**
     * Appends the given text of the given length, to the concetenation at the given offset
     *
     * @param text Text to append
     * @param offset Offset in the concetenation to add the text
     * @param length Length of the text to append
     */
    void append(char text[], int offset, int length) {
      buffer.append(text, offset, length);
      subwordCount++;
    }

    /**
     * Writes the concatenation to part buffer
     */
    void write() {
      char[] termPart = new char[buffer.length()];
      buffer.getChars(0, buffer.length(), termPart, 0);
      buffer(termPart, startPos, wordPos, startPart, endPart);
    }

    /**
     * Determines if the concatenation is empty
     *
     * @return {@code true} if the concatenation is empty, {@code false} otherwise
     */
    boolean isEmpty() {
      return buffer.length() == 0;
    }

    boolean isNotEmpty() {
      return isEmpty() == false;
    }

    /**
     * Clears the concatenation and resets its state
     */
    void clear() {
      buffer.setLength(0);
      startPart = endPart = type = subwordCount = 0;
    }
  }

  /** Returns string representation of configuration flags */
  public static String flagsToString(int flags) {
    StringBuilder b = new StringBuilder();
    if ((flags & GENERATE_WORD_PARTS) != 0) {
      b.append("GENERATE_WORD_PARTS");
    }
    if ((flags & GENERATE_NUMBER_PARTS) != 0) {
      if (b.length() > 0) {
        b.append(" | ");
      }
      b.append("GENERATE_NUMBER_PARTS");
    }
    if ((flags & CATENATE_WORDS) != 0) {
      if (b.length() > 0) {
        b.append(" | ");
      }
      b.append("CATENATE_WORDS");
    }
    if ((flags & CATENATE_NUMBERS) != 0) {
      if (b.length() > 0) {
        b.append(" | ");
      }
      b.append("CATENATE_NUMBERS");
    }
    if ((flags & CATENATE_ALL) != 0) {
      if (b.length() > 0) {
        b.append(" | ");
      }
      b.append("CATENATE_ALL");
    }
    if ((flags & PRESERVE_ORIGINAL) != 0) {
      if (b.length() > 0) {
        b.append(" | ");
      }
      b.append("PRESERVE_ORIGINAL");
    }
    if ((flags & SPLIT_ON_CASE_CHANGE) != 0) {
      if (b.length() > 0) {
        b.append(" | ");
      }
      b.append("SPLIT_ON_CASE_CHANGE");
    }
    if ((flags & SPLIT_ON_NUMERICS) != 0) {
      if (b.length() > 0) {
        b.append(" | ");
      }
      b.append("SPLIT_ON_NUMERICS");
    }
    if ((flags & STEM_ENGLISH_POSSESSIVE) != 0) {
      if (b.length() > 0) {
        b.append(" | ");
      }
      b.append("STEM_ENGLISH_POSSESSIVE");
    }

    return b.toString();
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("WordDelimiterGraphFilter(flags=");
    b.append(flagsToString(flags));
    b.append(')');
    return b.toString();
  }
  
  // questions:
  // negative numbers?  -42 indexed as just 42?
  // dollar sign?  $42
  // percent sign?  33%
  // downsides:  if source text is "powershot" then a query of "PowerShot" won't match!
}
