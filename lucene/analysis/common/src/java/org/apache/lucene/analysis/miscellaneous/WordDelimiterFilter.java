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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Arrays;

/**
 * Splits words into subwords and performs optional transformations on subword
 * groups. Words are split into subwords with the following rules:
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
 * The <b>combinations</b> parameter affects how subwords are combined:
 * <ul>
 * <li>combinations="0" causes no subword combinations: <code>"PowerShot"</code>
 * &#8594; <code>0:"Power", 1:"Shot"</code> (0 and 1 are the token positions)</li>
 * <li>combinations="1" means that in addition to the subwords, maximum runs of
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
 * </ul>
 * One use for {@link WordDelimiterFilter} is to help match words with different
 * subword delimiters. For example, if the source text contained "wi-fi" one may
 * want "wifi" "WiFi" "wi-fi" "wi+fi" queries to all match. One way of doing so
 * is to specify combinations="1" in the analyzer used for indexing, and
 * combinations="0" (the default) in the analyzer used for querying. Given that
 * the current {@link StandardTokenizer} immediately removes many intra-word
 * delimiters, it is recommended that this filter be used after a tokenizer that
 * does not do this (such as {@link WhitespaceTokenizer}).
 */
public final class WordDelimiterFilter extends TokenFilter {
  
  public static final int LOWER = 0x01;
  public static final int UPPER = 0x02;
  public static final int DIGIT = 0x04;
  public static final int SUBWORD_DELIM = 0x08;

  // combinations: for testing, not for setting bits
  public static final int ALPHA = 0x03;
  public static final int ALPHANUM = 0x07;

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
   * Causes maximum runs of word parts to be catenated:
   * <p>
   * "wi-fi" =&gt; "wifi"
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
   * If not set, causes case changes to be ignored (subwords will only be generated
   * given SUBWORD_DELIM tokens)
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
   * If not null is the set of tokens to protect from being delimited
   *
   */
  final CharArraySet protWords;

  private final int flags;
    
  private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncAttribute = addAttribute(PositionIncrementAttribute.class);
  private final TypeAttribute typeAttribute = addAttribute(TypeAttribute.class);

  // used for iterating word delimiter breaks
  private final WordDelimiterIterator iterator;

  // used for concatenating runs of similar typed subwords (word,number)
  private final WordDelimiterConcatenation concat = new WordDelimiterConcatenation();
  // number of subwords last output by concat.
  private int lastConcatCount = 0;

  // used for catenate all
  private final WordDelimiterConcatenation concatAll = new WordDelimiterConcatenation();

  // used for accumulating position increment gaps
  private int accumPosInc = 0;

  private char savedBuffer[] = new char[1024];
  private int savedStartOffset;
  private int savedEndOffset;
  private String savedType;
  private boolean hasSavedState = false;
  // if length by start + end offsets doesn't match the term text then assume
  // this is a synonym and don't adjust the offsets.
  private boolean hasIllegalOffsets = false;

  // for a run of the same subword type within a word, have we output anything?
  private boolean hasOutputToken = false;
  // when preserve original is on, have we output any token following it?
  // this token must have posInc=0!
  private boolean hasOutputFollowingOriginal = false;

  /**
   * Creates a new WordDelimiterFilter
   *
   * @param in TokenStream to be filtered
   * @param charTypeTable table containing character types
   * @param configurationFlags Flags configuring the filter
   * @param protWords If not null is the set of tokens to protect from being delimited
   */
  public WordDelimiterFilter(TokenStream in, byte[] charTypeTable, int configurationFlags, CharArraySet protWords) {
    super(in);
    this.flags = configurationFlags;
    this.protWords = protWords;
    this.iterator = new WordDelimiterIterator(
        charTypeTable, has(SPLIT_ON_CASE_CHANGE), has(SPLIT_ON_NUMERICS), has(STEM_ENGLISH_POSSESSIVE));
  }

  /**
   * Creates a new WordDelimiterFilter using {@link WordDelimiterIterator#DEFAULT_WORD_DELIM_TABLE}
   * as its charTypeTable
   *
   * @param in TokenStream to be filtered
   * @param configurationFlags Flags configuring the filter
   * @param protWords If not null is the set of tokens to protect from being delimited
   */
  public WordDelimiterFilter(TokenStream in, int configurationFlags, CharArraySet protWords) {
    this(in, WordDelimiterIterator.DEFAULT_WORD_DELIM_TABLE, configurationFlags, protWords);
  }

  @Override
  public boolean incrementToken() throws IOException {
    while (true) {
      if (!hasSavedState) {
        // process a new input word
        if (!input.incrementToken()) {
          return false;
        }

        int termLength = termAttribute.length();
        char[] termBuffer = termAttribute.buffer();
        
        accumPosInc += posIncAttribute.getPositionIncrement();

        iterator.setText(termBuffer, termLength);
        iterator.next();

        // word of no delimiters, or protected word: just return it
        if ((iterator.current == 0 && iterator.end == termLength) ||
            (protWords != null && protWords.contains(termBuffer, 0, termLength))) {
          posIncAttribute.setPositionIncrement(accumPosInc);
          accumPosInc = 0;
          first = false;
          return true;
        }
        
        // word of simply delimiters
        if (iterator.end == WordDelimiterIterator.DONE && !has(PRESERVE_ORIGINAL)) {
          // if the posInc is 1, simply ignore it in the accumulation
          // TODO: proper hole adjustment (FilteringTokenFilter-like) instead of this previous logic!
          if (posIncAttribute.getPositionIncrement() == 1 && !first) {
            accumPosInc--;
          }
          continue;
        }

        saveState();

        hasOutputToken = false;
        hasOutputFollowingOriginal = !has(PRESERVE_ORIGINAL);
        lastConcatCount = 0;
        
        if (has(PRESERVE_ORIGINAL)) {
          posIncAttribute.setPositionIncrement(accumPosInc);
          accumPosInc = 0;
          first = false;
          return true;
        }
      }
      
      // at the end of the string, output any concatenations
      if (iterator.end == WordDelimiterIterator.DONE) {
        if (!concat.isEmpty()) {
          if (flushConcatenation(concat)) {
            buffer();
            continue;
          }
        }
        
        if (!concatAll.isEmpty()) {
          // only if we haven't output this same combo above!
          if (concatAll.subwordCount > lastConcatCount) {
            concatAll.writeAndClear();
            buffer();
            continue;
          }
          concatAll.clear();
        }
        
        if (bufferedPos < bufferedLen) {
          if (bufferedPos == 0) {
            sorter.sort(0, bufferedLen);
          }
          clearAttributes();
          restoreState(buffered[bufferedPos++]);
          if (first && posIncAttribute.getPositionIncrement() == 0) {
            // can easily happen with strange combinations (e.g. not outputting numbers, but concat-all)
            posIncAttribute.setPositionIncrement(1);
          }
          first = false;
          return true;
        }
        
        // no saved concatenations, on to the next input word
        bufferedPos = bufferedLen = 0;
        hasSavedState = false;
        continue;
      }
      
      // word surrounded by delimiters: always output
      if (iterator.isSingleWord()) {
        generatePart(true);
        iterator.next();
        first = false;
        return true;
      }
      
      int wordType = iterator.type();
      
      // do we already have queued up incompatible concatenations?
      if (!concat.isEmpty() && (concat.type & wordType) == 0) {
        if (flushConcatenation(concat)) {
          hasOutputToken = false;
          buffer();
          continue;
        }
        hasOutputToken = false;
      }
      
      // add subwords depending upon options
      if (shouldConcatenate(wordType)) {
        if (concat.isEmpty()) {
          concat.type = wordType;
        }
        concatenate(concat);
      }
      
      // add all subwords (catenateAll)
      if (has(CATENATE_ALL)) {
        concatenate(concatAll);
      }
      
      // if we should output the word or number part
      if (shouldGenerateParts(wordType)) {
        generatePart(false);
        buffer();
      }
        
      iterator.next();
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    hasSavedState = false;
    concat.clear();
    concatAll.clear();
    accumPosInc = bufferedPos = bufferedLen = 0;
    first = true;
  }

  // ================================================= Helper Methods ================================================

  
  private AttributeSource.State buffered[] = new AttributeSource.State[8];
  private int startOff[] = new int[8];
  private int posInc[] = new int[8];
  private int bufferedLen = 0;
  private int bufferedPos = 0;
  private boolean first;
  
  private class OffsetSorter extends InPlaceMergeSorter {
    @Override
    protected int compare(int i, int j) {
      int cmp = Integer.compare(startOff[i], startOff[j]);
      if (cmp == 0) {
        cmp = Integer.compare(posInc[j], posInc[i]);
      }
      return cmp;
    }

    @Override
    protected void swap(int i, int j) {
      AttributeSource.State tmp = buffered[i];
      buffered[i] = buffered[j];
      buffered[j] = tmp;
      
      int tmp2 = startOff[i];
      startOff[i] = startOff[j];
      startOff[j] = tmp2;
      
      tmp2 = posInc[i];
      posInc[i] = posInc[j];
      posInc[j] = tmp2;
    }
  }
  
  final OffsetSorter sorter = new OffsetSorter();
  
  private void buffer() {
    if (bufferedLen == buffered.length) {
      int newSize = ArrayUtil.oversize(bufferedLen+1, 8);
      buffered = Arrays.copyOf(buffered, newSize);
      startOff = Arrays.copyOf(startOff, newSize);
      posInc = Arrays.copyOf(posInc, newSize);
    }
    startOff[bufferedLen] = offsetAttribute.startOffset();
    posInc[bufferedLen] = posIncAttribute.getPositionIncrement();
    buffered[bufferedLen] = captureState();
    bufferedLen++;
  }
  
  /**
   * Saves the existing attribute states
   */
  private void saveState() {
    // otherwise, we have delimiters, save state
    savedStartOffset = offsetAttribute.startOffset();
    savedEndOffset = offsetAttribute.endOffset();
    // if length by start + end offsets doesn't match the term text then assume this is a synonym and don't adjust the offsets.
    hasIllegalOffsets = (savedEndOffset - savedStartOffset != termAttribute.length());
    savedType = typeAttribute.type();

    if (savedBuffer.length < termAttribute.length()) {
      savedBuffer = new char[ArrayUtil.oversize(termAttribute.length(), RamUsageEstimator.NUM_BYTES_CHAR)];
    }

    System.arraycopy(termAttribute.buffer(), 0, savedBuffer, 0, termAttribute.length());
    iterator.text = savedBuffer;

    hasSavedState = true;
  }

  /**
   * Flushes the given WordDelimiterConcatenation by either writing its concat and then clearing, or just clearing.
   *
   * @param concatenation WordDelimiterConcatenation that will be flushed
   * @return {@code true} if the concatenation was written before it was cleared, {@code false} otherwise
   */
  private boolean flushConcatenation(WordDelimiterConcatenation concatenation) {
    lastConcatCount = concatenation.subwordCount;
    if (concatenation.subwordCount != 1 || !shouldGenerateParts(concatenation.type)) {
      concatenation.writeAndClear();
      return true;
    }
    concatenation.clear();
    return false;
  }

  /**
   * Determines whether to concatenate a word or number if the current word is the given type
   *
   * @param wordType Type of the current word used to determine if it should be concatenated
   * @return {@code true} if concatenation should occur, {@code false} otherwise
   */
  private boolean shouldConcatenate(int wordType) {
    return (has(CATENATE_WORDS) && isAlpha(wordType)) || (has(CATENATE_NUMBERS) && isDigit(wordType));
  }

  /**
   * Determines whether a word/number part should be generated for a word of the given type
   *
   * @param wordType Type of the word used to determine if a word/number part should be generated
   * @return {@code true} if a word/number part should be generated, {@code false} otherwise
   */
  private boolean shouldGenerateParts(int wordType) {
    return (has(GENERATE_WORD_PARTS) && isAlpha(wordType)) || (has(GENERATE_NUMBER_PARTS) && isDigit(wordType));
  }

  /**
   * Concatenates the saved buffer to the given WordDelimiterConcatenation
   *
   * @param concatenation WordDelimiterConcatenation to concatenate the buffer to
   */
  private void concatenate(WordDelimiterConcatenation concatenation) {
    if (concatenation.isEmpty()) {
      concatenation.startOffset = savedStartOffset + iterator.current;
    }
    concatenation.append(savedBuffer, iterator.current, iterator.end - iterator.current);
    concatenation.endOffset = savedStartOffset + iterator.end;
  }

  /**
   * Generates a word/number part, updating the appropriate attributes
   *
   * @param isSingleWord {@code true} if the generation is occurring from a single word, {@code false} otherwise
   */
  private void generatePart(boolean isSingleWord) {
    clearAttributes();
    termAttribute.copyBuffer(savedBuffer, iterator.current, iterator.end - iterator.current);

    int startOffset = savedStartOffset + iterator.current;
    int endOffset = savedStartOffset + iterator.end;
    
    if (hasIllegalOffsets) {
      // historically this filter did this regardless for 'isSingleWord', 
      // but we must do a sanity check:
      if (isSingleWord && startOffset <= savedEndOffset) {
        offsetAttribute.setOffset(startOffset, savedEndOffset);
      } else {
        offsetAttribute.setOffset(savedStartOffset, savedEndOffset);
      }
    } else {
      offsetAttribute.setOffset(startOffset, endOffset);
    }
    posIncAttribute.setPositionIncrement(position(false));
    typeAttribute.setType(savedType);
  }

  /**
   * Get the position increment gap for a subword or concatenation
   *
   * @param inject true if this token wants to be injected
   * @return position increment gap
   */
  private int position(boolean inject) {
    int posInc = accumPosInc;

    if (hasOutputToken) {
      accumPosInc = 0;
      return inject ? 0 : Math.max(1, posInc);
    }

    hasOutputToken = true;
    
    if (!hasOutputFollowingOriginal) {
      // the first token following the original is 0 regardless
      hasOutputFollowingOriginal = true;
      return 0;
    }
    // clear the accumulated position increment
    accumPosInc = 0;
    return Math.max(1, posInc);
  }

  /**
   * Checks if the given word type includes {@link #ALPHA}
   *
   * @param type Word type to check
   * @return {@code true} if the type contains ALPHA, {@code false} otherwise
   */
  static boolean isAlpha(int type) {
    return (type & ALPHA) != 0;
  }

  /**
   * Checks if the given word type includes {@link #DIGIT}
   *
   * @param type Word type to check
   * @return {@code true} if the type contains DIGIT, {@code false} otherwise
   */
  static boolean isDigit(int type) {
    return (type & DIGIT) != 0;
  }

  /**
   * Checks if the given word type includes {@link #SUBWORD_DELIM}
   *
   * @param type Word type to check
   * @return {@code true} if the type contains SUBWORD_DELIM, {@code false} otherwise
   */
  static boolean isSubwordDelim(int type) {
    return (type & SUBWORD_DELIM) != 0;
  }

  /**
   * Checks if the given word type includes {@link #UPPER}
   *
   * @param type Word type to check
   * @return {@code true} if the type contains UPPER, {@code false} otherwise
   */
  static boolean isUpper(int type) {
    return (type & UPPER) != 0;
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
    int startOffset;
    int endOffset;
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
     * Writes the concatenation to the attributes
     */
    void write() {
      clearAttributes();
      if (termAttribute.length() < buffer.length()) {
        termAttribute.resizeBuffer(buffer.length());
      }
      char termbuffer[] = termAttribute.buffer();
      
      buffer.getChars(0, buffer.length(), termbuffer, 0);
      termAttribute.setLength(buffer.length());
        
      if (hasIllegalOffsets) {
        offsetAttribute.setOffset(savedStartOffset, savedEndOffset);
      }
      else {
        offsetAttribute.setOffset(startOffset, endOffset);
      }
      posIncAttribute.setPositionIncrement(position(true));
      typeAttribute.setType(savedType);
      accumPosInc = 0;
    }

    /**
     * Determines if the concatenation is empty
     *
     * @return {@code true} if the concatenation is empty, {@code false} otherwise
     */
    boolean isEmpty() {
      return buffer.length() == 0;
    }

    /**
     * Clears the concatenation and resets its state
     */
    void clear() {
      buffer.setLength(0);
      startOffset = endOffset = type = subwordCount = 0;
    }

    /**
     * Convenience method for the common scenario of having to write the concetenation and then clearing its state
     */
    void writeAndClear() {
      write();
      clear();
    }
  }
  // questions:
  // negative numbers?  -42 indexed as just 42?
  // dollar sign?  $42
  // percent sign?  33%
  // downsides:  if source text is "powershot" then a query of "PowerShot" won't match!
}
