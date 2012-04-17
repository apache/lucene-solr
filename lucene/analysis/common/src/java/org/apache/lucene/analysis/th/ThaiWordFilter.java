package org.apache.lucene.analysis.th;

/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.lang.Character.UnicodeBlock;
import java.text.BreakIterator;
import java.util.Locale;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.util.CharArrayIterator;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Version;

/**
 * {@link TokenFilter} that use {@link java.text.BreakIterator} to break each 
 * Token that is Thai into separate Token(s) for each Thai word.
 * <p>Please note: Since matchVersion 3.1 on, this filter no longer lowercases non-thai text.
 * {@link ThaiAnalyzer} will insert a {@link LowerCaseFilter} before this filter
 * so the behaviour of the Analyzer does not change. With version 3.1, the filter handles
 * position increments correctly.
 * <p>WARNING: this filter may not be supported by all JREs.
 *    It is known to work with Sun/Oracle and Harmony JREs.
 *    If your application needs to be fully portable, consider using ICUTokenizer instead,
 *    which uses an ICU Thai BreakIterator that will always be available.
 */
public final class ThaiWordFilter extends TokenFilter {
  /** 
   * True if the JRE supports a working dictionary-based breakiterator for Thai.
   * If this is false, this filter will not work at all!
   */
  public static final boolean DBBI_AVAILABLE;
  private static final BreakIterator proto = BreakIterator.getWordInstance(new Locale("th"));
  static {
    // check that we have a working dictionary-based break iterator for thai
    proto.setText("ภาษาไทย");
    DBBI_AVAILABLE = proto.isBoundary(4);
  }
  private final BreakIterator breaker = (BreakIterator) proto.clone();
  private final CharArrayIterator charIterator = CharArrayIterator.newWordInstance();
  
  private final boolean handlePosIncr;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posAtt = addAttribute(PositionIncrementAttribute.class);
  
  private AttributeSource clonedToken = null;
  private CharTermAttribute clonedTermAtt = null;
  private OffsetAttribute clonedOffsetAtt = null;
  private boolean hasMoreTokensInClone = false;
  private boolean hasIllegalOffsets = false; // only if the length changed before this filter

  /** Creates a new ThaiWordFilter with the specified match version. */
  public ThaiWordFilter(Version matchVersion, TokenStream input) {
    super(matchVersion.onOrAfter(Version.LUCENE_31) ?
      input : new LowerCaseFilter(matchVersion, input));
    if (!DBBI_AVAILABLE)
      throw new UnsupportedOperationException("This JRE does not have support for Thai segmentation");
    handlePosIncr = matchVersion.onOrAfter(Version.LUCENE_31);
  }
  
  @Override
  public boolean incrementToken() throws IOException {
    if (hasMoreTokensInClone) {
      int start = breaker.current();
      int end = breaker.next();
      if (end != BreakIterator.DONE) {
        clonedToken.copyTo(this);
        termAtt.copyBuffer(clonedTermAtt.buffer(), start, end - start);
        if (hasIllegalOffsets) {
          offsetAtt.setOffset(clonedOffsetAtt.startOffset(), clonedOffsetAtt.endOffset());
        } else {
          offsetAtt.setOffset(clonedOffsetAtt.startOffset() + start, clonedOffsetAtt.startOffset() + end);
        }
        if (handlePosIncr) posAtt.setPositionIncrement(1);
        return true;
      }
      hasMoreTokensInClone = false;
    }

    if (!input.incrementToken()) {
      return false;
    }
    
    if (termAtt.length() == 0 || UnicodeBlock.of(termAtt.charAt(0)) != UnicodeBlock.THAI) {
      return true;
    }
    
    hasMoreTokensInClone = true;
    
    // if length by start + end offsets doesn't match the term text then assume
    // this is a synonym and don't adjust the offsets.
    hasIllegalOffsets = offsetAtt.endOffset() - offsetAtt.startOffset() != termAtt.length();

    // we lazy init the cloned token, as in ctor not all attributes may be added
    if (clonedToken == null) {
      clonedToken = cloneAttributes();
      clonedTermAtt = clonedToken.getAttribute(CharTermAttribute.class);
      clonedOffsetAtt = clonedToken.getAttribute(OffsetAttribute.class);
    } else {
      this.copyTo(clonedToken);
    }
    
    // reinit CharacterIterator
    charIterator.setText(clonedTermAtt.buffer(), 0, clonedTermAtt.length());
    breaker.setText(charIterator);
    int end = breaker.next();
    if (end != BreakIterator.DONE) {
      termAtt.setLength(end);
      if (hasIllegalOffsets) {
        offsetAtt.setOffset(clonedOffsetAtt.startOffset(), clonedOffsetAtt.endOffset());
      } else {
        offsetAtt.setOffset(clonedOffsetAtt.startOffset(), clonedOffsetAtt.startOffset() + end);
      }
      // position increment keeps as it is for first token
      return true;
    }
    return false;
  }
  
  @Override
  public void reset() throws IOException {
    super.reset();
    hasMoreTokensInClone = false;
    clonedToken = null;
    clonedTermAtt = null;
    clonedOffsetAtt = null;
  }
}
