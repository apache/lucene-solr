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
import java.util.Locale;
import java.lang.Character.UnicodeBlock;
import javax.swing.text.Segment;
import java.text.BreakIterator;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Version;

/**
 * {@link TokenFilter} that use {@link java.text.BreakIterator} to break each 
 * Token that is Thai into separate Token(s) for each Thai word.
 * <p>Please note: Since matchVersion 3.1 on, this filter no longer lowercases non-thai text.
 * {@link ThaiAnalyzer} will insert a {@link LowerCaseFilter} before this filter
 * so the behaviour of the Analyzer does not change. With version 3.1, the filter handles
 * position increments correctly.
 */
public final class ThaiWordFilter extends TokenFilter {
  
  private final BreakIterator breaker = BreakIterator.getWordInstance(new Locale("th"));
  private final Segment charIterator = new Segment();
  
  private final boolean handlePosIncr;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posAtt = addAttribute(PositionIncrementAttribute.class);
  
  private AttributeSource clonedToken = null;
  private CharTermAttribute clonedTermAtt = null;
  private OffsetAttribute clonedOffsetAtt = null;
  private boolean hasMoreTokensInClone = false;

  /** Creates a new ThaiWordFilter that also lowercases non-thai text.
   * @deprecated Use the ctor with {@code matchVersion} instead!
   */
  @Deprecated
  public ThaiWordFilter(TokenStream input) {
    this(Version.LUCENE_30, input);
  }
  
  /** Creates a new ThaiWordFilter with the specified match version. */
  public ThaiWordFilter(Version matchVersion, TokenStream input) {
    super(matchVersion.onOrAfter(Version.LUCENE_31) ?
      input : new LowerCaseFilter(matchVersion, input));
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
        offsetAtt.setOffset(clonedOffsetAtt.startOffset() + start, clonedOffsetAtt.startOffset() + end);
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

    // we lazy init the cloned token, as in ctor not all attributes may be added
    if (clonedToken == null) {
      clonedToken = cloneAttributes();
      clonedTermAtt = clonedToken.getAttribute(CharTermAttribute.class);
      clonedOffsetAtt = clonedToken.getAttribute(OffsetAttribute.class);
    } else {
      this.copyTo(clonedToken);
    }
    
    // reinit CharacterIterator
    charIterator.array = clonedTermAtt.buffer();
    charIterator.offset = 0;
    charIterator.count = clonedTermAtt.length();
    breaker.setText(charIterator);
    int end = breaker.next();
    if (end != BreakIterator.DONE) {
      termAtt.setLength(end);
      offsetAtt.setOffset(clonedOffsetAtt.startOffset(), clonedOffsetAtt.startOffset() + end);
      // position increment keeps as it is for first token
      return true;
    }
    return false;
  }
  
  @Override
  public void reset() throws IOException {
    super.reset();
    hasMoreTokensInClone = false;
  }
}
