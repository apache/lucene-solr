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
package org.apache.lucene.search.uhighlight;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * Provides a base class for analysis based offset strategies to extend from.
 * Requires an Analyzer and provides an override-able method for altering how
 * the TokenStream is created.
 *
 * @lucene.internal
 */
public abstract class AnalysisOffsetStrategy extends FieldOffsetStrategy {

  protected final Analyzer analyzer;

  public AnalysisOffsetStrategy(String field, BytesRef[] queryTerms, PhraseHelper phraseHelper, CharacterRunAutomaton[] automata, Analyzer analyzer) {
    super(field, queryTerms, phraseHelper, automata);
    this.analyzer = analyzer;
    if (analyzer.getOffsetGap(field) != 1) { // note: 1 is the default. It is RARELY changed.
      throw new IllegalArgumentException(
          "offset gap of the provided analyzer should be 1 (field " + field + ")");
    }
  }

  @Override
  public final UnifiedHighlighter.OffsetSource getOffsetSource() {
    return UnifiedHighlighter.OffsetSource.ANALYSIS;
  }

  protected TokenStream tokenStream(String content) throws IOException {
    // If there is no splitChar in content then we needn't wrap:
    int splitCharIdx = content.indexOf(UnifiedHighlighter.MULTIVAL_SEP_CHAR);
    if (splitCharIdx == -1) {
      return analyzer.tokenStream(field, content);
    }

    TokenStream subTokenStream = analyzer.tokenStream(field, content.substring(0, splitCharIdx));

    return new MultiValueTokenStream(subTokenStream, field, analyzer, content, UnifiedHighlighter.MULTIVAL_SEP_CHAR, splitCharIdx);
  }

  /**
   * Wraps an {@link Analyzer} and string text that represents multiple values delimited by a specified character. This
   * exposes a TokenStream that matches what would get indexed considering the
   * {@link Analyzer#getPositionIncrementGap(String)}. Currently this assumes {@link Analyzer#getOffsetGap(String)} is
   * 1; an exception will be thrown if it isn't.
   * <br />
   * It would be more orthogonal for this to be an Analyzer since we're wrapping an Analyzer but doing so seems like
   * more work.  The underlying components see a Reader not a String -- and the String is easy to
   * split up without redundant buffering.
   *
   * @lucene.internal
   */
  // TODO we could make this go away.  MemoryIndexOffsetStrategy could simply split and analyze each value into the
  //   MemoryIndex. TokenStreamOffsetStrategy's hack TokenStreamPostingsEnum could incorporate this logic,
  //   albeit with less code, less hack.
  private static final class MultiValueTokenStream extends TokenFilter {

    private final String fieldName;
    private final Analyzer indexAnalyzer;
    private final String content;
    private final char splitChar;

    private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    private int startValIdx = 0;
    private int endValIdx;
    private int remainingPosInc = 0;

    private MultiValueTokenStream(TokenStream subTokenStream, String fieldName, Analyzer indexAnalyzer,
                                  String content, char splitChar, int splitCharIdx) {
      super(subTokenStream); // subTokenStream is already initialized to operate on the first value
      this.fieldName = fieldName;
      this.indexAnalyzer = indexAnalyzer;
      this.content = content;
      this.splitChar = splitChar;
      this.endValIdx = splitCharIdx;
    }

    @Override
    public void reset() throws IOException {
      if (startValIdx != 0) {
        throw new IllegalStateException("This TokenStream wasn't developed to be re-used.");
        // ... although we could if a need for it arises.
      }
      super.reset();
    }

    @Override
    public boolean incrementToken() throws IOException {
      while (true) {

        if (input.incrementToken()) {
          // Position tracking:
          if (remainingPosInc > 0) {//usually true first token of additional values (not first val)
            posIncAtt.setPositionIncrement(remainingPosInc + posIncAtt.getPositionIncrement());
            remainingPosInc = 0;//reset
          }
          // Offset tracking:
          offsetAtt.setOffset(
              startValIdx + offsetAtt.startOffset(),
              startValIdx + offsetAtt.endOffset()
          );
          return true;
        }

        if (endValIdx == content.length()) {//no more
          return false;
        }

        input.end(); // might adjust position increment
        remainingPosInc += posIncAtt.getPositionIncrement();
        input.close();
        remainingPosInc += indexAnalyzer.getPositionIncrementGap(fieldName);

        // Get new tokenStream based on next segment divided by the splitChar
        startValIdx = endValIdx + 1;
        endValIdx = content.indexOf(splitChar, startValIdx);
        if (endValIdx == -1) {//EOF
          endValIdx = content.length();
        }
        TokenStream tokenStream = indexAnalyzer.tokenStream(fieldName, content.substring(startValIdx, endValIdx));
        if (tokenStream != input) {// (input is defined in TokenFilter set in the constructor)
          // This is a grand trick we do -- knowing that the analyzer's re-use strategy is going to produce the
          // very same tokenStream instance and thus have the same AttributeSource as this wrapping TokenStream
          // since we used it as our input in the constructor.
          // Were this not the case, we'd have to copy every attribute of interest since we can't alter the
          // AttributeSource of this wrapping TokenStream post-construction (it's all private/final).
          // If this is a problem, we could do that instead; maybe with a custom CharTermAttribute that allows
          // us to easily set the char[] reference without literally copying char by char.
          throw new IllegalStateException("Require TokenStream re-use.  Unsupported re-use strategy?: " +
              indexAnalyzer.getReuseStrategy());
        }
        tokenStream.reset();
      } // while loop to increment token of this new value
    }

    @Override
    public void end() throws IOException {
      super.end();
      // Offset tracking:
      offsetAtt.setOffset(
          startValIdx + offsetAtt.startOffset(),
          startValIdx + offsetAtt.endOffset());
    }

  }
}
