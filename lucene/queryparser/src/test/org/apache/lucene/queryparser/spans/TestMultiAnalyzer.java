package org.apache.lucene.queryparser.spans;

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

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.spans.SpanQueryParser;

/**
 * Test SpanQueryParser's ability to deal with Analyzers that return more
 * than one token per position or that return tokens with a position
 * increment &gt; 1.
 * 
 * Copied nearly verbatim from TestMultiAnalyzer for classic QueryParser!!!
 *
 */
public class TestMultiAnalyzer extends BaseTokenStreamTestCase {
  private static int multiToken = 0;

  public void testMultiAnalyzer() throws ParseException {

    SpanQueryParser qp = new SpanQueryParser(TEST_VERSION_CURRENT, "", new MultiAnalyzer());

    // trivial, no multiple tokens:
    assertEquals("foo", qp.parse("foo").toString());
    assertEquals("foo", qp.parse("\"foo\"").toString());
    assertEquals("foo foobar", qp.parse("foo foobar").toString());
    assertEquals("spanNear([foo, foobar], 0, true)", qp.parse("\"foo foobar\"").toString());
    assertEquals("spanNear([foo, foobar, blah], 0, true)", qp.parse("\"foo foobar blah\"").toString());

    // two tokens at the same position:
    assertEquals("spanOr([multi, multi2]) foo", qp.parse("multi foo").toString());
    assertEquals("foo spanOr([multi, multi2])", qp.parse("foo multi").toString());
    assertEquals("spanOr([multi, multi2]) spanOr([multi, multi2])", qp.parse("multi multi").toString());
    assertEquals("+(foo spanOr([multi, multi2])) +(bar spanOr([multi, multi2]))",
        qp.parse("+(foo multi) +(bar multi)").toString());
    assertEquals("+(foo spanOr([multi, multi2])) spanNear([field:bar, spanOr([field:multi, field:multi2])], 0, true)",
        qp.parse("+(foo multi) field:\"bar multi\"").toString());

    // phrases:
    assertEquals("spanNear([spanOr([multi, multi2]), foo], 0, true)", qp.parse("\"multi foo\"").toString());
    assertEquals("spanNear([foo, spanOr([multi, multi2])], 0, true)", qp.parse("\"foo multi\"").toString());
    assertEquals("spanNear([foo, spanOr([multi, multi2]), foobar, spanOr([multi, multi2])], 0, true)",
        qp.parse("\"foo multi foobar multi\"").toString());

    // fields:
    assertEquals("spanOr([field:multi, field:multi2]) field:foo", qp.parse("field:multi field:foo").toString());
    assertEquals("spanNear([spanOr([field:multi, field:multi2]), field:foo], 0, true)", qp.parse("field:\"multi foo\"").toString());

    // three tokens at one position:
    assertEquals("spanOr([triplemulti, multi3, multi2])", qp.parse("triplemulti").toString());
    assertEquals("foo spanOr([triplemulti, multi3, multi2]) foobar",
        qp.parse("foo triplemulti foobar").toString());

    // phrase with non-default slop:
    assertEquals("spanNear([spanOr([multi, multi2]), foo], 10, false)", qp.parse("\"multi foo\"~10").toString());

    // phrase with non-default boost:
    assertEquals("spanNear([spanOr([multi, multi2]), foo], 0, true)^2.0", qp.parse("\"multi foo\"^2").toString());

    // phrase after changing default slop
    qp.setPhraseSlop(99);
    assertEquals("spanNear([spanOr([multi, multi2]), foo], 99, false) bar",
        qp.parse("\"multi foo\" bar").toString());
    assertEquals("spanNear([spanOr([multi, multi2]), foo], 99, false) spanNear([foo, bar], 2, false)",
        qp.parse("\"multi foo\" \"foo bar\"~2").toString());
    qp.setPhraseSlop(0);
  }

  public void testPosIncrementAnalyzer() throws ParseException {
    SpanQueryParser qp = new SpanQueryParser(TEST_VERSION_CURRENT,"", new PosIncrementAnalyzer());
    assertEquals("quick brown", qp.parse("the quick brown").toString());
    assertEquals("quick brown fox", qp.parse("the quick brown fox").toString());
  }

  /**
   * Expands "multi" to "multi" and "multi2", both at the same position,
   * and expands "triplemulti" to "triplemulti", "multi3", and "multi2".  
   */
  private class MultiAnalyzer extends Analyzer {

    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer result = new MockTokenizer(MockTokenizer.WHITESPACE, true);
      return new TokenStreamComponents(result, new TestFilter(result));
    }
  }

  private final class TestFilter extends TokenFilter {

    private String prevType;
    private int prevStartOffset;
    private int prevEndOffset;

    private final CharTermAttribute termAtt;
    private final PositionIncrementAttribute posIncrAtt;
    private final OffsetAttribute offsetAtt;
    private final TypeAttribute typeAtt;

    public TestFilter(TokenStream in) {
      super(in);
      termAtt = addAttribute(CharTermAttribute.class);
      posIncrAtt = addAttribute(PositionIncrementAttribute.class);
      offsetAtt = addAttribute(OffsetAttribute.class);
      typeAtt = addAttribute(TypeAttribute.class);
    }

    @Override
    public final boolean incrementToken() throws IOException {
      if (multiToken > 0) {
        termAtt.setEmpty().append("multi"+(multiToken+1));
        offsetAtt.setOffset(prevStartOffset, prevEndOffset);
        typeAtt.setType(prevType);
        posIncrAtt.setPositionIncrement(0);
        multiToken--;
        return true;
      } else {
        boolean next = input.incrementToken();
        if (!next) {
          return false;
        }
        prevType = typeAtt.type();
        prevStartOffset = offsetAtt.startOffset();
        prevEndOffset = offsetAtt.endOffset();
        String text = termAtt.toString();
        if (text.equals("triplemulti")) {
          multiToken = 2;
          return true;
        } else if (text.equals("multi")) {
          multiToken = 1;
          return true;
        } else {
          return true;
        }
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      this.prevType = null;
      this.prevStartOffset = 0;
      this.prevEndOffset = 0;
    }
  }

  /**
   * Analyzes "the quick brown" as: quick(incr=2) brown(incr=1).
   * Does not work correctly for input other than "the quick brown ...".
   */
  private class PosIncrementAnalyzer extends Analyzer {

    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer result = new MockTokenizer(MockTokenizer.WHITESPACE, true);
      return new TokenStreamComponents(result, new TestPosIncrementFilter(result));
    }
  }

  private final class TestPosIncrementFilter extends TokenFilter {

    CharTermAttribute termAtt;
    PositionIncrementAttribute posIncrAtt;

    public TestPosIncrementFilter(TokenStream in) {
      super(in);
      termAtt = addAttribute(CharTermAttribute.class);
      posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    }

    @Override
    public final boolean incrementToken() throws IOException {
      while(input.incrementToken()) {
        if (termAtt.toString().equals("the")) {
          // stopword, do nothing
        } else if (termAtt.toString().equals("quick")) {
          posIncrAtt.setPositionIncrement(2);
          return true;
        } else {
          posIncrAtt.setPositionIncrement(1);
          return true;
        }
      }
      return false;
    }
  }
}
