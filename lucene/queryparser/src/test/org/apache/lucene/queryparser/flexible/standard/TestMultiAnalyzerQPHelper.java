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
package org.apache.lucene.queryparser.flexible.standard;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;
import org.apache.lucene.util.LuceneTestCase;

/**
 * This test case is a copy of the core Lucene query parser test, it was adapted
 * to use new QueryParserHelper instead of the old query parser.
 * 
 * Test QueryParser's ability to deal with Analyzers that return more than one
 * token per position or that return tokens with a position increment &gt; 1.
 */
public class TestMultiAnalyzerQPHelper extends LuceneTestCase {

  private static int multiToken = 0;

  public void testMultiAnalyzer() throws QueryNodeException {

    StandardQueryParser qp = new StandardQueryParser();
    qp.setAnalyzer(new MultiAnalyzer());

    // trivial, no multiple tokens:
    assertEquals("foo", qp.parse("foo", "").toString());
    assertEquals("foo", qp.parse("\"foo\"", "").toString());
    assertEquals("foo foobar", qp.parse("foo foobar", "").toString());
    assertEquals("\"foo foobar\"", qp.parse("\"foo foobar\"", "").toString());
    assertEquals("\"foo foobar blah\"", qp.parse("\"foo foobar blah\"", "")
        .toString());

    // two tokens at the same position:
    assertEquals("(multi multi2) foo", qp.parse("multi foo", "").toString());
    assertEquals("foo (multi multi2)", qp.parse("foo multi", "").toString());
    assertEquals("(multi multi2) (multi multi2)", qp.parse("multi multi", "")
        .toString());
    assertEquals("+(foo (multi multi2)) +(bar (multi multi2))", qp.parse(
        "+(foo multi) +(bar multi)", "").toString());
    assertEquals("+(foo (multi multi2)) field:\"bar (multi multi2)\"", qp
        .parse("+(foo multi) field:\"bar multi\"", "").toString());

    // phrases:
    assertEquals("\"(multi multi2) foo\"", qp.parse("\"multi foo\"", "")
        .toString());
    assertEquals("\"foo (multi multi2)\"", qp.parse("\"foo multi\"", "")
        .toString());
    assertEquals("\"foo (multi multi2) foobar (multi multi2)\"", qp.parse(
        "\"foo multi foobar multi\"", "").toString());

    // fields:
    assertEquals("(field:multi field:multi2) field:foo", qp.parse(
        "field:multi field:foo", "").toString());
    assertEquals("field:\"(multi multi2) foo\"", qp.parse(
        "field:\"multi foo\"", "").toString());

    // three tokens at one position:
    assertEquals("triplemulti multi3 multi2", qp.parse("triplemulti", "")
        .toString());
    assertEquals("foo (triplemulti multi3 multi2) foobar", qp.parse(
        "foo triplemulti foobar", "").toString());

    // phrase with non-default slop:
    assertEquals("\"(multi multi2) foo\"~10", qp.parse("\"multi foo\"~10", "")
        .toString());

    // phrase with non-default boost:
    assertEquals("\"(multi multi2) foo\"^2.0", qp.parse("\"multi foo\"^2", "")
        .toString());

    // phrase after changing default slop
    qp.setDefaultPhraseSlop(99);
    assertEquals("\"(multi multi2) foo\"~99 bar", qp.parse("\"multi foo\" bar",
        "").toString());
    assertEquals("\"(multi multi2) foo\"~99 \"foo bar\"~2", qp.parse(
        "\"multi foo\" \"foo bar\"~2", "").toString());
    qp.setDefaultPhraseSlop(0);

    // non-default operator:
    qp.setDefaultOperator(StandardQueryConfigHandler.Operator.AND);
    assertEquals("+(multi multi2) +foo", qp.parse("multi foo", "").toString());

  }

  // public void testMultiAnalyzerWithSubclassOfQueryParser() throws
  // ParseException {
  // this test doesn't make sense when using the new QueryParser API
  // DumbQueryParser qp = new DumbQueryParser("", new MultiAnalyzer());
  // qp.setPhraseSlop(99); // modified default slop
  //
  // // direct call to (super's) getFieldQuery to demonstrate differnce
  // // between phrase and multiphrase with modified default slop
  // assertEquals("\"foo bar\"~99",
  // qp.getSuperFieldQuery("","foo bar").toString());
  // assertEquals("\"(multi multi2) bar\"~99",
  // qp.getSuperFieldQuery("","multi bar").toString());
  //
  //    
  // // ask sublcass to parse phrase with modified default slop
  // assertEquals("\"(multi multi2) foo\"~99 bar",
  // qp.parse("\"multi foo\" bar").toString());
  //    
  // }

  public void testPosIncrementAnalyzer() throws QueryNodeException {
    StandardQueryParser qp = new StandardQueryParser();
    qp.setAnalyzer(new PosIncrementAnalyzer());

    assertEquals("quick brown", qp.parse("the quick brown", "").toString());
    assertEquals("\"? quick brown\"", qp.parse("\"the quick brown\"", "")
        .toString());
    assertEquals("quick brown fox", qp.parse("the quick brown fox", "")
        .toString());
    assertEquals("\"? quick brown fox\"", qp.parse("\"the quick brown fox\"", "")
        .toString());
  }

  /**
   * Expands "multi" to "multi" and "multi2", both at the same position, and
   * expands "triplemulti" to "triplemulti", "multi3", and "multi2".
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

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

    public TestFilter(TokenStream in) {
      super(in);
    }

    @Override
    public final boolean incrementToken() throws java.io.IOException {
      if (multiToken > 0) {
        termAtt.setEmpty().append("multi" + (multiToken + 1));
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
   * Analyzes "the quick brown" as: quick(incr=2) brown(incr=1). Does not work
   * correctly for input other than "the quick brown ...".
   */
  private class PosIncrementAnalyzer extends Analyzer {

    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer result = new MockTokenizer(MockTokenizer.WHITESPACE, true);
      return new TokenStreamComponents(result, new TestPosIncrementFilter(result));
    }
  }

  private class TestPosIncrementFilter extends TokenFilter {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

    public TestPosIncrementFilter(TokenStream in) {
      super(in);
    }

    @Override
    public final boolean incrementToken() throws java.io.IOException {
      while (input.incrementToken()) {
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
