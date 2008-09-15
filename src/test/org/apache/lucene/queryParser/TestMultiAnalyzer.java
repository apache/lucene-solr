package org.apache.lucene.queryParser;

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

import java.io.Reader;

import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.search.Query;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Test QueryParser's ability to deal with Analyzers that return more
 * than one token per position or that return tokens with a position
 * increment &gt; 1.
 *
 */
public class TestMultiAnalyzer extends LuceneTestCase {

  private static int multiToken = 0;

  public void testMultiAnalyzer() throws ParseException {
    
    QueryParser qp = new QueryParser("", new MultiAnalyzer());

    // trivial, no multiple tokens:
    assertEquals("foo", qp.parse("foo").toString());
    assertEquals("foo", qp.parse("\"foo\"").toString());
    assertEquals("foo foobar", qp.parse("foo foobar").toString());
    assertEquals("\"foo foobar\"", qp.parse("\"foo foobar\"").toString());
    assertEquals("\"foo foobar blah\"", qp.parse("\"foo foobar blah\"").toString());

    // two tokens at the same position:
    assertEquals("(multi multi2) foo", qp.parse("multi foo").toString());
    assertEquals("foo (multi multi2)", qp.parse("foo multi").toString());
    assertEquals("(multi multi2) (multi multi2)", qp.parse("multi multi").toString());
    assertEquals("+(foo (multi multi2)) +(bar (multi multi2))",
        qp.parse("+(foo multi) +(bar multi)").toString());
    assertEquals("+(foo (multi multi2)) field:\"bar (multi multi2)\"",
        qp.parse("+(foo multi) field:\"bar multi\"").toString());

    // phrases:
    assertEquals("\"(multi multi2) foo\"", qp.parse("\"multi foo\"").toString());
    assertEquals("\"foo (multi multi2)\"", qp.parse("\"foo multi\"").toString());
    assertEquals("\"foo (multi multi2) foobar (multi multi2)\"",
        qp.parse("\"foo multi foobar multi\"").toString());

    // fields:
    assertEquals("(field:multi field:multi2) field:foo", qp.parse("field:multi field:foo").toString());
    assertEquals("field:\"(multi multi2) foo\"", qp.parse("field:\"multi foo\"").toString());

    // three tokens at one position:
    assertEquals("triplemulti multi3 multi2", qp.parse("triplemulti").toString());
    assertEquals("foo (triplemulti multi3 multi2) foobar",
        qp.parse("foo triplemulti foobar").toString());

    // phrase with non-default slop:
    assertEquals("\"(multi multi2) foo\"~10", qp.parse("\"multi foo\"~10").toString());

    // phrase with non-default boost:
    assertEquals("\"(multi multi2) foo\"^2.0", qp.parse("\"multi foo\"^2").toString());

    // phrase after changing default slop
    qp.setPhraseSlop(99);
    assertEquals("\"(multi multi2) foo\"~99 bar",
                 qp.parse("\"multi foo\" bar").toString());
    assertEquals("\"(multi multi2) foo\"~99 \"foo bar\"~2",
                 qp.parse("\"multi foo\" \"foo bar\"~2").toString());
    qp.setPhraseSlop(0);

    // non-default operator:
    qp.setDefaultOperator(QueryParser.AND_OPERATOR);
    assertEquals("+(multi multi2) +foo", qp.parse("multi foo").toString());

  }
    
  public void testMultiAnalyzerWithSubclassOfQueryParser() throws ParseException {

    DumbQueryParser qp = new DumbQueryParser("", new MultiAnalyzer());
    qp.setPhraseSlop(99); // modified default slop

    // direct call to (super's) getFieldQuery to demonstrate differnce
    // between phrase and multiphrase with modified default slop
    assertEquals("\"foo bar\"~99",
                 qp.getSuperFieldQuery("","foo bar").toString());
    assertEquals("\"(multi multi2) bar\"~99",
                 qp.getSuperFieldQuery("","multi bar").toString());

    
    // ask sublcass to parse phrase with modified default slop
    assertEquals("\"(multi multi2) foo\"~99 bar",
                 qp.parse("\"multi foo\" bar").toString());
    
  }
    
  public void testPosIncrementAnalyzer() throws ParseException {
    QueryParser qp = new QueryParser("", new PosIncrementAnalyzer());
    assertEquals("quick brown", qp.parse("the quick brown").toString());
    assertEquals("\"quick brown\"", qp.parse("\"the quick brown\"").toString());
    assertEquals("quick brown fox", qp.parse("the quick brown fox").toString());
    assertEquals("\"quick brown fox\"", qp.parse("\"the quick brown fox\"").toString());
  }
  
  /**
   * Expands "multi" to "multi" and "multi2", both at the same position,
   * and expands "triplemulti" to "triplemulti", "multi3", and "multi2".  
   */
  private class MultiAnalyzer extends Analyzer {

    public MultiAnalyzer() {
    }

    public TokenStream tokenStream(String fieldName, Reader reader) {
      TokenStream result = new StandardTokenizer(reader);
      result = new TestFilter(result);
      result = new LowerCaseFilter(result);
      return result;
    }
  }

  private final class TestFilter extends TokenFilter {
    
    private Token prevToken;
    
    public TestFilter(TokenStream in) {
      super(in);
    }

    public final Token next(final Token reusableToken) throws java.io.IOException {
      if (multiToken > 0) {
        reusableToken.reinit("multi"+(multiToken+1), prevToken.startOffset(), prevToken.endOffset(), prevToken.type());
        reusableToken.setPositionIncrement(0);
        multiToken--;
        return reusableToken;
      } else {
        Token nextToken = input.next(reusableToken);
        if (nextToken == null) {
          prevToken = null;
          return null;
        }
        prevToken = (Token) nextToken.clone();
        String text = nextToken.term();
        if (text.equals("triplemulti")) {
          multiToken = 2;
          return nextToken;
        } else if (text.equals("multi")) {
          multiToken = 1;
          return nextToken;
        } else {
          return nextToken;
        }
      }
    }
  }

  /**
   * Analyzes "the quick brown" as: quick(incr=2) brown(incr=1).
   * Does not work correctly for input other than "the quick brown ...".
   */
  private class PosIncrementAnalyzer extends Analyzer {

    public PosIncrementAnalyzer() {
    }

    public TokenStream tokenStream(String fieldName, Reader reader) {
      TokenStream result = new StandardTokenizer(reader);
      result = new TestPosIncrementFilter(result);
      result = new LowerCaseFilter(result);
      return result;
    }
  }

  private final class TestPosIncrementFilter extends TokenFilter {
    
    public TestPosIncrementFilter(TokenStream in) {
      super(in);
    }

    public final Token next(final Token reusableToken) throws java.io.IOException {
      for (Token nextToken = input.next(reusableToken); nextToken != null; nextToken = input.next(reusableToken)) {
        if (nextToken.term().equals("the")) {
          // stopword, do nothing
        } else if (nextToken.term().equals("quick")) {
          nextToken.setPositionIncrement(2);
          return nextToken;
        } else {
          nextToken.setPositionIncrement(1);
          return nextToken;
        }
      }
      return null;
    }
  }

    /** a very simple subclass of QueryParser */
    private final static class DumbQueryParser extends QueryParser {
        
        public DumbQueryParser(String f, Analyzer a) {
            super(f, a);
        }

        /** expose super's version */
        public Query getSuperFieldQuery(String f, String t) 
            throws ParseException {
            return super.getFieldQuery(f,t);
        }
        /** wrap super's version */
        protected Query getFieldQuery(String f, String t)
            throws ParseException {
            return new DumbQueryWrapper(getSuperFieldQuery(f,t));
        }
    }
    
    /**
     * A very simple wrapper to prevent instanceof checks but uses
     * the toString of the query it wraps.
     */
    private final static class DumbQueryWrapper extends Query {

        private Query q;
        public DumbQueryWrapper(Query q) {
            super();
            this.q = q;
        }
        public String toString(String f) {
            return q.toString(f);
        }
    }
    
}
