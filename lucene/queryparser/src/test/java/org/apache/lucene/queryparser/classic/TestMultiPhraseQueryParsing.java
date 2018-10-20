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
package org.apache.lucene.queryparser.classic;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

public class TestMultiPhraseQueryParsing extends LuceneTestCase {

  private static class TokenAndPos {
      public final String token;
      public final int pos;
      public TokenAndPos(String token, int pos) {
        this.token = token;
        this.pos = pos;
      }
    }

  private static class CannedAnalyzer extends Analyzer {
    private final TokenAndPos[] tokens;

    public CannedAnalyzer(TokenAndPos[] tokens) {
      this.tokens = tokens;
    }

    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      return new TokenStreamComponents(new CannedTokenizer(tokens));
    }
  }

  private static class CannedTokenizer extends Tokenizer {
    private final TokenAndPos[] tokens;
    private int upto = 0;
    private int lastPos = 0;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

    public CannedTokenizer(TokenAndPos[] tokens) {
      super();
      this.tokens = tokens;
    }

    @Override
    public final boolean incrementToken() {
      clearAttributes();
      if (upto < tokens.length) {
        final TokenAndPos token = tokens[upto++];
        termAtt.setEmpty();
        termAtt.append(token.token);
        posIncrAtt.setPositionIncrement(token.pos - lastPos);
        lastPos = token.pos;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      this.upto = 0;
      this.lastPos = 0;
    }
  }

  public void testMultiPhraseQueryParsing() throws Exception {
    TokenAndPos[] INCR_0_QUERY_TOKENS_AND = new TokenAndPos[]{
        new TokenAndPos("a", 0),
        new TokenAndPos("1", 0),
        new TokenAndPos("b", 1),
        new TokenAndPos("1", 1),
        new TokenAndPos("c", 2)
    };

    QueryParser qp = new QueryParser("field", new CannedAnalyzer(INCR_0_QUERY_TOKENS_AND));
    Query q = qp.parse("\"this text is acually ignored\"");
    assertTrue("wrong query type!", q instanceof MultiPhraseQuery);

    MultiPhraseQuery.Builder multiPhraseQueryBuilder = new MultiPhraseQuery.Builder();
    multiPhraseQueryBuilder.add(new Term[]{ new Term("field", "a"), new Term("field", "1") }, -1);
    multiPhraseQueryBuilder.add(new Term[]{ new Term("field", "b"), new Term("field", "1") }, 0);
    multiPhraseQueryBuilder.add(new Term[]{ new Term("field", "c") }, 1);

    assertEquals(multiPhraseQueryBuilder.build(), q);
  }

}
