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
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.CharacterUtils;
import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

public class TestConditionalTokenFilter extends BaseTokenStreamTestCase {

  boolean closed = false;
  boolean ended = false;
  boolean reset = false;

  private final class AssertingLowerCaseFilter extends TokenFilter {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public AssertingLowerCaseFilter(TokenStream in) {
      super(in);
    }

    @Override
    public final boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        CharacterUtils.toLowerCase(termAtt.buffer(), 0, termAtt.length());
        return true;
      } else
        return false;
    }

    @Override
    public void end() throws IOException {
      super.end();
      ended = true;
    }

    @Override
    public void close() throws IOException {
      super.close();
      closed = true;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      reset = true;
    }
  }

  public void testSimple() throws IOException {

    CannedTokenStream cts = new CannedTokenStream(
        new Token("Alice", 1, 0, 5),
        new Token("Bob", 1, 6, 9),
        new Token("Clara", 1, 10, 15),
        new Token("David", 1, 16, 21)
    );

    TokenStream t = new ConditionalTokenFilter(cts, AssertingLowerCaseFilter::new) {
      CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
      @Override
      protected boolean shouldFilter() throws IOException {
        return termAtt.toString().contains("o") == false;
      }
    };

    assertTokenStreamContents(t, new String[]{ "alice", "Bob", "clara", "david" });
    assertTrue(closed);
    assertTrue(reset);
    assertTrue(ended);
  }

  private final class TokenSplitter extends TokenFilter {

    final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    String half;

    protected TokenSplitter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (half == null) {
        if (input.incrementToken() == false) {
          return false;
        }
        half = termAtt.toString().substring(4);
        termAtt.setLength(4);
        return true;
      }
      termAtt.setEmpty().append(half);
      half = null;
      return true;
    }
  }

  public void testMultitokenWrapping() throws IOException {
    CannedTokenStream cts = new CannedTokenStream(
        new Token("tokenpos1", 0, 9),
        new Token("tokenpos2", 10, 19),
        new Token("tokenpos3", 20, 29),
        new Token("tokenpos4", 30, 39)
    );

    TokenStream ts = new ConditionalTokenFilter(cts, TokenSplitter::new) {
      final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
      @Override
      protected boolean shouldFilter() throws IOException {
        return termAtt.toString().contains("2") == false;
      }
    };

    assertTokenStreamContents(ts, new String[]{
        "toke", "npos1", "tokenpos2", "toke", "npos3", "toke", "npos4"
    });
  }

  private final class EndTrimmingFilter extends FilteringTokenFilter {

    final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    public EndTrimmingFilter(TokenStream in) {
      super(in);
    }

    @Override
    protected boolean accept() throws IOException {
      return true;
    }

    @Override
    public void end() throws IOException {
      super.end();
      offsetAtt.setOffset(0, offsetAtt.endOffset() - 2);
    }
  }

  public void testEndPropagation() throws IOException {
    CannedTokenStream cts1 = new CannedTokenStream(0, 20,
        new Token("alice", 0, 5), new Token("bob", 6, 8)
    );
    TokenStream ts1 = new ConditionalTokenFilter(cts1, EndTrimmingFilter::new) {
      @Override
      protected boolean shouldFilter() throws IOException {
        return false;
      }
    };
    assertTokenStreamContents(ts1, new String[]{ "alice", "bob" },
        null, null, null, null, null, 20);

    CannedTokenStream cts2 = new CannedTokenStream(0, 20,
        new Token("alice", 0, 5), new Token("bob", 6, 8)
    );
    TokenStream ts2 = new ConditionalTokenFilter(cts2, EndTrimmingFilter::new) {
      @Override
      protected boolean shouldFilter() throws IOException {
        return true;
      }
    };
    assertTokenStreamContents(ts2, new String[]{ "alice", "bob" },
        null, null, null, null, null, 18);
  }

  public void testWrapGraphs() throws Exception {

    CannedTokenStream cts = new CannedTokenStream(
        new Token("a", 0, 1),
        new Token("b", 2, 3),
        new Token("c", 4, 5),
        new Token("d", 6, 7),
        new Token("e", 8, 9)
    );

    SynonymMap sm;
    try (Analyzer analyzer = new MockAnalyzer(random())) {
      SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
      parser.parse(new StringReader("a b, f\nc d, g"));
      sm = parser.build();
    }

    TokenStream ts = new ConditionalTokenFilter(cts, in -> new SynonymGraphFilter(in, sm, true)) {
      CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
      @Override
      protected boolean shouldFilter() throws IOException {
        return "c".equals(termAtt.toString()) == false;
      }
    };

    assertTokenStreamContents(ts, new String[]{
        "f", "a", "b", "c", "d", "e"
        },
        null, null, null,
        new int[]{
        1, 0, 1, 1, 1, 1
        },
        new int[]{
        2, 1, 1, 1, 1, 1
        });

  }

}
