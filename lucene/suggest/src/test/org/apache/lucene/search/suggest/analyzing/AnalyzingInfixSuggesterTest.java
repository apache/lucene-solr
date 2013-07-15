package org.apache.lucene.search.suggest.analyzing;

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

import java.io.File;
import java.io.Reader;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.TermFreqPayload;
import org.apache.lucene.search.suggest.TermFreqPayloadArrayIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

// Test requires postings offsets:
@SuppressCodecs({"Lucene3x","MockFixedIntBlock","MockVariableIntBlock","MockSep","MockRandom"})
public class AnalyzingInfixSuggesterTest extends LuceneTestCase {

  public void testBasic() throws Exception {
    TermFreqPayload keys[] = new TermFreqPayload[] {
      new TermFreqPayload("lend me your ear", 8, new BytesRef("foobar")),
      new TermFreqPayload("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    File tempDir = _TestUtil.getTempDir("AnalyzingInfixSuggesterTest");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected Directory getDirectory(File path) {
          return newDirectory();
        }
      };
    suggester.build(new TermFreqPayloadArrayIterator(keys));

    List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence("ear", random()), 10, true, true);
    assertEquals(2, results.size());
    assertEquals("a penny saved is a penny <b>ear</b>ned", results.get(0).key);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);

    assertEquals("lend me your <b>ear</b>", results.get(1).key);
    assertEquals(8, results.get(1).value);
    assertEquals(new BytesRef("foobar"), results.get(1).payload);

    results = suggester.lookup(_TestUtil.stringToCharSequence("ear ", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("lend me your <b>ear</b>", results.get(0).key);
    assertEquals(8, results.get(0).value);
    assertEquals(new BytesRef("foobar"), results.get(0).payload);

    results = suggester.lookup(_TestUtil.stringToCharSequence("pen", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a <b>pen</b>ny saved is a <b>pen</b>ny earned", results.get(0).key);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);

    results = suggester.lookup(_TestUtil.stringToCharSequence("p", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a <b>p</b>enny saved is a <b>p</b>enny earned", results.get(0).key);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);

    suggester.close();
  }

  public void testAfterLoad() throws Exception {
    TermFreqPayload keys[] = new TermFreqPayload[] {
      new TermFreqPayload("lend me your ear", 8, new BytesRef("foobar")),
      new TermFreqPayload("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    File tempDir = _TestUtil.getTempDir("AnalyzingInfixSuggesterTest");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected Directory getDirectory(File path) {
          return newFSDirectory(path);
        }
      };
    suggester.build(new TermFreqPayloadArrayIterator(keys));
    suggester.close();

    suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected Directory getDirectory(File path) {
          return newFSDirectory(path);
        }
      };
    List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence("ear", random()), 10, true, true);
    assertEquals(2, results.size());
    assertEquals("a penny saved is a penny <b>ear</b>ned", results.get(0).key);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);
    suggester.close();
  }

  public void testRandomMinPrefixLength() throws Exception {
    TermFreqPayload keys[] = new TermFreqPayload[] {
      new TermFreqPayload("lend me your ear", 8, new BytesRef("foobar")),
      new TermFreqPayload("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    File tempDir = _TestUtil.getTempDir("AnalyzingInfixSuggesterTest");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    int minPrefixLength = random().nextInt(10);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, minPrefixLength) {
        @Override
        protected Directory getDirectory(File path) {
          return newFSDirectory(path);
        }
      };
    suggester.build(new TermFreqPayloadArrayIterator(keys));

    for(int i=0;i<2;i++) {
      for(int j=0;j<2;j++) {
        boolean doHighlight = j == 0;

        List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence("ear", random()), 10, true, doHighlight);
        assertEquals(2, results.size());
        if (doHighlight) {
          assertEquals("a penny saved is a penny <b>ear</b>ned", results.get(0).key);
        } else {
          assertEquals("a penny saved is a penny earned", results.get(0).key);
        }
        assertEquals(10, results.get(0).value);
        if (doHighlight) {
          assertEquals("lend me your <b>ear</b>", results.get(1).key);
        } else {
          assertEquals("lend me your ear", results.get(1).key);
        }
        assertEquals(new BytesRef("foobaz"), results.get(0).payload);
        assertEquals(8, results.get(1).value);
        assertEquals(new BytesRef("foobar"), results.get(1).payload);

        results = suggester.lookup(_TestUtil.stringToCharSequence("ear ", random()), 10, true, doHighlight);
        assertEquals(1, results.size());
        if (doHighlight) {
          assertEquals("lend me your <b>ear</b>", results.get(0).key);
        } else {
          assertEquals("lend me your ear", results.get(0).key);
        }
        assertEquals(8, results.get(0).value);
        assertEquals(new BytesRef("foobar"), results.get(0).payload);

        results = suggester.lookup(_TestUtil.stringToCharSequence("pen", random()), 10, true, doHighlight);
        assertEquals(1, results.size());
        if (doHighlight) {
          assertEquals("a <b>pen</b>ny saved is a <b>pen</b>ny earned", results.get(0).key);
        } else {
          assertEquals("a penny saved is a penny earned", results.get(0).key);
        }
        assertEquals(10, results.get(0).value);
        assertEquals(new BytesRef("foobaz"), results.get(0).payload);

        results = suggester.lookup(_TestUtil.stringToCharSequence("p", random()), 10, true, doHighlight);
        assertEquals(1, results.size());
        if (doHighlight) {
          assertEquals("a <b>p</b>enny saved is a <b>p</b>enny earned", results.get(0).key);
        } else {
          assertEquals("a penny saved is a penny earned", results.get(0).key);
        }
        assertEquals(10, results.get(0).value);
        assertEquals(new BytesRef("foobaz"), results.get(0).payload);
      }

      // Make sure things still work after close and reopen:
      suggester.close();
      suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, minPrefixLength) {
          @Override
          protected Directory getDirectory(File path) {
            return newFSDirectory(path);
          }
        };
    }
    suggester.close();
  }

  public void testHighlight() throws Exception {
    TermFreqPayload keys[] = new TermFreqPayload[] {
      new TermFreqPayload("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    File tempDir = _TestUtil.getTempDir("AnalyzingInfixSuggesterTest");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected Directory getDirectory(File path) {
          return newDirectory();
        }
      };
    suggester.build(new TermFreqPayloadArrayIterator(keys));
    List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence("penn", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a <b>penn</b>y saved is a <b>penn</b>y earned", results.get(0).key);
    suggester.close();
  }

  public void testHighlightCaseChange() throws Exception {
    TermFreqPayload keys[] = new TermFreqPayload[] {
      new TermFreqPayload("a Penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    File tempDir = _TestUtil.getTempDir("AnalyzingInfixSuggesterTest");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected Directory getDirectory(File path) {
          return newDirectory();
        }
      };
    suggester.build(new TermFreqPayloadArrayIterator(keys));
    List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence("penn", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a <b>Penny</b> saved is a <b>penn</b>y earned", results.get(0).key);
    suggester.close();

    // Try again, but overriding addPrefixMatch to normalize case:
    suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected void addPrefixMatch(StringBuilder sb, String surface, String analyzed, String prefixToken) {
          prefixToken = prefixToken.toLowerCase(Locale.ROOT);
          String surfaceLower = surface.toLowerCase(Locale.ROOT);
          sb.append("<b>");
          if (surfaceLower.startsWith(prefixToken)) {
            sb.append(surface.substring(0, prefixToken.length()));
            sb.append("</b>");
            sb.append(surface.substring(prefixToken.length()));
          } else {
            sb.append(surface);
            sb.append("</b>");
          }
        }

        @Override
        protected Directory getDirectory(File path) {
          return newDirectory();
        }
      };
    suggester.build(new TermFreqPayloadArrayIterator(keys));
    results = suggester.lookup(_TestUtil.stringToCharSequence("penn", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a <b>Penn</b>y saved is a <b>penn</b>y earned", results.get(0).key);
    suggester.close();
  }

  public void testDoubleClose() throws Exception {
    TermFreqPayload keys[] = new TermFreqPayload[] {
      new TermFreqPayload("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    File tempDir = _TestUtil.getTempDir("AnalyzingInfixSuggesterTest");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected Directory getDirectory(File path) {
          return newDirectory();
        }
      };
    suggester.build(new TermFreqPayloadArrayIterator(keys));
    suggester.close();
    suggester.close();
  }

  public void testForkLastToken() throws Exception {
    Analyzer a = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
          MockTokenizer tokens = new MockTokenizer(reader);
          // ForkLastTokenFilter is a bit evil:
          tokens.setEnableChecks(false);
          return new TokenStreamComponents(tokens,
                                           new StopKeywordFilter(TEST_VERSION_CURRENT,
                                                                 new ForkLastTokenFilter(tokens), StopKeywordFilter.makeStopSet(TEST_VERSION_CURRENT, "a")));
        }
      };

    TermFreqPayload keys[] = new TermFreqPayload[] {
      new TermFreqPayload("a bob for apples", 10, new BytesRef("foobaz")),
    };

    File tempDir = _TestUtil.getTempDir("AnalyzingInfixSuggesterTest");

    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected Query finishQuery(BooleanQuery in, boolean allTermsRequired) {
          List<BooleanClause> clauses = in.clauses();
          if (clauses.size() >= 2 && allTermsRequired) {
            String t1 = getTerm(clauses.get(clauses.size()-2).getQuery());
            String t2 = getTerm(clauses.get(clauses.size()-1).getQuery());
            if (t1.equals(t2)) {
              // The last 2 tokens came from
              // ForkLastTokenFilter; we remove them and
              // replace them with a MUST BooleanQuery that
              // SHOULDs the two of them together:
              BooleanQuery sub = new BooleanQuery();
              BooleanClause other = clauses.get(clauses.size()-2);
              sub.add(new BooleanClause(clauses.get(clauses.size()-2).getQuery(), BooleanClause.Occur.SHOULD));
              sub.add(new BooleanClause(clauses.get(clauses.size()-1).getQuery(), BooleanClause.Occur.SHOULD));
              clauses.subList(clauses.size()-2, clauses.size()).clear();
              clauses.add(new BooleanClause(sub, BooleanClause.Occur.MUST));
            }
          }
          return in;
        }

        private String getTerm(Query query) {
          if (query instanceof TermQuery) {
            return ((TermQuery) query).getTerm().text();
          } else if (query instanceof PrefixQuery) {
            return ((PrefixQuery) query).getPrefix().text();
          } else {
            return null;
          }
        }

        @Override
        protected Directory getDirectory(File path) {
          return newDirectory();
        }
      };

    suggester.build(new TermFreqPayloadArrayIterator(keys));
    List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence("a", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a bob for <b>a</b>pples", results.get(0).key);
    suggester.close();
  }
}
