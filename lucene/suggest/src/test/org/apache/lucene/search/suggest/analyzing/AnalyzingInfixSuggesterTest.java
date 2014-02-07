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
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.Input;
import org.apache.lucene.search.suggest.InputArrayIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

// Test requires postings offsets:
@SuppressCodecs({"Lucene3x","MockFixedIntBlock","MockVariableIntBlock","MockSep","MockRandom"})
public class AnalyzingInfixSuggesterTest extends LuceneTestCase {

  public void testBasic() throws Exception {
    Input keys[] = new Input[] {
      new Input("lend me your ear", 8, new BytesRef("foobar")),
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    File tempDir = _TestUtil.getTempDir("AnalyzingInfixSuggesterTest");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected Directory getDirectory(File path) {
          return newDirectory();
        }
      };
    suggester.build(new InputArrayIterator(keys));

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
    Input keys[] = new Input[] {
      new Input("lend me your ear", 8, new BytesRef("foobar")),
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    File tempDir = _TestUtil.getTempDir("AnalyzingInfixSuggesterTest");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected Directory getDirectory(File path) {
          return newFSDirectory(path);
        }
      };
    suggester.build(new InputArrayIterator(keys));
    assertEquals(2, suggester.getCount());
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
    assertEquals(2, suggester.getCount());
    suggester.close();
  }

  /** Used to return highlighted result; see {@link
   *  LookupResult#highlightKey} */
  private static final class LookupHighlightFragment {
    /** Portion of text for this fragment. */
    public final String text;

    /** True if this text matched a part of the user's
     *  query. */
    public final boolean isHit;

    /** Sole constructor. */
    public LookupHighlightFragment(String text, boolean isHit) {
      this.text = text;
      this.isHit = isHit;
    }

    @Override
    public String toString() {
      return "LookupHighlightFragment(text=" + text + " isHit=" + isHit + ")";
    }
  }

  @SuppressWarnings("unchecked")
  public void testHighlightAsObject() throws Exception {
    Input keys[] = new Input[] {
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    File tempDir = _TestUtil.getTempDir("AnalyzingInfixSuggesterTest");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected Directory getDirectory(File path) {
          return newDirectory();
        }

        @Override
        protected Object highlight(String text, Set<String> matchedTokens, String prefixToken) throws IOException {
          TokenStream ts = queryAnalyzer.tokenStream("text", new StringReader(text));
          try {
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
            ts.reset();
            List<LookupHighlightFragment> fragments = new ArrayList<LookupHighlightFragment>();
            int upto = 0;
            while (ts.incrementToken()) {
              String token = termAtt.toString();
              int startOffset = offsetAtt.startOffset();
              int endOffset = offsetAtt.endOffset();
              if (upto < startOffset) {
                fragments.add(new LookupHighlightFragment(text.substring(upto, startOffset), false));
                upto = startOffset;
              } else if (upto > startOffset) {
                continue;
              }
              
              if (matchedTokens.contains(token)) {
                // Token matches.
                fragments.add(new LookupHighlightFragment(text.substring(startOffset, endOffset), true));
                upto = endOffset;
              } else if (prefixToken != null && token.startsWith(prefixToken)) {
                fragments.add(new LookupHighlightFragment(text.substring(startOffset, startOffset+prefixToken.length()), true));
                if (prefixToken.length() < token.length()) {
                  fragments.add(new LookupHighlightFragment(text.substring(startOffset+prefixToken.length(), startOffset+token.length()), false));
                }
                upto = endOffset;
              }
            }
            ts.end();
            int endOffset = offsetAtt.endOffset();
            if (upto < endOffset) {
              fragments.add(new LookupHighlightFragment(text.substring(upto), false));
            }
            
            return fragments;
          } finally {
            IOUtils.closeWhileHandlingException(ts);
          }
        }
      };
    suggester.build(new InputArrayIterator(keys));

    List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence("ear", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a penny saved is a penny <b>ear</b>ned", toString((List<LookupHighlightFragment>) results.get(0).highlightKey));
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);
    suggester.close();
  }

  public String toString(List<LookupHighlightFragment> fragments) {
    StringBuilder sb = new StringBuilder();
    for(LookupHighlightFragment fragment : fragments) {
      if (fragment.isHit) {
        sb.append("<b>");
      }
      sb.append(fragment.text);
      if (fragment.isHit) {
        sb.append("</b>");
      }
    }

    return sb.toString();
  }

  public void testRandomMinPrefixLength() throws Exception {
    Input keys[] = new Input[] {
      new Input("lend me your ear", 8, new BytesRef("foobar")),
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
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
    suggester.build(new InputArrayIterator(keys));

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
    Input keys[] = new Input[] {
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    File tempDir = _TestUtil.getTempDir("AnalyzingInfixSuggesterTest");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected Directory getDirectory(File path) {
          return newDirectory();
        }
      };
    suggester.build(new InputArrayIterator(keys));
    List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence("penn", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a <b>penn</b>y saved is a <b>penn</b>y earned", results.get(0).key);
    suggester.close();
  }

  public void testHighlightCaseChange() throws Exception {
    Input keys[] = new Input[] {
      new Input("a Penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    File tempDir = _TestUtil.getTempDir("AnalyzingInfixSuggesterTest");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected Directory getDirectory(File path) {
          return newDirectory();
        }
      };
    suggester.build(new InputArrayIterator(keys));
    List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence("penn", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a <b>Penn</b>y saved is a <b>penn</b>y earned", results.get(0).key);
    suggester.close();

    // Try again, but overriding addPrefixMatch to highlight
    // the entire hit:
    suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected void addPrefixMatch(StringBuilder sb, String surface, String analyzed, String prefixToken) {
          sb.append("<b>");
          sb.append(surface);
          sb.append("</b>");
        }

        @Override
        protected Directory getDirectory(File path) {
          return newDirectory();
        }
      };
    suggester.build(new InputArrayIterator(keys));
    results = suggester.lookup(_TestUtil.stringToCharSequence("penn", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a <b>Penny</b> saved is a <b>penny</b> earned", results.get(0).key);
    suggester.close();
  }

  public void testDoubleClose() throws Exception {
    Input keys[] = new Input[] {
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    File tempDir = _TestUtil.getTempDir("AnalyzingInfixSuggesterTest");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, a, a, 3) {
        @Override
        protected Directory getDirectory(File path) {
          return newDirectory();
        }
      };
    suggester.build(new InputArrayIterator(keys));
    suggester.close();
    suggester.close();
  }

  public void testSuggestStopFilter() throws Exception {
    final CharArraySet stopWords = StopFilter.makeStopSet(TEST_VERSION_CURRENT, "a");
    Analyzer indexAnalyzer = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
          MockTokenizer tokens = new MockTokenizer(reader);
          return new TokenStreamComponents(tokens,
                                           new StopFilter(TEST_VERSION_CURRENT, tokens, stopWords));
        }
      };

    Analyzer queryAnalyzer = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
          MockTokenizer tokens = new MockTokenizer(reader);
          return new TokenStreamComponents(tokens,
                                           new SuggestStopFilter(tokens, stopWords));
        }
      };

    File tempDir = _TestUtil.getTempDir("AnalyzingInfixSuggesterTest");

    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(TEST_VERSION_CURRENT, tempDir, indexAnalyzer, queryAnalyzer, 3) {
        @Override
        protected Directory getDirectory(File path) {
          return newDirectory();
        }
      };

    Input keys[] = new Input[] {
      new Input("a bob for apples", 10, new BytesRef("foobaz")),
    };

    suggester.build(new InputArrayIterator(keys));
    List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence("a", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a bob for <b>a</b>pples", results.get(0).key);
    suggester.close();
  }
}
