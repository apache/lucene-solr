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
package org.apache.lucene.search.suggest.analyzing;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.suggest.Input;
import org.apache.lucene.search.suggest.InputArrayIterator;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;

public class AnalyzingInfixSuggesterTest extends LuceneTestCase {

  public void testBasic() throws Exception {
    Input keys[] = new Input[] {
      new Input("lend me your ear", 8, new BytesRef("foobar")),
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, false);
    suggester.build(new InputArrayIterator(keys));

    List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), 10, true, true);
    assertEquals(2, results.size());
    assertEquals("a penny saved is a penny earned", results.get(0).key);
    assertEquals("a penny saved is a penny <b>ear</b>ned", results.get(0).highlightKey);
    assertEquals(10, results.get(0).value);
    assertEquals("foobaz", results.get(0).payload.utf8ToString());

    assertEquals("lend me your ear", results.get(1).key);
    assertEquals("lend me your <b>ear</b>", results.get(1).highlightKey);
    assertEquals(8, results.get(1).value);
    assertEquals(new BytesRef("foobar"), results.get(1).payload);

    results = suggester.lookup(TestUtil.stringToCharSequence("ear ", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("lend me your ear", results.get(0).key);
    assertEquals("lend me your <b>ear</b>", results.get(0).highlightKey);
    assertEquals(8, results.get(0).value);
    assertEquals(new BytesRef("foobar"), results.get(0).payload);

    results = suggester.lookup(TestUtil.stringToCharSequence("pen", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a penny saved is a penny earned", results.get(0).key);
    assertEquals("a <b>pen</b>ny saved is a <b>pen</b>ny earned", results.get(0).highlightKey);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);

    results = suggester.lookup(TestUtil.stringToCharSequence("p", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a penny saved is a penny earned", results.get(0).key);
    assertEquals("a <b>p</b>enny saved is a <b>p</b>enny earned", results.get(0).highlightKey);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);
    
    results = suggester.lookup(TestUtil.stringToCharSequence("money penny", random()), 10, false, true);
    assertEquals(1, results.size());
    assertEquals("a penny saved is a penny earned", results.get(0).key);
    assertEquals("a <b>penny</b> saved is a <b>penny</b> earned", results.get(0).highlightKey);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);
 
    results = suggester.lookup(TestUtil.stringToCharSequence("penny ea", random()), 10, false, true);
    assertEquals(2, results.size());
    assertEquals("a penny saved is a penny earned", results.get(0).key);
    assertEquals("a <b>penny</b> saved is a <b>penny</b> <b>ea</b>rned", results.get(0).highlightKey);
    assertEquals("lend me your ear", results.get(1).key);
    assertEquals("lend me your <b>ea</b>r", results.get(1).highlightKey);
        
    results = suggester.lookup(TestUtil.stringToCharSequence("money penny", random()), 10, false, false);
    assertEquals(1, results.size());
    assertEquals("a penny saved is a penny earned", results.get(0).key);
    assertNull(results.get(0).highlightKey);
    
    testConstructorDefaults(suggester, keys, a, true, true);
    testConstructorDefaults(suggester, keys, a, true, false);
    testConstructorDefaults(suggester, keys, a, false, false);
    testConstructorDefaults(suggester, keys, a, false, true);
    
    suggester.close();
    a.close();
  }

  private void testConstructorDefaults(AnalyzingInfixSuggester suggester, Input[] keys, Analyzer a, 
      boolean allTermsRequired, boolean highlight) throws IOException {
    AnalyzingInfixSuggester suggester2 = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, false, allTermsRequired, highlight);
    suggester2.build(new InputArrayIterator(keys));
    
    CharSequence key = TestUtil.stringToCharSequence("penny ea", random());
    
    List<LookupResult> results1 = suggester.lookup(key, 10, allTermsRequired, highlight);
    List<LookupResult> results2 = suggester2.lookup(key, false, 10);
    assertEquals(results1.size(), results2.size());
    assertEquals(results1.get(0).key, results2.get(0).key);
    assertEquals(results1.get(0).highlightKey, results2.get(0).highlightKey);
    
    suggester2.close();
  }

  public void testAfterLoad() throws Exception {
    Input keys[] = new Input[] {
      new Input("lend me your ear", 8, new BytesRef("foobar")),
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    Path tempDir = createTempDir("AnalyzingInfixSuggesterTest");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newFSDirectory(tempDir), a, a, 3, false);
    suggester.build(new InputArrayIterator(keys));
    assertEquals(2, suggester.getCount());
    suggester.close();

    suggester = new AnalyzingInfixSuggester(newFSDirectory(tempDir), a, a, 3, false);
    List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), 10, true, true);
    assertEquals(2, results.size());
    assertEquals("a penny saved is a penny earned", results.get(0).key);
    assertEquals("a penny saved is a penny <b>ear</b>ned", results.get(0).highlightKey);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);
    assertEquals(2, suggester.getCount());
    suggester.close();
    a.close();
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

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, false) {
        @Override
        protected Object highlight(String text, Set<String> matchedTokens, String prefixToken) throws IOException {
          try (TokenStream ts = queryAnalyzer.tokenStream("text", new StringReader(text))) {
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
            ts.reset();
            List<LookupHighlightFragment> fragments = new ArrayList<>();
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
          }
        }
      };
    suggester.build(new InputArrayIterator(keys));

    List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a penny saved is a penny <b>ear</b>ned", toString((List<LookupHighlightFragment>) results.get(0).highlightKey));
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);
    suggester.close();
    a.close();
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
    Path tempDir = createTempDir("AnalyzingInfixSuggesterTest");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    int minPrefixLength = random().nextInt(10);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newFSDirectory(tempDir), a, a, minPrefixLength, false);
    suggester.build(new InputArrayIterator(keys));

    for(int i=0;i<2;i++) {
      for(int j=0;j<2;j++) {
        boolean doHighlight = j == 0;

        List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), 10, true, doHighlight);
        assertEquals(2, results.size());
        assertEquals("a penny saved is a penny earned", results.get(0).key);
        if (doHighlight) {
          assertEquals("a penny saved is a penny <b>ear</b>ned", results.get(0).highlightKey);
        }
        assertEquals(10, results.get(0).value);
        assertEquals("lend me your ear", results.get(1).key);
        if (doHighlight) {
          assertEquals("lend me your <b>ear</b>", results.get(1).highlightKey);
        }
        assertEquals(new BytesRef("foobaz"), results.get(0).payload);
        assertEquals(8, results.get(1).value);
        assertEquals(new BytesRef("foobar"), results.get(1).payload);

        results = suggester.lookup(TestUtil.stringToCharSequence("ear ", random()), 10, true, doHighlight);
        assertEquals(1, results.size());
        assertEquals("lend me your ear", results.get(0).key);
        if (doHighlight) {
          assertEquals("lend me your <b>ear</b>", results.get(0).highlightKey);
        }
        assertEquals(8, results.get(0).value);
        assertEquals(new BytesRef("foobar"), results.get(0).payload);

        results = suggester.lookup(TestUtil.stringToCharSequence("pen", random()), 10, true, doHighlight);
        assertEquals(1, results.size());
        assertEquals("a penny saved is a penny earned", results.get(0).key);
        if (doHighlight) {
          assertEquals("a <b>pen</b>ny saved is a <b>pen</b>ny earned", results.get(0).highlightKey);
        }
        assertEquals(10, results.get(0).value);
        assertEquals(new BytesRef("foobaz"), results.get(0).payload);

        results = suggester.lookup(TestUtil.stringToCharSequence("p", random()), 10, true, doHighlight);
        assertEquals(1, results.size());
        assertEquals("a penny saved is a penny earned", results.get(0).key);
        if (doHighlight) {
          assertEquals("a <b>p</b>enny saved is a <b>p</b>enny earned", results.get(0).highlightKey);
        }
        assertEquals(10, results.get(0).value);
        assertEquals(new BytesRef("foobaz"), results.get(0).payload);
      }

      // Make sure things still work after close and reopen:
      suggester.close();
      suggester = new AnalyzingInfixSuggester(newFSDirectory(tempDir), a, a, minPrefixLength, false);
    }
    suggester.close();
    a.close();
  }

  public void testHighlight() throws Exception {
    Input keys[] = new Input[] {
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, false);
    suggester.build(new InputArrayIterator(keys));
    List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("penn", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a penny saved is a penny earned", results.get(0).key);
    assertEquals("a <b>penn</b>y saved is a <b>penn</b>y earned", results.get(0).highlightKey);
    suggester.close();
    a.close();
  }

  public void testHighlightCaseChange() throws Exception {
    Input keys[] = new Input[] {
      new Input("a Penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, false);
    suggester.build(new InputArrayIterator(keys));
    List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("penn", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a Penny saved is a penny earned", results.get(0).key);
    assertEquals("a <b>Penn</b>y saved is a <b>penn</b>y earned", results.get(0).highlightKey);
    suggester.close();

    // Try again, but overriding addPrefixMatch to highlight
    // the entire hit:
    suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, false) {
        @Override
        protected void addPrefixMatch(StringBuilder sb, String surface, String analyzed, String prefixToken) {
          sb.append("<b>");
          sb.append(surface);
          sb.append("</b>");
        }
      };
    suggester.build(new InputArrayIterator(keys));
    results = suggester.lookup(TestUtil.stringToCharSequence("penn", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a Penny saved is a penny earned", results.get(0).key);
    assertEquals("a <b>Penny</b> saved is a <b>penny</b> earned", results.get(0).highlightKey);
    suggester.close();
    a.close();
  }

  public void testDoubleClose() throws Exception {
    Input keys[] = new Input[] {
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
    };

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, false);
    suggester.build(new InputArrayIterator(keys));
    suggester.close();
    suggester.close();
    a.close();
  }

  public void testSuggestStopFilter() throws Exception {
    final CharArraySet stopWords = StopFilter.makeStopSet("a");
    Analyzer indexAnalyzer = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          MockTokenizer tokens = new MockTokenizer();
          return new TokenStreamComponents(tokens,
                                           new StopFilter(tokens, stopWords));
        }
      };

    Analyzer queryAnalyzer = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          MockTokenizer tokens = new MockTokenizer();
          return new TokenStreamComponents(tokens,
                                           new SuggestStopFilter(tokens, stopWords));
        }
      };

    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newDirectory(), indexAnalyzer, queryAnalyzer, 3, false);

    Input keys[] = new Input[] {
      new Input("a bob for apples", 10, new BytesRef("foobaz")),
    };

    suggester.build(new InputArrayIterator(keys));
    List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("a", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a bob for apples", results.get(0).key);
    assertEquals("a bob for <b>a</b>pples", results.get(0).highlightKey);
    suggester.close();
    IOUtils.close(suggester, indexAnalyzer, queryAnalyzer);
  }

  public void testEmptyAtStart() throws Exception {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, false);
    suggester.build(new InputArrayIterator(new Input[0]));
    suggester.add(new BytesRef("a penny saved is a penny earned"), null, 10, new BytesRef("foobaz"));
    suggester.add(new BytesRef("lend me your ear"), null, 8, new BytesRef("foobar"));
    suggester.refresh();
    List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), 10, true, true);
    assertEquals(2, results.size());
    assertEquals("a penny saved is a penny earned", results.get(0).key);
    assertEquals("a penny saved is a penny <b>ear</b>ned", results.get(0).highlightKey);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);

    assertEquals("lend me your ear", results.get(1).key);
    assertEquals("lend me your <b>ear</b>", results.get(1).highlightKey);
    assertEquals(8, results.get(1).value);
    assertEquals(new BytesRef("foobar"), results.get(1).payload);

    results = suggester.lookup(TestUtil.stringToCharSequence("ear ", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("lend me your ear", results.get(0).key);
    assertEquals("lend me your <b>ear</b>", results.get(0).highlightKey);
    assertEquals(8, results.get(0).value);
    assertEquals(new BytesRef("foobar"), results.get(0).payload);

    results = suggester.lookup(TestUtil.stringToCharSequence("pen", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a penny saved is a penny earned", results.get(0).key);
    assertEquals("a <b>pen</b>ny saved is a <b>pen</b>ny earned", results.get(0).highlightKey);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);

    results = suggester.lookup(TestUtil.stringToCharSequence("p", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a penny saved is a penny earned", results.get(0).key);
    assertEquals("a <b>p</b>enny saved is a <b>p</b>enny earned", results.get(0).highlightKey);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);

    suggester.close();
    a.close();
  }

  public void testBothExactAndPrefix() throws Exception {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, false);
    suggester.build(new InputArrayIterator(new Input[0]));
    suggester.add(new BytesRef("the pen is pretty"), null, 10, new BytesRef("foobaz"));
    suggester.refresh();

    List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("pen p", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("the pen is pretty", results.get(0).key);
    assertEquals("the <b>pen</b> is <b>p</b>retty", results.get(0).highlightKey);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);
    suggester.close();
    a.close();
  }

  private static String randomText() {
    int numWords = TestUtil.nextInt(random(), 1, 4);
      
    StringBuilder b = new StringBuilder();
    for(int i=0;i<numWords;i++) {
      if (i > 0) {
        b.append(' ');
      }
      b.append(TestUtil.randomSimpleString(random(), 1, 10));
    }

    return b.toString();
  }

  private static class Update {
    long weight;
    int index;
  }

  private static class LookupThread extends Thread {
    private final AnalyzingInfixSuggester suggester;
    private volatile boolean stop;

    public LookupThread(AnalyzingInfixSuggester suggester) {
      this.suggester = suggester;
    }

    public void finish() throws InterruptedException {
      stop = true;
      this.join();
    }

    @Override
    public void run() {
      while (stop == false) {
        String query = randomText();
        int topN = TestUtil.nextInt(random(), 1, 100);
        boolean allTermsRequired = random().nextBoolean();
        boolean doHilite = random().nextBoolean();
        // We don't verify the results; just doing
        // simultaneous lookups while adding/updating to
        // see if there are any thread hazards:
        try {
          suggester.lookup(TestUtil.stringToCharSequence(query, random()),
                           topN, allTermsRequired, doHilite);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    }
  }

  public void testRandomNRT() throws Exception {
    final Path tempDir = createTempDir("AnalyzingInfixSuggesterTest");
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    int minPrefixChars = random().nextInt(7);
    if (VERBOSE) {
      System.out.println("  minPrefixChars=" + minPrefixChars);
    }

    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newFSDirectory(tempDir), a, a, minPrefixChars, false);

    // Initial suggester built with nothing:
    suggester.build(new InputArrayIterator(new Input[0]));

    LookupThread lookupThread = new LookupThread(suggester);
    lookupThread.start();

    int iters = atLeast(1000);
    int visibleUpto = 0;

    Set<Long> usedWeights = new HashSet<>();
    Set<String> usedKeys = new HashSet<>();

    List<Input> inputs = new ArrayList<>();
    List<Update> pendingUpdates = new ArrayList<>();

    for(int iter=0;iter<iters;iter++) {
      String text;
      while (true) {
        text = randomText();
        if (usedKeys.contains(text) == false) {
          usedKeys.add(text);
          break;
        }
      }

      // Carefully pick a weight we never used, to sidestep
      // tie-break problems:
      long weight;
      while (true) {
        weight = random().nextInt(10*iters);
        if (usedWeights.contains(weight) == false) {
          usedWeights.add(weight);
          break;
        }
      }

      if (inputs.size() > 0 && random().nextInt(4) == 1) {
        // Update an existing suggestion
        Update update = new Update();
        update.index = random().nextInt(inputs.size());
        update.weight = weight;
        Input input = inputs.get(update.index);
        pendingUpdates.add(update);
        if (VERBOSE) {
          System.out.println("TEST: iter=" + iter + " update input=" + input.term.utf8ToString() + "/" + weight);
        }
        suggester.update(input.term, null, weight, input.term);
        
      } else {
        // Add a new suggestion
        inputs.add(new Input(text, weight, new BytesRef(text)));
        if (VERBOSE) {
          System.out.println("TEST: iter=" + iter + " add input=" + text + "/" + weight);
        }
        BytesRef br = new BytesRef(text);
        suggester.add(br, null, weight, br);
      }

      if (random().nextInt(15) == 7) {
        if (VERBOSE) {
          System.out.println("TEST: now refresh suggester");
        }
        suggester.refresh();
        visibleUpto = inputs.size();
        for(Update update : pendingUpdates) {
          Input oldInput = inputs.get(update.index);
          Input newInput = new Input(oldInput.term, update.weight, oldInput.payload);
          inputs.set(update.index, newInput);
        }
        pendingUpdates.clear();
      }
      
      if (random().nextInt(50) == 7) {
        if (VERBOSE) {
          System.out.println("TEST: now close/reopen suggester");
        }
        lookupThread.finish();
        suggester.close();
        suggester = new AnalyzingInfixSuggester(newFSDirectory(tempDir), a, a, minPrefixChars, false);
        lookupThread = new LookupThread(suggester);
        lookupThread.start();

        visibleUpto = inputs.size();
        for(Update update : pendingUpdates) {
          Input oldInput = inputs.get(update.index);
          Input newInput = new Input(oldInput.term, update.weight, oldInput.payload);
          inputs.set(update.index, newInput);
        }
        pendingUpdates.clear();
      }

      if (visibleUpto > 0) {
        String query = randomText();
        boolean lastPrefix = random().nextInt(5) != 1;
        if (lastPrefix == false) {
          query += " ";
        }

        String[] queryTerms = query.split("\\s");
        boolean allTermsRequired = random().nextInt(10) == 7;
        boolean doHilite = random().nextBoolean();

        if (VERBOSE) {
          System.out.println("TEST: lookup \"" + query + "\" allTermsRequired=" + allTermsRequired + " doHilite=" + doHilite);
        }

        // Stupid slow but hopefully correct matching:
        List<Input> expected = new ArrayList<>();
        for(int i=0;i<visibleUpto;i++) {
          Input input = inputs.get(i);
          String[] inputTerms = input.term.utf8ToString().split("\\s");
          boolean match = false;
          for(int j=0;j<queryTerms.length;j++) {
            if (j < queryTerms.length-1 || lastPrefix == false) {
              // Exact match
              for(int k=0;k<inputTerms.length;k++) {
                if (inputTerms[k].equals(queryTerms[j])) {
                  match = true;
                  break;
                }
              }
            } else {
              // Prefix match
              for(int k=0;k<inputTerms.length;k++) {
                if (inputTerms[k].startsWith(queryTerms[j])) {
                  match = true;
                  break;
                }
              }
            }
            if (match) {
              if (allTermsRequired == false) {
                // At least one query term does match:
                break;
              }
              match = false;
            } else if (allTermsRequired) {
              // At least one query term does not match:
              break;
            }
          }

          if (match) {
            if (doHilite) {
              expected.add(new Input(hilite(lastPrefix, inputTerms, queryTerms), input.v, input.term));
            } else {
              expected.add(input);
            }
          }
        }

        Collections.sort(expected,
            (a1, b) -> {
              if (a1.v > b.v) {
                return -1;
              } else if (a1.v < b.v) {
                return 1;
              } else {
                return 0;
              }
            });

        if (expected.isEmpty() == false) {

          int topN = TestUtil.nextInt(random(), 1, expected.size());

          List<LookupResult> actual = suggester.lookup(TestUtil.stringToCharSequence(query, random()), topN, allTermsRequired, doHilite);

          int expectedCount = Math.min(topN, expected.size());

          if (VERBOSE) {
            System.out.println("  expected:");
            for(int i=0;i<expectedCount;i++) {
              Input x = expected.get(i);
              System.out.println("    " + x.term.utf8ToString() + "/" + x.v);
            }
            System.out.println("  actual:");
            for(LookupResult result : actual) {
              System.out.println("    " + result);
            }
          }

          assertEquals(expectedCount, actual.size());
          for(int i=0;i<expectedCount;i++) {
            if (doHilite) {
              assertEquals(expected.get(i).term.utf8ToString(), actual.get(i).highlightKey);
            } else {
              assertEquals(expected.get(i).term.utf8ToString(), actual.get(i).key);
            }
            assertEquals(expected.get(i).v, actual.get(i).value);
            assertEquals(expected.get(i).payload, actual.get(i).payload);
          }
        } else {
          if (VERBOSE) {
            System.out.println("  no expected matches");
          }
        }
      }
    }

    lookupThread.finish();
    suggester.close();
    a.close();
  }

  private static String hilite(boolean lastPrefix, String[] inputTerms, String[] queryTerms) {
    // Stupid slow but hopefully correct highlighter:
    //System.out.println("hilite: lastPrefix=" + lastPrefix + " inputTerms=" + Arrays.toString(inputTerms) + " queryTerms=" + Arrays.toString(queryTerms));
    StringBuilder b = new StringBuilder();
    for(int i=0;i<inputTerms.length;i++) {
      if (i > 0) {
        b.append(' ');
      }
      String inputTerm = inputTerms[i];
      //System.out.println("  inputTerm=" + inputTerm);
      boolean matched = false;
      for(int j=0;j<queryTerms.length;j++) {
        String queryTerm = queryTerms[j];
        //System.out.println("    queryTerm=" + queryTerm);
        if (j < queryTerms.length-1 || lastPrefix == false) {
          //System.out.println("      check exact");
          if (inputTerm.equals(queryTerm)) {
            b.append("<b>");
            b.append(inputTerm);
            b.append("</b>");
            matched = true;
            break;
          }
        } else if (inputTerm.startsWith(queryTerm)) {
          b.append("<b>");
          b.append(queryTerm);
          b.append("</b>");
          b.append(inputTerm.substring(queryTerm.length(), inputTerm.length()));
          matched = true;
          break;
        }
      }

      if (matched == false) {
        b.append(inputTerm);
      }
    }

    return b.toString();
  }

  public void testBasicNRT() throws Exception {
    Input keys[] = new Input[] {
      new Input("lend me your ear", 8, new BytesRef("foobar")),
    };

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, false);
    suggester.build(new InputArrayIterator(keys));

    List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("lend me your ear", results.get(0).key);
    assertEquals("lend me your <b>ear</b>", results.get(0).highlightKey);
    assertEquals(8, results.get(0).value);
    assertEquals(new BytesRef("foobar"), results.get(0).payload);

    // Add a new suggestion:
    suggester.add(new BytesRef("a penny saved is a penny earned"), null, 10, new BytesRef("foobaz"));

    // Must refresh to see any newly added suggestions:
    suggester.refresh();

    results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), 10, true, true);
    assertEquals(2, results.size());
    assertEquals("a penny saved is a penny earned", results.get(0).key);
    assertEquals("a penny saved is a penny <b>ear</b>ned", results.get(0).highlightKey);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);

    assertEquals("lend me your ear", results.get(1).key);
    assertEquals("lend me your <b>ear</b>", results.get(1).highlightKey);
    assertEquals(8, results.get(1).value);
    assertEquals(new BytesRef("foobar"), results.get(1).payload);

    results = suggester.lookup(TestUtil.stringToCharSequence("ear ", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("lend me your ear", results.get(0).key);
    assertEquals("lend me your <b>ear</b>", results.get(0).highlightKey);
    assertEquals(8, results.get(0).value);
    assertEquals(new BytesRef("foobar"), results.get(0).payload);

    results = suggester.lookup(TestUtil.stringToCharSequence("pen", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a penny saved is a penny earned", results.get(0).key);
    assertEquals("a <b>pen</b>ny saved is a <b>pen</b>ny earned", results.get(0).highlightKey);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);

    results = suggester.lookup(TestUtil.stringToCharSequence("p", random()), 10, true, true);
    assertEquals(1, results.size());
    assertEquals("a penny saved is a penny earned", results.get(0).key);
    assertEquals("a <b>p</b>enny saved is a <b>p</b>enny earned", results.get(0).highlightKey);
    assertEquals(10, results.get(0).value);
    assertEquals(new BytesRef("foobaz"), results.get(0).payload);

    // Change the weight:
    suggester.update(new BytesRef("lend me your ear"), null, 12, new BytesRef("foobox"));

    // Must refresh to see any newly added suggestions:
    suggester.refresh();

    results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), 10, true, true);
    assertEquals(2, results.size());
    assertEquals("lend me your ear", results.get(0).key);
    assertEquals("lend me your <b>ear</b>", results.get(0).highlightKey);
    assertEquals(12, results.get(0).value);
    assertEquals(new BytesRef("foobox"), results.get(0).payload);
    assertEquals("a penny saved is a penny earned", results.get(1).key);
    assertEquals("a penny saved is a penny <b>ear</b>ned", results.get(1).highlightKey);
    assertEquals(10, results.get(1).value);
    assertEquals(new BytesRef("foobaz"), results.get(1).payload);
    suggester.close();
    a.close();
  }

  public void testNRTWithParallelAdds() throws IOException, InterruptedException {
    String[] keys = new String[] {"python", "java", "c", "scala", "ruby", "clojure", "erlang", "go", "swift", "lisp"};
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    Path tempDir = createTempDir("AIS_NRT_PERSIST_TEST");
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newFSDirectory(tempDir), a, a, 3, false);
    Thread[] multiAddThreads = new Thread[10];
    // Cannot call refresh on an suggester when no docs are added to the index
    expectThrows(IllegalStateException.class, () -> {
      suggester.refresh();
    });

    for(int i=0; i<10; i++) {
      multiAddThreads[i] = new Thread(new IndexDocument(suggester, keys[i]));
    }
    for(int i=0; i<10; i++) {
      multiAddThreads[i].start();
    }
    //Make sure all threads have completed indexing
    for(int i=0; i<10; i++) {
      multiAddThreads[i].join();
    }

    suggester.refresh();
    List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("python", random()), 10, true, false);
    assertEquals(1, results.size());
    assertEquals("python", results.get(0).key);

    //Test if the index is getting persisted correctly and can be reopened.
    suggester.commit();
    suggester.close();

    AnalyzingInfixSuggester suggester2 = new AnalyzingInfixSuggester(newFSDirectory(tempDir), a, a, 3, false);
    results = suggester2.lookup(TestUtil.stringToCharSequence("python", random()), 10, true, false);
    assertEquals(1, results.size());
    assertEquals("python", results.get(0).key);

    suggester2.close();
    a.close();
  }

  private class IndexDocument implements Runnable {
    AnalyzingInfixSuggester suggester;
    String key;

    private IndexDocument(AnalyzingInfixSuggester suggester, String key) {
      this.suggester = suggester;
      this.key = key;
    }

    @Override
    public void run() {
      try {
        suggester.add(new BytesRef(key), null, 10, null);
      } catch (IOException e) {
        fail("Could not build suggest dictionary correctly");
      }
    }
  }

  private Set<BytesRef> asSet(String... values) {
    HashSet<BytesRef> result = new HashSet<>();
    for(String value : values) {
      result.add(new BytesRef(value));
    }

    return result;
  }

  private Set<BytesRef> asSet(byte[]... values) {
    HashSet<BytesRef> result = new HashSet<>();
    for(byte[] value : values) {
      result.add(new BytesRef(value));
    }

    return result;
  }

  // LUCENE-5528 and LUCENE-6464
  public void testBasicContext() throws Exception {
    Input keys[] = new Input[] {
      new Input("lend me your ear", 8, new BytesRef("foobar"), asSet("foo", "bar")),
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz"), asSet("foo", "baz"))
    };

    Path tempDir = createTempDir("analyzingInfixContext");

    for(int iter=0;iter<2;iter++) {
      AnalyzingInfixSuggester suggester;
      Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
      if (iter == 0) {
        suggester = new AnalyzingInfixSuggester(newFSDirectory(tempDir), a, a, 3, false);
        suggester.build(new InputArrayIterator(keys));
      } else {
        // Test again, after close/reopen:
        suggester = new AnalyzingInfixSuggester(newFSDirectory(tempDir), a, a, 3, false);
      }

      // No context provided, all results returned
      List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), 10, true, true);
      assertEquals(2, results.size());
      LookupResult result = results.get(0);
      assertEquals("a penny saved is a penny earned", result.key);
      assertEquals("a penny saved is a penny <b>ear</b>ned", result.highlightKey);
      assertEquals(10, result.value);
      assertEquals(new BytesRef("foobaz"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef("foo")));
      assertTrue(result.contexts.contains(new BytesRef("baz")));

      result = results.get(1);
      assertEquals("lend me your ear", result.key);
      assertEquals("lend me your <b>ear</b>", result.highlightKey);
      assertEquals(8, result.value);
      assertEquals(new BytesRef("foobar"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef("foo")));
      assertTrue(result.contexts.contains(new BytesRef("bar")));

      // Both have "foo" context:
      results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), asSet("foo"), 10, true, true);
      assertEquals(2, results.size());

      result = results.get(0);
      assertEquals("a penny saved is a penny earned", result.key);
      assertEquals("a penny saved is a penny <b>ear</b>ned", result.highlightKey);
      assertEquals(10, result.value);
      assertEquals(new BytesRef("foobaz"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef("foo")));
      assertTrue(result.contexts.contains(new BytesRef("baz")));

      result = results.get(1);
      assertEquals("lend me your ear", result.key);
      assertEquals("lend me your <b>ear</b>", result.highlightKey);
      assertEquals(8, result.value);
      assertEquals(new BytesRef("foobar"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef("foo")));
      assertTrue(result.contexts.contains(new BytesRef("bar")));

      // Only one has "bar" context:
      results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), asSet("bar"), 10, true, true);
      assertEquals(1, results.size());

      result = results.get(0);
      assertEquals("lend me your ear", result.key);
      assertEquals("lend me your <b>ear</b>", result.highlightKey);
      assertEquals(8, result.value);
      assertEquals(new BytesRef("foobar"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef("foo")));
      assertTrue(result.contexts.contains(new BytesRef("bar")));

      // None do not have "foo" context:
      Map<BytesRef, BooleanClause.Occur> contextInfo = new HashMap<>();
      contextInfo.put(new BytesRef("foo"), BooleanClause.Occur.MUST_NOT);
      results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), contextInfo, 10, true, true);
      assertEquals(0, results.size());

      // Only one does not have "bar" context:
      contextInfo.clear();
      contextInfo.put(new BytesRef("bar"), BooleanClause.Occur.MUST_NOT);
      results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), contextInfo, 10, true, true);
      assertEquals(1, results.size());

      result = results.get(0);
      assertEquals("a penny saved is a penny earned", result.key);
      assertEquals("a penny saved is a penny <b>ear</b>ned", result.highlightKey);
      assertEquals(10, result.value);
      assertEquals(new BytesRef("foobaz"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef("foo")));
      assertTrue(result.contexts.contains(new BytesRef("baz")));

      // Both have "foo" or "bar" context:
      results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), asSet("foo", "bar"), 10, true, true);
      assertEquals(2, results.size());

      result = results.get(0);
      assertEquals("a penny saved is a penny earned", result.key);
      assertEquals("a penny saved is a penny <b>ear</b>ned", result.highlightKey);
      assertEquals(10, result.value);
      assertEquals(new BytesRef("foobaz"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef("foo")));
      assertTrue(result.contexts.contains(new BytesRef("baz")));

      result = results.get(1);
      assertEquals("lend me your ear", result.key);
      assertEquals("lend me your <b>ear</b>", result.highlightKey);
      assertEquals(8, result.value);
      assertEquals(new BytesRef("foobar"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef("foo")));
      assertTrue(result.contexts.contains(new BytesRef("bar")));

      // Both have "bar" or "baz" context:
      results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), asSet("bar", "baz"), 10, true, true);
      assertEquals(2, results.size());

      result = results.get(0);
      assertEquals("a penny saved is a penny earned", result.key);
      assertEquals("a penny saved is a penny <b>ear</b>ned", result.highlightKey);
      assertEquals(10, result.value);
      assertEquals(new BytesRef("foobaz"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef("foo")));
      assertTrue(result.contexts.contains(new BytesRef("baz")));

      result = results.get(1);
      assertEquals("lend me your ear", result.key);
      assertEquals("lend me your <b>ear</b>", result.highlightKey);
      assertEquals(8, result.value);
      assertEquals(new BytesRef("foobar"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef("foo")));
      assertTrue(result.contexts.contains(new BytesRef("bar")));

      // Only one has "foo" and "bar" context:
      contextInfo.clear();
      contextInfo.put(new BytesRef("foo"), BooleanClause.Occur.MUST);
      contextInfo.put(new BytesRef("bar"), BooleanClause.Occur.MUST);
      results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), contextInfo, 10, true, true);
      assertEquals(1, results.size());

      result = results.get(0);
      assertEquals("lend me your ear", result.key);
      assertEquals("lend me your <b>ear</b>", result.highlightKey);
      assertEquals(8, result.value);
      assertEquals(new BytesRef("foobar"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef("foo")));
      assertTrue(result.contexts.contains(new BytesRef("bar")));

      // None have "bar" and "baz" context:
      contextInfo.clear();
      contextInfo.put(new BytesRef("bar"), BooleanClause.Occur.MUST);
      contextInfo.put(new BytesRef("baz"), BooleanClause.Occur.MUST);
      results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), contextInfo, 10, true, true);
      assertEquals(0, results.size());

      // None do not have "foo" and do not have "bar" context:
      contextInfo.clear();
      contextInfo.put(new BytesRef("foo"), BooleanClause.Occur.MUST_NOT);
      contextInfo.put(new BytesRef("bar"), BooleanClause.Occur.MUST_NOT);
      results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), contextInfo, 10, true, true);
      assertEquals(0, results.size());

      // Both do not have "bar" and do not have "baz" context:
      contextInfo.clear();
      contextInfo.put(new BytesRef("bar"), BooleanClause.Occur.MUST_NOT);
      contextInfo.put(new BytesRef("baz"), BooleanClause.Occur.MUST_NOT);
      results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), asSet("bar", "baz"), 10, true, true);
      assertEquals(2, results.size());

      result = results.get(0);
      assertEquals("a penny saved is a penny earned", result.key);
      assertEquals("a penny saved is a penny <b>ear</b>ned", result.highlightKey);
      assertEquals(10, result.value);
      assertEquals(new BytesRef("foobaz"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef("foo")));
      assertTrue(result.contexts.contains(new BytesRef("baz")));

      result = results.get(1);
      assertEquals("lend me your ear", result.key);
      assertEquals("lend me your <b>ear</b>", result.highlightKey);
      assertEquals(8, result.value);
      assertEquals(new BytesRef("foobar"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef("foo")));
      assertTrue(result.contexts.contains(new BytesRef("bar")));

      // Only one has "foo" and does not have "bar" context:
      contextInfo.clear();
      contextInfo.put(new BytesRef("foo"), BooleanClause.Occur.MUST);
      contextInfo.put(new BytesRef("bar"), BooleanClause.Occur.MUST_NOT);
      results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), contextInfo, 10, true, true);
      assertEquals(1, results.size());

      result = results.get(0);
      assertEquals("a penny saved is a penny earned", result.key);
      assertEquals("a penny saved is a penny <b>ear</b>ned", result.highlightKey);
      assertEquals(10, result.value);
      assertEquals(new BytesRef("foobaz"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef("foo")));
      assertTrue(result.contexts.contains(new BytesRef("baz")));
      
      //LUCENE-6464 Using the advanced context filtering by query. 
      //Note that this is just a sanity test as all the above tests run through the filter by query method
      BooleanQuery.Builder query = new BooleanQuery.Builder();
      suggester.addContextToQuery(query, new BytesRef("foo"), BooleanClause.Occur.MUST);
      suggester.addContextToQuery(query, new BytesRef("bar"), BooleanClause.Occur.MUST_NOT);
      results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), query.build(), 10, true, true);
      assertEquals(1, results.size());
      
      suggester.close();
      a.close();
    }
  }

  @Test
  public void testAddPrefixMatch() throws IOException {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    Directory dir = newDirectory();
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(dir, a);

    assertEquals("<b>Sol</b>r", pfmToString(suggester, "Solr", "Sol"));
    assertEquals("<b>Solr</b>", pfmToString(suggester, "Solr", "Solr"));

    // Test SOLR-6085 - the analyzed tokens match due to ss->ß normalization
    assertEquals("<b>daß</b>", pfmToString(suggester, "daß", "dass"));

    dir.close();
    suggester.close();
    a.close();
  }

  private String pfmToString(AnalyzingInfixSuggester suggester, String surface, String prefix) throws IOException {
    StringBuilder sb = new StringBuilder();
    suggester.addPrefixMatch(sb, surface, "", prefix);
    return sb.toString();
  }

  public void testBinaryContext() throws Exception {
    byte[] context1 = new byte[4];
    byte[] context2 = new byte[5];
    byte[] context3 = new byte[1];
    context3[0] = (byte) 0xff;

    Input keys[] = new Input[] {
      new Input("lend me your ear", 8, new BytesRef("foobar"), asSet(context1, context2)),
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz"), asSet(context1, context3))
    };

    Path tempDir = createTempDir("analyzingInfixContext");

    for(int iter=0;iter<2;iter++) {
      AnalyzingInfixSuggester suggester;
      Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
      if (iter == 0) {
        suggester = new AnalyzingInfixSuggester(newFSDirectory(tempDir), a, a, 3, false);
        suggester.build(new InputArrayIterator(keys));
      } else {
        // Test again, after close/reopen:
        suggester = new AnalyzingInfixSuggester(newFSDirectory(tempDir), a, a, 3, false);
      }

      // Both have context1:
      List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), asSet(context1), 10, true, true);
      assertEquals(2, results.size());

      LookupResult result = results.get(0);
      assertEquals("a penny saved is a penny earned", result.key);
      assertEquals("a penny saved is a penny <b>ear</b>ned", result.highlightKey);
      assertEquals(10, result.value);
      assertEquals(new BytesRef("foobaz"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef(context1)));
      assertTrue(result.contexts.contains(new BytesRef(context3)));

      result = results.get(1);
      assertEquals("lend me your ear", result.key);
      assertEquals("lend me your <b>ear</b>", result.highlightKey);
      assertEquals(8, result.value);
      assertEquals(new BytesRef("foobar"), result.payload);
      assertNotNull(result.contexts);
      assertEquals(2, result.contexts.size());
      assertTrue(result.contexts.contains(new BytesRef(context1)));
      assertTrue(result.contexts.contains(new BytesRef(context2)));

      suggester.close();
      a.close();
    }
  }

  public void testContextNotAllTermsRequired() throws Exception {

    Input keys[] = new Input[] {
      new Input("lend me your ear", 8, new BytesRef("foobar"), asSet("foo", "bar")),
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz"), asSet("foo", "baz"))
    };
    Path tempDir = createTempDir("analyzingInfixContext");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newFSDirectory(tempDir), a, a, 3, false);
    suggester.build(new InputArrayIterator(keys));

    // No context provided, all results returned
    List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), 10, false, true);
    assertEquals(2, results.size());
    LookupResult result = results.get(0);
    assertEquals("a penny saved is a penny earned", result.key);
    assertEquals("a penny saved is a penny <b>ear</b>ned", result.highlightKey);
    assertEquals(10, result.value);
    assertEquals(new BytesRef("foobaz"), result.payload);
    assertNotNull(result.contexts);
    assertEquals(2, result.contexts.size());
    assertTrue(result.contexts.contains(new BytesRef("foo")));
    assertTrue(result.contexts.contains(new BytesRef("baz")));

    result = results.get(1);
    assertEquals("lend me your ear", result.key);
    assertEquals("lend me your <b>ear</b>", result.highlightKey);
    assertEquals(8, result.value);
    assertEquals(new BytesRef("foobar"), result.payload);
    assertNotNull(result.contexts);
    assertEquals(2, result.contexts.size());
    assertTrue(result.contexts.contains(new BytesRef("foo")));
    assertTrue(result.contexts.contains(new BytesRef("bar")));

    // Both have "foo" context:
    results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), asSet("foo"), 10, false, true);
    assertEquals(2, results.size());

    result = results.get(0);
    assertEquals("a penny saved is a penny earned", result.key);
    assertEquals("a penny saved is a penny <b>ear</b>ned", result.highlightKey);
    assertEquals(10, result.value);
    assertEquals(new BytesRef("foobaz"), result.payload);
    assertNotNull(result.contexts);
    assertEquals(2, result.contexts.size());
    assertTrue(result.contexts.contains(new BytesRef("foo")));
    assertTrue(result.contexts.contains(new BytesRef("baz")));

    result = results.get(1);
    assertEquals("lend me your ear", result.key);
    assertEquals("lend me your <b>ear</b>", result.highlightKey);
    assertEquals(8, result.value);
    assertEquals(new BytesRef("foobar"), result.payload);
    assertNotNull(result.contexts);
    assertEquals(2, result.contexts.size());
    assertTrue(result.contexts.contains(new BytesRef("foo")));
    assertTrue(result.contexts.contains(new BytesRef("bar")));

    // Only one has "foo" context and len
    results = suggester.lookup(TestUtil.stringToCharSequence("len", random()), asSet("foo"), 10, false, true);
    assertEquals(1, results.size());

    result = results.get(0);
    assertEquals("lend me your ear", result.key);
    assertEquals("<b>len</b>d me your ear", result.highlightKey);
    assertEquals(8, result.value);
    assertEquals(new BytesRef("foobar"), result.payload);
    assertNotNull(result.contexts);
    assertEquals(2, result.contexts.size());
    assertTrue(result.contexts.contains(new BytesRef("foo")));
    assertTrue(result.contexts.contains(new BytesRef("bar")));

    suggester.close();
  }
  
  public void testCloseIndexWriterOnBuild() throws Exception {
    class MyAnalyzingInfixSuggester extends AnalyzingInfixSuggester {
      public MyAnalyzingInfixSuggester(Directory dir, Analyzer indexAnalyzer, Analyzer queryAnalyzer, 
                                       int minPrefixChars, boolean commitOnBuild, boolean allTermsRequired,
                                       boolean highlight, boolean closeIndexWriterOnBuild) throws IOException {
        super(dir, indexAnalyzer, queryAnalyzer, minPrefixChars, commitOnBuild, 
              allTermsRequired, highlight, closeIndexWriterOnBuild);
      }
      public IndexWriter getIndexWriter() {
        return writer;
      } 
      public SearcherManager getSearcherManager() {
        return searcherMgr;
      }
    }

    // After build(), when closeIndexWriterOnBuild = true: 
    // * The IndexWriter should be null 
    // * The SearcherManager should be non-null
    // * SearcherManager's IndexWriter reference should be closed 
    //   (as evidenced by maybeRefreshBlocking() throwing AlreadyClosedException)
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    MyAnalyzingInfixSuggester suggester = new MyAnalyzingInfixSuggester(newDirectory(), a, a, 3, false,
        AnalyzingInfixSuggester.DEFAULT_ALL_TERMS_REQUIRED, AnalyzingInfixSuggester.DEFAULT_HIGHLIGHT, true);
    suggester.build(new InputArrayIterator(sharedInputs));
    assertNull(suggester.getIndexWriter());
    assertNotNull(suggester.getSearcherManager());
    expectThrows(AlreadyClosedException.class, () -> suggester.getSearcherManager().maybeRefreshBlocking());
    
    suggester.close();
    a.close();
  }
  
  public void testCommitAfterBuild() throws Exception {
    performOperationWithAllOptionCombinations(suggester -> {
      suggester.build(new InputArrayIterator(sharedInputs));
      suggester.commit();
    });    
  }

  public void testRefreshAfterBuild() throws Exception {
    performOperationWithAllOptionCombinations(suggester -> {
      suggester.build(new InputArrayIterator(sharedInputs)); 
      suggester.refresh(); 
    });
  }
  
  public void testDisallowCommitBeforeBuild() throws Exception {
    performOperationWithAllOptionCombinations
        (suggester -> expectThrows(IllegalStateException.class, suggester::commit));
  }

  public void testDisallowRefreshBeforeBuild() throws Exception {
    performOperationWithAllOptionCombinations
        (suggester -> expectThrows(IllegalStateException.class, suggester::refresh));
  }

  private Input sharedInputs[] = new Input[] {
      new Input("lend me your ear", 8, new BytesRef("foobar")),
      new Input("a penny saved is a penny earned", 10, new BytesRef("foobaz")),
  };

  private interface SuggesterOperation {
    void operate(AnalyzingInfixSuggester suggester) throws Exception;
  }

  /**
   * Perform the given operation on suggesters constructed with all combinations of options
   * commitOnBuild and closeIndexWriterOnBuild, including defaults.
   */
  private void performOperationWithAllOptionCombinations(SuggesterOperation operation) throws Exception {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);

    AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(newDirectory(), a);
    operation.operate(suggester);
    suggester.close();

    suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, false);
    operation.operate(suggester);
    suggester.close();

    suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, true);
    operation.operate(suggester);
    suggester.close();

    suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, true,
        AnalyzingInfixSuggester.DEFAULT_ALL_TERMS_REQUIRED, AnalyzingInfixSuggester.DEFAULT_HIGHLIGHT, true);
    operation.operate(suggester);
    suggester.close();

    suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, true,
        AnalyzingInfixSuggester.DEFAULT_ALL_TERMS_REQUIRED, AnalyzingInfixSuggester.DEFAULT_HIGHLIGHT, false);
    operation.operate(suggester);
    suggester.close();

    suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, false,
        AnalyzingInfixSuggester.DEFAULT_ALL_TERMS_REQUIRED, AnalyzingInfixSuggester.DEFAULT_HIGHLIGHT, true);
    operation.operate(suggester);
    suggester.close();

    suggester = new AnalyzingInfixSuggester(newDirectory(), a, a, 3, false,
        AnalyzingInfixSuggester.DEFAULT_ALL_TERMS_REQUIRED, AnalyzingInfixSuggester.DEFAULT_HIGHLIGHT, false);
    operation.operate(suggester);
    suggester.close();

    a.close();
  }
}
