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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.suggest.Input;
import org.apache.lucene.search.suggest.InputArrayIterator;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Ignore;

public class TestFreeTextSuggester extends LuceneTestCase {

  public void testBasic() throws Exception {
    Iterable<Input> keys = AnalyzingSuggesterTest.shuffle(
        new Input("foo bar baz blah", 50),
        new Input("boo foo bar foo bee", 20)
    );

    Analyzer a = new MockAnalyzer(random());
    FreeTextSuggester sug = new FreeTextSuggester(a, a, 2, (byte) 0x20);
    sug.build(new InputArrayIterator(keys));
    assertEquals(2, sug.getCount());

    for(int i=0;i<2;i++) {

      // Uses bigram model and unigram backoff:
      assertEquals("foo bar/0.67 foo bee/0.33 baz/0.04 blah/0.04 boo/0.04",
                   toString(sug.lookup("foo b", 10)));

      // Uses only bigram model:
      assertEquals("foo bar/0.67 foo bee/0.33",
                   toString(sug.lookup("foo ", 10)));

      // Uses only unigram model:
      assertEquals("foo/0.33",
                   toString(sug.lookup("foo", 10)));

      // Uses only unigram model:
      assertEquals("bar/0.22 baz/0.11 bee/0.11 blah/0.11 boo/0.11",
                   toString(sug.lookup("b", 10)));

      // Try again after save/load:
      Path tmpDir = createTempDir("FreeTextSuggesterTest");

      Path path = tmpDir.resolve("suggester");

      OutputStream os = Files.newOutputStream(path);
      sug.store(os);
      os.close();

      InputStream is = Files.newInputStream(path);
      sug = new FreeTextSuggester(a, a, 2, (byte) 0x20);
      sug.load(is);
      is.close();
      assertEquals(2, sug.getCount());
    }
    a.close();
  }

  public void testIllegalByteDuringBuild() throws Exception {
    // Default separator is INFORMATION SEPARATOR TWO
    // (0x1e), so no input token is allowed to contain it
    Iterable<Input> keys = AnalyzingSuggesterTest.shuffle(
        new Input("foo\u001ebar baz", 50)
    );
    Analyzer analyzer = new MockAnalyzer(random());
    FreeTextSuggester sug = new FreeTextSuggester(analyzer);
    expectThrows(IllegalArgumentException.class, () -> {
      sug.build(new InputArrayIterator(keys));
    });

    analyzer.close();
  }

  public void testIllegalByteDuringQuery() throws Exception {
    // Default separator is INFORMATION SEPARATOR TWO
    // (0x1e), so no input token is allowed to contain it
    Iterable<Input> keys = AnalyzingSuggesterTest.shuffle(
        new Input("foo bar baz", 50)
    );
    Analyzer analyzer = new MockAnalyzer(random());
    FreeTextSuggester sug = new FreeTextSuggester(analyzer);
    sug.build(new InputArrayIterator(keys));

    expectThrows(IllegalArgumentException.class, () -> {
      sug.lookup("foo\u001eb", 10);
    });

    analyzer.close();
  }

  @Ignore
  public void testWiki() throws Exception {
    final LineFileDocs lfd = new LineFileDocs(null, "/lucenedata/enwiki/enwiki-20120502-lines-1k.txt");
    // Skip header:
    lfd.nextDoc();
    Analyzer analyzer = new MockAnalyzer(random());
    FreeTextSuggester sug = new FreeTextSuggester(analyzer);
    sug.build(new InputIterator() {

        private int count;

        @Override
        public long weight() {
          return 1;
        }

        @Override
        public BytesRef next() {
          Document doc;
          try {
            doc = lfd.nextDoc();
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
          if (doc == null) {
            return null;
          }
          if (count++ == 10000) {
            return null;
          }
          return new BytesRef(doc.get("body"));
        }

        @Override
        public BytesRef payload() {
          return null;
        }

        @Override
        public boolean hasPayloads() {
          return false;
        }

        @Override
        public Set<BytesRef> contexts() {
          return null;
        }

        @Override
        public boolean hasContexts() {
          return false;
        }
      });
    if (VERBOSE) {
      System.out.println(sug.ramBytesUsed() + " bytes");

      List<LookupResult> results = sug.lookup("general r", 10);
      System.out.println("results:");
      for(LookupResult result : results) {
        System.out.println("  " + result);
      }
    }
    analyzer.close();
    lfd.close();
  }

  // Make sure you can suggest based only on unigram model:
  public void testUnigrams() throws Exception {
    Iterable<Input> keys = AnalyzingSuggesterTest.shuffle(
        new Input("foo bar baz blah boo foo bar foo bee", 50)
    );

    Analyzer a = new MockAnalyzer(random());
    FreeTextSuggester sug = new FreeTextSuggester(a, a, 1, (byte) 0x20);
    sug.build(new InputArrayIterator(keys));
    // Sorts first by count, descending, second by term, ascending
    assertEquals("bar/0.22 baz/0.11 bee/0.11 blah/0.11 boo/0.11",
                 toString(sug.lookup("b", 10)));
    a.close();
  }

  // Make sure the last token is not duplicated
  public void testNoDupsAcrossGrams() throws Exception {
    Iterable<Input> keys = AnalyzingSuggesterTest.shuffle(
        new Input("foo bar bar bar bar", 50)
    );
    Analyzer a = new MockAnalyzer(random());
    FreeTextSuggester sug = new FreeTextSuggester(a, a, 2, (byte) 0x20);
    sug.build(new InputArrayIterator(keys));
    assertEquals("foo bar/1.00",
                 toString(sug.lookup("foo b", 10)));
    a.close();
  }

  // Lookup of just empty string produces unicode only matches:
  public void testEmptyString() throws Exception {
    Iterable<Input> keys = AnalyzingSuggesterTest.shuffle(
        new Input("foo bar bar bar bar", 50)
    );
    Analyzer a = new MockAnalyzer(random());
    FreeTextSuggester sug = new FreeTextSuggester(a, a, 2, (byte) 0x20);
    sug.build(new InputArrayIterator(keys));
    expectThrows(IllegalArgumentException.class, () -> {
      sug.lookup("", 10);
    });

    a.close();
  }

  // With one ending hole, ShingleFilter produces "of _" and
  // we should properly predict from that:
  public void testEndingHole() throws Exception {
    // Just deletes "of"
    Analyzer a = new Analyzer() {
        @Override
        public TokenStreamComponents createComponents(String field) {
          Tokenizer tokenizer = new MockTokenizer();
          CharArraySet stopSet = StopFilter.makeStopSet("of");
          return new TokenStreamComponents(tokenizer, new StopFilter(tokenizer, stopSet));
        }
      };

    Iterable<Input> keys = AnalyzingSuggesterTest.shuffle(
        new Input("wizard of oz", 50)
    );
    FreeTextSuggester sug = new FreeTextSuggester(a, a, 3, (byte) 0x20);
    sug.build(new InputArrayIterator(keys));
    assertEquals("wizard _ oz/1.00",
                 toString(sug.lookup("wizard of", 10)));

    // Falls back to unigram model, with backoff 0.4 times
    // prop 0.5:
    assertEquals("oz/0.20",
                 toString(sug.lookup("wizard o", 10)));
    a.close();
  }

  // If the number of ending holes exceeds the ngrams window
  // then there are no predictions, because ShingleFilter
  // does not produce e.g. a hole only "_ _" token:
  public void testTwoEndingHoles() throws Exception {
    // Just deletes "of"
    Analyzer a = new Analyzer() {
        @Override
        public TokenStreamComponents createComponents(String field) {
          Tokenizer tokenizer = new MockTokenizer();
          CharArraySet stopSet = StopFilter.makeStopSet("of");
          return new TokenStreamComponents(tokenizer, new StopFilter(tokenizer, stopSet));
        }
      };

    Iterable<Input> keys = AnalyzingSuggesterTest.shuffle(
        new Input("wizard of of oz", 50)
    );
    FreeTextSuggester sug = new FreeTextSuggester(a, a, 3, (byte) 0x20);
    sug.build(new InputArrayIterator(keys));
    assertEquals("",
                 toString(sug.lookup("wizard of of", 10)));
    a.close();
  }

  private static Comparator<LookupResult> byScoreThenKey = new Comparator<LookupResult>() {
    @Override
    public int compare(LookupResult a, LookupResult b) {
      if (a.value > b.value) {
        return -1;
      } else if (a.value < b.value) {
        return 1;
      } else {
        // Tie break by UTF16 sort order:
        return ((String) a.key).compareTo((String) b.key);
      }
    }
  };

  public void testRandom() throws IOException {
    String[] terms = new String[TestUtil.nextInt(random(), 2, 10)];
    Set<String> seen = new HashSet<>();
    while (seen.size() < terms.length) {
      String token = TestUtil.randomSimpleString(random(), 1, 5);
      if (!seen.contains(token)) {
        terms[seen.size()] = token;
        seen.add(token);
      }
    }

    Analyzer a = new MockAnalyzer(random());

    int numDocs = atLeast(10);
    long totTokens = 0;
    final String[][] docs = new String[numDocs][];
    for(int i=0;i<numDocs;i++) {
      docs[i] = new String[atLeast(100)];
      if (VERBOSE) {
        System.out.print("  doc " + i + ":");
      }
      for(int j=0;j<docs[i].length;j++) {
        docs[i][j] = getZipfToken(terms);
        if (VERBOSE) {
          System.out.print(" " + docs[i][j]);
        }
      }
      if (VERBOSE) {
        System.out.println();
      }
      totTokens += docs[i].length;
    }

    int grams = TestUtil.nextInt(random(), 1, 4);

    if (VERBOSE) {
      System.out.println("TEST: " + terms.length + " terms; " + numDocs + " docs; " + grams + " grams");
    }

    // Build suggester model:
    FreeTextSuggester sug = new FreeTextSuggester(a, a, grams, (byte) 0x20);
    sug.build(new InputIterator() {
        int upto;

        @Override
        public BytesRef next() {
          if (upto == docs.length) {
            return null;
          } else {
            StringBuilder b = new StringBuilder();
            for(String token : docs[upto]) {
              b.append(' ');
              b.append(token);
            }
            upto++;
            return new BytesRef(b.toString());
          }
        }

        @Override
        public long weight() {
          return random().nextLong();
        }

        @Override
        public BytesRef payload() {
          return null;
        }

        @Override
        public boolean hasPayloads() {
          return false;
        }

        @Override
        public Set<BytesRef> contexts() {
          return null;
        }

        @Override
        public boolean hasContexts() {
          return false;
        }
      });

    // Build inefficient but hopefully correct model:
    List<Map<String,Integer>> gramCounts = new ArrayList<>(grams);
    for(int gram=0;gram<grams;gram++) {
      if (VERBOSE) {
        System.out.println("TEST: build model for gram=" + gram);
      }
      Map<String,Integer> model = new HashMap<>();
      gramCounts.add(model);
      for(String[] doc : docs) {
        for(int i=0;i<doc.length-gram;i++) {
          StringBuilder b = new StringBuilder();
          for(int j=i;j<=i+gram;j++) {
            if (j > i) {
              b.append(' ');
            }
            b.append(doc[j]);
          }
          String token = b.toString();
          Integer curCount = model.get(token);
          if (curCount == null) {
            model.put(token, 1);
          } else {
            model.put(token, 1 + curCount);
          }
          if (VERBOSE) {
            System.out.println("  add '" + token + "' -> count=" + model.get(token));
          }
        }
      }
    }

    int lookups = atLeast(100);
    for(int iter=0;iter<lookups;iter++) {
      String[] tokens = new String[TestUtil.nextInt(random(), 1, 5)];
      for(int i=0;i<tokens.length;i++) {
        tokens[i] = getZipfToken(terms);
      }

      // Maybe trim last token; be sure not to create the
      // empty string:
      int trimStart;
      if (tokens.length == 1) {
        trimStart = 1;
      } else {
        trimStart = 0;
      }
      int trimAt = TestUtil.nextInt(random(), trimStart, tokens[tokens.length - 1].length());
      tokens[tokens.length-1] = tokens[tokens.length-1].substring(0, trimAt);

      int num = TestUtil.nextInt(random(), 1, 100);
      StringBuilder b = new StringBuilder();
      for(String token : tokens) {
        b.append(' ');
        b.append(token);
      }
      String query = b.toString();
      query = query.substring(1);

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " query='" + query + "' num=" + num);
      }

      // Expected:
      List<LookupResult> expected = new ArrayList<>();
      double backoff = 1.0;
      seen = new HashSet<>();

      if (VERBOSE) {
        System.out.println("  compute expected");
      }
      for(int i=grams-1;i>=0;i--) {
        if (VERBOSE) {
          System.out.println("    grams=" + i);
        }

        if (tokens.length < i+1) {
          // Don't have enough tokens to use this model
          if (VERBOSE) {
            System.out.println("      skip");
          }
          continue;
        }

        if (i == 0 && tokens[tokens.length-1].length() == 0) {
          // Never suggest unigrams from empty string:
          if (VERBOSE) {
            System.out.println("      skip unigram priors only");
          }
          continue;
        }

        // Build up "context" ngram:
        b = new StringBuilder();
        for(int j=tokens.length-i-1;j<tokens.length-1;j++) {
          b.append(' ');
          b.append(tokens[j]);
        }
        String context = b.toString();
        if (context.length() > 0) {
          context = context.substring(1);
        }
        if (VERBOSE) {
          System.out.println("      context='" + context + "'");
        }
        long contextCount;
        if (context.length() == 0) {
          contextCount = totTokens;
        } else {
          Integer count = gramCounts.get(i-1).get(context);
          if (count == null) {
            // We never saw this context:
            backoff *= FreeTextSuggester.ALPHA;
            if (VERBOSE) {
              System.out.println("      skip: never saw context");
            }
            continue;
          }
          contextCount = count;
        }
        if (VERBOSE) {
          System.out.println("      contextCount=" + contextCount);
        }
        Map<String,Integer> model = gramCounts.get(i);

        // First pass, gather all predictions for this model:
        if (VERBOSE) {
          System.out.println("      find terms w/ prefix=" + tokens[tokens.length-1]);
        }
        List<LookupResult> tmp = new ArrayList<>();
        for(String term : terms) {
          if (term.startsWith(tokens[tokens.length-1])) {
            if (VERBOSE) {
              System.out.println("        term=" + term);
            }
            if (seen.contains(term)) {
              if (VERBOSE) {
                System.out.println("          skip seen");
              }
              continue;
            }
            String ngram = (context + " " + term).trim();
            Integer count = model.get(ngram);
            if (count != null) {
              LookupResult lr = new LookupResult(ngram, (long) (Long.MAX_VALUE * (backoff * (double) count / contextCount)));
              tmp.add(lr);
              if (VERBOSE) {
                System.out.println("      add tmp key='" + lr.key + "' score=" + lr.value);
              }
            }
          }
        }

        // Second pass, trim to only top N, and fold those
        // into overall suggestions:
        Collections.sort(tmp, byScoreThenKey);
        if (tmp.size() > num) {
          tmp.subList(num, tmp.size()).clear();
        }
        for(LookupResult result : tmp) {
          String key = result.key.toString();
          int idx = key.lastIndexOf(' ');
          String lastToken;
          if (idx != -1) {
            lastToken = key.substring(idx+1);
          } else {
            lastToken = key;
          }
          if (!seen.contains(lastToken)) {
            seen.add(lastToken);
            expected.add(result);
            if (VERBOSE) {
              System.out.println("      keep key='" + result.key + "' score=" + result.value);
            }
          }
        }
        
        backoff *= FreeTextSuggester.ALPHA;
      }

      Collections.sort(expected, byScoreThenKey);

      if (expected.size() > num) {
        expected.subList(num, expected.size()).clear();
      }

      // Actual:
      List<LookupResult> actual = sug.lookup(query, num);

      if (VERBOSE) {
        System.out.println("  expected: " + expected);
        System.out.println("    actual: " + actual);
      }

      assertEquals(expected.toString(), actual.toString());
    }
    a.close();
  }

  private static String getZipfToken(String[] tokens) {
    // Zipf-like distribution:
    for(int k=0;k<tokens.length;k++) {
      if (random().nextBoolean() || k == tokens.length-1) {
        return tokens[k];
      }
    }
    assert false;
    return null;
  }

  private static String toString(List<LookupResult> results) {
    StringBuilder b = new StringBuilder();
    for(LookupResult result : results) {
      b.append(' ');
      b.append(result.key);
      b.append('/');
      b.append(String.format(Locale.ROOT, "%.2f", ((double) result.value)/Long.MAX_VALUE));
    }
    return b.toString().trim();
  }
}

