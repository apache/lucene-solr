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
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedBinaryTokenStream;
import org.apache.lucene.analysis.CannedBinaryTokenStream.BinaryToken;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.MockUTF16TermAttributeImpl;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.suggest.Input;
import org.apache.lucene.search.suggest.InputArrayIterator;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class AnalyzingSuggesterTest extends LuceneTestCase {

  /** this is basically the WFST test ported to KeywordAnalyzer. so it acts the same */
  public void testKeyword() throws Exception {
    Iterable<Input> keys = shuffle(
        new Input("foo", 50),
        new Input("bar", 10),
        new Input("barbar", 10),
        new Input("barbar", 12),
        new Input("barbara", 6),
        new Input("bar", 5),
        new Input("barbara", 1)
    );

    Directory tempDir = getDirectory();

    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.KEYWORD, false);
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", analyzer);
    suggester.build(new InputArrayIterator(keys));
    
    // top N of 2, but only foo is available
    List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("f", random()), false, 2);
    assertEquals(1, results.size());
    assertEquals("foo", results.get(0).key.toString());
    assertEquals(50, results.get(0).value, 0.01F);
    
    // top N of 1 for 'bar': we return this even though
    // barbar is higher because exactFirst is enabled:
    results = suggester.lookup(TestUtil.stringToCharSequence("bar", random()), false, 1);
    assertEquals(1, results.size());
    assertEquals("bar", results.get(0).key.toString());
    assertEquals(10, results.get(0).value, 0.01F);
    
    // top N Of 2 for 'b'
    results = suggester.lookup(TestUtil.stringToCharSequence("b", random()), false, 2);
    assertEquals(2, results.size());
    assertEquals("barbar", results.get(0).key.toString());
    assertEquals(12, results.get(0).value, 0.01F);
    assertEquals("bar", results.get(1).key.toString());
    assertEquals(10, results.get(1).value, 0.01F);
    
    // top N of 3 for 'ba'
    results = suggester.lookup(TestUtil.stringToCharSequence("ba", random()), false, 3);
    assertEquals(3, results.size());
    assertEquals("barbar", results.get(0).key.toString());
    assertEquals(12, results.get(0).value, 0.01F);
    assertEquals("bar", results.get(1).key.toString());
    assertEquals(10, results.get(1).value, 0.01F);
    assertEquals("barbara", results.get(2).key.toString());
    assertEquals(6, results.get(2).value, 0.01F);
    
    IOUtils.close(analyzer, tempDir);
  }
  
  public void testKeywordWithPayloads() throws Exception {
    Iterable<Input> keys = shuffle(
      new Input("foo", 50, new BytesRef("hello")),
      new Input("bar", 10, new BytesRef("goodbye")),
      new Input("barbar", 12, new BytesRef("thank you")),
      new Input("bar", 9, new BytesRef("should be deduplicated")),
      new Input("bar", 8, new BytesRef("should also be deduplicated")),
      new Input("barbara", 6, new BytesRef("for all the fish")));
    
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.KEYWORD, false);
    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", analyzer);
    suggester.build(new InputArrayIterator(keys));
    for (int i = 0; i < 2; i++) {
      // top N of 2, but only foo is available
      List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("f", random()), false, 2);
      assertEquals(1, results.size());
      assertEquals("foo", results.get(0).key.toString());
      assertEquals(50, results.get(0).value, 0.01F);
      assertEquals(new BytesRef("hello"), results.get(0).payload);
      
      // top N of 1 for 'bar': we return this even though
      // barbar is higher because exactFirst is enabled:
      results = suggester.lookup(TestUtil.stringToCharSequence("bar", random()), false, 1);
      assertEquals(1, results.size());
      assertEquals("bar", results.get(0).key.toString());
      assertEquals(10, results.get(0).value, 0.01F);
      assertEquals(new BytesRef("goodbye"), results.get(0).payload);
      
      // top N Of 2 for 'b'
      results = suggester.lookup(TestUtil.stringToCharSequence("b", random()), false, 2);
      assertEquals(2, results.size());
      assertEquals("barbar", results.get(0).key.toString());
      assertEquals(12, results.get(0).value, 0.01F);
      assertEquals(new BytesRef("thank you"), results.get(0).payload);
      assertEquals("bar", results.get(1).key.toString());
      assertEquals(10, results.get(1).value, 0.01F);
      assertEquals(new BytesRef("goodbye"), results.get(1).payload);
      
      // top N of 3 for 'ba'
      results = suggester.lookup(TestUtil.stringToCharSequence("ba", random()), false, 3);
      assertEquals(3, results.size());
      assertEquals("barbar", results.get(0).key.toString());
      assertEquals(12, results.get(0).value, 0.01F);
      assertEquals(new BytesRef("thank you"), results.get(0).payload);
      assertEquals("bar", results.get(1).key.toString());
      assertEquals(10, results.get(1).value, 0.01F);
      assertEquals(new BytesRef("goodbye"), results.get(1).payload);
      assertEquals("barbara", results.get(2).key.toString());
      assertEquals(6, results.get(2).value, 0.01F);
      assertEquals(new BytesRef("for all the fish"), results.get(2).payload);
    }
    IOUtils.close(analyzer, tempDir);
  }
  
  public void testRandomRealisticKeys() throws IOException {
    LineFileDocs lineFile = new LineFileDocs(random());
    Map<String, Long> mapping = new HashMap<>();
    List<Input> keys = new ArrayList<>();
    
    int howMany = atLeast(100); // this might bring up duplicates
    for (int i = 0; i < howMany; i++) {
      Document nextDoc = lineFile.nextDoc();
      String title = nextDoc.getField("title").stringValue();
      int randomWeight = random().nextInt(100);
      keys.add(new Input(title, randomWeight));
      if (!mapping.containsKey(title) || mapping.get(title) < randomWeight) {
          mapping.put(title, Long.valueOf(randomWeight));
      }
    }
    Analyzer indexAnalyzer = new MockAnalyzer(random());
    Analyzer queryAnalyzer = new MockAnalyzer(random());
    Directory tempDir = getDirectory();

    AnalyzingSuggester analyzingSuggester = new AnalyzingSuggester(tempDir, "suggest", indexAnalyzer, queryAnalyzer,
        AnalyzingSuggester.EXACT_FIRST | AnalyzingSuggester.PRESERVE_SEP, 256, -1, random().nextBoolean());
    boolean doPayloads = random().nextBoolean();
    if (doPayloads) {
      List<Input> keysAndPayloads = new ArrayList<>();
      for (Input termFreq : keys) {
        keysAndPayloads.add(new Input(termFreq.term, termFreq.v, new BytesRef(Long.toString(termFreq.v))));
      }
      analyzingSuggester.build(new InputArrayIterator(keysAndPayloads));
    } else {
      analyzingSuggester.build(new InputArrayIterator(keys));  
    }
    
    for (Input termFreq : keys) {
      List<LookupResult> lookup = analyzingSuggester.lookup(termFreq.term.utf8ToString(), false, keys.size());
      for (LookupResult lookupResult : lookup) {
        assertEquals(mapping.get(lookupResult.key), Long.valueOf(lookupResult.value));
        if (doPayloads) {
          assertEquals(lookupResult.payload.utf8ToString(), Long.toString(lookupResult.value));
        } else {
          assertNull(lookupResult.payload);
        }
      }
    }
    
    IOUtils.close(lineFile, indexAnalyzer, queryAnalyzer, tempDir);
  }
  
  // TODO: more tests
  /**
   * basic "standardanalyzer" test with stopword removal
   */
  public void testStandard() throws Exception {
    Input keys[] = new Input[] {
        new Input("the ghost of christmas past", 50),
    };
    
    Directory tempDir = getDirectory();
    Analyzer standard = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET);
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", standard, standard, 
        AnalyzingSuggester.EXACT_FIRST | AnalyzingSuggester.PRESERVE_SEP, 256, -1, false);

    suggester.build(new InputArrayIterator(keys));
    
    List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("the ghost of chris", random()), false, 1);
    assertEquals(1, results.size());
    assertEquals("the ghost of christmas past", results.get(0).key.toString());
    assertEquals(50, results.get(0).value, 0.01F);

    // omit the 'the' since it's a stopword, it's suggested anyway
    results = suggester.lookup(TestUtil.stringToCharSequence("ghost of chris", random()), false, 1);
    assertEquals(1, results.size());
    assertEquals("the ghost of christmas past", results.get(0).key.toString());
    assertEquals(50, results.get(0).value, 0.01F);

    // omit the 'the' and 'of' since they are stopwords, it's suggested anyway
    results = suggester.lookup(TestUtil.stringToCharSequence("ghost chris", random()), false, 1);
    assertEquals(1, results.size());
    assertEquals("the ghost of christmas past", results.get(0).key.toString());
    assertEquals(50, results.get(0).value, 0.01F);
    
    IOUtils.close(standard, tempDir);
  }

  public void testEmpty() throws Exception {
    Analyzer standard = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET);
    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", standard);
    suggester.build(new InputArrayIterator(new Input[0]));

    List<LookupResult> result = suggester.lookup("a", false, 20);
    assertTrue(result.isEmpty());
    IOUtils.close(standard, tempDir);
  }

  public void testNoSeps() throws Exception {
    Input[] keys = new Input[] {
      new Input("ab cd", 0),
      new Input("abcd", 1),
    };

    int options = 0;

    Analyzer a = new MockAnalyzer(random());
    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a, a, options, 256, -1, true);
    suggester.build(new InputArrayIterator(keys));
    // TODO: would be nice if "ab " would allow the test to
    // pass, and more generally if the analyzer can know
    // that the user's current query has ended at a word, 
    // but, analyzers don't produce SEP tokens!
    List<LookupResult> r = suggester.lookup(TestUtil.stringToCharSequence("ab c", random()), false, 2);
    assertEquals(2, r.size());

    // With no PRESERVE_SEPS specified, "ab c" should also
    // complete to "abcd", which has higher weight so should
    // appear first:
    assertEquals("abcd", r.get(0).key.toString());
    IOUtils.close(a, tempDir);
  }

  public void testGraphDups() throws Exception {

    final Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
        
        return new TokenStreamComponents(tokenizer) {
          int tokenStreamCounter = 0;
          final TokenStream[] tokenStreams = new TokenStream[] {
            new CannedTokenStream(new Token[] {
                token("wifi",1,1),
                token("hotspot",0,2),
                token("network",1,1),
                token("is",1,1),
                token("slow",1,1)
              }),
            new CannedTokenStream(new Token[] {
                token("wi",1,1),
                token("hotspot",0,3),
                token("fi",1,1),
                token("network",1,1),
                token("is",1,1),
                token("fast",1,1)

              }),
            new CannedTokenStream(new Token[] {
                token("wifi",1,1),
                token("hotspot",0,2),
                token("network",1,1)
              }),
          };

          @Override
          public TokenStream getTokenStream() {
            TokenStream result = tokenStreams[tokenStreamCounter];
            tokenStreamCounter++;
            return result;
          }
         
          @Override
          protected void setReader(final Reader reader) {
          }
        };
      }
    };

    Input keys[] = new Input[] {
        new Input("wifi network is slow", 50),
        new Input("wi fi network is fast", 10),
    };
    //AnalyzingSuggester suggester = new AnalyzingSuggester(analyzer, AnalyzingSuggester.EXACT_FIRST, 256, -1);
    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", analyzer);
    suggester.build(new InputArrayIterator(keys));
    List<LookupResult> results = suggester.lookup("wifi network", false, 10);
    if (VERBOSE) {
      System.out.println("Results: " + results);
    }
    assertEquals(2, results.size());
    assertEquals("wifi network is slow", results.get(0).key);
    assertEquals(50, results.get(0).value);
    assertEquals("wi fi network is fast", results.get(1).key);
    assertEquals(10, results.get(1).value);
    IOUtils.close(analyzer, tempDir);
  }

  public void testInputPathRequired() throws Exception {

    //  SynonymMap.Builder b = new SynonymMap.Builder(false);
    //  b.add(new CharsRef("ab"), new CharsRef("ba"), true);
    //  final SynonymMap map = b.build();

    //  The Analyzer below mimics the functionality of the SynonymAnalyzer
    //  using the above map, so that the suggest module does not need a dependency on the 
    //  synonym module 

    final Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
        
        return new TokenStreamComponents(tokenizer) {
          int tokenStreamCounter = 0;
          final TokenStream[] tokenStreams = new TokenStream[] {
            new CannedTokenStream(new Token[] {
                token("ab",1,1),
                token("ba",0,1),
                token("xc",1,1)
              }),
            new CannedTokenStream(new Token[] {
                token("ba",1,1),          
                token("xd",1,1)
              }),
            new CannedTokenStream(new Token[] {
                token("ab",1,1),
                token("ba",0,1),
                token("x",1,1)
              })
          };

          @Override
          public TokenStream getTokenStream() {
            TokenStream result = tokenStreams[tokenStreamCounter];
            tokenStreamCounter++;
            return result;
          }
         
          @Override
          protected void setReader(final Reader reader) {
          }
        };
      }
    };

    Input keys[] = new Input[] {
        new Input("ab xc", 50),
        new Input("ba xd", 50),
    };
    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", analyzer);
    suggester.build(new InputArrayIterator(keys));
    List<LookupResult> results = suggester.lookup("ab x", false, 1);
    assertTrue(results.size() == 1);
    IOUtils.close(analyzer, tempDir);
  }

  private static Token token(String term, int posInc, int posLength) {
    final Token t = new Token(term, 0, 0);
    t.setPositionIncrement(posInc);
    t.setPositionLength(posLength);
    return t;
  }

  private static BinaryToken token(BytesRef term) {
    return new BinaryToken(term);
  }

  /*
  private void printTokens(final Analyzer analyzer, String input) throws IOException {
    System.out.println("Tokens for " + input);
    TokenStream ts = analyzer.tokenStream("", new StringReader(input));
    ts.reset();
    final TermToBytesRefAttribute termBytesAtt = ts.addAttribute(TermToBytesRefAttribute.class);
    final PositionIncrementAttribute posIncAtt = ts.addAttribute(PositionIncrementAttribute.class);
    final PositionLengthAttribute posLengthAtt = ts.addAttribute(PositionLengthAttribute.class);
    
    while(ts.incrementToken()) {
      termBytesAtt.fillBytesRef();
      System.out.println(String.format("%s,%s,%s", termBytesAtt.getBytesRef().utf8ToString(), posIncAtt.getPositionIncrement(), posLengthAtt.getPositionLength()));      
    }
    ts.end();
    ts.close();
  } 
  */ 

  private final Analyzer getUnusualAnalyzer() {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
        
        return new TokenStreamComponents(tokenizer) {

          int count;

          @Override
          public TokenStream getTokenStream() {
            // 4th time we are called, return tokens a b,
            // else just a:
            if (count++ != 3) {
              return new CannedTokenStream(new Token[] {
                  token("a", 1, 1),
                });
            } else {
              // After that "a b":
              return new CannedTokenStream(new Token[] {
                  token("a", 1, 1),
                  token("b", 1, 1),
                });
            }
          }
         
          @Override
          protected void setReader(final Reader reader)  {
          }
        };
      }
    };
  }

  public void testExactFirst() throws Exception {

    Analyzer a = getUnusualAnalyzer();
    int options = AnalyzingSuggester.EXACT_FIRST | AnalyzingSuggester.PRESERVE_SEP;
    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a, a, options, 256, -1, true);
    suggester.build(new InputArrayIterator(new Input[] {
          new Input("x y", 1),
          new Input("x y z", 3),
          new Input("x", 2),
          new Input("z z z", 20),
        }));

    //System.out.println("ALL: " + suggester.lookup("x y", false, 6));

    for(int topN=1;topN<6;topN++) {
      List<LookupResult> results = suggester.lookup("x y", false, topN);
      //System.out.println("topN=" + topN + " " + results);

      assertEquals(Math.min(topN, 4), results.size());

      assertEquals("x y", results.get(0).key);
      assertEquals(1, results.get(0).value);

      if (topN > 1) {
        assertEquals("z z z", results.get(1).key);
        assertEquals(20, results.get(1).value);

        if (topN > 2) {
          assertEquals("x y z", results.get(2).key);
          assertEquals(3, results.get(2).value);

          if (topN > 3) {
            assertEquals("x", results.get(3).key);
            assertEquals(2, results.get(3).value);
          }
        }
      }
    }
    IOUtils.close(a, tempDir);
  }

  public void testNonExactFirst() throws Exception {

    Analyzer a = getUnusualAnalyzer();
    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a, a, AnalyzingSuggester.PRESERVE_SEP, 256, -1, true);

    suggester.build(new InputArrayIterator(new Input[] {
          new Input("x y", 1),
          new Input("x y z", 3),
          new Input("x", 2),
          new Input("z z z", 20),
        }));

    for(int topN=1;topN<6;topN++) {
      List<LookupResult> results = suggester.lookup("p", false, topN);

      assertEquals(Math.min(topN, 4), results.size());

      assertEquals("z z z", results.get(0).key);
      assertEquals(20, results.get(0).value);

      if (topN > 1) {
        assertEquals("x y z", results.get(1).key);
        assertEquals(3, results.get(1).value);

        if (topN > 2) {
          assertEquals("x", results.get(2).key);
          assertEquals(2, results.get(2).value);
          
          if (topN > 3) {
            assertEquals("x y", results.get(3).key);
            assertEquals(1, results.get(3).value);
          }
        }
      }
    }
    IOUtils.close(a, tempDir);
  }
  
  // Holds surface form separately:
  private static class TermFreq2 implements Comparable<TermFreq2> {
    public final String surfaceForm;
    public final String analyzedForm;
    public final long weight;
    public final BytesRef payload;

    public TermFreq2(String surfaceForm, String analyzedForm, long weight, BytesRef payload) {
      this.surfaceForm = surfaceForm;
      this.analyzedForm = analyzedForm;
      this.weight = weight;
      this.payload = payload;
    }

    @Override
    public int compareTo(TermFreq2 other) {
      int cmp = analyzedForm.compareTo(other.analyzedForm);
      if (cmp != 0) {
        return cmp;
      } else if (weight > other.weight) {
        return -1;
      } else if (weight < other.weight) {
        return 1;
      } else {
        assert false;
        return 0;
      }
    }

    @Override
    public String toString() {
      return surfaceForm + "/" + weight;
    }
  }

  static boolean isStopChar(char ch, int numStopChars) {
    //System.out.println("IS? " + ch + ": " + (ch - 'a') + ": " + ((ch - 'a') < numStopChars));
    return (ch - 'a') < numStopChars;
  }

  // Like StopFilter:
  private static class TokenEater extends TokenFilter {
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final int numStopChars;
    private final boolean preserveHoles;
    private boolean first;

    public TokenEater(boolean preserveHoles, TokenStream in, int numStopChars) {
      super(in);
      this.preserveHoles = preserveHoles;
      this.numStopChars = numStopChars;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      first = true;
    }

    @Override
    public final boolean incrementToken() throws IOException {
      int skippedPositions = 0;
      while (input.incrementToken()) {
        if (termAtt.length() != 1 || !isStopChar(termAtt.charAt(0), numStopChars)) {
          int posInc = posIncrAtt.getPositionIncrement() + skippedPositions;
          if (first) {
            if (posInc == 0) {
              // first token having posinc=0 is illegal.
              posInc = 1;
            }
            first = false;
          }
          posIncrAtt.setPositionIncrement(posInc);
          //System.out.println("RETURN term=" + termAtt + " numStopChars=" + numStopChars);
          return true;
        }
        if (preserveHoles) {
          skippedPositions += posIncrAtt.getPositionIncrement();
        }
      }

      return false;
    }
  }

  private static class MockTokenEatingAnalyzer extends Analyzer {
    private int numStopChars;
    private boolean preserveHoles;

    public MockTokenEatingAnalyzer(int numStopChars, boolean preserveHoles) {
      this.preserveHoles = preserveHoles;
      this.numStopChars = numStopChars;
    }

    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      MockTokenizer tokenizer = new MockTokenizer(MockUTF16TermAttributeImpl.UTF16_TERM_ATTRIBUTE_FACTORY,
          MockTokenizer.WHITESPACE, false, MockTokenizer.DEFAULT_MAX_TOKEN_LENGTH);
      tokenizer.setEnableChecks(true);
      TokenStream next;
      if (numStopChars != 0) {
        next = new TokenEater(preserveHoles, tokenizer, numStopChars);
      } else {
        next = tokenizer;
      }
      return new TokenStreamComponents(tokenizer, next);
    }
  }

  private static char SEP = '\u001F';

  public void testRandom() throws Exception {

    int numQueries = atLeast(1000);
    
    final List<TermFreq2> slowCompletor = new ArrayList<>();
    final TreeSet<String> allPrefixes = new TreeSet<>();
    final Set<String> seen = new HashSet<>();
    
    boolean doPayloads = random().nextBoolean();

    Input[] keys = null;
    Input[] payloadKeys = null;
    if (doPayloads) {
      payloadKeys = new Input[numQueries];
    } else {
      keys = new Input[numQueries];
    }

    boolean preserveSep = random().nextBoolean();

    final int numStopChars = random().nextInt(10);
    final boolean preserveHoles = random().nextBoolean();

    if (VERBOSE) {
      System.out.println("TEST: " + numQueries + " words; preserveSep=" + preserveSep + " numStopChars=" + numStopChars + " preserveHoles=" + preserveHoles);
    }
    
    for (int i = 0; i < numQueries; i++) {
      int numTokens = TestUtil.nextInt(random(), 1, 4);
      String key;
      String analyzedKey;
      while(true) {
        key = "";
        analyzedKey = "";
        boolean lastRemoved = false;
        for(int token=0;token < numTokens;token++) {
          String s;
          while (true) {
            // TODO: would be nice to fix this slowCompletor/comparator to
            // use full range, but we might lose some coverage too...
            s = TestUtil.randomSimpleString(random());
            if (s.length() > 0) {
              if (token > 0) {
                key += " ";
              }
              if (preserveSep && analyzedKey.length() > 0 && analyzedKey.charAt(analyzedKey.length()-1) != SEP) {
                analyzedKey += SEP;
              }
              key += s;
              if (s.length() == 1 && isStopChar(s.charAt(0), numStopChars)) {
                lastRemoved = true;
                if (preserveSep && preserveHoles) {
                  analyzedKey += SEP;
                }
              } else {
                lastRemoved = false;
                analyzedKey += s;
              }
              break;
            }
          }
        }

        analyzedKey = analyzedKey.replaceAll("(^|" + SEP + ")" + SEP + "$", "");

        if (preserveSep && lastRemoved) {
          analyzedKey += SEP;
        }

        // Don't add same surface form more than once:
        if (!seen.contains(key)) {
          seen.add(key);
          break;
        }
      }

      for (int j = 1; j < key.length(); j++) {
        allPrefixes.add(key.substring(0, j));
      }
      // we can probably do Integer.MAX_VALUE here, but why worry.
      int weight = random().nextInt(1<<24);
      BytesRef payload;
      if (doPayloads) {
        byte[] bytes = new byte[random().nextInt(10)];
        random().nextBytes(bytes);
        payload = new BytesRef(bytes);
        payloadKeys[i] = new Input(key, weight, payload);
      } else {
        keys[i] = new Input(key, weight);
        payload = null;
      }

      slowCompletor.add(new TermFreq2(key, analyzedKey, weight, payload));
    }

    if (VERBOSE) {
      // Don't just sort original list, to avoid VERBOSE
      // altering the test:
      List<TermFreq2> sorted = new ArrayList<>(slowCompletor);
      Collections.sort(sorted);
      for(TermFreq2 ent : sorted) {
        System.out.println("  surface='" + ent.surfaceForm + "' analyzed='" + ent.analyzedForm + "' weight=" + ent.weight);
      }
    }

    Analyzer a = new MockTokenEatingAnalyzer(numStopChars, preserveHoles);
    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a, a,
                                                          preserveSep ? AnalyzingSuggester.PRESERVE_SEP : 0, 256, -1, true);
    if (doPayloads) {
      suggester.build(new InputArrayIterator(shuffle(payloadKeys)));
    } else {
      suggester.build(new InputArrayIterator(shuffle(keys)));
    }

    for (String prefix : allPrefixes) {

      if (VERBOSE) {
        System.out.println("\nTEST: prefix=" + prefix);
      }

      final int topN = TestUtil.nextInt(random(), 1, 10);
      List<LookupResult> r = suggester.lookup(TestUtil.stringToCharSequence(prefix, random()), false, topN);

      // 2. go thru whole set to find suggestions:
      List<TermFreq2> matches = new ArrayList<>();

      // "Analyze" the key:
      String[] tokens = prefix.split(" ");
      StringBuilder builder = new StringBuilder();
      boolean lastRemoved = false;
      for(int i=0;i<tokens.length;i++) {
        String token = tokens[i];
        if (preserveSep && builder.length() > 0 && !builder.toString().endsWith(""+SEP)) {
          builder.append(SEP);
        }

        if (token.length() == 1 && isStopChar(token.charAt(0), numStopChars)) {
          if (preserveSep && preserveHoles) {
            builder.append(SEP);
          }
          lastRemoved = true;
        } else {
          builder.append(token);
          lastRemoved = false;
        }
      }

      String analyzedKey = builder.toString();

      // Remove trailing sep/holes (TokenStream.end() does
      // not tell us any trailing holes, yet ... there is an
      // issue open for this):
      while (true) {
        String s = analyzedKey.replaceAll(SEP + "$", "");
        if (s.equals(analyzedKey)) {
          break;
        }
        analyzedKey = s;
      }

      if (analyzedKey.length() == 0) {
        // Currently suggester can't suggest from the empty
        // string!  You get no results, not all results...
        continue;
      }

      if (preserveSep && (prefix.endsWith(" ") || lastRemoved)) {
        analyzedKey += SEP;
      }

      if (VERBOSE) {
        System.out.println("  analyzed: " + analyzedKey);
      }

      // TODO: could be faster... but it's slowCompletor for a reason
      for (TermFreq2 e : slowCompletor) {
        if (e.analyzedForm.startsWith(analyzedKey)) {
          matches.add(e);
        }
      }

      assertTrue(numStopChars > 0 || matches.size() > 0);

      if (matches.size() > 1) {
        Collections.sort(matches, new Comparator<TermFreq2>() {
            @Override
            public int compare(TermFreq2 left, TermFreq2 right) {
              int cmp = Float.compare(right.weight, left.weight);
              if (cmp == 0) {
                return left.analyzedForm.compareTo(right.analyzedForm);
              } else {
                return cmp;
              }
            }
          });
      }

      if (matches.size() > topN) {
        matches = matches.subList(0, topN);
      }

      if (VERBOSE) {
        System.out.println("  expected:");
        for(TermFreq2 lr : matches) {
          System.out.println("    key=" + lr.surfaceForm + " weight=" + lr.weight);
        }

        System.out.println("  actual:");
        for(LookupResult lr : r) {
          System.out.println("    key=" + lr.key + " weight=" + lr.value);
        }
      }

      assertEquals(matches.size(), r.size());

      for(int hit=0;hit<r.size();hit++) {
        //System.out.println("  check hit " + hit);
        assertEquals(matches.get(hit).surfaceForm.toString(), r.get(hit).key.toString());
        assertEquals(matches.get(hit).weight, r.get(hit).value, 0f);
        if (doPayloads) {
          assertEquals(matches.get(hit).payload, r.get(hit).payload);
        }
      }
    }
    IOUtils.close(a, tempDir);
  }

  public void testMaxSurfaceFormsPerAnalyzedForm() throws Exception {
    Analyzer a = new MockAnalyzer(random());
    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a, a, 0, 2, -1, true);
    suggester.build(new InputArrayIterator(shuffle(new Input("a", 40),
        new Input("a ", 50), new Input(" a", 60))));

    List<LookupResult> results = suggester.lookup("a", false, 5);
    assertEquals(2, results.size());
    assertEquals(" a", results.get(0).key);
    assertEquals(60, results.get(0).value);
    assertEquals("a ", results.get(1).key);
    assertEquals(50, results.get(1).value);
    IOUtils.close(a, tempDir);
  }

  public void testQueueExhaustion() throws Exception {
    Analyzer a = new MockAnalyzer(random());
    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a, a, AnalyzingSuggester.EXACT_FIRST, 256, -1, true);

    suggester.build(new InputArrayIterator(new Input[] {
          new Input("a", 2),
          new Input("a b c", 3),
          new Input("a c a", 1),
          new Input("a c b", 1),
        }));

    suggester.lookup("a", false, 4);
    IOUtils.close(a, tempDir);
  }

  public void testExactFirstMissingResult() throws Exception {

    Analyzer a = new MockAnalyzer(random());

    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a, a, AnalyzingSuggester.EXACT_FIRST, 256, -1, true);

    suggester.build(new InputArrayIterator(new Input[] {
          new Input("a", 5),
          new Input("a b", 3),
          new Input("a c", 4),
        }));

    assertEquals(3, suggester.getCount());
    List<LookupResult> results = suggester.lookup("a", false, 3);
    assertEquals(3, results.size());
    assertEquals("a", results.get(0).key);
    assertEquals(5, results.get(0).value);
    assertEquals("a c", results.get(1).key);
    assertEquals(4, results.get(1).value);
    assertEquals("a b", results.get(2).key);
    assertEquals(3, results.get(2).value);

    // Try again after save/load:
    Path tmpDir = createTempDir("AnalyzingSuggesterTest");

    Path path = tmpDir.resolve("suggester");

    OutputStream os = Files.newOutputStream(path);
    suggester.store(os);
    os.close();

    InputStream is = Files.newInputStream(path);
    suggester.load(is);
    is.close();

    assertEquals(3, suggester.getCount());
    results = suggester.lookup("a", false, 3);
    assertEquals(3, results.size());
    assertEquals("a", results.get(0).key);
    assertEquals(5, results.get(0).value);
    assertEquals("a c", results.get(1).key);
    assertEquals(4, results.get(1).value);
    assertEquals("a b", results.get(2).key);
    assertEquals(3, results.get(2).value);
    IOUtils.close(a, tempDir);
  }

  public void testDupSurfaceFormsMissingResults() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
        
        return new TokenStreamComponents(tokenizer) {

          @Override
          public TokenStream getTokenStream() {
            return new CannedTokenStream(new Token[] {
                token("hairy", 1, 1),
                token("smelly", 0, 1),
                token("dog", 1, 1),
              });
          }
         
          @Override
          protected void setReader(final Reader reader) {
          }
        };
      }
    };

    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a, a, 0, 256, -1, true);

    suggester.build(new InputArrayIterator(shuffle(
          new Input("hambone", 6),
          new Input("nellie", 5))));

    List<LookupResult> results = suggester.lookup("nellie", false, 2);
    assertEquals(2, results.size());
    assertEquals("hambone", results.get(0).key);
    assertEquals(6, results.get(0).value);
    assertEquals("nellie", results.get(1).key);
    assertEquals(5, results.get(1).value);

    // Try again after save/load:
    Path tmpDir = createTempDir("AnalyzingSuggesterTest");

    Path path = tmpDir.resolve("suggester");

    OutputStream os = Files.newOutputStream(path);
    suggester.store(os);
    os.close();

    InputStream is = Files.newInputStream(path);
    suggester.load(is);
    is.close();

    results = suggester.lookup("nellie", false, 2);
    assertEquals(2, results.size());
    assertEquals("hambone", results.get(0).key);
    assertEquals(6, results.get(0).value);
    assertEquals("nellie", results.get(1).key);
    assertEquals(5, results.get(1).value);
    IOUtils.close(a, tempDir);
  }

  public void testDupSurfaceFormsMissingResults2() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
        
        return new TokenStreamComponents(tokenizer) {

          int count;

          @Override
          public TokenStream getTokenStream() {
            if (count == 0) {
              count++;
              return new CannedTokenStream(new Token[] {
                  token("p", 1, 1),
                  token("q", 1, 1),
                  token("r", 0, 1),
                  token("s", 0, 1),
                });
            } else {
              return new CannedTokenStream(new Token[] {
                  token("p", 1, 1),
                });
            }
          }
         
          @Override
          protected void setReader(final Reader reader) {
          }
        };
      }
    };

    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a, a, 0, 256, -1, true);

    suggester.build(new InputArrayIterator(new Input[] {
          new Input("a", 6),
          new Input("b", 5),
        }));

    List<LookupResult> results = suggester.lookup("a", false, 2);
    assertEquals(2, results.size());
    assertEquals("a", results.get(0).key);
    assertEquals(6, results.get(0).value);
    assertEquals("b", results.get(1).key);
    assertEquals(5, results.get(1).value);

    // Try again after save/load:
    Path tmpDir = createTempDir("AnalyzingSuggesterTest");

    Path path = tmpDir.resolve("suggester");

    OutputStream os = Files.newOutputStream(path);
    suggester.store(os);
    os.close();

    InputStream is = Files.newInputStream(path);
    suggester.load(is);
    is.close();

    results = suggester.lookup("a", false, 2);
    assertEquals(2, results.size());
    assertEquals("a", results.get(0).key);
    assertEquals(6, results.get(0).value);
    assertEquals("b", results.get(1).key);
    assertEquals(5, results.get(1).value);
    IOUtils.close(a, tempDir);
  }

  public void test0ByteKeys() throws Exception {
    final Analyzer a = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
        
          return new TokenStreamComponents(tokenizer) {
            int tokenStreamCounter = 0;
            final TokenStream[] tokenStreams = new TokenStream[] {
              new CannedBinaryTokenStream(new BinaryToken[] {
                  token(new BytesRef(new byte[] {0x0, 0x0, 0x0})),
                }),
              new CannedBinaryTokenStream(new BinaryToken[] {
                  token(new BytesRef(new byte[] {0x0, 0x0})),
                }),
              new CannedBinaryTokenStream(new BinaryToken[] {
                  token(new BytesRef(new byte[] {0x0, 0x0, 0x0})),
                }),
              new CannedBinaryTokenStream(new BinaryToken[] {
                  token(new BytesRef(new byte[] {0x0, 0x0})),
                }),
            };

            @Override
            public TokenStream getTokenStream() {
              TokenStream result = tokenStreams[tokenStreamCounter];
              tokenStreamCounter++;
              return result;
            }
         
            @Override
            protected void setReader(final Reader reader) {
            }
          };
        }
      };

    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a, a, 0, 256, -1, true);

    suggester.build(new InputArrayIterator(new Input[] {
          new Input("a a", 50),
          new Input("a b", 50),
        }));
    
    IOUtils.close(a, tempDir);
  }

  public void testDupSurfaceFormsMissingResults3() throws Exception {
    Analyzer a = new MockAnalyzer(random());
    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a, a, AnalyzingSuggester.PRESERVE_SEP, 256, -1, true);
    suggester.build(new InputArrayIterator(new Input[] {
          new Input("a a", 7),
          new Input("a a", 7),
          new Input("a c", 6),
          new Input("a c", 3),
          new Input("a b", 5),
        }));
    assertEquals("[a a/7, a c/6, a b/5]", suggester.lookup("a", false, 3).toString());
    IOUtils.close(tempDir, a);
  }

  public void testEndingSpace() throws Exception {
    Analyzer a = new MockAnalyzer(random());
    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a, a, AnalyzingSuggester.PRESERVE_SEP, 256, -1, true);
    suggester.build(new InputArrayIterator(new Input[] {
          new Input("i love lucy", 7),
          new Input("isla de muerta", 8),
        }));
    assertEquals("[isla de muerta/8, i love lucy/7]", suggester.lookup("i", false, 3).toString());
    assertEquals("[i love lucy/7]", suggester.lookup("i ", false, 3).toString());
    IOUtils.close(a, tempDir);
  }

  public void testTooManyExpansions() throws Exception {

    final Analyzer a = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
        
          return new TokenStreamComponents(tokenizer) {
            @Override
            public TokenStream getTokenStream() {
              Token a = new Token("a", 0, 1);
              a.setPositionIncrement(1);
              Token b = new Token("b", 0, 1);
              b.setPositionIncrement(0);
              return new CannedTokenStream(new Token[] {a, b});
            }
         
            @Override
            protected void setReader(final Reader reader) {
            }
          };
        }
      };

    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a, a, 0, 256, 1, true);
    suggester.build(new InputArrayIterator(new Input[] {new Input("a", 1)}));
    assertEquals("[a/1]", suggester.lookup("a", false, 1).toString());
    IOUtils.close(a, tempDir);
  }
  
  public void testIllegalLookupArgument() throws Exception {
    Analyzer a = new MockAnalyzer(random());
    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a, a, 0, 256, -1, true);
    suggester.build(new InputArrayIterator(new Input[] {
        new Input("а где Люси?", 7),
    }));
    expectThrows(IllegalArgumentException.class, () -> {
      suggester.lookup("а\u001E", false, 3);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      suggester.lookup("а\u001F", false, 3);
    });

    IOUtils.close(a, tempDir);
  }

  static final Iterable<Input> shuffle(Input...values) {
    final List<Input> asList = new ArrayList<>(values.length);
    for (Input value : values) {
      asList.add(value);
    }
    Collections.shuffle(asList, random());
    return asList;
  }

  // TODO: we need BaseSuggesterTestCase?
  public void testTooLongSuggestion() throws Exception {
    Analyzer a = new MockAnalyzer(random());
    Directory tempDir = getDirectory();
    AnalyzingSuggester suggester = new AnalyzingSuggester(tempDir, "suggest", a);
    String bigString = TestUtil.randomSimpleString(random(), 30000, 30000);
    try {
      suggester.build(new InputArrayIterator(new Input[] {
            new Input(bigString, 7)}));
      fail("did not hit expected exception");
    } catch (StackOverflowError soe) {
      // OK
    } catch (IllegalArgumentException iae) {
      // expected
    }
    IOUtils.close(a, tempDir);
  }

  private Directory getDirectory() {     
    return newDirectory();
  }
}
