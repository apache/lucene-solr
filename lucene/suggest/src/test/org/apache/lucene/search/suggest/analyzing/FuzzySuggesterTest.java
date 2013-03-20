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

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenStreamToAutomaton;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.TermFreq;
import org.apache.lucene.search.suggest.TermFreqArrayIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.State;
import org.apache.lucene.util.fst.Util;

public class FuzzySuggesterTest extends LuceneTestCase {
  
  public void testRandomEdits() throws IOException {
    List<TermFreq> keys = new ArrayList<TermFreq>();
    int numTerms = atLeast(100);
    for (int i = 0; i < numTerms; i++) {
      keys.add(new TermFreq("boo" + _TestUtil.randomSimpleString(random()), 1 + random().nextInt(100)));
    }
    keys.add(new TermFreq("foo bar boo far", 12));
    FuzzySuggester suggester = new FuzzySuggester(new MockAnalyzer(random(), MockTokenizer.KEYWORD, false));
    suggester.build(new TermFreqArrayIterator(keys));
    int numIters = atLeast(10);
    for (int i = 0; i < numIters; i++) {
      String addRandomEdit = addRandomEdit("foo bar boo", FuzzySuggester.DEFAULT_NON_FUZZY_PREFIX);
      List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence(addRandomEdit, random()), false, 2);
      assertEquals(addRandomEdit, 1, results.size());
      assertEquals("foo bar boo far", results.get(0).key.toString());
      assertEquals(12, results.get(0).value, 0.01F);  
    }
  }
  
  /** this is basically the WFST test ported to KeywordAnalyzer. so it acts the same */
  public void testKeyword() throws Exception {
    TermFreq keys[] = new TermFreq[] {
        new TermFreq("foo", 50),
        new TermFreq("bar", 10),
        new TermFreq("barbar", 12),
        new TermFreq("barbara", 6)
    };
    
    FuzzySuggester suggester = new FuzzySuggester(new MockAnalyzer(random(), MockTokenizer.KEYWORD, false));
    suggester.build(new TermFreqArrayIterator(keys));
    
    List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence("bariar", random()), false, 2);
    assertEquals(2, results.size());
    assertEquals("barbar", results.get(0).key.toString());
    assertEquals(12, results.get(0).value, 0.01F);
    
    results = suggester.lookup(_TestUtil.stringToCharSequence("barbr", random()), false, 2);
    assertEquals(2, results.size());
    assertEquals("barbar", results.get(0).key.toString());
    assertEquals(12, results.get(0).value, 0.01F);
    
    results = suggester.lookup(_TestUtil.stringToCharSequence("barbara", random()), false, 2);
    assertEquals(2, results.size());
    assertEquals("barbara", results.get(0).key.toString());
    assertEquals(6, results.get(0).value, 0.01F);
    
    results = suggester.lookup(_TestUtil.stringToCharSequence("barbar", random()), false, 2);
    assertEquals(2, results.size());
    assertEquals("barbar", results.get(0).key.toString());
    assertEquals(12, results.get(0).value, 0.01F);
    assertEquals("barbara", results.get(1).key.toString());
    assertEquals(6, results.get(1).value, 0.01F);
    
    results = suggester.lookup(_TestUtil.stringToCharSequence("barbaa", random()), false, 2);
    assertEquals(2, results.size());
    assertEquals("barbar", results.get(0).key.toString());
    assertEquals(12, results.get(0).value, 0.01F);
    assertEquals("barbara", results.get(1).key.toString());
    assertEquals(6, results.get(1).value, 0.01F);
    
    // top N of 2, but only foo is available
    results = suggester.lookup(_TestUtil.stringToCharSequence("f", random()), false, 2);
    assertEquals(1, results.size());
    assertEquals("foo", results.get(0).key.toString());
    assertEquals(50, results.get(0).value, 0.01F);
    
    // top N of 1 for 'bar': we return this even though
    // barbar is higher because exactFirst is enabled:
    results = suggester.lookup(_TestUtil.stringToCharSequence("bar", random()), false, 1);
    assertEquals(1, results.size());
    assertEquals("bar", results.get(0).key.toString());
    assertEquals(10, results.get(0).value, 0.01F);
    
    // top N Of 2 for 'b'
    results = suggester.lookup(_TestUtil.stringToCharSequence("b", random()), false, 2);
    assertEquals(2, results.size());
    assertEquals("barbar", results.get(0).key.toString());
    assertEquals(12, results.get(0).value, 0.01F);
    assertEquals("bar", results.get(1).key.toString());
    assertEquals(10, results.get(1).value, 0.01F);
    
    // top N of 3 for 'ba'
    results = suggester.lookup(_TestUtil.stringToCharSequence("ba", random()), false, 3);
    assertEquals(3, results.size());
    assertEquals("barbar", results.get(0).key.toString());
    assertEquals(12, results.get(0).value, 0.01F);
    assertEquals("bar", results.get(1).key.toString());
    assertEquals(10, results.get(1).value, 0.01F);
    assertEquals("barbara", results.get(2).key.toString());
    assertEquals(6, results.get(2).value, 0.01F);
  }
  
  /**
   * basic "standardanalyzer" test with stopword removal
   */
  public void testStandard() throws Exception {
    TermFreq keys[] = new TermFreq[] {
        new TermFreq("the ghost of christmas past", 50),
    };
    
    Analyzer standard = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET, false);
    FuzzySuggester suggester = new FuzzySuggester(standard);
    suggester.build(new TermFreqArrayIterator(keys));
    
    List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence("the ghost of chris", random()), false, 1);
    assertEquals(1, results.size());
    assertEquals("the ghost of christmas past", results.get(0).key.toString());
    assertEquals(50, results.get(0).value, 0.01F);

    // omit the 'the' since its a stopword, its suggested anyway
    results = suggester.lookup(_TestUtil.stringToCharSequence("ghost of chris", random()), false, 1);
    assertEquals(1, results.size());
    assertEquals("the ghost of christmas past", results.get(0).key.toString());
    assertEquals(50, results.get(0).value, 0.01F);

    // omit the 'the' and 'of' since they are stopwords, its suggested anyway
    results = suggester.lookup(_TestUtil.stringToCharSequence("ghost chris", random()), false, 1);
    assertEquals(1, results.size());
    assertEquals("the ghost of christmas past", results.get(0).key.toString());
    assertEquals(50, results.get(0).value, 0.01F);
  }

  public void testNoSeps() throws Exception {
    TermFreq[] keys = new TermFreq[] {
      new TermFreq("ab cd", 0),
      new TermFreq("abcd", 1),
    };

    int options = 0;

    Analyzer a = new MockAnalyzer(random());
    FuzzySuggester suggester = new FuzzySuggester(a, a, options, 256, -1, 1, true, 1, 3);
    suggester.build(new TermFreqArrayIterator(keys));
    // TODO: would be nice if "ab " would allow the test to
    // pass, and more generally if the analyzer can know
    // that the user's current query has ended at a word, 
    // but, analyzers don't produce SEP tokens!
    List<LookupResult> r = suggester.lookup(_TestUtil.stringToCharSequence("ab c", random()), false, 2);
    assertEquals(2, r.size());

    // With no PRESERVE_SEPS specified, "ab c" should also
    // complete to "abcd", which has higher weight so should
    // appear first:
    assertEquals("abcd", r.get(0).key.toString());
  }

  public void testGraphDups() throws Exception {

    final Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.SIMPLE, true);
        
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
          protected void setReader(final Reader reader) throws IOException {
          }
        };
      }
    };

    TermFreq keys[] = new TermFreq[] {
        new TermFreq("wifi network is slow", 50),
        new TermFreq("wi fi network is fast", 10),
    };
    FuzzySuggester suggester = new FuzzySuggester(analyzer);
    suggester.build(new TermFreqArrayIterator(keys));
    
    List<LookupResult> results = suggester.lookup("wifi network", false, 10);
    if (VERBOSE) {
      System.out.println("Results: " + results);
    }
    assertEquals(2, results.size());
    assertEquals("wifi network is slow", results.get(0).key);
    assertEquals(50, results.get(0).value);
    assertEquals("wi fi network is fast", results.get(1).key);
    assertEquals(10, results.get(1).value);
  }

  public void testEmpty() throws Exception {
    FuzzySuggester suggester = new FuzzySuggester(new MockAnalyzer(random(), MockTokenizer.KEYWORD, false));
    suggester.build(new TermFreqArrayIterator(new TermFreq[0]));

    List<LookupResult> result = suggester.lookup("a", false, 20);
    assertTrue(result.isEmpty());
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
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.SIMPLE, true);
        
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
          protected void setReader(final Reader reader) throws IOException {
          }
        };
      }
    };

    TermFreq keys[] = new TermFreq[] {
        new TermFreq("ab xc", 50),
        new TermFreq("ba xd", 50),
    };
    FuzzySuggester suggester = new FuzzySuggester(analyzer);
    suggester.build(new TermFreqArrayIterator(keys));
    List<LookupResult> results = suggester.lookup("ab x", false, 1);
    assertTrue(results.size() == 1);
  }

  private static Token token(String term, int posInc, int posLength) {
    final Token t = new Token(term, 0, 0);
    t.setPositionIncrement(posInc);
    t.setPositionLength(posLength);
    return t;
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
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.SIMPLE, true);
        
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
          protected void setReader(final Reader reader) throws IOException {
          }
        };
      }
    };
  }

  public void testExactFirst() throws Exception {

    Analyzer a = getUnusualAnalyzer();
    FuzzySuggester suggester = new FuzzySuggester(a, a, AnalyzingSuggester.EXACT_FIRST | AnalyzingSuggester.PRESERVE_SEP, 256, -1, 1, true, 1, 3);
    suggester.build(new TermFreqArrayIterator(new TermFreq[] {
          new TermFreq("x y", 1),
          new TermFreq("x y z", 3),
          new TermFreq("x", 2),
          new TermFreq("z z z", 20),
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
  }

  public void testNonExactFirst() throws Exception {

    Analyzer a = getUnusualAnalyzer();
    FuzzySuggester suggester = new FuzzySuggester(a, a, AnalyzingSuggester.PRESERVE_SEP, 256, -1, 1, true, 1, 3);

    suggester.build(new TermFreqArrayIterator(new TermFreq[] {
          new TermFreq("x y", 1),
          new TermFreq("x y z", 3),
          new TermFreq("x", 2),
          new TermFreq("z z z", 20),
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
  }
  
  // Holds surface form separately:
  private static class TermFreq2 implements Comparable<TermFreq2> {
    public final String surfaceForm;
    public final String analyzedForm;
    public final long weight;

    public TermFreq2(String surfaceForm, String analyzedForm, long weight) {
      this.surfaceForm = surfaceForm;
      this.analyzedForm = analyzedForm;
      this.weight = weight;
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
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      MockTokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false, MockTokenizer.DEFAULT_MAX_TOKEN_LENGTH);
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

  public void testRandom() throws Exception {

    int numQueries = atLeast(100);
    
    final List<TermFreq2> slowCompletor = new ArrayList<TermFreq2>();
    final TreeSet<String> allPrefixes = new TreeSet<String>();
    final Set<String> seen = new HashSet<String>();
    
    TermFreq[] keys = new TermFreq[numQueries];

    boolean preserveSep = random().nextBoolean();

    final int numStopChars = random().nextInt(10);
    final boolean preserveHoles = random().nextBoolean();

    if (VERBOSE) {
      System.out.println("TEST: " + numQueries + " words; preserveSep=" + preserveSep + " numStopChars=" + numStopChars + " preserveHoles=" + preserveHoles);
    }
    
    for (int i = 0; i < numQueries; i++) {
      int numTokens = _TestUtil.nextInt(random(), 1, 4);
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
            s = _TestUtil.randomSimpleString(random());
            if (s.length() > 0) {
              if (token > 0) {
                key += " ";
              }
              if (preserveSep && analyzedKey.length() > 0 && analyzedKey.charAt(analyzedKey.length()-1) != ' ') {
                analyzedKey += " ";
              }
              key += s;
              if (s.length() == 1 && isStopChar(s.charAt(0), numStopChars)) {
                if (preserveSep && preserveHoles) {
                  analyzedKey += '\u0000';
                }
                lastRemoved = true;
              } else {
                analyzedKey += s;
                lastRemoved = false;
              }
              break;
            }
          }
        }

        analyzedKey = analyzedKey.replaceAll("(^| )\u0000$", "");

        if (preserveSep && lastRemoved) {
          analyzedKey += " ";
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
      keys[i] = new TermFreq(key, weight);

      slowCompletor.add(new TermFreq2(key, analyzedKey, weight));
    }

    if (VERBOSE) {
      // Don't just sort original list, to avoid VERBOSE
      // altering the test:
      List<TermFreq2> sorted = new ArrayList<TermFreq2>(slowCompletor);
      Collections.sort(sorted);
      for(TermFreq2 ent : sorted) {
        System.out.println("  surface='" + ent.surfaceForm + " analyzed='" + ent.analyzedForm + "' weight=" + ent.weight);
      }
    }

    Analyzer a = new MockTokenEatingAnalyzer(numStopChars, preserveHoles);
    FuzzySuggester suggester = new FuzzySuggester(a, a,
                                                  preserveSep ? AnalyzingSuggester.PRESERVE_SEP : 0, 256, -1, 1, false, 1, 3);
    suggester.build(new TermFreqArrayIterator(keys));

    for (String prefix : allPrefixes) {

      if (VERBOSE) {
        System.out.println("\nTEST: prefix=" + prefix);
      }

      final int topN = _TestUtil.nextInt(random(), 1, 10);
      List<LookupResult> r = suggester.lookup(_TestUtil.stringToCharSequence(prefix, random()), false, topN);

      // 2. go thru whole set to find suggestions:
      List<LookupResult> matches = new ArrayList<LookupResult>();

      // "Analyze" the key:
      String[] tokens = prefix.split(" ");
      StringBuilder builder = new StringBuilder();
      boolean lastRemoved = false;
      for(int i=0;i<tokens.length;i++) {
        String token = tokens[i];
        if (preserveSep && builder.length() > 0 && !builder.toString().endsWith(" ")) {
          builder.append(' ');
        }

        if (token.length() == 1 && isStopChar(token.charAt(0), numStopChars)) {
          if (preserveSep && preserveHoles) {
            builder.append("\u0000");
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
        String s = analyzedKey.replaceAll("(^| )\u0000$", "");
        s = s.replaceAll("\\s+$", "");
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
        analyzedKey += " ";
      }

      if (VERBOSE) {
        System.out.println("  analyzed: " + analyzedKey);
      }
      TokenStreamToAutomaton tokenStreamToAutomaton = suggester.getTokenStreamToAutomaton();

      // NOTE: not great that we ask the suggester to give
      // us the "answer key" (ie maybe we have a bug in
      // suggester.toLevA ...) ... but testRandom2() fixes
      // this:
      Automaton automaton = suggester.toLevenshteinAutomata(suggester.toLookupAutomaton(analyzedKey));
      assertTrue(automaton.isDeterministic());
      // TODO: could be faster... but its slowCompletor for a reason
      BytesRef spare = new BytesRef();
      for (TermFreq2 e : slowCompletor) {
        spare.copyChars(e.analyzedForm);
        Set<IntsRef> finiteStrings = suggester.toFiniteStrings(spare, tokenStreamToAutomaton);
        for (IntsRef intsRef : finiteStrings) {
          State p = automaton.getInitialState();
          BytesRef ref = Util.toBytesRef(intsRef, spare);
          boolean added = false;
          for (int i = ref.offset; i < ref.length; i++) {
            State q = p.step(ref.bytes[i] & 0xff);
            if (q == null) {
              break;
            } else if (q.isAccept()) {
              matches.add(new LookupResult(e.surfaceForm, e.weight));
              added = true;
              break;
            }
            p = q;
          }
          if (!added && p.isAccept()) {
            matches.add(new LookupResult(e.surfaceForm, e.weight));
          } 
        }
      }

      assertTrue(numStopChars > 0 || matches.size() > 0);

      if (matches.size() > 1) {
        Collections.sort(matches, new Comparator<LookupResult>() {
            @Override
            public int compare(LookupResult left, LookupResult right) {
              int cmp = Float.compare(right.value, left.value);
              if (cmp == 0) {
                return left.compareTo(right);
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
        for(LookupResult lr : matches) {
          System.out.println("    key=" + lr.key + " weight=" + lr.value);
        }

        System.out.println("  actual:");
        for(LookupResult lr : r) {
          System.out.println("    key=" + lr.key + " weight=" + lr.value);
        }
      }
      
      assertEquals(prefix + "  " + topN, matches.size(), r.size());
      for(int hit=0;hit<r.size();hit++) {
        //System.out.println("  check hit " + hit);
        assertEquals(prefix + "  " + topN, matches.get(hit).key.toString(), r.get(hit).key.toString());
        assertEquals(matches.get(hit).value, r.get(hit).value, 0f);
      }
    }
  }

  public void testMaxSurfaceFormsPerAnalyzedForm() throws Exception {
    Analyzer a = new MockAnalyzer(random());
    FuzzySuggester suggester = new FuzzySuggester(a, a, 0, 2, -1, 1, true, 1, 3);

    List<TermFreq> keys = Arrays.asList(new TermFreq[] {
        new TermFreq("a", 40),
        new TermFreq("a ", 50),
        new TermFreq(" a", 60),
      });

    Collections.shuffle(keys, random());
    suggester.build(new TermFreqArrayIterator(keys));

    List<LookupResult> results = suggester.lookup("a", false, 5);
    assertEquals(2, results.size());
    assertEquals(" a", results.get(0).key);
    assertEquals(60, results.get(0).value);
    assertEquals("a ", results.get(1).key);
    assertEquals(50, results.get(1).value);
  }

  public void testEditSeps() throws Exception {
    Analyzer a = new MockAnalyzer(random());
    FuzzySuggester suggester = new FuzzySuggester(a, a, FuzzySuggester.PRESERVE_SEP, 2, -1, 2, true, 1, 3);

    List<TermFreq> keys = Arrays.asList(new TermFreq[] {
        new TermFreq("foo bar", 40),
        new TermFreq("foo bar baz", 50),
        new TermFreq("barbaz", 60),
        new TermFreq("barbazfoo", 10),
      });

    Collections.shuffle(keys, random());
    suggester.build(new TermFreqArrayIterator(keys));

    assertEquals("[foo bar baz/50, foo bar/40]", suggester.lookup("foobar", false, 5).toString());
    assertEquals("[foo bar baz/50]", suggester.lookup("foobarbaz", false, 5).toString());
    assertEquals("[barbaz/60, barbazfoo/10]", suggester.lookup("bar baz", false, 5).toString());
    assertEquals("[barbazfoo/10]", suggester.lookup("bar baz foo", false, 5).toString());
  }
  
  @SuppressWarnings("fallthrough")
  private static String addRandomEdit(String string, int prefixLength) {
    char[] input = string.toCharArray();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < input.length; i++) {
      if (i >= prefixLength && random().nextBoolean() && i < input.length-1) {
        switch(random().nextInt(4)) {
          case 3:
            if (i < input.length-1) {
              // Transpose input[i] and input[1+i]:
              builder.append(input[i+1]);
              builder.append(input[i]);
              for(int j=i+2;j<input.length;j++) {
                builder.append(input[j]);
              }
              return builder.toString();
            }
            // NOTE: fall through to delete:
          case 2:
            // Delete input[i]
            for (int j = i+1; j < input.length; j++) {
              builder.append(input[j]);  
            }
            return builder.toString();
          case 1:
            // Insert input[i+1] twice
            if (i+1<input.length) {
              builder.append(input[i+1]);
              builder.append(input[i++]);
              i++;
            }
            for (int j = i; j < input.length; j++) {
              builder.append(input[j]);
            }
            return builder.toString();
          case 0:
            // Insert random byte.
            // NOTE: can only use ascii here so that, in
            // UTF8 byte space it's still a single
            // insertion:
            int x = random().nextInt(128);
            builder.append((char) x);
            for (int j = i; j < input.length; j++) {
              builder.append(input[j]);  
            }
            return builder.toString();
        }
      }

      builder.append(input[i]);
    }

    return builder.toString();
  }

  private String randomSimpleString(int maxLen) {
    final int len = _TestUtil.nextInt(random(), 1, maxLen);
    final char[] chars = new char[len];
    for(int j=0;j<len;j++) {
      chars[j] = (char) ('a' + random().nextInt(4));
    }
    return new String(chars);
  }

  public void testRandom2() throws Throwable {
    final int NUM = atLeast(200);
    final List<TermFreq> answers = new ArrayList<TermFreq>();
    final Set<String> seen = new HashSet<String>();
    for(int i=0;i<NUM;i++) {
      final String s = randomSimpleString(8);
      if (!seen.contains(s)) {
        answers.add(new TermFreq(s, random().nextInt(1000)));
        seen.add(s);
      }
    }

    Collections.sort(answers, new Comparator<TermFreq>() {
        @Override
        public int compare(TermFreq a, TermFreq b) {
          return a.term.compareTo(b.term);
        }
      });
    if (VERBOSE) {
      System.out.println("\nTEST: targets");
      for(TermFreq tf : answers) {
        System.out.println("  " + tf.term.utf8ToString() + " freq=" + tf.v);
      }
    }

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.KEYWORD, false);
    int maxEdits = random().nextBoolean() ? 1 : 2;
    int prefixLen = random().nextInt(4);
    boolean transpositions = random().nextBoolean();
    // TODO: test graph analyzers
    // TODO: test exactFirst / preserveSep permutations
    FuzzySuggester suggest = new FuzzySuggester(a, a, 0, 256, -1, maxEdits, transpositions, prefixLen, prefixLen);

    if (VERBOSE) {
      System.out.println("TEST: maxEdits=" + maxEdits + " prefixLen=" + prefixLen + " transpositions=" + transpositions + " num=" + NUM);
    }

    Collections.shuffle(answers, random());
    suggest.build(new TermFreqArrayIterator(answers.toArray(new TermFreq[answers.size()])));

    final int ITERS = atLeast(100);
    for(int iter=0;iter<ITERS;iter++) {
      final String frag = randomSimpleString(6);
      if (VERBOSE) {
        System.out.println("\nTEST: iter frag=" + frag);
      }
      final List<LookupResult> expected = slowFuzzyMatch(prefixLen, maxEdits, transpositions, answers, frag);
      if (VERBOSE) {
        System.out.println("  expected: " + expected.size());
        for(LookupResult c : expected) {
          System.out.println("    " + c);
        }
      }
      final List<LookupResult> actual = suggest.lookup(frag, false, NUM);
      if (VERBOSE) {
        System.out.println("  actual: " + actual.size());
        for(LookupResult c : actual) {
          System.out.println("    " + c);
        }
      }

      Collections.sort(actual, new CompareByCostThenAlpha());

      final int limit = Math.min(expected.size(), actual.size());
      for(int ans=0;ans<limit;ans++) {
        final LookupResult c0 = expected.get(ans);
        final LookupResult c1 = actual.get(ans);
        assertEquals("expected " + c0.key +
                     " but got " + c1.key,
                     0,
                     CHARSEQUENCE_COMPARATOR.compare(c0.key, c1.key));
        assertEquals(c0.value, c1.value);
      }
      assertEquals(expected.size(), actual.size());
    }
  }

  private List<LookupResult> slowFuzzyMatch(int prefixLen, int maxEdits, boolean allowTransposition, List<TermFreq> answers, String frag) {
    final List<LookupResult> results = new ArrayList<LookupResult>();
    final int fragLen = frag.length();
    for(TermFreq tf : answers) {
      //System.out.println("  check s=" + tf.term.utf8ToString());
      boolean prefixMatches = true;
      for(int i=0;i<prefixLen;i++) {
        if (i == fragLen) {
          // Prefix still matches:
          break;
        }
        if (i == tf.term.length || tf.term.bytes[i] != (byte) frag.charAt(i)) {
          prefixMatches = false;
          break;
        }
      }
      //System.out.println("    prefixMatches=" + prefixMatches);

      if (prefixMatches) {
        final int len = tf.term.length;
        if (len >= fragLen-maxEdits) {
          // OK it's possible:
          //System.out.println("    possible");
          int d;
          final String s = tf.term.utf8ToString();
          if (fragLen == prefixLen) {
            d = 0;
          } else if (false && len < fragLen) {
            d = getDistance(frag, s, allowTransposition);
          } else {
            //System.out.println("    try loop");
            d = maxEdits + 1;
            //for(int ed=-maxEdits;ed<=maxEdits;ed++) {
            for(int ed=-maxEdits;ed<=maxEdits;ed++) {
              if (s.length() < fragLen - ed) {
                continue;
              }
              String check = s.substring(0, fragLen-ed);
              d = getDistance(frag, check, allowTransposition);
              //System.out.println("    sub check s=" + check + " d=" + d);
              if (d <= maxEdits) {
                break;
              }
            }
          }
          if (d <= maxEdits) {
            results.add(new LookupResult(tf.term.utf8ToString(), tf.v));
          }
        }
      }

      Collections.sort(results, new CompareByCostThenAlpha());
    }

    return results;
  }

  private static class CharSequenceComparator implements Comparator<CharSequence> {

    @Override
    public int compare(CharSequence o1, CharSequence o2) {
      final int l1 = o1.length();
      final int l2 = o2.length();
      
      final int aStop = Math.min(l1, l2);
      for (int i = 0; i < aStop; i++) {
        int diff = o1.charAt(i) - o2.charAt(i);
        if (diff != 0) {
          return diff;
        }
      }
      // One is a prefix of the other, or, they are equal:
      return l1 - l2;
    }
  }

  private static final Comparator<CharSequence> CHARSEQUENCE_COMPARATOR = new CharSequenceComparator();

  public class CompareByCostThenAlpha implements Comparator<LookupResult> {
    @Override
    public int compare(LookupResult a, LookupResult b) {
      if (a.value > b.value) {
        return -1;
      } else if (a.value < b.value) {
        return 1;
      } else {
        final int c = CHARSEQUENCE_COMPARATOR.compare(a.key, b.key);
        assert c != 0: "term=" + a.key;
        return c;
      }
    }
  }

  // NOTE: copied from
  // modules/suggest/src/java/org/apache/lucene/search/spell/LuceneLevenshteinDistance.java
  // and tweaked to return the edit distance not the float
  // lucene measure

  /* Finds unicode (code point) Levenstein (edit) distance
   * between two strings, including transpositions. */
  public int getDistance(String target, String other, boolean allowTransposition) {
    IntsRef targetPoints;
    IntsRef otherPoints;
    int n;
    int d[][]; // cost array
    
    // NOTE: if we cared, we could 3*m space instead of m*n space, similar to 
    // what LevenshteinDistance does, except cycling thru a ring of three 
    // horizontal cost arrays... but this comparator is never actually used by 
    // DirectSpellChecker, its only used for merging results from multiple shards 
    // in "distributed spellcheck", and its inefficient in other ways too...

    // cheaper to do this up front once
    targetPoints = toIntsRef(target);
    otherPoints = toIntsRef(other);
    n = targetPoints.length;
    final int m = otherPoints.length;
    d = new int[n+1][m+1];
    
    if (n == 0 || m == 0) {
      if (n == m) {
        return 0;
      }
      else {
        return Math.max(n, m);
      }
    } 

    // indexes into strings s and t
    int i; // iterates through s
    int j; // iterates through t

    int t_j; // jth character of t

    int cost; // cost

    for (i = 0; i<=n; i++) {
      d[i][0] = i;
    }
    
    for (j = 0; j<=m; j++) {
      d[0][j] = j;
    }

    for (j = 1; j<=m; j++) {
      t_j = otherPoints.ints[j-1];

      for (i=1; i<=n; i++) {
        cost = targetPoints.ints[i-1]==t_j ? 0 : 1;
        // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
        d[i][j] = Math.min(Math.min(d[i-1][j]+1, d[i][j-1]+1), d[i-1][j-1]+cost);
        // transposition
        if (allowTransposition && i > 1 && j > 1 && targetPoints.ints[i-1] == otherPoints.ints[j-2] && targetPoints.ints[i-2] == otherPoints.ints[j-1]) {
          d[i][j] = Math.min(d[i][j], d[i-2][j-2] + cost);
        }
      }
    }
    
    return d[n][m];
  }
  
  private static IntsRef toIntsRef(String s) {
    IntsRef ref = new IntsRef(s.length()); // worst case
    int utf16Len = s.length();
    for (int i = 0, cp = 0; i < utf16Len; i += Character.charCount(cp)) {
      cp = ref.ints[ref.length++] = Character.codePointAt(s, i);
    }
    return ref;
  }
}
