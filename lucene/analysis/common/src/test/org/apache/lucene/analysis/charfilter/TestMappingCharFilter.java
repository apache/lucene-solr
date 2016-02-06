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
package org.apache.lucene.analysis.charfilter;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.UnicodeUtil;

public class TestMappingCharFilter extends BaseTokenStreamTestCase {

  NormalizeCharMap normMap;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();

    builder.add( "aa", "a" );
    builder.add( "bbb", "b" );
    builder.add( "cccc", "cc" );

    builder.add( "h", "i" );
    builder.add( "j", "jj" );
    builder.add( "k", "kkk" );
    builder.add( "ll", "llll" );

    builder.add( "empty", "" );

    // BMP (surrogate pair):
    builder.add(UnicodeUtil.newString(new int[] {0x1D122}, 0, 1), "fclef");

    builder.add("\uff01", "full-width-exclamation");

    normMap = builder.build();
  }

  public void testReaderReset() throws Exception {
    CharFilter cs = new MappingCharFilter( normMap, new StringReader( "x" ) );
    char[] buf = new char[10];
    int len = cs.read(buf, 0, 10);
    assertEquals( 1, len );
    assertEquals( 'x', buf[0]) ;
    len = cs.read(buf, 0, 10);
    assertEquals( -1, len );

    // rewind
    cs.reset();
    len = cs.read(buf, 0, 10);
    assertEquals( 1, len );
    assertEquals( 'x', buf[0]) ;
  }

  public void testNothingChange() throws Exception {
    CharFilter cs = new MappingCharFilter( normMap, new StringReader( "x" ) );
    TokenStream ts =whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts, new String[]{"x"}, new int[]{0}, new int[]{1}, 1);
  }

  public void test1to1() throws Exception {
    CharFilter cs = new MappingCharFilter( normMap, new StringReader( "h" ) );
    TokenStream ts =whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts, new String[]{"i"}, new int[]{0}, new int[]{1}, 1);
  }

  public void test1to2() throws Exception {
    CharFilter cs = new MappingCharFilter( normMap, new StringReader( "j" ) );
    TokenStream ts =whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts, new String[]{"jj"}, new int[]{0}, new int[]{1}, 1);
  }

  public void test1to3() throws Exception {
    CharFilter cs = new MappingCharFilter( normMap, new StringReader( "k" ) );
    TokenStream ts =whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts, new String[]{"kkk"}, new int[]{0}, new int[]{1}, 1);
  }

  public void test2to4() throws Exception {
    CharFilter cs = new MappingCharFilter( normMap, new StringReader( "ll" ) );
    TokenStream ts =whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts, new String[]{"llll"}, new int[]{0}, new int[]{2}, 2);
  }

  public void test2to1() throws Exception {
    CharFilter cs = new MappingCharFilter( normMap, new StringReader( "aa" ) );
    TokenStream ts =whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts, new String[]{"a"}, new int[]{0}, new int[]{2}, 2);
  }

  public void test3to1() throws Exception {
    CharFilter cs = new MappingCharFilter( normMap, new StringReader( "bbb" ) );
    TokenStream ts =whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts, new String[]{"b"}, new int[]{0}, new int[]{3}, 3);
  }

  public void test4to2() throws Exception {
    CharFilter cs = new MappingCharFilter( normMap, new StringReader( "cccc" ) );
    TokenStream ts =whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts, new String[]{"cc"}, new int[]{0}, new int[]{4}, 4);
  }

  public void test5to0() throws Exception {
    CharFilter cs = new MappingCharFilter( normMap, new StringReader( "empty" ) );
    TokenStream ts =whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts, new String[0], new int[]{}, new int[]{}, 5);
  }

  public void testNonBMPChar() throws Exception {
    CharFilter cs = new MappingCharFilter( normMap, new StringReader( UnicodeUtil.newString(new int[] {0x1D122}, 0, 1) ) );
    TokenStream ts =whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts, new String[]{"fclef"}, new int[]{0}, new int[]{2}, 2);
  }

  public void testFullWidthChar() throws Exception {
    CharFilter cs = new MappingCharFilter( normMap, new StringReader( "\uff01") );
    TokenStream ts =whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts, new String[]{"full-width-exclamation"}, new int[]{0}, new int[]{1}, 1);
  }

  //
  //                1111111111222
  //      01234567890123456789012
  //(in)  h i j k ll cccc bbb aa
  //
  //                1111111111222
  //      01234567890123456789012
  //(out) i i jj kkk llll cc b a
  //
  //    h, 0, 1 =>    i, 0, 1
  //    i, 2, 3 =>    i, 2, 3
  //    j, 4, 5 =>   jj, 4, 5
  //    k, 6, 7 =>  kkk, 6, 7
  //   ll, 8,10 => llll, 8,10
  // cccc,11,15 =>   cc,11,15
  //  bbb,16,19 =>    b,16,19
  //   aa,20,22 =>    a,20,22
  //
  public void testTokenStream() throws Exception {
    String testString = "h i j k ll cccc bbb aa";
    CharFilter cs = new MappingCharFilter( normMap, new StringReader( testString ) );
    TokenStream ts =whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
      new String[]{"i","i","jj","kkk","llll","cc","b","a"},
      new int[]{0,2,4,6,8,11,16,20},
      new int[]{1,3,5,7,10,15,19,22},
      testString.length()
    );
  }

  //
  //
  //        0123456789
  //(in)    aaaa ll h
  //(out-1) aa llll i
  //(out-2) a llllllll i
  //
  // aaaa,0,4 => a,0,4
  //   ll,5,7 => llllllll,5,7
  //    h,8,9 => i,8,9
  public void testChained() throws Exception {
    String testString = "aaaa ll h";
    CharFilter cs = new MappingCharFilter( normMap,
        new MappingCharFilter( normMap, new StringReader( testString ) ) );
    TokenStream ts =whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
      new String[]{"a","llllllll","i"},
      new int[]{0,5,8},
      new int[]{4,7,9},
      testString.length()
    );
  }
  
  public void testRandom() throws Exception {
    Analyzer analyzer = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return new MappingCharFilter(normMap, reader);
      }
    };
    
    int numRounds = RANDOM_MULTIPLIER * 10000;
    checkRandomData(random(), analyzer, numRounds);
    analyzer.close();
  }

  //@Ignore("wrong finalOffset: https://issues.apache.org/jira/browse/LUCENE-3971")
  public void testFinalOffsetSpecialCase() throws Exception {  
    final NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
    builder.add("t", "");
    // even though this below rule has no effect, the test passes if you remove it!!
    builder.add("tmakdbl", "c");
    
    final NormalizeCharMap map = builder.build();

    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return new MappingCharFilter(map, reader);
      }
    };
    
    String text = "gzw f quaxot";
    checkAnalysisConsistency(random(), analyzer, false, text);
    analyzer.close();
  }
  
  //@Ignore("wrong finalOffset: https://issues.apache.org/jira/browse/LUCENE-3971")
  public void testRandomMaps() throws Exception {
    int numIterations = atLeast(3);
    for (int i = 0; i < numIterations; i++) {
      final NormalizeCharMap map = randomMap();
      Analyzer analyzer = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
          return new TokenStreamComponents(tokenizer, tokenizer);
        }

        @Override
        protected Reader initReader(String fieldName, Reader reader) {
          return new MappingCharFilter(map, reader);
        }
      };
      int numRounds = 100;
      checkRandomData(random(), analyzer, numRounds);
      analyzer.close();
    }
  }
  
  private NormalizeCharMap randomMap() {
    Random random = random();
    NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
    // we can't add duplicate keys, or NormalizeCharMap gets angry
    Set<String> keys = new HashSet<>();
    int num = random.nextInt(5);
    //System.out.println("NormalizeCharMap=");
    for (int i = 0; i < num; i++) {
      String key = TestUtil.randomSimpleString(random);
      if (!keys.contains(key) && key.length() != 0) {
        String value = TestUtil.randomSimpleString(random);
        builder.add(key, value);
        keys.add(key);
        //System.out.println("mapping: '" + key + "' => '" + value + "'");
      }
    }
    return builder.build();
  }

  public void testRandomMaps2() throws Exception {
    final Random random = random();
    final int numIterations = atLeast(3);
    for(int iter=0;iter<numIterations;iter++) {

      if (VERBOSE) {
        System.out.println("\nTEST iter=" + iter);
      }

      final char endLetter = (char) TestUtil.nextInt(random, 'b', 'z');

      final Map<String,String> map = new HashMap<>();
      final NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
      final int numMappings = atLeast(5);
      if (VERBOSE) {
        System.out.println("  mappings:");
      }
      while (map.size() < numMappings) {
        final String key = TestUtil.randomSimpleStringRange(random, 'a', endLetter, 7);
        if (key.length() != 0 && !map.containsKey(key)) {
          final String value = TestUtil.randomSimpleString(random);
          map.put(key, value);
          builder.add(key, value);
          if (VERBOSE) {
            System.out.println("    " + key + " -> " + value);
          }
        }
      }

      final NormalizeCharMap charMap = builder.build();

      if (VERBOSE) {
        System.out.println("  test random documents...");
      }

      for(int iter2=0;iter2<100;iter2++) {
        final String content = TestUtil.randomSimpleStringRange(random, 'a', endLetter, atLeast(1000));

        if (VERBOSE) {
          System.out.println("  content=" + content);
        }

        // Do stupid dog-slow mapping:

        // Output string:
        final StringBuilder output = new StringBuilder();

        // Maps output offset to input offset:
        final List<Integer> inputOffsets = new ArrayList<>();

        int cumDiff = 0;
        int charIdx = 0;
        while(charIdx < content.length()) {

          int matchLen = -1;
          String matchRepl = null;

          for(Map.Entry<String,String> ent : map.entrySet()) {
            final String match = ent.getKey();
            if (charIdx + match.length() <= content.length()) {
              final int limit = charIdx+match.length();
              boolean matches = true;
              for(int charIdx2=charIdx;charIdx2<limit;charIdx2++) {
                if (match.charAt(charIdx2-charIdx) != content.charAt(charIdx2)) {
                  matches = false;
                  break;
                }
              }

              if (matches) {
                final String repl = ent.getValue();
                if (match.length() > matchLen) {
                  // Greedy: longer match wins
                  matchLen = match.length();
                  matchRepl = repl;
                }
              }
            }
          }

          if (matchLen != -1) {
            // We found a match here!
            if (VERBOSE) {
              System.out.println("    match=" + content.substring(charIdx, charIdx+matchLen) + " @ off=" + charIdx + " repl=" + matchRepl);
            }
            output.append(matchRepl);
            final int minLen = Math.min(matchLen, matchRepl.length());

            // Common part, directly maps back to input
            // offset:
            for(int outIdx=0;outIdx<minLen;outIdx++) {
              inputOffsets.add(output.length() - matchRepl.length() + outIdx + cumDiff);
            }

            cumDiff += matchLen - matchRepl.length();
            charIdx += matchLen;

            if (matchRepl.length() < matchLen) {
              // Replacement string is shorter than matched
              // input: nothing to do
            } else if (matchRepl.length() > matchLen) {
              // Replacement string is longer than matched
              // input: for all the "extra" chars we map
              // back to a single input offset:
              for(int outIdx=matchLen;outIdx<matchRepl.length();outIdx++) {
                inputOffsets.add(output.length() + cumDiff - 1);
              }
            } else {
              // Same length: no change to offset
            }

            assert inputOffsets.size() == output.length(): "inputOffsets.size()=" + inputOffsets.size() + " vs output.length()=" + output.length();
          } else {
            inputOffsets.add(output.length() + cumDiff);
            output.append(content.charAt(charIdx));
            charIdx++;
          }
        }

        final String expected = output.toString();
        if (VERBOSE) {
          System.out.print("    expected:");
          for(int charIdx2=0;charIdx2<expected.length();charIdx2++) {
            System.out.print(" " + expected.charAt(charIdx2) + "/" + inputOffsets.get(charIdx2));
          }
          System.out.println();
        }

        final MappingCharFilter mapFilter = new MappingCharFilter(charMap, new StringReader(content));

        final StringBuilder actualBuilder = new StringBuilder();
        final List<Integer> actualInputOffsets = new ArrayList<>();

        // Now consume the actual mapFilter, somewhat randomly:
        while (true) {
          if (random.nextBoolean()) {
            final int ch = mapFilter.read();
            if (ch == -1) {
              break;
            }
            actualBuilder.append((char) ch);
          } else {
            final char[] buffer = new char[TestUtil.nextInt(random, 1, 100)];
            final int off = buffer.length == 1 ? 0 : random.nextInt(buffer.length-1);
            final int count = mapFilter.read(buffer, off, buffer.length-off);
            if (count == -1) {
              break;
            } else {
              actualBuilder.append(buffer, off, count);
            }
          }

          if (random.nextInt(10) == 7) {
            // Map offsets
            while(actualInputOffsets.size() < actualBuilder.length()) {
              actualInputOffsets.add(mapFilter.correctOffset(actualInputOffsets.size()));
            }
          }
        }

        // Finish mappping offsets
        while(actualInputOffsets.size() < actualBuilder.length()) {
          actualInputOffsets.add(mapFilter.correctOffset(actualInputOffsets.size()));
        }

        final String actual = actualBuilder.toString();

        // Verify:
        assertEquals(expected, actual);
        assertEquals(inputOffsets, actualInputOffsets);
      }        
    }
  }
}
