package org.apache.lucene.analysis.synonym;

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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.*;

/**
 * @deprecated Remove this test in Lucene 5.0
 */
@Deprecated
public class TestSlowSynonymFilter extends BaseTokenStreamTestCase {

  static List<String> strings(String str) {
    String[] arr = str.split(" ");
    return Arrays.asList(arr);
  }

  static void assertTokenizesTo(SlowSynonymMap dict, String input,
      String expected[]) throws IOException {
    Tokenizer tokenizer = new MockTokenizer(new StringReader(input), MockTokenizer.WHITESPACE, false);
    SlowSynonymFilter stream = new SlowSynonymFilter(tokenizer, dict);
    assertTokenStreamContents(stream, expected);
  }
  
  static void assertTokenizesTo(SlowSynonymMap dict, String input,
      String expected[], int posIncs[]) throws IOException {
    Tokenizer tokenizer = new MockTokenizer(new StringReader(input), MockTokenizer.WHITESPACE, false);
    SlowSynonymFilter stream = new SlowSynonymFilter(tokenizer, dict);
    assertTokenStreamContents(stream, expected, posIncs);
  }
  
  static void assertTokenizesTo(SlowSynonymMap dict, List<Token> input,
      String expected[], int posIncs[])
      throws IOException {
    TokenStream tokenizer = new IterTokenStream(input);
    SlowSynonymFilter stream = new SlowSynonymFilter(tokenizer, dict);
    assertTokenStreamContents(stream, expected, posIncs);
  }
  
  static void assertTokenizesTo(SlowSynonymMap dict, List<Token> input,
      String expected[], int startOffsets[], int endOffsets[], int posIncs[])
      throws IOException {
    TokenStream tokenizer = new IterTokenStream(input);
    SlowSynonymFilter stream = new SlowSynonymFilter(tokenizer, dict);
    assertTokenStreamContents(stream, expected, startOffsets, endOffsets,
        posIncs);
  }
  
  public void testMatching() throws IOException {
    SlowSynonymMap map = new SlowSynonymMap();

    boolean orig = false;
    boolean merge = true;
    map.add(strings("a b"), tokens("ab"), orig, merge);
    map.add(strings("a c"), tokens("ac"), orig, merge);
    map.add(strings("a"), tokens("aa"), orig, merge);
    map.add(strings("b"), tokens("bb"), orig, merge);
    map.add(strings("z x c v"), tokens("zxcv"), orig, merge);
    map.add(strings("x c"), tokens("xc"), orig, merge);

    assertTokenizesTo(map, "$", new String[] { "$" });
    assertTokenizesTo(map, "a", new String[] { "aa" });
    assertTokenizesTo(map, "a $", new String[] { "aa", "$" });
    assertTokenizesTo(map, "$ a", new String[] { "$", "aa" });
    assertTokenizesTo(map, "a a", new String[] { "aa", "aa" });
    assertTokenizesTo(map, "b", new String[] { "bb" });
    assertTokenizesTo(map, "z x c v", new String[] { "zxcv" });
    assertTokenizesTo(map, "z x c $", new String[] { "z", "xc", "$" });

    // repeats
    map.add(strings("a b"), tokens("ab"), orig, merge);
    map.add(strings("a b"), tokens("ab"), orig, merge);
    
    // FIXME: the below test intended to be { "ab" }
    assertTokenizesTo(map, "a b", new String[] { "ab", "ab", "ab"  });

    // check for lack of recursion
    map.add(strings("zoo"), tokens("zoo"), orig, merge);
    assertTokenizesTo(map, "zoo zoo $ zoo", new String[] { "zoo", "zoo", "$", "zoo" });
    map.add(strings("zoo"), tokens("zoo zoo"), orig, merge);
    // FIXME: the below test intended to be { "zoo", "zoo", "zoo", "zoo", "$", "zoo", "zoo" }
    // maybe this was just a typo in the old test????
    assertTokenizesTo(map, "zoo zoo $ zoo", new String[] { "zoo", "zoo", "zoo", "zoo", "zoo", "zoo", "$", "zoo", "zoo", "zoo" });
  }

  public void testIncludeOrig() throws IOException {
    SlowSynonymMap map = new SlowSynonymMap();

    boolean orig = true;
    boolean merge = true;
    map.add(strings("a b"), tokens("ab"), orig, merge);
    map.add(strings("a c"), tokens("ac"), orig, merge);
    map.add(strings("a"), tokens("aa"), orig, merge);
    map.add(strings("b"), tokens("bb"), orig, merge);
    map.add(strings("z x c v"), tokens("zxcv"), orig, merge);
    map.add(strings("x c"), tokens("xc"), orig, merge);

    assertTokenizesTo(map, "$", 
        new String[] { "$" },
        new int[] { 1 });
    assertTokenizesTo(map, "a", 
        new String[] { "a", "aa" },
        new int[] { 1, 0 });
    assertTokenizesTo(map, "a", 
        new String[] { "a", "aa" },
        new int[] { 1, 0 });
    assertTokenizesTo(map, "$ a", 
        new String[] { "$", "a", "aa" },
        new int[] { 1, 1, 0 });
    assertTokenizesTo(map, "a $", 
        new String[] { "a", "aa", "$" },
        new int[] { 1, 0, 1 });
    assertTokenizesTo(map, "$ a !", 
        new String[] { "$", "a", "aa", "!" },
        new int[] { 1, 1, 0, 1 });
    assertTokenizesTo(map, "a a", 
        new String[] { "a", "aa", "a", "aa" },
        new int[] { 1, 0, 1, 0 });
    assertTokenizesTo(map, "b", 
        new String[] { "b", "bb" },
        new int[] { 1, 0 });
    assertTokenizesTo(map, "z x c v",
        new String[] { "z", "zxcv", "x", "c", "v" },
        new int[] { 1, 0, 1, 1, 1 });
    assertTokenizesTo(map, "z x c $",
        new String[] { "z", "x", "xc", "c", "$" },
        new int[] { 1, 1, 0, 1, 1 });

    // check for lack of recursion
    map.add(strings("zoo zoo"), tokens("zoo"), orig, merge);
    // CHECKME: I think the previous test (with 4 zoo's), was just a typo.
    assertTokenizesTo(map, "zoo zoo $ zoo",
        new String[] { "zoo", "zoo", "zoo", "$", "zoo" },
        new int[] { 1, 0, 1, 1, 1 });

    map.add(strings("zoo"), tokens("zoo zoo"), orig, merge);
    assertTokenizesTo(map, "zoo zoo $ zoo",
        new String[] { "zoo", "zoo", "zoo", "$", "zoo", "zoo", "zoo" },
        new int[] { 1, 0, 1, 1, 1, 0, 1 });
  }


  public void testMapMerge() throws IOException {
    SlowSynonymMap map = new SlowSynonymMap();

    boolean orig = false;
    boolean merge = true;
    map.add(strings("a"), tokens("a5,5"), orig, merge);
    map.add(strings("a"), tokens("a3,3"), orig, merge);

    assertTokenizesTo(map, "a",
        new String[] { "a3", "a5" },
        new int[] { 1, 2 });

    map.add(strings("b"), tokens("b3,3"), orig, merge);
    map.add(strings("b"), tokens("b5,5"), orig, merge);

    assertTokenizesTo(map, "b",
        new String[] { "b3", "b5" },
        new int[] { 1, 2 });

    map.add(strings("a"), tokens("A3,3"), orig, merge);
    map.add(strings("a"), tokens("A5,5"), orig, merge);
    
    assertTokenizesTo(map, "a",
        new String[] { "a3", "A3", "a5", "A5" },
        new int[] { 1, 0, 2, 0 });

    map.add(strings("a"), tokens("a1"), orig, merge);
    assertTokenizesTo(map, "a",
        new String[] { "a1", "a3", "A3", "a5", "A5" },
        new int[] { 1, 2, 0, 2, 0 });

    map.add(strings("a"), tokens("a2,2"), orig, merge);
    map.add(strings("a"), tokens("a4,4 a6,2"), orig, merge);
    assertTokenizesTo(map, "a",
        new String[] { "a1", "a2", "a3", "A3", "a4", "a5", "A5", "a6" },
        new int[] { 1, 1, 1, 0, 1, 1, 0, 1  });
  }


  public void testOverlap() throws IOException {
    SlowSynonymMap map = new SlowSynonymMap();

    boolean orig = false;
    boolean merge = true;
    map.add(strings("qwe"), tokens("qq/ww/ee"), orig, merge);
    map.add(strings("qwe"), tokens("xx"), orig, merge);
    map.add(strings("qwe"), tokens("yy"), orig, merge);
    map.add(strings("qwe"), tokens("zz"), orig, merge);
    assertTokenizesTo(map, "$", new String[] { "$" });
    assertTokenizesTo(map, "qwe",
        new String[] { "qq", "ww", "ee", "xx", "yy", "zz" },
        new int[] { 1, 0, 0, 0, 0, 0 });

    // test merging within the map

    map.add(strings("a"), tokens("a5,5 a8,3 a10,2"), orig, merge);
    map.add(strings("a"), tokens("a3,3 a7,4 a9,2 a11,2 a111,100"), orig, merge);
    assertTokenizesTo(map, "a",
        new String[] { "a3", "a5", "a7", "a8", "a9", "a10", "a11", "a111" },
        new int[] { 1, 2, 2, 1, 1, 1, 1, 100 });
  }

  public void testPositionIncrements() throws IOException {
    SlowSynonymMap map = new SlowSynonymMap();

    boolean orig = false;
    boolean merge = true;

    // test that generated tokens start at the same posInc as the original
    map.add(strings("a"), tokens("aa"), orig, merge);
    assertTokenizesTo(map, tokens("a,5"), 
        new String[] { "aa" },
        new int[] { 5 });
    assertTokenizesTo(map, tokens("b,1 a,0"),
        new String[] { "b", "aa" },
        new int[] { 1, 0 });

    // test that offset of first replacement is ignored (always takes the orig offset)
    map.add(strings("b"), tokens("bb,100"), orig, merge);
    assertTokenizesTo(map, tokens("b,5"),
        new String[] { "bb" },
        new int[] { 5 });
    assertTokenizesTo(map, tokens("c,1 b,0"),
        new String[] { "c", "bb" },
        new int[] { 1, 0 });

    // test that subsequent tokens are adjusted accordingly
    map.add(strings("c"), tokens("cc,100 c2,2"), orig, merge);
    assertTokenizesTo(map, tokens("c,5"),
        new String[] { "cc", "c2" },
        new int[] { 5, 2 });
    assertTokenizesTo(map, tokens("d,1 c,0"),
        new String[] { "d", "cc", "c2" },
        new int[] { 1, 0, 2 });
  }


  public void testPositionIncrementsWithOrig() throws IOException {
    SlowSynonymMap map = new SlowSynonymMap();

    boolean orig = true;
    boolean merge = true;

    // test that generated tokens start at the same offset as the original
    map.add(strings("a"), tokens("aa"), orig, merge);
    assertTokenizesTo(map, tokens("a,5"),
        new String[] { "a", "aa" },
        new int[] { 5, 0 });
    assertTokenizesTo(map, tokens("b,1 a,0"),
        new String[] { "b", "a", "aa" },
        new int[] { 1, 0, 0 });

    // test that offset of first replacement is ignored (always takes the orig offset)
    map.add(strings("b"), tokens("bb,100"), orig, merge);
    assertTokenizesTo(map, tokens("b,5"),
        new String[] { "b", "bb" },
        new int[] { 5, 0 });
    assertTokenizesTo(map, tokens("c,1 b,0"),
        new String[] { "c", "b", "bb" },
        new int[] { 1, 0, 0 });

    // test that subsequent tokens are adjusted accordingly
    map.add(strings("c"), tokens("cc,100 c2,2"), orig, merge);
    assertTokenizesTo(map, tokens("c,5"),
        new String[] { "c", "cc", "c2" },
        new int[] { 5, 0, 2 });
    assertTokenizesTo(map, tokens("d,1 c,0"),
        new String[] { "d", "c", "cc", "c2" },
        new int[] { 1, 0, 0, 2 });
  }


  public void testOffsetBug() throws IOException {
    // With the following rules:
    // a a=>b
    // x=>y
    // analysing "a x" causes "y" to have a bad offset (end less than start)
    // SOLR-167
    SlowSynonymMap map = new SlowSynonymMap();

    boolean orig = false;
    boolean merge = true;

    map.add(strings("a a"), tokens("b"), orig, merge);
    map.add(strings("x"), tokens("y"), orig, merge);

    // "a a x" => "b y"
    assertTokenizesTo(map, tokens("a,1,0,1 a,1,2,3 x,1,4,5"),
        new String[] { "b", "y" },
        new int[] { 0, 4 },
        new int[] { 3, 5 },
        new int[] { 1, 1 });
  }

  
  /***
   * Return a list of tokens according to a test string format:
   * a b c  =>  returns List<Token> [a,b,c]
   * a/b   => tokens a and b share the same spot (b.positionIncrement=0)
   * a,3/b/c => a,b,c all share same position (a.positionIncrement=3, b.positionIncrement=0, c.positionIncrement=0)
   * a,1,10,11  => "a" with positionIncrement=1, startOffset=10, endOffset=11
   * @deprecated (3.0) does not support attributes api
   */
  @Deprecated
  private List<Token> tokens(String str) {
    String[] arr = str.split(" ");
    List<Token> result = new ArrayList<Token>();
    for (int i=0; i<arr.length; i++) {
      String[] toks = arr[i].split("/");
      String[] params = toks[0].split(",");

      int posInc;
      int start;
      int end;

      if (params.length > 1) {
        posInc = Integer.parseInt(params[1]);
      } else {
        posInc = 1;
      }

      if (params.length > 2) {
        start = Integer.parseInt(params[2]);
      } else {
        start = 0;
      }

      if (params.length > 3) {
        end = Integer.parseInt(params[3]);
      } else {
        end = start + params[0].length();
      }

      Token t = new Token(params[0],start,end,"TEST");
      t.setPositionIncrement(posInc);
      
      result.add(t);
      for (int j=1; j<toks.length; j++) {
        t = new Token(toks[j],0,0,"TEST");
        t.setPositionIncrement(0);
        result.add(t);
      }
    }
    return result;
  }
  
  /**
   * @deprecated (3.0) does not support custom attributes
   */
  @Deprecated
  private static class IterTokenStream extends TokenStream {
    final Token tokens[];
    int index = 0;
    CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    FlagsAttribute flagsAtt = addAttribute(FlagsAttribute.class);
    TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);
    
    public IterTokenStream(Token... tokens) {
      super();
      this.tokens = tokens;
    }
    
    public IterTokenStream(Collection<Token> tokens) {
      this(tokens.toArray(new Token[tokens.size()]));
    }
    
    @Override
    public boolean incrementToken() throws IOException {
      if (index >= tokens.length)
        return false;
      else {
        clearAttributes();
        Token token = tokens[index++];
        termAtt.setEmpty().append(token);
        offsetAtt.setOffset(token.startOffset(), token.endOffset());
        posIncAtt.setPositionIncrement(token.getPositionIncrement());
        flagsAtt.setFlags(token.getFlags());
        typeAtt.setType(token.type());
        payloadAtt.setPayload(token.getPayload());
        return true;
      }
    }
  }
}
