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
package org.apache.lucene.analysis.pattern;


import java.io.StringReader;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

public class TestPatternCaptureGroupTokenFilter extends BaseTokenStreamTestCase {

  public void testNoPattern() throws Exception {
    testPatterns(
        "foobarbaz",
        new String[] {},
        new String[] {"foobarbaz"},
        new int[] {0},
        new int[] {9},
        new int[] {1},
        false
    );
    testPatterns(
        "foobarbaz",
        new String[] {},
        new String[] {"foobarbaz"},
        new int[] {0},
        new int[] {9},
        new int[] {1},
        true
    );

    testPatterns(
        "foo bar baz",
        new String[] {},
        new String[] {"foo","bar","baz"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        false
    );

    testPatterns(
        "foo bar baz",
        new String[] {},
        new String[] {"foo","bar","baz"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        true
    );
  }

  public void testNoMatch() throws Exception {
    testPatterns(
        "foobarbaz",
        new String[] {"xx"},
        new String[] {"foobarbaz"},
        new int[] {0},
        new int[] {9},
        new int[] {1},
        false
    );
    testPatterns(
        "foobarbaz",
        new String[] {"xx"},
        new String[] {"foobarbaz"},
        new int[] {0},
        new int[] {9},
        new int[] {1},
        true
    );

    testPatterns(
        "foo bar baz",
        new String[] {"xx"},
        new String[] {"foo","bar","baz"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        false
    );

    testPatterns(
        "foo bar baz",
        new String[] {"xx"},
        new String[] {"foo","bar","baz"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        true
    );
  }

  public void testNoCapture() throws Exception {
    testPatterns(
        "foobarbaz",
        new String[] {".."},
        new String[] {"foobarbaz"},
        new int[] {0},
        new int[] {9},
        new int[] {1},
        false
    );
    testPatterns(
        "foobarbaz",
        new String[] {".."},
        new String[] {"foobarbaz"},
        new int[] {0},
        new int[] {9},
        new int[] {1},
        true
    );

    testPatterns(
        "foo bar baz",
        new String[] {".."},
        new String[] {"foo","bar","baz"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        false
    );

    testPatterns(
        "foo bar baz",
        new String[] {".."},
        new String[] {"foo","bar","baz"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        true
    );
  }

  public void testEmptyCapture() throws Exception {
    testPatterns(
        "foobarbaz",
        new String[] {".(y*)"},
        new String[] {"foobarbaz"},
        new int[] {0},
        new int[] {9},
        new int[] {1},
        false
    );
    testPatterns(
        "foobarbaz",
        new String[] {".(y*)"},
        new String[] {"foobarbaz"},
        new int[] {0},
        new int[] {9},
        new int[] {1},
        true
    );

    testPatterns(
        "foo bar baz",
        new String[] {".(y*)"},
        new String[] {"foo","bar","baz"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        false
    );

    testPatterns(
        "foo bar baz",
        new String[] {".(y*)"},
        new String[] {"foo","bar","baz"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        true
    );
  }

  public void testCaptureAll() throws Exception {
    testPatterns(
        "foobarbaz",
        new String[] {"(.+)"},
        new String[] {"foobarbaz"},
        new int[] {0},
        new int[] {9},
        new int[] {1},
        false
    );
    testPatterns(
        "foobarbaz",
        new String[] {"(.+)"},
        new String[] {"foobarbaz"},
        new int[] {0},
        new int[] {9},
        new int[] {1},
        true
    );

    testPatterns(
        "foo bar baz",
        new String[] {"(.+)"},
        new String[] {"foo","bar","baz"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        false
    );

    testPatterns(
        "foo bar baz",
        new String[] {"(.+)"},
        new String[] {"foo","bar","baz"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        true
    );
  }

  public void testCaptureStart() throws Exception {
    testPatterns(
        "foobarbaz",
        new String[] {"^(.)"},
        new String[] {"f"},
        new int[] {0},
        new int[] {9},
        new int[] {1},
        false
    );
    testPatterns(
        "foobarbaz",
        new String[] {"^(.)"},
        new String[] {"foobarbaz","f"},
        new int[] {0,0},
        new int[] {9,9},
        new int[] {1,0},
        true
    );

    testPatterns(
        "foo bar baz",
        new String[] {"^(.)"},
        new String[] {"f","b","b"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        false
    );

    testPatterns(
        "foo bar baz",
        new String[] {"^(.)"},
        new String[] {"foo","f","bar","b","baz","b"},
        new int[] {0,0,4,4,8,8},
        new int[] {3,3,7,7,11,11},
        new int[] {1,0,1,0,1,0},
        true
    );
  }

  public void testCaptureMiddle() throws Exception {
    testPatterns(
        "foobarbaz",
        new String[] {"^.(.)."},
        new String[] {"o"},
        new int[] {0},
        new int[] {9},
        new int[] {1},
        false
    );
    testPatterns(
        "foobarbaz",
        new String[] {"^.(.)."},
        new String[] {"foobarbaz","o"},
        new int[] {0,0},
        new int[] {9,9},
        new int[] {1,0},
        true
    );

    testPatterns(
        "foo bar baz",
        new String[] {"^.(.)."},
        new String[] {"o","a","a"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        false
    );

    testPatterns(
        "foo bar baz",
        new String[] {"^.(.)."},
        new String[] {"foo","o","bar","a","baz","a"},
        new int[] {0,0,4,4,8,8},
        new int[] {3,3,7,7,11,11},
        new int[] {1,0,1,0,1,0},
        true
    );
  }

  public void testCaptureEnd() throws Exception {
    testPatterns(
        "foobarbaz",
        new String[] {"(.)$"},
        new String[] {"z"},
        new int[] {0},
        new int[] {9},
        new int[] {1},
        false
    );
    testPatterns(
        "foobarbaz",
        new String[] {"(.)$"},
        new String[] {"foobarbaz","z"},
        new int[] {0,0},
        new int[] {9,9},
        new int[] {1,0},
        true
    );

    testPatterns(
        "foo bar baz",
        new String[] {"(.)$"},
        new String[] {"o","r","z"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        false
    );

    testPatterns(
        "foo bar baz",
        new String[] {"(.)$"},
        new String[] {"foo","o","bar","r","baz","z"},
        new int[] {0,0,4,4,8,8},
        new int[] {3,3,7,7,11,11},
        new int[] {1,0,1,0,1,0},
        true
    );
  }

  public void testCaptureStartMiddle() throws Exception {
    testPatterns(
        "foobarbaz",
        new String[] {"^(.)(.)"},
        new String[] {"f","o"},
        new int[] {0,0},
        new int[] {9,9},
        new int[] {1,0},
        false
    );
    testPatterns(
        "foobarbaz",
        new String[] {"^(.)(.)"},
        new String[] {"foobarbaz","f","o"},
        new int[] {0,0,0},
        new int[] {9,9,9},
        new int[] {1,0,0},
        true
    );

    testPatterns(
        "foo bar baz",
        new String[] {"^(.)(.)"},
        new String[] {"f","o","b","a","b","a"},
        new int[] {0,0,4,4,8,8},
        new int[] {3,3,7,7,11,11},
        new int[] {1,0,1,0,1,0},
        false
    );

    testPatterns(
        "foo bar baz",
        new String[] {"^(.)(.)"},
        new String[] {"foo","f","o","bar","b","a","baz","b","a"},
        new int[] {0,0,0,4,4,4,8,8,8},
        new int[] {3,3,3,7,7,7,11,11,11},
        new int[] {1,0,0,1,0,0,1,0,0},
        true
    );
  }

  public void testCaptureStartEnd() throws Exception {
    testPatterns(
        "foobarbaz",
        new String[] {"^(.).+(.)$"},
        new String[] {"f","z"},
        new int[] {0,0},
        new int[] {9,9},
        new int[] {1,0},
        false
    );
    testPatterns(
        "foobarbaz",
        new String[] {"^(.).+(.)$"},
        new String[] {"foobarbaz","f","z"},
        new int[] {0,0,0},
        new int[] {9,9,9},
        new int[] {1,0,0},
        true
    );

    testPatterns(
        "foo bar baz",
        new String[] {"^(.).+(.)$"},
        new String[] {"f","o","b","r","b","z"},
        new int[] {0,0,4,4,8,8},
        new int[] {3,3,7,7,11,11},
        new int[] {1,0,1,0,1,0},
        false
    );

    testPatterns(
        "foo bar baz",
        new String[] {"^(.).+(.)$"},
        new String[] {"foo","f","o","bar","b","r","baz","b","z"},
        new int[] {0,0,0,4,4,4,8,8,8},
        new int[] {3,3,3,7,7,7,11,11,11},
        new int[] {1,0,0,1,0,0,1,0,0},
        true
    );
  }

  public void testCaptureMiddleEnd() throws Exception {
    testPatterns(
        "foobarbaz",
        new String[] {"(.)(.)$"},
        new String[] {"a","z"},
        new int[] {0,0},
        new int[] {9,9},
        new int[] {1,0},
        false
    );
    testPatterns(
        "foobarbaz",
        new String[] {"(.)(.)$"},
        new String[] {"foobarbaz","a","z"},
        new int[] {0,0,0},
        new int[] {9,9,9},
        new int[] {1,0,0},
        true
    );

    testPatterns(
        "foo bar baz",
        new String[] {"(.)(.)$"},
        new String[] {"o","o","a","r","a","z"},
        new int[] {0,0,4,4,8,8},
        new int[] {3,3,7,7,11,11},
        new int[] {1,0,1,0,1,0},
        false
    );

    testPatterns(
        "foo bar baz",
        new String[] {"(.)(.)$"},
        new String[] {"foo","o","o","bar","a","r","baz","a","z"},
        new int[] {0,0,0,4,4,4,8,8,8},
        new int[] {3,3,3,7,7,7,11,11,11},
        new int[] {1,0,0,1,0,0,1,0,0},
        true
    );
  }

  public void testMultiCaptureOverlap() throws Exception {
    testPatterns(
        "foobarbaz",
        new String[] {"(.(.(.)))"},
        new String[] {"foo","oo","o","bar","ar","r","baz","az","z"},
        new int[] {0,0,0,0,0,0,0,0,0},
        new int[] {9,9,9,9,9,9,9,9,9},
        new int[] {1,0,0,0,0,0,0,0,0},
        false
    );
    testPatterns(
        "foobarbaz",
        new String[] {"(.(.(.)))"},
        new String[] {"foobarbaz","foo","oo","o","bar","ar","r","baz","az","z"},
        new int[] {0,0,0,0,0,0,0,0,0,0},
        new int[] {9,9,9,9,9,9,9,9,9,9},
        new int[] {1,0,0,0,0,0,0,0,0,0},
        true
    );

    testPatterns(
        "foo bar baz",
        new String[] {"(.(.(.)))"},
        new String[] {"foo","oo","o","bar","ar","r","baz","az","z"},
        new int[] {0,0,0,4,4,4,8,8,8},
        new int[] {3,3,3,7,7,7,11,11,11},
        new int[] {1,0,0,1,0,0,1,0,0},
        false
    );

    testPatterns(
        "foo bar baz",
        new String[] {"(.(.(.)))"},
        new String[] {"foo","oo","o","bar","ar","r","baz","az","z"},
        new int[] {0,0,0,4,4,4,8,8,8},
        new int[] {3,3,3,7,7,7,11,11,11},
        new int[] {1,0,0,1,0,0,1,0,0},
        true
    );
  }

  public void testMultiPattern() throws Exception {
    testPatterns(
        "aaabbbaaa",
        new String[] {"(aaa)","(bbb)","(ccc)"},
        new String[] {"aaa","bbb","aaa"},
        new int[] {0,0,0},
        new int[] {9,9,9},
        new int[] {1,0,0},
        false
    );
    testPatterns(
        "aaabbbaaa",
        new String[] {"(aaa)","(bbb)","(ccc)"},
        new String[] {"aaabbbaaa","aaa","bbb","aaa"},
        new int[] {0,0,0,0},
        new int[] {9,9,9,9},
        new int[] {1,0,0,0},
        true
    );

    testPatterns(
        "aaa bbb aaa",
        new String[] {"(aaa)","(bbb)","(ccc)"},
        new String[] {"aaa","bbb","aaa"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        false
    );

    testPatterns(
        "aaa bbb aaa",
        new String[] {"(aaa)","(bbb)","(ccc)"},
        new String[] {"aaa","bbb","aaa"},
        new int[] {0,4,8},
        new int[] {3,7,11},
        new int[] {1,1,1},
        true
    );
  }


  public void testCamelCase() throws Exception {
    testPatterns(
        "letsPartyLIKEits1999_dude",
        new String[] {
            "([A-Z]{2,})",
            "(?<![A-Z])([A-Z][a-z]+)",
            "(?:^|\\b|(?<=[0-9_])|(?<=[A-Z]{2}))([a-z]+)",
            "([0-9]+)"
        },
        new String[] {"lets","Party","LIKE","its","1999","dude"},
        new int[] {0,0,0,0,0,0},
        new int[] {25,25,25,25,25,25},
        new int[] {1,0,0,0,0,0,0},
        false
    );
    testPatterns(
        "letsPartyLIKEits1999_dude",
        new String[] {
            "([A-Z]{2,})",
            "(?<![A-Z])([A-Z][a-z]+)",
            "(?:^|\\b|(?<=[0-9_])|(?<=[A-Z]{2}))([a-z]+)",
            "([0-9]+)"
        },
        new String[] {"letsPartyLIKEits1999_dude","lets","Party","LIKE","its","1999","dude"},
        new int[] {0,0,0,0,0,0,0},
        new int[] {25,25,25,25,25,25,25},
        new int[] {1,0,0,0,0,0,0,0},
        true
    );
  }

  public void testRandomString() throws Exception {
    Analyzer a = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer,
            new PatternCaptureGroupTokenFilter(tokenizer, false,
                Pattern.compile("((..)(..))")));
      }
    };

    checkRandomData(random(), a, 1000 * RANDOM_MULTIPLIER);
    a.close();
  }

  private void testPatterns(String input, String[] regexes, String[] tokens,
      int[] startOffsets, int[] endOffsets, int[] positions,
      boolean preserveOriginal) throws Exception {
    Pattern[] patterns = new Pattern[regexes.length];
    for (int i = 0; i < regexes.length; i++) {
      patterns[i] = Pattern.compile(regexes[i]);
    }

    Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenizer.setReader( new StringReader(input));
    TokenStream ts = new PatternCaptureGroupTokenFilter(tokenizer, preserveOriginal, patterns);
    assertTokenStreamContents(ts, tokens, startOffsets, endOffsets, positions);
  }

}
