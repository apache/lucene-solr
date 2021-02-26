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
package org.apache.lucene.analysis.path;

import static org.apache.lucene.analysis.path.ReversePathHierarchyTokenizer.DEFAULT_DELIMITER;
import static org.apache.lucene.analysis.path.ReversePathHierarchyTokenizer.DEFAULT_SKIP;

import java.io.StringReader;
import java.util.Random;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;

public class TestReversePathHierarchyTokenizer extends BaseTokenStreamTestCase {

  public void testBasicReverse() throws Exception {
    String path = "/a/b/c";
    ReversePathHierarchyTokenizer t =
        new ReversePathHierarchyTokenizer(
            newAttributeFactory(), DEFAULT_DELIMITER, DEFAULT_DELIMITER, DEFAULT_SKIP);
    t.setReader(new StringReader(path));
    assertTokenStreamContents(
        t,
        new String[] {"/a/b/c", "a/b/c", "b/c", "c"},
        new int[] {0, 1, 3, 5},
        new int[] {6, 6, 6, 6},
        new int[] {1, 0, 0, 0},
        path.length());
  }

  public void testEndOfDelimiterReverse() throws Exception {
    String path = "/a/b/c/";
    ReversePathHierarchyTokenizer t =
        new ReversePathHierarchyTokenizer(
            newAttributeFactory(), DEFAULT_DELIMITER, DEFAULT_DELIMITER, DEFAULT_SKIP);
    t.setReader(new StringReader(path));
    assertTokenStreamContents(
        t,
        new String[] {"/a/b/c/", "a/b/c/", "b/c/", "c/"},
        new int[] {0, 1, 3, 5},
        new int[] {7, 7, 7, 7},
        new int[] {1, 0, 0, 0},
        path.length());
  }

  public void testStartOfCharReverse() throws Exception {
    String path = "a/b/c";
    ReversePathHierarchyTokenizer t =
        new ReversePathHierarchyTokenizer(
            newAttributeFactory(), DEFAULT_DELIMITER, DEFAULT_DELIMITER, DEFAULT_SKIP);
    t.setReader(new StringReader(path));
    assertTokenStreamContents(
        t,
        new String[] {"a/b/c", "b/c", "c"},
        new int[] {0, 2, 4},
        new int[] {5, 5, 5},
        new int[] {1, 0, 0},
        path.length());
  }

  public void testStartOfCharEndOfDelimiterReverse() throws Exception {
    String path = "a/b/c/";
    ReversePathHierarchyTokenizer t =
        new ReversePathHierarchyTokenizer(
            newAttributeFactory(), DEFAULT_DELIMITER, DEFAULT_DELIMITER, DEFAULT_SKIP);
    t.setReader(new StringReader(path));
    assertTokenStreamContents(
        t,
        new String[] {"a/b/c/", "b/c/", "c/"},
        new int[] {0, 2, 4},
        new int[] {6, 6, 6},
        new int[] {1, 0, 0},
        path.length());
  }

  public void testOnlyDelimiterReverse() throws Exception {
    String path = "/";
    ReversePathHierarchyTokenizer t =
        new ReversePathHierarchyTokenizer(
            newAttributeFactory(), DEFAULT_DELIMITER, DEFAULT_DELIMITER, DEFAULT_SKIP);
    t.setReader(new StringReader(path));
    assertTokenStreamContents(
        t, new String[] {"/"}, new int[] {0}, new int[] {1}, new int[] {1}, path.length());
  }

  public void testOnlyDelimitersReverse() throws Exception {
    String path = "//";
    ReversePathHierarchyTokenizer t =
        new ReversePathHierarchyTokenizer(
            newAttributeFactory(), DEFAULT_DELIMITER, DEFAULT_DELIMITER, DEFAULT_SKIP);
    t.setReader(new StringReader(path));
    assertTokenStreamContents(
        t,
        new String[] {"//", "/"},
        new int[] {0, 1},
        new int[] {2, 2},
        new int[] {1, 0},
        path.length());
  }

  public void testEndOfDelimiterReverseSkip() throws Exception {
    String path = "/a/b/c/";
    ReversePathHierarchyTokenizer t =
        new ReversePathHierarchyTokenizer(
            newAttributeFactory(), DEFAULT_DELIMITER, DEFAULT_DELIMITER, 1);
    t.setReader(new StringReader(path));
    new StringReader(path);
    assertTokenStreamContents(
        t,
        new String[] {"/a/b/", "a/b/", "b/"},
        new int[] {0, 1, 3},
        new int[] {5, 5, 5},
        new int[] {1, 0, 0},
        path.length());
  }

  public void testStartOfCharReverseSkip() throws Exception {
    String path = "a/b/c";
    ReversePathHierarchyTokenizer t =
        new ReversePathHierarchyTokenizer(
            newAttributeFactory(), DEFAULT_DELIMITER, DEFAULT_DELIMITER, 1);
    t.setReader(new StringReader(path));
    assertTokenStreamContents(
        t,
        new String[] {"a/b/", "b/"},
        new int[] {0, 2},
        new int[] {4, 4},
        new int[] {1, 0},
        path.length());
  }

  public void testStartOfCharEndOfDelimiterReverseSkip() throws Exception {
    String path = "a/b/c/";
    ReversePathHierarchyTokenizer t =
        new ReversePathHierarchyTokenizer(
            newAttributeFactory(), DEFAULT_DELIMITER, DEFAULT_DELIMITER, 1);
    t.setReader(new StringReader(path));
    assertTokenStreamContents(
        t,
        new String[] {"a/b/", "b/"},
        new int[] {0, 2},
        new int[] {4, 4},
        new int[] {1, 0},
        path.length());
  }

  public void testOnlyDelimiterReverseSkip() throws Exception {
    String path = "/";
    ReversePathHierarchyTokenizer t =
        new ReversePathHierarchyTokenizer(
            newAttributeFactory(), DEFAULT_DELIMITER, DEFAULT_DELIMITER, 1);
    t.setReader(new StringReader(path));
    assertTokenStreamContents(
        t, new String[] {}, new int[] {}, new int[] {}, new int[] {}, path.length());
  }

  public void testOnlyDelimitersReverseSkip() throws Exception {
    String path = "//";
    ReversePathHierarchyTokenizer t =
        new ReversePathHierarchyTokenizer(
            newAttributeFactory(), DEFAULT_DELIMITER, DEFAULT_DELIMITER, 1);
    t.setReader(new StringReader(path));
    assertTokenStreamContents(
        t, new String[] {"/"}, new int[] {0}, new int[] {1}, new int[] {1}, path.length());
  }

  public void testReverseSkip2() throws Exception {
    String path = "/a/b/c/";
    ReversePathHierarchyTokenizer t =
        new ReversePathHierarchyTokenizer(
            newAttributeFactory(), DEFAULT_DELIMITER, DEFAULT_DELIMITER, 2);
    t.setReader(new StringReader(path));
    assertTokenStreamContents(
        t,
        new String[] {"/a/", "a/"},
        new int[] {0, 1},
        new int[] {3, 3},
        new int[] {1, 0},
        path.length());
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer =
                new ReversePathHierarchyTokenizer(
                    newAttributeFactory(), DEFAULT_DELIMITER, DEFAULT_DELIMITER, DEFAULT_SKIP);
            return new TokenStreamComponents(tokenizer, tokenizer);
          }
        };
    // TODO: properly support positionLengthAttribute
    checkRandomData(random(), a, 200 * RANDOM_MULTIPLIER, 20, false, false);
    a.close();
  }

  /** blast some random large strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    Random random = random();
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer =
                new ReversePathHierarchyTokenizer(
                    newAttributeFactory(), DEFAULT_DELIMITER, DEFAULT_DELIMITER, DEFAULT_SKIP);
            return new TokenStreamComponents(tokenizer, tokenizer);
          }
        };
    // TODO: properly support positionLengthAttribute
    checkRandomData(random, a, 100 * RANDOM_MULTIPLIER, 1027, false, false);
    a.close();
  }
}
