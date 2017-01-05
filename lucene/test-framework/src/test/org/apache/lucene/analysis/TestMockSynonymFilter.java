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
package org.apache.lucene.analysis;

import java.io.IOException;

/** test the mock synonym filter */
public class TestMockSynonymFilter extends BaseTokenStreamTestCase {

  /** test the mock synonym filter */
  public void test() throws IOException {
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        MockTokenizer tokenizer = new MockTokenizer();
        return new TokenStreamComponents(tokenizer, new MockSynonymFilter(tokenizer));
      }
    };

    assertAnalyzesTo(analyzer, "dogs",
        new String[]{"dogs", "dog"},
        new int[]{0, 0}, // start offset
        new int[]{4, 4}, // end offset
        null,
        new int[]{1, 0}, // position increment
        new int[]{1, 1}, // position length
        true); // check that offsets are correct

    assertAnalyzesTo(analyzer, "small dogs",
        new String[]{"small", "dogs", "dog"},
        new int[]{0, 6, 6},   // start offset
        new int[]{5, 10, 10}, // end offset
        null,
        new int[]{1, 1, 0},   // position increment
        new int[]{1, 1, 1},   // position length
        true); // check that offsets are correct

    assertAnalyzesTo(analyzer, "dogs running",
        new String[]{"dogs", "dog", "running"},
        new int[]{0, 0, 5},  // start offset
        new int[]{4, 4, 12}, // end offset
        null,
        new int[]{1, 0, 1},  // position increment
        new int[]{1, 1, 1},  // position length
        true); // check that offsets are correct

    assertAnalyzesTo(analyzer, "small dogs running",
        new String[]{"small", "dogs", "dog", "running"},
        new int[]{0, 6, 6, 11},   // start offset
        new int[]{5, 10, 10, 18}, // end offset
        null,
        new int[]{1, 1, 0, 1},    // position increment
        new int[]{1, 1, 1, 1},    // position length
        true); // check that offsets are correct

    assertAnalyzesTo(analyzer, "guinea",
        new String[]{"guinea"},
        new int[]{0}, // start offset
        new int[]{6}, // end offset
        null,
        new int[]{1}, // position increment
        new int[]{1}, // position length
        true); // check that offsets are correct

    assertAnalyzesTo(analyzer, "pig",
        new String[]{"pig"},
        new int[]{0}, // start offset
        new int[]{3}, // end offset
        null,
        new int[]{1}, // position increment
        new int[]{1}, // position length
        true); // check that offsets are correct

    assertAnalyzesTo(analyzer, "guinea pig",
        new String[]{"guinea", "cavy", "pig"},
        new int[]{0, 0, 7},   // start offset
        new int[]{6, 10, 10}, // end offset
        null,
        new int[]{1, 0, 1},   // position increment
        new int[]{1, 2, 1},   // position length
        true); // check that offsets are correct

    assertAnalyzesTo(analyzer, "guinea dogs",
        new String[]{"guinea", "dogs", "dog"},
        new int[]{0, 7, 7},   // start offset
        new int[]{6, 11, 11}, // end offset
        null,
        new int[]{1, 1, 0},   // position increment
        new int[]{1, 1, 1},   // position length
        true); // check that offsets are correct

    assertAnalyzesTo(analyzer, "dogs guinea",
        new String[]{"dogs", "dog", "guinea"},
        new int[]{0, 0, 5},  // start offset
        new int[]{4, 4, 11}, // end offset
        null,
        new int[]{1, 0, 1},  // position increment
        new int[]{1, 1, 1},  // position length
        true); // check that offsets are correct

    assertAnalyzesTo(analyzer, "dogs guinea pig",
        new String[]{"dogs", "dog", "guinea", "cavy", "pig"},
        new int[]{0, 0, 5, 5, 12},   // start offset
        new int[]{4, 4, 11, 15, 15}, // end offset
        null,
        new int[]{1, 0, 1, 0, 1},    // position increment
        new int[]{1, 1, 1, 2, 1},    // position length
        true); // check that offsets are correct

    assertAnalyzesTo(analyzer, "guinea pig dogs",
        new String[]{"guinea", "cavy", "pig", "dogs", "dog"},
        new int[]{0, 0, 7, 11, 11},   // start offset
        new int[]{6, 10, 10, 15, 15}, // end offset
        null,
        new int[]{1, 0, 1, 1, 0},     // position increment
        new int[]{1, 2, 1, 1, 1},     // position length
        true); // check that offsets are correct

    assertAnalyzesTo(analyzer, "small dogs and guinea pig running",
        new String[]{"small", "dogs", "dog", "and", "guinea", "cavy", "pig", "running"},
        new int[]{0, 6, 6, 11, 15, 15, 22, 26},   // start offset
        new int[]{5, 10, 10, 14, 21, 25, 25, 33}, // end offset
        null,
        new int[]{1, 1, 0, 1, 1, 0, 1, 1},        // position increment
        new int[]{1, 1, 1, 1, 1, 2, 1, 1},        // position length
        true); // check that offsets are correct

    assertAnalyzesTo(analyzer, "small guinea pig and dogs running",
        new String[]{"small", "guinea", "cavy", "pig", "and", "dogs", "dog", "running"},
        new int[]{0, 6, 6, 13, 17, 21, 21, 26},   // start offset
        new int[]{5, 12, 16, 16, 20, 25, 25, 33}, // end offset
        null,
        new int[]{1, 1, 0, 1, 1, 1, 0, 1},        // position increment
        new int[]{1, 1, 2, 1, 1, 1, 1, 1},        // position length
        true); // check that offsets are correct
  }
}
