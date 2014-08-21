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
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.search.suggest.Input;
import org.apache.lucene.search.suggest.InputArrayIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class BlendedInfixSuggesterTest extends LuceneTestCase {

  /**
   * Test the weight transformation depending on the position
   * of the matching term.
   */
  public void testBlendedSort() throws IOException {

    BytesRef payload = new BytesRef("star");

    Input keys[] = new Input[]{
        new Input("star wars: episode v - the empire strikes back", 8, payload)
    };

    File tempDir = createTempDir("BlendedInfixSuggesterTest");

    Analyzer a = new StandardAnalyzer(CharArraySet.EMPTY_SET);
    BlendedInfixSuggester suggester = new BlendedInfixSuggester(TEST_VERSION_CURRENT, newFSDirectory(tempDir), a, a,
                                                                AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS,
                                                                BlendedInfixSuggester.BlenderType.POSITION_LINEAR,
                                                                BlendedInfixSuggester.DEFAULT_NUM_FACTOR);
    suggester.build(new InputArrayIterator(keys));

    // we query for star wars and check that the weight
    // is smaller when we search for tokens that are far from the beginning

    long w0 = getInResults(suggester, "star ", payload, 1);
    long w1 = getInResults(suggester, "war", payload, 1);
    long w2 = getInResults(suggester, "empire ba", payload, 1);
    long w3 = getInResults(suggester, "back", payload, 1);
    long w4 = getInResults(suggester, "bacc", payload, 1);

    assertTrue(w0 > w1);
    assertTrue(w1 > w2);
    assertTrue(w2 > w3);

    assertTrue(w4 < 0);

    suggester.close();
  }

  /**
   * Verify the different flavours of the blender types
   */
  public void testBlendingType() throws IOException {

    BytesRef pl = new BytesRef("lake");
    long w = 20;

    Input keys[] = new Input[]{
        new Input("top of the lake", w, pl)
    };

    File tempDir = createTempDir("BlendedInfixSuggesterTest");
    Analyzer a = new StandardAnalyzer(CharArraySet.EMPTY_SET);

    // BlenderType.LINEAR is used by default (remove position*10%)
    BlendedInfixSuggester suggester = new BlendedInfixSuggester(TEST_VERSION_CURRENT, newFSDirectory(tempDir), a);
    suggester.build(new InputArrayIterator(keys));

    assertEquals(w, getInResults(suggester, "top", pl, 1));
    assertEquals((int) (w * (1 - 0.10 * 2)), getInResults(suggester, "the", pl, 1));
    assertEquals((int) (w * (1 - 0.10 * 3)), getInResults(suggester, "lake", pl, 1));

    suggester.close();

    // BlenderType.RECIPROCAL is using 1/(1+p) * w where w is weight and p the position of the word
    suggester = new BlendedInfixSuggester(TEST_VERSION_CURRENT, newFSDirectory(tempDir), a, a,
                                          AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS, BlendedInfixSuggester.BlenderType.POSITION_RECIPROCAL, 1);
    suggester.build(new InputArrayIterator(keys));

    assertEquals(w, getInResults(suggester, "top", pl, 1));
    assertEquals((int) (w * 1 / (1 + 2)), getInResults(suggester, "the", pl, 1));
    assertEquals((int) (w * 1 / (1 + 3)), getInResults(suggester, "lake", pl, 1));

    suggester.close();
  }

  /**
   * Assert that the factor is important to get results that might be lower in term of weight but
   * would be pushed up after the blending transformation
   */
  public void testRequiresMore() throws IOException {

    BytesRef lake = new BytesRef("lake");
    BytesRef star = new BytesRef("star");
    BytesRef ret = new BytesRef("ret");

    Input keys[] = new Input[]{
        new Input("top of the lake", 18, lake),
        new Input("star wars: episode v - the empire strikes back", 12, star),
        new Input("the returned", 10, ret),
    };

    File tempDir = createTempDir("BlendedInfixSuggesterTest");
    Analyzer a = new StandardAnalyzer(CharArraySet.EMPTY_SET);

    // if factor is small, we don't get the expected element
    BlendedInfixSuggester suggester = new BlendedInfixSuggester(TEST_VERSION_CURRENT, newFSDirectory(tempDir), a, a,
                                                                AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS, BlendedInfixSuggester.BlenderType.POSITION_RECIPROCAL, 1);

    suggester.build(new InputArrayIterator(keys));


    // we don't find it for in the 2 first
    assertEquals(2, suggester.lookup("the", null, 2, true, false).size());
    long w0 = getInResults(suggester, "the", ret, 2);
    assertTrue(w0 < 0);

    // but it's there if we search for 3 elements
    assertEquals(3, suggester.lookup("the", null, 3, true, false).size());
    long w1 = getInResults(suggester, "the", ret, 3);
    assertTrue(w1 > 0);

    suggester.close();

    // if we increase the factor we have it
    suggester = new BlendedInfixSuggester(TEST_VERSION_CURRENT, newFSDirectory(tempDir), a, a,
                                          AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS, BlendedInfixSuggester.BlenderType.POSITION_RECIPROCAL, 2);
    suggester.build(new InputArrayIterator(keys));

    // we have it
    long w2 = getInResults(suggester, "the", ret, 2);
    assertTrue(w2 > 0);

    // but we don't have the other
    long w3 = getInResults(suggester, "the", star, 2);
    assertTrue(w3 < 0);

    suggester.close();
  }

  public void /*testT*/rying() throws IOException {

    BytesRef lake = new BytesRef("lake");
    BytesRef star = new BytesRef("star");
    BytesRef ret = new BytesRef("ret");

    Input keys[] = new Input[]{
        new Input("top of the lake", 15, lake),
        new Input("star wars: episode v - the empire strikes back", 12, star),
        new Input("the returned", 10, ret),
    };

    File tempDir = createTempDir("BlendedInfixSuggesterTest");
    Analyzer a = new StandardAnalyzer(CharArraySet.EMPTY_SET);

    // if factor is small, we don't get the expected element
    BlendedInfixSuggester suggester = new BlendedInfixSuggester(TEST_VERSION_CURRENT, newFSDirectory(tempDir), a, a,
                                                                AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS, BlendedInfixSuggester.BlenderType.POSITION_RECIPROCAL,
                                                                BlendedInfixSuggester.DEFAULT_NUM_FACTOR);
    suggester.build(new InputArrayIterator(keys));


    List<Lookup.LookupResult> responses = suggester.lookup("the", null, 4, true, false);

    for (Lookup.LookupResult response : responses) {
      System.out.println(response);
    }

    suggester.close();
  }

  private static long getInResults(BlendedInfixSuggester suggester, String prefix, BytesRef payload, int num) throws IOException {

    List<Lookup.LookupResult> responses = suggester.lookup(prefix, null, num, true, false);

    for (Lookup.LookupResult response : responses) {
      if (response.payload.equals(payload)) {
        return response.value;
      }
    }

    return -1;
  }
}
