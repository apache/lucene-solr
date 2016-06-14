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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.suggest.Input;
import org.apache.lucene.search.suggest.InputArrayIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

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

    Path tempDir = createTempDir("BlendedInfixSuggesterTest");

    Analyzer a = new StandardAnalyzer(CharArraySet.EMPTY_SET);
    BlendedInfixSuggester suggester = new BlendedInfixSuggester(newFSDirectory(tempDir), a, a,
                                                                AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS,
                                                                BlendedInfixSuggester.BlenderType.POSITION_LINEAR,
                                                                BlendedInfixSuggester.DEFAULT_NUM_FACTOR, false);
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

    Path tempDir = createTempDir("BlendedInfixSuggesterTest");
    Analyzer a = new StandardAnalyzer(CharArraySet.EMPTY_SET);

    // BlenderType.LINEAR is used by default (remove position*10%)
    BlendedInfixSuggester suggester = new BlendedInfixSuggester(newFSDirectory(tempDir), a);
    suggester.build(new InputArrayIterator(keys));

    assertEquals(w, getInResults(suggester, "top", pl, 1));
    assertEquals((int) (w * (1 - 0.10 * 2)), getInResults(suggester, "the", pl, 1));
    assertEquals((int) (w * (1 - 0.10 * 3)), getInResults(suggester, "lake", pl, 1));

    suggester.close();

    // BlenderType.RECIPROCAL is using 1/(1+p) * w where w is weight and p the position of the word
    suggester = new BlendedInfixSuggester(newFSDirectory(tempDir), a, a,
                                          AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS,
                                          BlendedInfixSuggester.BlenderType.POSITION_RECIPROCAL, 1, false);
    suggester.build(new InputArrayIterator(keys));

    assertEquals(w, getInResults(suggester, "top", pl, 1));
    assertEquals((int) (w * 1 / (1 + 2)), getInResults(suggester, "the", pl, 1));
    assertEquals((int) (w * 1 / (1 + 3)), getInResults(suggester, "lake", pl, 1));
    suggester.close();

    // BlenderType.EXPONENTIAL_RECIPROCAL is using 1/(pow(1+p, exponent)) * w where w is weight and p the position of the word
    suggester = new BlendedInfixSuggester(newFSDirectory(tempDir), a, a,
        AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS,
        BlendedInfixSuggester.BlenderType.POSITION_EXPONENTIAL_RECIPROCAL, 1, 4.0, false, true, false);

    suggester.build(new InputArrayIterator(keys));

    assertEquals(w, getInResults(suggester, "top", pl, 1));
    assertEquals((int) (w * 1 / (Math.pow(1 + 2, 4.0))), getInResults(suggester, "the", pl, 1));
    assertEquals((int) (w * 1 / (Math.pow(1 + 3, 4.0))), getInResults(suggester, "lake", pl, 1));

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

    Path tempDir = createTempDir("BlendedInfixSuggesterTest");
    Analyzer a = new StandardAnalyzer(CharArraySet.EMPTY_SET);

    // if factor is small, we don't get the expected element
    BlendedInfixSuggester suggester = new BlendedInfixSuggester(newFSDirectory(tempDir), a, a,
                                                                AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS,
                                                                BlendedInfixSuggester.BlenderType.POSITION_RECIPROCAL, 1, false);

    suggester.build(new InputArrayIterator(keys));


    // we don't find it for in the 2 first
    assertEquals(2, suggester.lookup("the", 2, true, false).size());
    long w0 = getInResults(suggester, "the", ret, 2);
    assertTrue(w0 < 0);

    // but it's there if we search for 3 elements
    assertEquals(3, suggester.lookup("the", 3, true, false).size());
    long w1 = getInResults(suggester, "the", ret, 3);
    assertTrue(w1 > 0);

    suggester.close();

    // if we increase the factor we have it
    suggester = new BlendedInfixSuggester(newFSDirectory(tempDir), a, a,
                                          AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS,
                                          BlendedInfixSuggester.BlenderType.POSITION_RECIPROCAL, 2, false);
    suggester.build(new InputArrayIterator(keys));

    // we have it
    long w2 = getInResults(suggester, "the", ret, 2);
    assertTrue(w2 > 0);

    // but we don't have the other
    long w3 = getInResults(suggester, "the", star, 2);
    assertTrue(w3 < 0);

    suggester.close();
  }

  /**
   * Handle trailing spaces that result in no prefix token LUCENE-6093
   */
  public void testNullPrefixToken() throws IOException {

    BytesRef payload = new BytesRef("lake");

    Input keys[] = new Input[]{
        new Input("top of the lake", 8, payload)
    };

    Path tempDir = createTempDir("BlendedInfixSuggesterTest");

    Analyzer a = new StandardAnalyzer(CharArraySet.EMPTY_SET);
    BlendedInfixSuggester suggester = new BlendedInfixSuggester(newFSDirectory(tempDir), a, a,
                                                                AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS,
                                                                BlendedInfixSuggester.BlenderType.POSITION_LINEAR,
                                                                BlendedInfixSuggester.DEFAULT_NUM_FACTOR, false);
    suggester.build(new InputArrayIterator(keys));

    getInResults(suggester, "of ", payload, 1);
    getInResults(suggester, "the ", payload, 1);
    getInResults(suggester, "lake ", payload, 1);

    suggester.close();
  }

  public void testBlendedInfixSuggesterDedupsOnWeightTitleAndPayload() throws Exception {

    //exactly same inputs
    Input[] inputDocuments = new Input[] {
        new Input("lend me your ear", 7, new BytesRef("uid1")),
        new Input("lend me your ear", 7, new BytesRef("uid1")),
    };
    duplicateCheck(inputDocuments, 1);

    // inputs differ on payload
    inputDocuments = new Input[] {
        new Input("lend me your ear", 7, new BytesRef("uid1")),
        new Input("lend me your ear", 7, new BytesRef("uid2")),
    };
    duplicateCheck(inputDocuments, 2);

    //exactly same input without payloads
    inputDocuments = new Input[] {
        new Input("lend me your ear", 7),
        new Input("lend me your ear", 7),
    };
    duplicateCheck(inputDocuments, 1);

    //Same input with first has payloads, second does not
    inputDocuments = new Input[] {
        new Input("lend me your ear", 7, new BytesRef("uid1")),
        new Input("lend me your ear", 7),
    };
    duplicateCheck(inputDocuments, 2);

    /**same input, first not having a payload, the second having payload
     * we would expect 2 entries out but we are getting only 1 because
     * the InputArrayIterator#hasPayloads() returns false because the first
     * item has no payload, therefore, when ingested, none of the 2 input has payload and become 1
     */
    inputDocuments = new Input[] {
        new Input("lend me your ear", 7),
        new Input("lend me your ear", 7, new BytesRef("uid2")),
    };
    List<Lookup.LookupResult> results = duplicateCheck(inputDocuments, 1);
    assertNull(results.get(0).payload);


    //exactly same inputs but different weight
    inputDocuments = new Input[] {
        new Input("lend me your ear", 1, new BytesRef("uid1")),
        new Input("lend me your ear", 7, new BytesRef("uid1")),
    };
    duplicateCheck(inputDocuments, 2);

    //exactly same inputs but different text
    inputDocuments = new Input[] {
        new Input("lend me your earings", 7, new BytesRef("uid1")),
        new Input("lend me your ear", 7, new BytesRef("uid1")),
    };
    duplicateCheck(inputDocuments, 2);

  }


  public void testSuggesterCountForAllLookups() throws IOException {


    Input keys[] = new Input[]{
        new Input("lend me your ears", 1),
        new Input("as you sow so shall you reap", 1),
    };

    Path tempDir = createTempDir("BlendedInfixSuggesterTest");
    Analyzer a = new StandardAnalyzer(CharArraySet.EMPTY_SET);

    // BlenderType.LINEAR is used by default (remove position*10%)
    BlendedInfixSuggester suggester = new BlendedInfixSuggester(newFSDirectory(tempDir), a);
    suggester.build(new InputArrayIterator(keys));


    String term = "you";

    List<Lookup.LookupResult> responses = suggester.lookup(term, false, 1);
    assertEquals(1, responses.size());

    responses = suggester.lookup(term, false, 2);
    assertEquals(2, responses.size());


    responses = suggester.lookup(term, 1, false, false);
    assertEquals(1, responses.size());

    responses = suggester.lookup(term, 2, false, false);
    assertEquals(2, responses.size());


    responses = suggester.lookup(term, (Map) null, 1, false, false);
    assertEquals(1, responses.size());

    responses = suggester.lookup(term, (Map) null, 2, false, false);
    assertEquals(2, responses.size());


    responses = suggester.lookup(term, (Set) null, 1, false, false);
    assertEquals(1, responses.size());

    responses = suggester.lookup(term, (Set) null, 2, false, false);
    assertEquals(2, responses.size());


    responses = suggester.lookup(term, null, false, 1);
    assertEquals(1, responses.size());

    responses = suggester.lookup(term, null, false, 2);
    assertEquals(2, responses.size());


    responses = suggester.lookup(term, (BooleanQuery) null, 1, false, false);
    assertEquals(1, responses.size());

    responses = suggester.lookup(term, (BooleanQuery) null, 2, false, false);
    assertEquals(2, responses.size());


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

    Path tempDir = createTempDir("BlendedInfixSuggesterTest");
    Analyzer a = new StandardAnalyzer(CharArraySet.EMPTY_SET);

    // if factor is small, we don't get the expected element
    BlendedInfixSuggester suggester = new BlendedInfixSuggester(newFSDirectory(tempDir), a, a,
                                                                AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS,
                                                                BlendedInfixSuggester.BlenderType.POSITION_RECIPROCAL,
                                                                BlendedInfixSuggester.DEFAULT_NUM_FACTOR, false);
    suggester.build(new InputArrayIterator(keys));


    List<Lookup.LookupResult> responses = suggester.lookup("the", 4, true, false);

    for (Lookup.LookupResult response : responses) {
      System.out.println(response);
    }

    suggester.close();
  }

  private static long getInResults(BlendedInfixSuggester suggester, String prefix, BytesRef payload, int num) throws IOException {

    List<Lookup.LookupResult> responses = suggester.lookup(prefix, num, true, false);

    for (Lookup.LookupResult response : responses) {
      if (response.payload.equals(payload)) {
        return response.value;
      }
    }

    return -1;
  }

  private List<Lookup.LookupResult> duplicateCheck(Input[] inputs, int expectedSuggestionCount) throws IOException {

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    BlendedInfixSuggester suggester = new BlendedInfixSuggester(newDirectory(), a, a,  AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS,
        BlendedInfixSuggester.BlenderType.POSITION_RECIPROCAL,10, false);

    InputArrayIterator inputArrayIterator = new InputArrayIterator(inputs);
    suggester.build(inputArrayIterator);

    List<Lookup.LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("ear", random()), 10, true, true);
    assertEquals(expectedSuggestionCount, results.size());

    suggester.close();
    a.close();

    return results;
  }

}
