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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.suggest.Input;
import org.apache.lucene.search.suggest.InputArrayIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import static org.hamcrest.core.Is.is;

public class BlendedInfixSuggesterTest extends LuceneTestCase {

  public void testSinglePositionalCoefficient_linearBlendingType_shouldCalculateLinearCoefficient() throws IOException {
    BlendedInfixSuggester linearSuggester = this.getBlendedInfixSuggester(new Input[]{}, BlendedInfixSuggester.BlenderType.POSITION_LINEAR);
    int inputPosition = 5;

    double positionalCoefficient = linearSuggester.calculatePositionalCoefficient(inputPosition);

    assertEquals(1 - 0.1 * inputPosition, positionalCoefficient, 0.01);

    linearSuggester.close();
  }

  public void testSinglePositionalCoefficient_reciprocalBlendingType_shouldCalculateLinearCoefficient() throws IOException {
    BlendedInfixSuggester reciprocalSuggester = this.getBlendedInfixSuggester(new Input[]{}, BlendedInfixSuggester.BlenderType.POSITION_RECIPROCAL);
    int inputPosition = 5;

    double positionalCoefficient = reciprocalSuggester.calculatePositionalCoefficient(inputPosition);

    assertEquals(1. / (inputPosition + 1.0), positionalCoefficient, 0.01);

    reciprocalSuggester.close();
  }

  public void testSinglePositionalCoefficient_exponentialBlendingType_shouldCalculateLinearCoefficient() throws IOException {
    BlendedInfixSuggester exponentialReciprocalSuggester = this.getBlendedInfixSuggester(new Input[]{}, BlendedInfixSuggester.BlenderType.POSITION_EXPONENTIAL_RECIPROCAL);
    int inputPosition = 5;

    double positionalCoefficient = exponentialReciprocalSuggester.calculatePositionalCoefficient(inputPosition);

    assertEquals(1. / Math.pow((inputPosition + 1.0), 2), positionalCoefficient, 0.01);

    exponentialReciprocalSuggester.close();
  }

  public void testProductDiscontinuedPositionalCoefficient_linearBlendingType_shouldCalculateLinearCoefficient() throws IOException {
    BlendedInfixSuggester linearSuggester = this.getBlendedInfixSuggester(new Input[]{}, BlendedInfixSuggester.BlenderType.POSITION_LINEAR);
    int[] inputPosition1 = new int[]{0, 1, 2};
    int[] inputPosition2 = new int[]{0, 1, 3};
    int[] inputPosition3 = new int[]{0, 1, 4};
    int[] inputPosition4 = new int[]{0, 2, 3};
    int[] inputPosition5 = new int[]{1, 2, 3};

    double positionalCoefficient1 = linearSuggester.calculateProductPositionalCoefficient(inputPosition1);
    double positionalCoefficient2 = linearSuggester.calculateProductPositionalCoefficient(inputPosition2);
    double positionalCoefficient3 = linearSuggester.calculateProductPositionalCoefficient(inputPosition3);
    double positionalCoefficient4 = linearSuggester.calculateProductPositionalCoefficient(inputPosition4);
    double positionalCoefficient5 = linearSuggester.calculateProductPositionalCoefficient(inputPosition5);

    assertTrue(positionalCoefficient1 > positionalCoefficient2);
    assertTrue(positionalCoefficient2 > positionalCoefficient3);
    assertTrue(positionalCoefficient3 > positionalCoefficient4);
    assertTrue(positionalCoefficient4 > positionalCoefficient5);

    linearSuggester.close();
  }

  public void testProductDiscontinuedPositionalCoefficient_reciprocalBlendingType_shouldCalculateLinearCoefficient() throws IOException {
    BlendedInfixSuggester reciprocalSuggester = this.getBlendedInfixSuggester(new Input[]{}, BlendedInfixSuggester.BlenderType.POSITION_RECIPROCAL);
    int[] inputPosition1 = new int[]{0, 1, 2};
    int[] inputPosition2 = new int[]{0, 1, 3};
    int[] inputPosition3 = new int[]{0, 1, 4};
    int[] inputPosition4 = new int[]{0, 2, 3};
    int[] inputPosition5 = new int[]{1, 2, 3};

    double positionalCoefficient1 = reciprocalSuggester.calculateProductPositionalCoefficient(inputPosition1);
    double positionalCoefficient2 = reciprocalSuggester.calculateProductPositionalCoefficient(inputPosition2);
    double positionalCoefficient3 = reciprocalSuggester.calculateProductPositionalCoefficient(inputPosition3);
    double positionalCoefficient4 = reciprocalSuggester.calculateProductPositionalCoefficient(inputPosition4);
    double positionalCoefficient5 = reciprocalSuggester.calculateProductPositionalCoefficient(inputPosition5);

    assertTrue(positionalCoefficient1 > positionalCoefficient2);
    assertTrue(positionalCoefficient2 > positionalCoefficient3);
    assertTrue(positionalCoefficient3 > positionalCoefficient4);
    assertTrue(positionalCoefficient4 > positionalCoefficient5);

    reciprocalSuggester.close();
  }

  /**
   * An exponential reciprocal approach will penalise less based on the first position of the first mismatched, favouring consecutive matches that happen
   * a little bit later on the field content.
   *
   */
  public void testProductDiscontinuedPositionalCoefficient_exponentialBlendingType_shouldCalculateLinearCoefficient() throws IOException {
    BlendedInfixSuggester exponentialReciprocalSuggester = this.getBlendedInfixSuggester(new Input[]{}, BlendedInfixSuggester.BlenderType.POSITION_EXPONENTIAL_RECIPROCAL);
    int[] inputPosition1 = new int[]{0, 1, 2};
    int[] inputPosition2 = new int[]{0, 1, 3};
    int[] inputPosition3 = new int[]{0, 2, 3};
    int[] inputPosition4 = new int[]{0, 1, 4};
    int[] inputPosition5 = new int[]{1, 2, 3};
    int[] inputPosition6 = new int[]{0, 2, 4};
    int[] inputPosition7 = new int[]{2, 3, 4};

    double positionalCoefficient1 = exponentialReciprocalSuggester.calculateProductPositionalCoefficient(inputPosition1);
    double positionalCoefficient2 = exponentialReciprocalSuggester.calculateProductPositionalCoefficient(inputPosition2);
    double positionalCoefficient3 = exponentialReciprocalSuggester.calculateProductPositionalCoefficient(inputPosition3);
    double positionalCoefficient4 = exponentialReciprocalSuggester.calculateProductPositionalCoefficient(inputPosition4);
    double positionalCoefficient5 = exponentialReciprocalSuggester.calculateProductPositionalCoefficient(inputPosition5);
    double positionalCoefficient6 = exponentialReciprocalSuggester.calculateProductPositionalCoefficient(inputPosition6);
    double positionalCoefficient7 = exponentialReciprocalSuggester.calculateProductPositionalCoefficient(inputPosition7);

    assertTrue(positionalCoefficient1 > positionalCoefficient2);
    assertTrue(positionalCoefficient2 > positionalCoefficient3);
    assertTrue(positionalCoefficient3 > positionalCoefficient4);
    assertTrue(positionalCoefficient4 > positionalCoefficient5);
    assertTrue(positionalCoefficient5 > positionalCoefficient6);
    assertTrue(positionalCoefficient6 > positionalCoefficient7);

    exponentialReciprocalSuggester.close();
  }
  
  /**
   * Test the weight transformation depending on the position
   * of the matching term.
   */
  public void testBlendedSort() throws IOException {
    BytesRef payload = new BytesRef("star");
    Input keys[] = new Input[]{
        new Input("star wars: episode v - the empire strikes back", 8, payload)
    };
    BlendedInfixSuggester suggester = getBlendedInfixSuggester(keys, BlendedInfixSuggester.BlenderType.POSITION_LINEAR);

    assertSuggestionsRanking(payload, suggester);
  }

  /**
   * Test to validate the suggestions ranking according to the position coefficient,
   * even if the weight associated to the suggestion is unitary.
   */
  public void testBlendedSort_fieldWeightUnitary_shouldRankSuggestionsByPositionMatch() throws IOException {
    BytesRef payload = new BytesRef("star");
    Input keys[] = new Input[]{
        new Input("star wars: episode v - the empire strikes back", 1, payload)
    };
    BlendedInfixSuggester suggester = getBlendedInfixSuggester(keys, BlendedInfixSuggester.BlenderType.POSITION_LINEAR);

    assertSuggestionsRanking(payload, suggester);
  }

  /**
   * Test to validate the suggestions ranking according to the position coefficient,
   * even if the weight associated to the suggestion is zero.
   */
  public void testBlendedSort_fieldWeightZero_shouldRankSuggestionsByPositionMatch() throws IOException {
    BytesRef payload = new BytesRef("star");
    Input keys[] = new Input[]{
        new Input("star wars: episode v - the empire strikes back", 0, payload)
    };
    BlendedInfixSuggester suggester = getBlendedInfixSuggester(keys, BlendedInfixSuggester.BlenderType.POSITION_LINEAR);

    assertSuggestionsRanking(payload, suggester);
  }

  /**
   * Test the weight transformation depending on the position
   * of the matching term.
   */
  public void testBlendedSort_multiTermQuery_shouldRankSuggestionsAccordinEachTermPosition() throws IOException {

    BytesRef payload = new BytesRef("star");

    Input keys[] = new Input[]{
        new Input("Mini Bar something Fridge", 1, payload),
        new Input("Mini Bar Fridge something", 1, payload),
        new Input("Mini Bar something else Fridge", 1, payload),
        new Input("something Mini Bar Fridge", 1, payload),
        new Input("something else Mini Bar Fridge", 1, payload),
        new Input("Mini Bar Fridge something else", 1, payload),
        new Input("Mini something Bar Fridge", 1, payload),
        new Input("Mini Bar Fridge a a a a a a a a a a a a a a a a a a a a a a", 1, payload),
        new Input("Bar Fridge Mini", 1, payload)
    };

    BlendedInfixSuggester linearSuggester = getBlendedInfixSuggester(keys, BlendedInfixSuggester.BlenderType.POSITION_LINEAR);

    // we query for star wars and check that the weight
    // is smaller when we search for tokens that are far from the beginning

    List<Lookup.LookupResult> responses = linearSuggester.lookup("Mini Bar Fridge", 10, true, false);
    assertThat(responses.get(0).key, is("Mini Bar Fridge something"));
    assertThat(responses.get(1).key, is("Mini Bar Fridge something else"));
    assertThat(responses.get(2).key, is("Mini Bar Fridge a a a a a a a a a a a a a a a a a a a a a a"));
    assertThat(responses.get(3).key, is("Mini Bar something Fridge"));
    assertThat(responses.get(4).key, is("Mini Bar something else Fridge"));
    assertThat(responses.get(5).key, is("Mini something Bar Fridge"));
    assertThat(responses.get(6).key, is("something Mini Bar Fridge"));
    assertThat(responses.get(7).key, is("something else Mini Bar Fridge"));
    assertThat(responses.get(8).key, is("Bar Fridge Mini"));

    assertTrue(responses.get(1).value < responses.get(0).value);
    assertTrue(responses.get(2).value < responses.get(1).value);
    assertTrue(responses.get(3).value < responses.get(2).value);
    assertTrue(responses.get(4).value < responses.get(3).value);
    assertTrue(responses.get(5).value < responses.get(4).value);
    assertTrue(responses.get(6).value < responses.get(5).value);
    assertTrue(responses.get(7).value < responses.get(6).value);
    assertTrue(responses.get(8).value < responses.get(7).value);
    linearSuggester.close();
  }

  /**
   * Test the weight transformation depending on the position
   * of the matching term.
   */
  public void testBlendedSort_multiTermQueryWithRepetitions_shouldRankSuggestionsAccordinEachTermPosition() throws IOException {

    BytesRef payload = new BytesRef("star");

    Input keys[] = new Input[]{
        new Input("Mini Bar something Fridge Bar", 1, payload),
        new Input("Mini Bar Fridge Bar something", 1, payload),
        new Input("Mini Bar something else Fridge Bar", 1, payload),
        new Input("something Mini Bar Fridge Bar", 1, payload),
        new Input("something else Mini Bar Fridge Bar", 1, payload),
        new Input("Mini Bar Fridge Bar something else", 1, payload),
        new Input("Mini something Bar Fridge Bar", 1, payload),
        new Input("Mini Bar Fridge Bar a a a a a a a a a a a a a a a a a a a a a a", 1, payload),
        new Input("Bar Fridge Mini Bar", 1, payload)
    };

    BlendedInfixSuggester linearSuggester = getBlendedInfixSuggester(keys, BlendedInfixSuggester.BlenderType.POSITION_LINEAR);

    // we query for star wars and check that the weight
    // is smaller when we search for tokens that are far from the beginning

    List<Lookup.LookupResult> responses = linearSuggester.lookup("Mini Bar Fridge Bar", 10, true, false);
    assertThat(responses.get(0).key, is("Mini Bar Fridge Bar something"));
    assertThat(responses.get(1).key, is("Mini Bar Fridge Bar something else"));
    assertThat(responses.get(2).key, is("Mini Bar Fridge Bar a a a a a a a a a a a a a a a a a a a a a a"));
    assertThat(responses.get(3).key, is("Mini Bar something Fridge Bar"));
    assertThat(responses.get(4).key, is("Mini Bar something else Fridge Bar"));
    assertThat(responses.get(5).key, is("Mini something Bar Fridge Bar"));
    assertThat(responses.get(6).key, is("something Mini Bar Fridge Bar"));
    assertThat(responses.get(7).key, is("something else Mini Bar Fridge Bar"));
    assertThat(responses.get(8).key, is("Bar Fridge Mini Bar"));

    assertTrue(responses.get(1).value < responses.get(0).value);
    assertTrue(responses.get(2).value < responses.get(1).value);
    assertTrue(responses.get(3).value < responses.get(2).value);
    assertTrue(responses.get(4).value < responses.get(3).value);
    assertTrue(responses.get(5).value < responses.get(4).value);
    assertTrue(responses.get(6).value < responses.get(5).value);
    assertTrue(responses.get(7).value < responses.get(6).value);
    assertTrue(responses.get(8).value < responses.get(7).value);
    linearSuggester.close();
  }

  private void assertSuggestionsRanking(BytesRef payload, BlendedInfixSuggester suggester) throws IOException {
    // we query for star wars and check that the weight
    // is smaller when we search for tokens that are far from the beginning

    long w0 = getInResults(suggester, "star ", payload, 1);
    long w1 = getInResults(suggester, "war", payload, 1);
    long w2 = getInResults(suggester, "empire stri", payload, 1);
    long w3 = getInResults(suggester, "back", payload, 1);
    long w4 = getInResults(suggester, "empire bac", payload, 1);
    long w5 = getInResults(suggester, "bacc", payload, 1);

    assertTrue(w0 > w1);
    assertTrue(w1 > w2);
    assertTrue(w2 > w3); // consecutive terms "empire stri" with first mach position better than "back"
    assertTrue(w3 > w4); // "empire" is occurring before "back", but "stri" is also not positioned correctly
    assertTrue(w5 < 0);

    suggester.close();
  }

  private BlendedInfixSuggester getBlendedInfixSuggester(Input[] keys, BlendedInfixSuggester.BlenderType type) throws IOException {
    Path tempDir = createTempDir("BlendedInfixSuggesterTest");

    Analyzer a = new StandardAnalyzer(CharArraySet.EMPTY_SET);
    BlendedInfixSuggester suggester = new BlendedInfixSuggester(newFSDirectory(tempDir), a, a,
        AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS,
        type,
        BlendedInfixSuggester.DEFAULT_NUM_FACTOR, false);
    suggester.build(new InputArrayIterator(keys));
    return suggester;
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

    BlendedInfixSuggester suggester = getBlendedInfixSuggester(keys, BlendedInfixSuggester.BlenderType.POSITION_LINEAR);

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


    responses = suggester.lookup(term, (Map<BytesRef, BooleanClause.Occur>) null, 1, false, false);
    assertEquals(1, responses.size());

    responses = suggester.lookup(term, (Map<BytesRef, BooleanClause.Occur>) null, 2, false, false);
    assertEquals(2, responses.size());


    responses = suggester.lookup(term, (Set<BytesRef>) null, 1, false, false);
    assertEquals(1, responses.size());

    responses = suggester.lookup(term, (Set<BytesRef>) null, 2, false, false);
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
