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
package org.apache.solr.request.macro;

import java.util.Map;
import java.util.HashMap;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

/*
 * Tests for the MacroExpander
 */
public class TestMacroExpander extends LuceneTestCase {

  @Test
  public void testExamples() {
    final Map<String,String[]> testParams = new HashMap<String,String[]>();
    final MacroExpander me;
    // example behavior unaffected by absence or value of failOnMissingParams
    if (random().nextBoolean()) {
      me = new MacroExpander(testParams);
    } else {
      final boolean failOnMissingParams = random().nextBoolean();
      me = new MacroExpander(testParams, failOnMissingParams);
    }

    //default examples: https://cwiki.apache.org/confluence/display/solr/Parameter+Substitution
    // and http://yonik.com/solr-query-parameter-substitution/

    //using params
    String[] lowParams = {"50"};
    testParams.put("low",lowParams);
    String[] highParams = {"100"};
    testParams.put("high",highParams);

    String testQuery = "q=popularity:[ ${low} TO ${high} ]";

    assertEquals("q=popularity:[ 50 TO 100 ]", me.expand(testQuery));

    //using default values
    testQuery = "q=popularity:[ ${low:10} TO ${high:20} ]";
    assertEquals("q=popularity:[ 50 TO 100 ]", me.expand(testQuery));

    testParams.clear();
    assertEquals("q=popularity:[ 10 TO 20 ]", me.expand(testQuery));

    //multiple levels of substitutions
    testQuery = "q=${pop_query}";
    String[] popQueryParams = {"${pop_field}:[ ${low} TO ${high} ] AND inStock:true"};
    String[] popFieldParams = {"popularity"};
    testParams.put("low",lowParams);
    testParams.put("high",highParams);
    testParams.put("pop_query",popQueryParams);
    testParams.put("pop_field",popFieldParams);

    assertEquals("q=popularity:[ 50 TO 100 ] AND inStock:true", me.expand(testQuery));

    //end default examples
  }

  @Test
  public void testOnMissingParams() {
    final Map<String,String[]> testParams = new HashMap<String,String[]>();
    final MacroExpander meSkipOnMissingParams = new MacroExpander(testParams);
    final MacroExpander meFailOnMissingParams = new MacroExpander(testParams, true);

    final String low = "50";
    final String high = "100";
    testParams.put("low", new String[]{ low });
    testParams.put("high", new String[]{ high });

    final String testQuery = "q=popularity:[ ${low} TO ${high} ]";

    //when params all present the expansion results match
    final String expandedQuery = "q=popularity:[ "+low+" TO "+high+" ]";
    assertEquals(expandedQuery, meSkipOnMissingParams.expand(testQuery));
    assertEquals(expandedQuery, meFailOnMissingParams.expand(testQuery));

    //when param(s) missing and have no default the expansion results differ
    final String expandedLow;
    final String expandedHigh;
    if (random().nextBoolean()) { // keep low
      expandedLow = low;
    } else {
      expandedLow = "";
      testParams.remove("low");
    }
    if (random().nextBoolean()) { // keep high
      expandedHigh = high;
    } else {
      expandedHigh = "";
      testParams.remove("high");
    }
    assertEquals("q=popularity:[ "+expandedLow+" TO "+expandedHigh+" ]",
        meSkipOnMissingParams.expand(testQuery));
    if (testParams.size() < 2) { // at least one of the two parameters missing
      assertEquals(null, meFailOnMissingParams.expand(testQuery));
    }
  }

  @Test
  public void testMap() { // see SOLR-9740, the second fq param was being dropped.
    final Map<String,String[]> request = new HashMap<>();
    request.put("fq", new String[] {"zero", "${one_ref}", "two", "${three_ref}"});
    request.put("one_ref",new String[] {"one"});
    request.put("three_ref",new String[] {"three"});
    Map expanded = MacroExpander.expand(request);
    assertEquals("zero", ((String[])expanded.get("fq"))[0]);
    assertEquals("one", ((String[])expanded.get("fq"))[1]);
    assertEquals("two", ((String[]) expanded.get("fq"))[2]);
    assertEquals("three", ((String[]) expanded.get("fq"))[3]);
  }

}
