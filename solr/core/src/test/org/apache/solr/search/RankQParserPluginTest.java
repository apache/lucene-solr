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
package org.apache.solr.search;

import static org.apache.solr.search.RankQParserPlugin.EXPONENT;
import static org.apache.solr.search.RankQParserPlugin.FIELD;
import static org.apache.solr.search.RankQParserPlugin.FUNCTION;
import static org.apache.solr.search.RankQParserPlugin.NAME;
import static org.apache.solr.search.RankQParserPlugin.PIVOT;
import static org.apache.solr.search.RankQParserPlugin.SCALING_FACTOR;
import static org.apache.solr.search.RankQParserPlugin.WEIGHT;

import java.io.IOException;

import org.apache.lucene.search.Query;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.RankQParserPlugin.RankQParser;
import org.hamcrest.CoreMatchers;
import org.junit.BeforeClass;

public class RankQParserPluginTest extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }  

  public void testParamCompatibility() {
    assertEquals("RankQParserPlugin.NAME changed in an incompatible way", "rank", NAME);
    assertEquals("RankQParserPlugin.FIELD changed in an incompatible way", "f", FIELD);
    assertEquals("RankQParserPlugin.FUNCTION changed in an incompatible way", "function", FUNCTION);
    assertEquals("RankQParserPlugin.PIVOT changed in an incompatible way", "pivot", PIVOT);
    assertEquals("RankQParserPlugin.SCALING_FACTOR changed in an incompatible way", "scalingFactor", SCALING_FACTOR);
    assertEquals("RankQParserPlugin.WEIGHT changed in an incompatible way", "weight", WEIGHT);
  }
  
  public void testCreateParser() throws IOException {
    try (RankQParserPlugin rankQPPlugin = new RankQParserPlugin()) {
      QParser parser = rankQPPlugin.createParser("", new ModifiableSolrParams(), null, req()); 
      assertNotNull(parser);
      assertTrue(parser instanceof RankQParser);
    }
  }
  
  public void testSyntaxErrors() throws IOException, SyntaxError {
    assertSyntaxError("No Field", "Field can't be empty", () ->
      getRankQParser("pagerank", new ModifiableSolrParams(), null, req()).parse());
    assertSyntaxError("Field empty", "Field can't be empty", () ->
      getRankQParser("pagerank",
          params(FIELD, ""), null, req()).parse());
    assertSyntaxError("Field doesn't exist", "Field \"foo\" not found", () ->
      getRankQParser("pagerank",
          params(FIELD, "foo"), null, req()).parse());
    assertSyntaxError("Feature can't be empty", "Feature can't be empty", () ->
      getRankQParser("",
          params(FIELD, "id"), null, req()).parse());
    assertSyntaxError("Feature can't be empty", "Feature can't be empty", () ->
      getRankQParser("",
          params(FIELD, "id"), null, req()).parse());
    //TODO: Test for "not a feature field
  }
  
  public void testBadLogParameters() throws IOException, SyntaxError {
    assertSyntaxError("Expecting bad weight", "weight must be in", () ->
      getRankQParser("pagerank",
          params(FIELD, "id",
                 FUNCTION, "log",
                 WEIGHT, "0"), null, req()).parse());
    assertSyntaxError("Expecting bad scaling factor", "scalingFactor must be", () ->
      getRankQParser("pagerank",
          params(FIELD, "id",
                 FUNCTION, "log",
                 SCALING_FACTOR, "0"), null, req()).parse());
  }
  
  public void testBadSaturationParameters() throws IOException, SyntaxError {
    assertSyntaxError("Expecting a pivot value", "A pivot value", () ->
      getRankQParser("pagerank", 
          params(FIELD, "id", 
                 FUNCTION, "satu",
                 WEIGHT, "2"), null, req()).parse());
    assertSyntaxError("Expecting bad weight", "weight must be in", () ->
      getRankQParser("pagerank",
          params(FIELD, "id",
                 FUNCTION, "satu",
                 PIVOT, "1", 
                 WEIGHT, "-1"), null, req()).parse());
  }
  
  public void testBadSigmoidParameters() throws IOException, SyntaxError {
    assertSyntaxError("Expecting bad weight", "A pivot value", () ->
      getRankQParser("pagerank", 
          params(FIELD, "id", 
                 FUNCTION, "sigm",
                 EXPONENT, "1"), null, req()).parse());
    assertSyntaxError("Expecting bad weight", "An exponent value", () ->
    getRankQParser("pagerank", 
        params(FIELD, "id", 
               FUNCTION, "sigm",
               PIVOT, "1"), null, req()).parse());
    assertSyntaxError("Expecting bad weight", "weight must be in", () ->
      getRankQParser("pagerank",
          params(FIELD, "id",
                 FUNCTION, "sigm",
                 PIVOT, "1",
                 EXPONENT, "1",
                 WEIGHT, "-1"), null, req()).parse());
    assertSyntaxError("Expecting bad pivot", "pivot must be", () ->
    getRankQParser("pagerank",
        params(FIELD, "id",
               FUNCTION, "sigm",
               PIVOT, "0",
               EXPONENT, "1"), null, req()).parse());
    assertSyntaxError("Expecting bad exponent", "exp must be", () ->
    getRankQParser("pagerank",
        params(FIELD, "id",
               FUNCTION, "sigm",
               PIVOT, "1", 
               EXPONENT, "0"), null, req()).parse());
    assertSyntaxError("Expecting bad weight", "weight must be", () ->
      getRankQParser("pagerank",
          params(FIELD, "id",
                 FUNCTION, "sigm",
                 PIVOT, "1", 
                 EXPONENT, "1",
                 WEIGHT, "-1"), null, req()).parse());
  }
  
  public void testUnknownFunction() throws IOException, SyntaxError {
    assertSyntaxError("Expecting bad function", "Unknown function in rank query: \"foo\"", () ->
      getRankQParser("pagerank",
          params(FIELD, "id",
                 FUNCTION, "foo"), null, req()).parse());
  }
  
  public void testParseLog() throws IOException, SyntaxError {
    assertValidRankQuery(expectedFeatureQueryToString("id", "pagerank", expectedLogToString(1), 1), 
        params(FIELD, "id",
               FUNCTION, "log",
               SCALING_FACTOR, "1", 
               WEIGHT, "1"));
    
    assertValidRankQuery(expectedFeatureQueryToString("id", "pagerank", expectedLogToString(2.5f), 1), 
        params(FIELD, "id",
               FUNCTION, "log",
               SCALING_FACTOR, "2.5", 
               WEIGHT, "1"));
    
    assertValidRankQuery(expectedFeatureQueryToString("id", "pagerank", expectedLogToString(1), 2.5f), 
        params(FIELD, "id",
               FUNCTION, "log",
               SCALING_FACTOR, "1", 
               WEIGHT, "2.5"));
    
    assertValidRankQuery(expectedFeatureQueryToString("id", "pagerank", expectedLogToString(1), 2.5f), 
        params(FIELD, "id",
               FUNCTION, "Log", //use different case
               SCALING_FACTOR, "1", 
               WEIGHT, "2.5"));
  }
  
  public void testParseSigm() throws IOException, SyntaxError {
    assertValidRankQuery(expectedFeatureQueryToString("id", "pagerank", expectedSigmoidToString(1.5f, 2f), 1), 
        params(FIELD, "id",
               FUNCTION, "sigm",
               PIVOT, "1.5", 
               EXPONENT, "2",
               WEIGHT, "1"));
    
    assertValidRankQuery(expectedFeatureQueryToString("id", "pagerank", expectedSigmoidToString(1.5f, 2f), 2),
        params(FIELD, "id",
               FUNCTION, "sigm",
               PIVOT, "1.5", 
               EXPONENT, "2",
               WEIGHT, "2"));
  }

  public void testParseSatu() throws IOException, SyntaxError {
    
    assertValidRankQuery(expectedFeatureQueryToString("id", "pagerank", expectedSaturationToString(1.5f), 1), 
        params(FIELD, "id",
               FUNCTION, "satu",
               PIVOT, "1.5", 
               WEIGHT, "1"));
    
    assertValidRankQuery(expectedFeatureQueryToString("id", "pagerank", expectedSaturationToString(1.5f), 2), 
        params(FIELD, "id",
               FUNCTION, "satu",
               PIVOT, "1.5", 
               WEIGHT, "2"));
    
    assertValidRankQuery(expectedFeatureQueryToString("id", "pagerank", expectedSaturationToString(null), 1), 
        params(FIELD, "id",
               FUNCTION, "satu"));
    
    assertValidRankQuery(expectedFeatureQueryToString("id", "pagerank", expectedSaturationToString(null), 1), 
        params(FIELD, "id",
               FUNCTION, "satu",
               WEIGHT, "1"));
    
    assertValidRankQuery(expectedFeatureQueryToString("id", "pagerank", expectedSaturationToString(1.5f), 1), 
        params(FIELD, "id",
               FUNCTION, "satu",
               PIVOT, "1.5"));
  }
  
  public void testParseDefault() throws IOException, SyntaxError {
    assertValidRankQuery(expectedFeatureQueryToString("id", "pagerank", expectedSaturationToString(null), 1), 
        params(FIELD, "id"));
  }
  
  private void assertValidRankQuery(String expctedToString, SolrParams localParams) throws IOException, SyntaxError {
    QParser parser = getRankQParser("pagerank", localParams, null, req());
    Query q = parser.parse();
    assertNotNull(q);
    assertThat(q.toString(), CoreMatchers.equalTo(expctedToString));
  }
  
  private String expectedFeatureQueryToString(String fieldName, String featureName, String function, float boost) {
    String featureQueryStr = "FeatureQuery(field=" + fieldName + ", feature=" + featureName + ", function=" + function + ")";
    if (boost == 1f) {
      return featureQueryStr;
    }
    return "(" + featureQueryStr + ")^" + boost;
  }
  
  private String expectedLogToString(float scalingFactor) {
    return "LogFunction(scalingFactor=" + scalingFactor + ")";
  }
  
  private String expectedSigmoidToString(float pivot, float exp) {
    return "SigmoidFunction(pivot=" + pivot + ", a=" + exp + ")";
  }
  
  private String expectedSaturationToString(Float pivot) {
    return "SaturationFunction(pivot=" + pivot + ")";
  }
  
  private void assertSyntaxError(String assertionMsg, String expectedExceptionMsg, ThrowingRunnable runnable) {
    SyntaxError se = expectThrows(SyntaxError.class, assertionMsg, runnable);
    assertThat(se.getMessage(), CoreMatchers.containsString(expectedExceptionMsg));
  }
  
  private RankQParser getRankQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) throws IOException {
    try (RankQParserPlugin rankQPPlugin = new RankQParserPlugin()) {
      return (RankQParser) rankQPPlugin.createParser(qstr, localParams, params, req);
    }
  }

}
