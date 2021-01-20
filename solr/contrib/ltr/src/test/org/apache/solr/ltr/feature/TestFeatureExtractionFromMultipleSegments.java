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
package org.apache.solr.ltr.feature;

import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.util.Utils;
import org.apache.solr.ltr.TestRerankBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestFeatureExtractionFromMultipleSegments extends TestRerankBase {
  static final String AB = "abcdefghijklmnopqrstuvwxyz";

  static String randomString( int len ){
    StringBuilder sb = new StringBuilder( len );
    for( int i = 0; i < len; i++ ) {
      sb.append( AB.charAt( random().nextInt(AB.length()) ) );
    }
    return sb.toString();
 }

  @BeforeClass
  public static void before() throws Exception {
    // solrconfig-multiseg.xml contains the merge policy to restrict merging
    setuptest("solrconfig-multiseg.xml", "schema.xml");
    // index 400 documents
    for(int i = 0; i<400;i=i+20) {
      assertU(adoc("id", Integer.toString(i),   "popularity", "201", "description", "apple is a company " + randomString(i%6+3), "normHits", "0.1"));
      assertU(adoc("id", Integer.toString(i+1), "popularity", "201", "description", "d " + randomString(i%6+3), "normHits", "0.11"));

      assertU(adoc("id", Integer.toString(i+2), "popularity", "201", "description", "apple is a company too " + randomString(i%6+3), "normHits", "0.1"));
      assertU(adoc("id", Integer.toString(i+3), "popularity", "201", "description", "new york city is big apple " + randomString(i%6+3), "normHits", "0.11"));

      assertU(adoc("id", Integer.toString(i+6), "popularity", "301", "description", "function name " + randomString(i%6+3), "normHits", "0.1"));
      assertU(adoc("id", Integer.toString(i+7), "popularity", "301", "description", "function " + randomString(i%6+3), "normHits", "0.1"));

      assertU(adoc("id", Integer.toString(i+8), "popularity", "301", "description", "This is a sample function for testing " + randomString(i%6+3), "normHits", "0.1"));
      assertU(adoc("id", Integer.toString(i+9), "popularity", "301", "description", "Function to check out stock prices "+randomString(i%6+3), "normHits", "0.1"));
      assertU(adoc("id", Integer.toString(i+10),"popularity", "301", "description", "Some descriptions "+randomString(i%6+3), "normHits", "0.1"));

      assertU(adoc("id", Integer.toString(i+11), "popularity", "201", "description", "apple apple is a company " + randomString(i%6+3), "normHits", "0.1"));
      assertU(adoc("id", Integer.toString(i+12), "popularity", "201", "description", "Big Apple is New York.", "normHits", "0.01"));
      assertU(adoc("id", Integer.toString(i+13), "popularity", "201", "description", "New some York is Big. "+ randomString(i%6+3), "normHits", "0.1"));

      assertU(adoc("id", Integer.toString(i+14), "popularity", "201", "description", "apple apple is a company " + randomString(i%6+3), "normHits", "0.1"));
      assertU(adoc("id", Integer.toString(i+15), "popularity", "201", "description", "Big Apple is New York.", "normHits", "0.01"));
      assertU(adoc("id", Integer.toString(i+16), "popularity", "401", "description", "barack h", "normHits", "0.0"));
      assertU(adoc("id", Integer.toString(i+17), "popularity", "201", "description", "red delicious apple " + randomString(i%6+3), "normHits", "0.1"));
      assertU(adoc("id", Integer.toString(i+18), "popularity", "201", "description", "nyc " + randomString(i%6+3), "normHits", "0.11"));
    }

    assertU(commit());

    loadFeatures("comp_features.json");
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void testFeatureExtractionFromMultipleSegments() throws Exception {

    final SolrQuery query = new SolrQuery();
    query.setQuery("{!edismax qf='description^1' boost='sum(product(pow(normHits, 0.7), 1600), .1)' v='apple'}");
    // request 100 rows, if any rows are fetched from the second or subsequent segments the tests should succeed if LTRRescorer::extractFeaturesInfo() advances the doc iterator properly
    int numRows = 100;
    query.add("rows", Integer.toString(numRows));
    query.add("wt", "json");
    query.add("fq", "popularity:201");
    query.add("fl", "*, score,id,normHits,description,fv:[features store='feature-store-6' format='dense' efi.user_text='apple']");
    String res = restTestHarness.query("/query" + query.toQueryString());

    @SuppressWarnings({"unchecked"})
    Map<String,Object> resultJson = (Map<String,Object>) Utils.fromJSONString(res);

    @SuppressWarnings({"unchecked"})
    List<Map<String,Object>> docs = (List<Map<String,Object>>)((Map<String,Object>)resultJson.get("response")).get("docs");
    int passCount = 0;
    for (final Map<String,Object> doc : docs) {
       String features = (String)doc.get("fv");
       assert(features.length() > 0);
       ++passCount;
    }
    assert(passCount == numRows);
  }
}
