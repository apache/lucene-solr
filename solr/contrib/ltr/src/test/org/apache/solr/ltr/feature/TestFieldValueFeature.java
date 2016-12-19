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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.FeatureLoggerTestUtils;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.model.LinearModel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFieldValueFeature extends TestRerankBase {

  private static final float FIELD_VALUE_FEATURE_DEFAULT_VAL = 0.0f;

  @BeforeClass
  public static void before() throws Exception {
    setuptest("solrconfig-ltr.xml", "schema.xml");

    assertU(adoc("id", "1", "title", "w1", "description", "w1", "popularity",
        "1","isTrendy","true"));
    assertU(adoc("id", "2", "title", "w2 2asd asdd didid", "description",
        "w2 2asd asdd didid", "popularity", "2"));
    assertU(adoc("id", "3", "title", "w3", "description", "w3", "popularity",
        "3","isTrendy","true"));
    assertU(adoc("id", "4", "title", "w4", "description", "w4", "popularity",
        "4","isTrendy","false"));
    assertU(adoc("id", "5", "title", "w5", "description", "w5", "popularity",
        "5","isTrendy","true"));
    assertU(adoc("id", "6", "title", "w1 w2", "description", "w1 w2",
        "popularity", "6","isTrendy","false"));
    assertU(adoc("id", "7", "title", "w1 w2 w3 w4 w5", "description",
        "w1 w2 w3 w4 w5 w8", "popularity", "7","isTrendy","true"));
    assertU(adoc("id", "8", "title", "w1 w1 w1 w2 w2 w8", "description",
        "w1 w1 w1 w2 w2", "popularity", "8","isTrendy","false"));

    // a document without the popularity field
    assertU(adoc("id", "42", "title", "NO popularity", "description", "NO popularity"));

    assertU(commit());

    loadFeature("popularity", FieldValueFeature.class.getCanonicalName(),
            "{\"field\":\"popularity\"}");

    loadModel("popularity-model", LinearModel.class.getCanonicalName(),
            new String[] {"popularity"}, "{\"weights\":{\"popularity\":1.0}}");
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void testRanking() throws Exception {

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score");
    query.add("rows", "4");

    // Normal term match
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='7'");

    query.add("rq", "{!ltr model=popularity-model reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='1'");

    query.setQuery("*:*");
    query.remove("rows");
    query.add("rows", "8");
    query.remove("rq");
    query.add("rq", "{!ltr model=popularity-model reRankDocs=8}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='5'");
  }


  @Test
  public void testIfADocumentDoesntHaveAFieldDefaultValueIsReturned() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery("id:42");
    query.add("fl", "*, score");
    query.add("rows", "4");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='42'");
    query = new SolrQuery();
    query.setQuery("id:42");
    query.add("rq", "{!ltr model=popularity-model reRankDocs=4}");
    query.add("fl", "[fv]");
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
    assertJQ("/query" + query.toQueryString(),
            "/response/docs/[0]/=={'[fv]':'"+FeatureLoggerTestUtils.toFeatureVector("popularity",Float.toString(FIELD_VALUE_FEATURE_DEFAULT_VAL))+"'}");

  }


  @Test
  public void testIfADocumentDoesntHaveAFieldASetDefaultValueIsReturned() throws Exception {

    final String fstore = "testIfADocumentDoesntHaveAFieldASetDefaultValueIsReturned";

    loadFeature("popularity42", FieldValueFeature.class.getCanonicalName(), fstore,
            "{\"field\":\"popularity\",\"defaultValue\":\"42.0\"}");

    SolrQuery query = new SolrQuery();
    query.setQuery("id:42");
    query.add("fl", "*, score");
    query.add("rows", "4");

    loadModel("popularity-model42", LinearModel.class.getCanonicalName(),
            new String[] {"popularity42"}, fstore, "{\"weights\":{\"popularity42\":1.0}}");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='42'");
    query = new SolrQuery();
    query.setQuery("id:42");
    query.add("rq", "{!ltr model=popularity-model42 reRankDocs=4}");
    query.add("fl", "[fv]");
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
    assertJQ("/query" + query.toQueryString(),
            "/response/docs/[0]/=={'[fv]':'"+FeatureLoggerTestUtils.toFeatureVector("popularity42","42.0")+"'}");

  }

  @Test
  public void testThatIfaFieldDoesNotExistDefaultValueIsReturned() throws Exception {
    // using a different fstore to avoid a clash with the other tests
    final String fstore = "testThatIfaFieldDoesNotExistDefaultValueIsReturned";
    loadFeature("not-existing-field", FieldValueFeature.class.getCanonicalName(), fstore,
            "{\"field\":\"cowabunga\"}");

    loadModel("not-existing-field-model", LinearModel.class.getCanonicalName(),
            new String[] {"not-existing-field"}, fstore, "{\"weights\":{\"not-existing-field\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("id:42");
    query.add("rq", "{!ltr model=not-existing-field-model reRankDocs=4}");
    query.add("fl", "[fv]");
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
    assertJQ("/query" + query.toQueryString(),
            "/response/docs/[0]/=={'[fv]':'"+FeatureLoggerTestUtils.toFeatureVector("not-existing-field",Float.toString(FIELD_VALUE_FEATURE_DEFAULT_VAL))+"'}");

  }

  @Test
  public void testBooleanValue() throws Exception {
    final String fstore = "test_boolean_store";
    loadFeature("trendy", FieldValueFeature.class.getCanonicalName(), fstore,
            "{\"field\":\"isTrendy\"}");

    loadModel("trendy-model", LinearModel.class.getCanonicalName(),
            new String[] {"trendy"}, fstore, "{\"weights\":{\"trendy\":1.0}}");

    SolrQuery query = new SolrQuery();
    query.setQuery("id:4");
    query.add("rq", "{!ltr model=trendy-model reRankDocs=4}");
    query.add("fl", "[fv]");
    assertJQ("/query" + query.toQueryString(),
            "/response/docs/[0]/=={'[fv]':'"+FeatureLoggerTestUtils.toFeatureVector("trendy","0.0")+"'}");


    query = new SolrQuery();
    query.setQuery("id:5");
    query.add("rq", "{!ltr model=trendy-model reRankDocs=4}");
    query.add("fl", "[fv]");
    assertJQ("/query" + query.toQueryString(),
            "/response/docs/[0]/=={'[fv]':'"+FeatureLoggerTestUtils.toFeatureVector("trendy","1.0")+"'}");

    // check default value is false
    query = new SolrQuery();
    query.setQuery("id:2");
    query.add("rq", "{!ltr model=trendy-model reRankDocs=4}");
    query.add("fl", "[fv]");
    assertJQ("/query" + query.toQueryString(),
            "/response/docs/[0]/=={'[fv]':'"+FeatureLoggerTestUtils.toFeatureVector("trendy","0.0")+"'}");

  }


}
