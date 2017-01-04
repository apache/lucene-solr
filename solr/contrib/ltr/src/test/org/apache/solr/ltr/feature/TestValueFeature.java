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
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.model.LinearModel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestValueFeature extends TestRerankBase {

  @BeforeClass
  public static void before() throws Exception {
    setuptest(false);

    assertU(adoc("id", "1", "title", "w1"));
    assertU(adoc("id", "2", "title", "w2"));
    assertU(adoc("id", "3", "title", "w3"));
    assertU(adoc("id", "4", "title", "w4"));
    assertU(adoc("id", "5", "title", "w5"));
    assertU(adoc("id", "6", "title", "w1 w2"));
    assertU(adoc("id", "7", "title", "w1 w2 w3 w4 w5"));
    assertU(adoc("id", "8", "title", "w1 w1 w1 w2 w2"));
    assertU(commit());
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void testValueFeatureWithEmptyValue() throws Exception {
    final RuntimeException expectedException =
        new RuntimeException("mismatch: '0'!='500' @ responseHeader/status");
    try {
        loadFeature("c2", ValueFeature.class.getCanonicalName(), "{\"value\":\"\"}");
        fail("testValueFeatureWithEmptyValue failed to throw exception: "+expectedException);
    } catch (RuntimeException actualException) {
      assertEquals(expectedException.toString(), actualException.toString());
    }
  }

  @Test
  public void testValueFeatureWithWhitespaceValue() throws Exception {
    final RuntimeException expectedException =
        new RuntimeException("mismatch: '0'!='500' @ responseHeader/status");
    try {
        loadFeature("c2", ValueFeature.class.getCanonicalName(),
              "{\"value\":\" \"}");
        fail("testValueFeatureWithWhitespaceValue failed to throw exception: "+expectedException);
    } catch (RuntimeException actualException) {
      assertEquals(expectedException.toString(), actualException.toString());
    }
  }

  @Test
  public void testRerankingWithConstantValueFeatureReplacesDocScore() throws Exception {
    loadFeature("c3", ValueFeature.class.getCanonicalName(), "c3",
        "{\"value\":2}");
    loadModel("m3", LinearModel.class.getCanonicalName(), new String[] {"c3"},
        "c3", "{\"weights\":{\"c3\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("wt", "json");
    query.add("rq", "{!ltr model=m3 reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==2.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==2.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==2.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/score==2.0");
  }

  @Test
  public void testRerankingWithEfiValueFeatureReplacesDocScore() throws Exception {
    loadFeature("c6", ValueFeature.class.getCanonicalName(), "c6",
        "{\"value\":\"${val6}\"}");
    loadModel("m6", LinearModel.class.getCanonicalName(), new String[] {"c6"},
        "c6", "{\"weights\":{\"c6\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("wt", "json");
    query.add("rq", "{!ltr model=m6 reRankDocs=4 efi.val6='2'}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==2.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==2.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==2.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/score==2.0");
  }


  @Test
  public void testValueFeatureImplicitlyNotRequiredShouldReturnOkStatusCode() throws Exception {
    loadFeature("c5", ValueFeature.class.getCanonicalName(), "c5",
        "{\"value\":\"${val6}\"}");
    loadModel("m5", LinearModel.class.getCanonicalName(), new String[] {"c5"},
        "c5", "{\"weights\":{\"c5\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score,fvonly:[fvonly]");
    query.add("rows", "4");
    query.add("wt", "json");
    query.add("rq", "{!ltr model=m5 reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(), "/responseHeader/status==0");
  }

  @Test
  public void testValueFeatureExplictlyNotRequiredShouldReturnOkStatusCode() throws Exception {
    loadFeature("c7", ValueFeature.class.getCanonicalName(), "c7",
        "{\"value\":\"${val7}\",\"required\":false}");
    loadModel("m7", LinearModel.class.getCanonicalName(), new String[] {"c7"},
        "c7", "{\"weights\":{\"c7\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score,fvonly:[fvonly]");
    query.add("rows", "4");
    query.add("wt", "json");
    query.add("rq", "{!ltr model=m7 reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(), "/responseHeader/status==0");
  }

  @Test
  public void testValueFeatureRequiredShouldReturn400StatusCode() throws Exception {
    loadFeature("c8", ValueFeature.class.getCanonicalName(), "c8",
        "{\"value\":\"${val8}\",\"required\":true}");
    loadModel("m8", LinearModel.class.getCanonicalName(), new String[] {"c8"},
        "c8", "{\"weights\":{\"c8\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score,fvonly:[fvonly]");
    query.add("rows", "4");
    query.add("wt", "json");
    query.add("rq", "{!ltr model=m8 reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(), "/responseHeader/status==400");
  }

}
