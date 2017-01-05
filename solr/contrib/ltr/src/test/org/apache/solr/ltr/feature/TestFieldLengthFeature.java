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

public class TestFieldLengthFeature extends TestRerankBase {

  @BeforeClass
  public static void before() throws Exception {
    setuptest(false);

    assertU(adoc("id", "1", "title", "w1", "description", "w1"));
    assertU(adoc("id", "2", "title", "w2 2asd asdd didid", "description",
        "w2 2asd asdd didid"));
    assertU(adoc("id", "3", "title", "w3", "description", "w3"));
    assertU(adoc("id", "4", "title", "w4", "description", "w4"));
    assertU(adoc("id", "5", "title", "w5", "description", "w5"));
    assertU(adoc("id", "6", "title", "w1 w2", "description", "w1 w2"));
    assertU(adoc("id", "7", "title", "w1 w2 w3 w4 w5", "description",
        "w1 w2 w3 w4 w5 w8"));
    assertU(adoc("id", "8", "title", "w1 w1 w1 w2 w2 w8", "description",
        "w1 w1 w1 w2 w2"));
    assertU(commit());
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void testIfFieldIsMissingInDocumentLengthIsZero() throws Exception {
    // add a document without the field 'description'
    assertU(adoc("id", "42", "title", "w10"));
    assertU(commit());

    loadFeature("description-length2", FieldLengthFeature.class.getCanonicalName(),
            "{\"field\":\"description\"}");

    loadModel("description-model2", LinearModel.class.getCanonicalName(),
            new String[] {"description-length2"}, "{\"weights\":{\"description-length2\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w10");
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("rq", "{!ltr model=description-model2 reRankDocs=8}");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==0.0");
  }


  @Test
  public void testIfFieldIsEmptyLengthIsZero() throws Exception {
    // add a document without the field 'description'
    assertU(adoc("id", "43", "title", "w11", "description", ""));
    assertU(commit());

    loadFeature("description-length3", FieldLengthFeature.class.getCanonicalName(),
            "{\"field\":\"description\"}");

    loadModel("description-model3", LinearModel.class.getCanonicalName(),
            new String[] {"description-length3"}, "{\"weights\":{\"description-length3\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w11");
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("rq", "{!ltr model=description-model3 reRankDocs=8}");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==0.0");
  }


  @Test
  public void testRanking() throws Exception {
    loadFeature("title-length", FieldLengthFeature.class.getCanonicalName(),
        "{\"field\":\"title\"}");

    loadModel("title-model", LinearModel.class.getCanonicalName(),
        new String[] {"title-length"}, "{\"weights\":{\"title-length\":1.0}}");

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
    // Normal term match

    query.add("rq", "{!ltr model=title-model reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='1'");

    query.setQuery("*:*");
    query.remove("rows");
    query.add("rows", "8");
    query.remove("rq");
    query.add("rq", "{!ltr model=title-model reRankDocs=8}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='2'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='6'");

    loadFeature("description-length",
        FieldLengthFeature.class.getCanonicalName(),
        "{\"field\":\"description\"}");
    loadModel("description-model", LinearModel.class.getCanonicalName(),
        new String[] {"description-length"},
        "{\"weights\":{\"description-length\":1.0}}");
    query.setQuery("title:w1");
    query.remove("rq");
    query.remove("rows");
    query.add("rows", "4");
    query.add("rq", "{!ltr model=description-model reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='1'");
  }





}
