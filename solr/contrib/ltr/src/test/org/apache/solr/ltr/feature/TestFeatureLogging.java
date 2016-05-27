package org.apache.solr.ltr.feature;

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

import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.impl.FieldValueFeature;
import org.apache.solr.ltr.feature.impl.SolrFeature;
import org.apache.solr.ltr.feature.impl.ValueFeature;
import org.apache.solr.ltr.ranking.LTRComponent;
import org.apache.solr.ltr.ranking.RankSVMModel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressSSL
public class TestFeatureLogging extends TestRerankBase {

  @BeforeClass
  public static void setup() throws Exception {
    setuptest();
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void testGeneratedFeatures() throws Exception {
    loadFeature("c1", ValueFeature.class.getCanonicalName(), "test1",
        "{\"value\":1.0}");
    loadFeature("c2", ValueFeature.class.getCanonicalName(), "test1",
        "{\"value\":2.0}");
    loadFeature("c3", ValueFeature.class.getCanonicalName(), "test1",
        "{\"value\":3.0}");
    loadFeature("pop", FieldValueFeature.class.getCanonicalName(), "test1",
        "{\"field\":\"popularity\"}");
    loadFeature("nomatch", SolrFeature.class.getCanonicalName(), "test1",
        "{\"q\":\"{!terms f=title}foobarbat\"}");
    loadFeature("yesmatch", SolrFeature.class.getCanonicalName(), "test1",
        "{\"q\":\"{!terms f=popularity}2\"}");

    loadModel("sum1", RankSVMModel.class.getCanonicalName(), new String[] {
        "c1", "c2", "c3"}, "test1",
        "{\"weights\":{\"c1\":1.0,\"c2\":1.0,\"c3\":1.0}}");

    SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.add("fl", "title,description,id,popularity,[fv]");
    query.add("rows", "3");
    query.add("debugQuery", "on");
    query.add(LTRComponent.LTRParams.FV, "true");
    query.add("rq", "{!ltr reRankDocs=3 model=sum1}");

    String res = restTestHarness.query("/query" + query.toQueryString());
    System.out.println(res);
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'title':'bloomberg bloomberg ', 'description':'bloomberg','id':'7', 'popularity':2,  '[fv]':'c1:1.0;c2:2.0;c3:3.0;pop:2.0;yesmatch:1.0'}");

    query.remove("fl");
    query.add("fl", "[fv]");
    query.add("rows", "3");
    query.add(LTRComponent.LTRParams.FV, "true");
    query.add("rq", "{!ltr reRankDocs=3 model=sum1}");

    res = restTestHarness.query("/query" + query.toQueryString());
    System.out.println(res);
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/=={'[fv]':'c1:1.0;c2:2.0;c3:3.0;pop:2.0;yesmatch:1.0'}");
  }

  @Test
  public void testGeneratedOnlyFeatures() throws Exception {
    loadFeature("c1", ValueFeature.class.getCanonicalName(), "test3",
        "{\"value\":1.0}");
    loadFeature("c2", ValueFeature.class.getCanonicalName(), "test3",
        "{\"value\":2.0}");
    loadFeature("c3", ValueFeature.class.getCanonicalName(), "test3",
        "{\"value\":3.0}");
    loadFeature("pop", FieldValueFeature.class.getCanonicalName(), "test3",
        "{\"field\":\"popularity\"}");

    loadModel("sumonly", RankSVMModel.class.getCanonicalName(), new String[] {
        "c1", "c2", "c3"}, "test3",
        "{\"weights\":{\"c1\":1.0,\"c2\":1.0,\"c3\":1.0}}");

    SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.add("fl", "title,description,id,popularity,[fv]");
    query.add("rows", "3");
    query.add("debugQuery", "on");
    query.add(LTRComponent.LTRParams.FV, "true");
    query.add("rq", "{!ltr reRankDocs=3 model=sumonly}");

    String res = restTestHarness.query("/query" + query.toQueryString());
    System.out.println(res);
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'title':'bloomberg bloomberg ', 'description':'bloomberg','id':'7', 'popularity':2,  '[fv]':'c1:1.0;c2:2.0;c3:3.0;pop:2.0'}");

    query.remove("fl");
    query.add("fl", "fv:[fv]");
    query.add("rows", "3");

    query.add(LTRComponent.LTRParams.FV, "true");
    query.add("rq", "{!ltr reRankDocs=3 model=sumonly}");

    res = restTestHarness.query("/query" + query.toQueryString());
    System.out.println(res);
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'c1:1.0;c2:2.0;c3:3.0;pop:2.0'}");

  }

  @Test
  public void testGeneratedGroup() throws Exception {
    loadFeature("c1", ValueFeature.class.getCanonicalName(), "testgroup",
        "{\"value\":1.0}");
    loadFeature("c2", ValueFeature.class.getCanonicalName(), "testgroup",
        "{\"value\":2.0}");
    loadFeature("c3", ValueFeature.class.getCanonicalName(), "testgroup",
        "{\"value\":3.0}");
    loadFeature("pop", FieldValueFeature.class.getCanonicalName(), "testgroup",
        "{\"field\":\"popularity\"}");

    loadModel("sumgroup", RankSVMModel.class.getCanonicalName(), new String[] {
        "c1", "c2", "c3"}, "testgroup",
        "{\"weights\":{\"c1\":1.0,\"c2\":1.0,\"c3\":1.0}}");

    SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.add("fl", "*,[fv]");
    query.add("debugQuery", "on");

    query.remove("fl");
    query.add("fl", "fv:[fv]");
    query.add("rows", "3");
    query.add("group", "true");
    query.add("group.field", "title");

    query.add(LTRComponent.LTRParams.FV, "true");
    query.add("rq", "{!ltr reRankDocs=3 model=sumgroup}");

    String res = restTestHarness.query("/query" + query.toQueryString());
    System.out.println(res);
    assertJQ(
        "/query" + query.toQueryString(),
        "/grouped/title/groups/[0]/doclist/docs/[0]/=={'fv':'c1:1.0;c2:2.0;c3:3.0;pop:5.0'}");

    query.add("fvwt", "json");
    res = restTestHarness.query("/query" + query.toQueryString());
    System.out.println(res);
    assertJQ(
        "/query" + query.toQueryString(),
        "/grouped/title/groups/[0]/doclist/docs/[0]/fv/=={'c1':1.0,'c2':2.0,'c3':3.0,'pop':5.0}");
    query.remove("fl");
    query.add("fl", "fv:[fv]");

    assertJQ(
        "/query" + query.toQueryString(),
        "/grouped/title/groups/[0]/doclist/docs/[0]/fv/=={'c3':3.0,'pop':5.0,'c1':1.0,'c2':2.0}");
  }

}
