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
package org.apache.solr.ltr.response.transform;

import java.util.Random;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.SolrFeature;
import org.apache.solr.ltr.interleaving.algorithms.TeamDraftInterleaving;
import org.apache.solr.ltr.model.LinearModel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFeatureLoggerTransformer extends TestRerankBase {
  @Before
  public void before() throws Exception {
    setuptest(false);

    assertU(adoc("id", "1", "title", "w1", "description", "w5", "popularity",
        "1"));
    assertU(adoc("id", "2", "title", "w2 2asd asdd didid", "description",
        "w2 2asd asdd didid", "popularity", "2"));
    assertU(adoc("id", "3", "title", "w1", "description", "w5", "popularity",
        "3"));
    assertU(adoc("id", "4", "title", "w1", "description", "w1", "popularity",
        "6"));
    assertU(adoc("id", "5", "title", "w5", "description", "w5", "popularity",
        "5"));
    assertU(adoc("id", "6", "title", "w6 w2", "description", "w1 w2",
        "popularity", "6"));
    assertU(adoc("id", "7", "title", "w1 w2 w3 w4 w5", "description",
        "w6 w2 w3 w4 w5 w8", "popularity", "88888"));
    assertU(adoc("id", "8", "title", "w1 w1 w1 w2 w2 w8", "description",
        "w1 w1 w1 w2 w2 w5", "popularity", "88888"));
    assertU(commit());


  }

  @After
  public void after() throws Exception {
    aftertest();
  }

  protected void loadFeaturesAndModels() throws Exception {
    loadFeature("featureA1", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=popularity}88888\"]}");
    loadFeature("featureA2", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=title}${user_query}\"]}");
    loadFeature("featureAB", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=title}${user_query}\"]}");
    loadFeature("featureB1", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=popularity}6\"]}");
    loadFeature("featureB2", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=description}${user_query}\"]}");
    loadFeature("featureC1", SolrFeature.class.getName(),"featureStore2",
        "{\"fq\":[\"{!terms f=popularity}6\"]}");
    loadFeature("featureC2", SolrFeature.class.getName(),"featureStore2",
        "{\"fq\":[\"{!terms f=description}${user_query}\"]}");
    loadFeature("featureC3", SolrFeature.class.getName(),"featureStore2",
        "{\"fq\":[\"{!terms f=description}${user_query}\"]}");

    loadModel("modelA", LinearModel.class.getName(),
        new String[]{"featureA1", "featureA2", "featureAB"},
        "{\"weights\":{\"featureA1\":3.0, \"featureA2\":9.0, \"featureAB\":27.0}}");

    loadModel("modelB", LinearModel.class.getName(),
        new String[]{"featureB1", "featureB2", "featureAB"},
        "{\"weights\":{\"featureB1\":2.0, \"featureB2\":4.0, \"featureAB\":8.0}}");
    
    loadModel("modelC", LinearModel.class.getName(),
        new String[]{"featureC1", "featureC2"},"featureStore2",
        "{\"weights\":{\"featureC1\":5.0, \"featureC2\":25.0}}");
  }
  
  @Test
  public void interleaving_featureTransformer_shouldWorkInSparseFormat() throws Exception {
    TeamDraftInterleaving.setRANDOM(new Random(10101010));//Random Boolean Choices Generation from Seed: [0,1,1]
    loadFeaturesAndModels();

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*, score,features:[fv format=sparse]");
    query.add("rows", "10");
    query.add("debugQuery", "true");
    query.add("fq", "{!terms f=title}w1"); // 1,3,4,7,8
    query.add("rq",
        "{!ltr model=modelA model=modelB reRankDocs=10 efi.user_query='w5'}");

    /*
    Doc1 = "featureB2=1.0", ScoreA(0), ScoreB(4)
    Doc3 = "featureB2=1.0", ScoreA(0), ScoreB(4)
    Doc4 = "featureB1=1.0", ScoreA(0), ScoreB(2)
    Doc7 ="featureA1=1.0,featureA2=1.0,featureAB=1.0,featureB2=1.0", ScoreA(39), ScoreB(12)
    Doc8 = "featureA1=1.0,featureB2=1.0", ScoreA(3), ScoreB(4)
    ModelARerankedList = [7,8,1,3,4]
    ModelBRerankedList = [7,1,3,8,4]
   
    Random Boolean Choices Generation from Seed: [0,1,1]
    */
    String[] expectedFeatureVectors = new String[]{"featureA1\\=1.0\\,featureA2\\=1.0\\,featureAB\\=1.0\\,featureB2\\=1.0", "featureB2\\=1.0", "featureB2\\=1.0", "featureA1\\=1.0\\,featureB2\\=1.0", "featureB1\\=1.0"};
    int[] expectedInterleaved = new int[]{7, 1, 3, 8, 4};

    String[] tests = new String[11];
    tests[0] = "/response/numFound/==5";
    for (int i = 1; i <= 5; i++) {
      tests[i] = "/response/docs/[" + (i - 1) + "]/id==\"" + expectedInterleaved[(i - 1)] + "\"";
      tests[i + 5] = "/response/docs/[" + (i - 1) + "]/features==" + expectedFeatureVectors[(i - 1)];
    }
    assertJQ("/query" + query.toQueryString(), tests);
  }

  @Test
  public void interleaving_featureTransformer_shouldWorkInDenseFormat() throws Exception {
    TeamDraftInterleaving.setRANDOM(new Random(10101010));//Random Boolean Choices Generation from Seed: [0,1,1]
    loadFeaturesAndModels();

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*, score,features:[fv format=dense]");
    query.add("rows", "10");
    query.add("debugQuery", "true");
    query.add("fq", "{!terms f=title}w1"); // 1,3,4,7,8
    query.add("rq",
        "{!ltr model=modelA model=modelB reRankDocs=10 efi.user_query='w5'}");

    /*
    Doc1 = "featureB2=1.0", ScoreA(0), ScoreB(4)
    Doc3 = "featureB2=1.0", ScoreA(0), ScoreB(4)
    Doc4 = "featureB1=1.0", ScoreA(0), ScoreB(2)
    Doc7 ="featureA1=1.0,featureA2=1.0,featureAB=1.0,featureB2=1.0", ScoreA(39), ScoreB(12)
    Doc8 = "featureA1=1.0,featureB2=1.0", ScoreA(3), ScoreB(4)
    ModelARerankedList = [7,8,1,3,4]
    ModelBRerankedList = [7,1,3,8,4]
   
    Random Boolean Choices Generation from Seed: [0,1,1]
    */
    String[] expectedFeatureVectors = new String[]{"featureA1\\=1.0\\,featureA2\\=1.0\\,featureAB\\=1.0\\,featureB1\\=0.0\\,featureB2\\=1.0",
        "featureA1\\=0.0\\,featureA2\\=0.0\\,featureAB\\=0.0\\,featureB1\\=0.0\\,featureB2\\=1.0",
        "featureA1\\=0.0\\,featureA2\\=0.0\\,featureAB\\=0.0\\,featureB1\\=0.0\\,featureB2\\=1.0",
        "featureA1\\=1.0\\,featureA2\\=0.0\\,featureAB\\=0.0\\,featureB1\\=0.0\\,featureB2\\=1.0",
        "featureA1\\=0.0\\,featureA2\\=0.0\\,featureAB\\=0.0\\,featureB1\\=1.0\\,featureB2\\=0.0"};
    int[] expectedInterleaved = new int[]{7, 1, 3, 8, 4};

    String[] tests = new String[11];
    tests[0] = "/response/numFound/==5";
    for (int i = 1; i <= 5; i++) {
      tests[i] = "/response/docs/[" + (i - 1) + "]/id==\"" + expectedInterleaved[(i - 1)] + "\"";
      tests[i + 5] = "/response/docs/[" + (i - 1) + "]/features==" + expectedFeatureVectors[(i - 1)];
    }
    assertJQ("/query" + query.toQueryString(), tests);
  }

  @Test
  public void interleaving_explicitNewFeatureStore_shouldExtractAllFeaturesFromNewStore() throws Exception {
    TeamDraftInterleaving.setRANDOM(new Random(10101010));//Random Boolean Choices Generation from Seed: [0,1,1]
    loadFeaturesAndModels();

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*, score,features:[fv store=featureStore2 efi.user_query='w5' format=dense]");
    query.add("rows", "10");
    query.add("debugQuery", "true");
    query.add("fq", "{!terms f=title}w1"); // 1,3,4,7,8
    query.add("rq",
        "{!ltr model=modelA model=modelB reRankDocs=10 efi.user_query='w5'}");

    /*
    Doc1 = "featureB2=1.0", ScoreA(0), ScoreB(4)
    Doc3 = "featureB2=1.0", ScoreA(0), ScoreB(4)
    Doc4 = "featureB1=1.0", ScoreA(0), ScoreB(2)
    Doc7 ="featureA1=1.0,featureA2=1.0,featureAB=1.0,featureB2=1.0", ScoreA(39), ScoreB(12)
    Doc8 = "featureA1=1.0,featureB2=1.0", ScoreA(3), ScoreB(4)
    ModelARerankedList = [7,8,1,3,4]
    ModelBRerankedList = [7,1,3,8,4]
   
    Random Boolean Choices Generation from Seed: [0,1,1]
    */
    String[] expectedFeatureVectors = new String[]{
        "featureC1\\=0.0\\,featureC2\\=1.0\\,featureC3\\=1.0",
        "featureC1\\=0.0\\,featureC2\\=1.0\\,featureC3\\=1.0",
        "featureC1\\=0.0\\,featureC2\\=1.0\\,featureC3\\=1.0",
        "featureC1\\=0.0\\,featureC2\\=1.0\\,featureC3\\=1.0",
        "featureC1\\=1.0\\,featureC2\\=0.0\\,featureC3\\=0.0"};
    int[] expectedInterleaved = new int[]{7, 1, 3, 8, 4};

    String[] tests = new String[16];
    tests[0] = "/response/numFound/==5";
    for (int i = 1; i <= 5; i++) {
      tests[i] = "/response/docs/[" + (i - 1) + "]/id==\"" + expectedInterleaved[(i - 1)] + "\"";
      tests[i + 10] = "/response/docs/[" + (i - 1) + "]/features==" + expectedFeatureVectors[(i - 1)];
    }
    assertJQ("/query" + query.toQueryString(), tests);
  }
  
  @Test
  public void interleaving_withOriginalRankingAndExplicitFeatureStore_shouldReturnNewCalculatedFeatureVector() throws Exception {
    TeamDraftInterleaving.setRANDOM(new Random(10101010));//Random Boolean Choices Generation from Seed: [0,1,1]
    loadFeaturesAndModels();

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*, score,features:[fv store=featureStore2 efi.user_query='w5' format=sparse]");
    query.add("rows", "10");
    query.add("debugQuery", "true");
    query.add("fq", "{!terms f=title}w1"); // 1,3,4,7,8
    query.add("rq",
        "{!ltr model=modelA model=_OriginalRanking_ reRankDocs=10 efi.user_query='w5'}");

    /*
    Doc1 = "featureB2=1.0", ScoreA(0)
    Doc3 = "featureB2=1.0", ScoreA(0)
    Doc4 = "featureB1=1.0", ScoreA(0)
    Doc7 ="featureA1=1.0,featureA2=1.0,featureAB=1.0,featureB2=1.0", ScoreA(39)
    Doc8 = "featureA1=1.0,featureB2=1.0", ScoreA(3)
    
    ModelARerankedList = [7,8,1,3,4]
    _OriginalRanking_ = [1,3,4,7,8]

   
    Random Boolean Choices Generation from Seed: [0,1,1]
    */
    String[] expectedFeatureVectors = new String[]{
        "featureC2\\=1.0\\,featureC3\\=1.0",
        "featureC2\\=1.0\\,featureC3\\=1.0",
        "featureC2\\=1.0\\,featureC3\\=1.0",
        "featureC2\\=1.0\\,featureC3\\=1.0",
        "featureC1\\=1.0"};
    int[] expectedInterleaved = new int[]{7, 1, 3, 8, 4};

    String[] tests = new String[16];
    tests[0] = "/response/numFound/==5";
    for (int i = 1; i <= 5; i++) {
      tests[i] = "/response/docs/[" + (i - 1) + "]/id==\"" + expectedInterleaved[(i - 1)] + "\"";
      if (expectedFeatureVectors[(i - 1)] != null) {
        tests[i + 10] = "/response/docs/[" + (i - 1) + "]/features==" + expectedFeatureVectors[(i - 1)];
      }
    }
    assertJQ("/query" + query.toQueryString(), tests);

    int[] nullFeatureVectorIndexes = new int[]{1, 2, 4};
    for (int index : nullFeatureVectorIndexes) {
      TeamDraftInterleaving.setRANDOM(new Random(10101010));
      String[] nullFeatureVectorTests = new String[1];
      try {
        nullFeatureVectorTests[0] = "/response/docs/[" + index + "]/features==";
        assertJQ("/query" + query.toQueryString(), nullFeatureVectorTests);
      } catch (Exception e) {
        assertEquals("Path not found: /response/docs/[" + index + "]/features", e.getMessage());
        continue;
      }

    }

  }

  @Test
  public void interleaving_modelsFromDifferentFeatureStores_shouldLogFeaturesCorrectly() throws Exception {
    TeamDraftInterleaving.setRANDOM(new Random(10101010));//Random Boolean Choices Generation from Seed: [0,1,1]
    loadFeaturesAndModels();

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*, score,features:[fv format=sparse]");
    query.add("rows", "10");
    query.add("debugQuery", "true");
    query.add("fq", "{!terms f=title}w1"); // 1,3,4,7,8
    query.add("rq",
        "{!ltr model=modelA model=modelC reRankDocs=10 efi.user_query='w5'}");

    /*
    Doc1 = "featureC2=1.0", ScoreA(0), ScoreC(25)
    Doc3 = "featureC2=1.0", ScoreA(0), ScoreC(25)
    Doc4 = "featureC1=1.0", ScoreA(0), ScoreC(5)
    Doc7 ="featureA1=1.0,featureA2=1.0,featureAB=1.0", ScoreA(39), ScoreC(0)
    Doc8 = "featureA1=1.0,featureC2=1.0", ScoreA(3), ScoreC(25)
    ModelARerankedList = [7,8,1,3,4]
    ModelCRerankedList = [1,3,8,4,7]
   
    Random Boolean Choices Generation from Seed: [0,1,1]
    */
    String[] expectedFeatureVectors = new String[]{
        "featureA1\\=1.0\\,featureA2\\=1.0\\,featureAB\\=1.0\\,featureB2\\=1.0",
        "featureC2\\=1.0\\,featureC3\\=1.0",
        "featureC2\\=1.0\\,featureC3\\=1.0",
        "featureA1\\=1.0\\,featureB2\\=1.0",
        "featureC1\\=1.0"};
    int[] expectedInterleaved = new int[]{7, 1, 3, 8, 4};

    String[] tests = new String[16];
    tests[0] = "/response/numFound/==5";
    for (int i = 1; i <= 5; i++) {
      tests[i] = "/response/docs/[" + (i - 1) + "]/id==\"" + expectedInterleaved[(i - 1)] + "\"";
      tests[i + 10] = "/response/docs/[" + (i - 1) + "]/features==" + expectedFeatureVectors[(i - 1)];
    }
    assertJQ("/query" + query.toQueryString(), tests);
  }

  @Test
  public void interleaving_featureLoggerFromNewFeatureStoreWithDifferentEfi_shouldReturnNewCalculatedFeatureVector() throws Exception {
    TeamDraftInterleaving.setRANDOM(new Random(10101010));//Random Boolean Choices Generation from Seed: [0,1,1]
    loadFeaturesAndModels();

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*, score,features:[fv store=featureStore2 efi.user_query='w8' format=sparse]");
    query.add("rows", "10");
    query.add("debugQuery", "true");
    query.add("fq", "{!terms f=title}w1"); // 1,3,4,7,8
    query.add("rq",
        "{!ltr model=modelA model=modelB reRankDocs=10 efi.user_query='w5'}");

    /*
    Doc1 = "featureB2=1.0", ScoreA(0), ScoreB(4)
    Doc3 = "featureB2=1.0", ScoreA(0), ScoreB(4)
    Doc4 = "featureB1=1.0", ScoreA(0), ScoreB(2)
    Doc7 ="featureA1=1.0,featureA2=1.0,featureAB=1.0,featureB2=1.0", ScoreA(39), ScoreB(12)
    Doc8 = "featureA1=1.0,featureB2=1.0", ScoreA(3), ScoreB(4)
    ModelARerankedList = [7,8,1,3,4]
    ModelBRerankedList = [7,1,3,8,4]
   
    Random Boolean Choices Generation from Seed: [0,1,1]
    */
    String[] expectedFeatureVectors = new String[]{
        "featureC2\\=1.0\\,featureC3\\=1.0",
        "",
        "",
        "",
        ""};
    int[] expectedInterleaved = new int[]{7, 1, 3, 8, 4};

    String[] tests = new String[16];
    tests[0] = "/response/numFound/==5";
    for (int i = 1; i <= 5; i++) {
      tests[i] = "/response/docs/[" + (i - 1) + "]/id==\"" + expectedInterleaved[(i - 1)] + "\"";
      tests[i + 10] = "/response/docs/[" + (i - 1) + "]/features==" + expectedFeatureVectors[(i - 1)];
    }
    assertJQ("/query" + query.toQueryString(), tests);
  }

  @Test
  public void interleaving_explicitFeatureStoreReusableFromModel_shouldLogFeaturesCorrectly() throws Exception {
    TeamDraftInterleaving.setRANDOM(new Random(10101010));//Random Boolean Choices Generation from Seed: [0,1,1]
    loadFeaturesAndModels();

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*, score,features:[fv store=featureStore2 format=sparse]");
    query.add("rows", "10");
    query.add("debugQuery", "true");
    query.add("fq", "{!terms f=title}w1"); // 1,3,4,7,8
    query.add("rq",
        "{!ltr model=modelA model=modelC reRankDocs=10 efi.user_query='w5'}");

    /*
    Doc1 = "featureC2=1.0", ScoreA(0), ScoreC(25)
    Doc3 = "featureC2=1.0", ScoreA(0), ScoreC(25)
    Doc4 = "featureC1=1.0", ScoreA(0), ScoreC(5)
    Doc7 ="featureA1=1.0,featureA2=1.0,featureAB=1.0", ScoreA(39), ScoreC(0)
    Doc8 = "featureA1=1.0,featureC2=1.0", ScoreA(3), ScoreC(25)
    ModelARerankedList = [7,8,1,3,4]
    ModelCRerankedList = [1,3,8,4,7]
   
    Random Boolean Choices Generation from Seed: [0,1,1]
    */
    String[] expectedFeatureVectors = new String[]{
        "featureC2\\=1.0\\,featureC3\\=1.0",
        "featureC2\\=1.0\\,featureC3\\=1.0",
        "featureC2\\=1.0\\,featureC3\\=1.0",
        "featureC2\\=1.0\\,featureC3\\=1.0",
        "featureC1\\=1.0"};
    int[] expectedInterleaved = new int[]{7, 1, 3, 8, 4};

    String[] tests = new String[16];
    tests[0] = "/response/numFound/==5";
    for (int i = 1; i <= 5; i++) {
      tests[i] = "/response/docs/[" + (i - 1) + "]/id==\"" + expectedInterleaved[(i - 1)] + "\"";
      tests[i + 10] = "/response/docs/[" + (i - 1) + "]/features==" + expectedFeatureVectors[(i - 1)];
    }
    assertJQ("/query" + query.toQueryString(), tests);
  }
  

}
