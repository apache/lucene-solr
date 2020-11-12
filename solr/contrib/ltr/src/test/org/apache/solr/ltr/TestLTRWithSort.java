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

package org.apache.solr.ltr;

import java.util.Random;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.feature.SolrFeature;
import org.apache.solr.ltr.interleaving.algorithms.TeamDraftInterleaving;
import org.apache.solr.ltr.model.LinearModel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLTRWithSort extends TestRerankBase {

  @Before
  public void before() throws Exception {
    setuptest(false);
    assertU(adoc("id", "1", "title", "a1", "description", "E", "popularity",
        "1"));
    assertU(adoc("id", "2", "title", "a1 b1", "description",
        "B", "popularity", "2"));
    assertU(adoc("id", "3", "title", "a1 b1 c1", "description", "B", "popularity",
        "3"));
    assertU(adoc("id", "4", "title", "a1 b1 c1 d1", "description", "B", "popularity",
        "4"));
    assertU(adoc("id", "5", "title", "a1 b1 c1 d1 e1", "description", "E", "popularity",
        "5"));
    assertU(adoc("id", "6", "title", "a1 b1 c1 d1 e1 f1", "description", "B",
        "popularity", "6"));
    assertU(adoc("id", "7", "title", "a1 b1 c1 d1 e1 f1 g1", "description",
        "C", "popularity", "7"));
    assertU(adoc("id", "8", "title", "a1 b1 c1 d1 e1 f1 g1 h1", "description",
        "D", "popularity", "8"));
    assertU(commit());
  }
  
  @After
  public void after() throws Exception {
    aftertest();
  }

  @Test
  public void testRankingSolrSort() throws Exception {
    // before();
    loadFeature("powpularityS", SolrFeature.class.getName(),
        "{\"q\":\"{!func}pow(popularity,2)\"}");

    loadModel("powpularityS-model", LinearModel.class.getName(),
        new String[] {"powpularityS"}, "{\"weights\":{\"powpularityS\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:a1");
    query.add("fl", "*, score");
    query.add("rows", "4");

    // Normal term match
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==8");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='2'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='4'");

    //Add sort
    query.add("sort", "description desc");
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==8");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='5'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='7'");

    query.add("rq", "{!ltr model=powpularityS-model reRankDocs=4}");
    query.set("debugQuery", "on");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==8");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==64.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==49.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='5'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==25.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/score==1.0");

  }

  @Test
  public void interleavingTwoModelsWithSort_shouldInterleave() throws Exception {
    TeamDraftInterleaving.setRANDOM(new Random(10));//Random Boolean Choices Generation from Seed: [1,0]

    loadFeature("featureA", SolrFeature.class.getName(),
        "{\"q\":\"{!func}pow(popularity,2)\"}");

    loadFeature("featureB", SolrFeature.class.getName(),
        "{\"q\":\"{!func}pow(popularity,-2)\"}");

    loadModel("modelA", LinearModel.class.getName(),
        new String[] {"featureA"}, "{\"weights\":{\"featureA\":1.0}}");

    loadModel("modelB", LinearModel.class.getName(),
        new String[] {"featureB"}, "{\"weights\":{\"featureB\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:a1");
    query.add("rows", "10");
    query.add("rq", "{!ltr reRankDocs=4 model=modelA model=modelB}");
    query.add("fl", "*,score");
    query.add("sort", "description desc");

    /*
    Doc1 = "popularity=1", ScoreA(1) ScoreB(1)
    Doc5 = "popularity=5", ScoreA(25) ScoreB(0.04)
    Doc7 = "popularity=7", ScoreA(49) ScoreB(0.02)
    Doc8 = "popularity=8", ScoreA(64) ScoreB(0.01)
    
    ModelARerankedList = [8,7,5,1]
    ModelBRerankedList = [1,5,7,8]
    
    OriginalRanking = [1,5,8,7]

    Random Boolean Choices Generation from Seed: [1,0]
    */

    int[] expectedInterleaved = new int[]{1, 8, 7, 5};

    String[] tests = new String[5];
    tests[0] = "/response/numFound/==8";
    for (int i = 1; i <= 4; i++) {
      tests[i] = "/response/docs/[" + (i - 1) + "]/id==\"" + expectedInterleaved[(i - 1)] + "\"";
    }
    assertJQ("/query" + query.toQueryString(), tests);

  }

  @Test
  public void interleavingModelsWithOriginalRankingSort_shouldInterleave() throws Exception {

    loadFeature("powpularityS", SolrFeature.class.getName(),
        "{\"q\":\"{!func}pow(popularity,2)\"}");

    loadModel("powpularityS-model", LinearModel.class.getName(),
        new String[] {"powpularityS"}, "{\"weights\":{\"powpularityS\":1.0}}");

    for (boolean originalRankingLast : new boolean[] { true, false }) {
      TeamDraftInterleaving.setRANDOM(new Random(10));//Random Boolean Choices Generation from Seed: [1,0]

      final SolrQuery query = new SolrQuery();
      query.setQuery("title:a1");
      query.add("rows", "10");
      if (originalRankingLast) {
        query.add("rq", "{!ltr reRankDocs=4 model=powpularityS-model model=_OriginalRanking_}");
      } else {
        query.add("rq", "{!ltr reRankDocs=4 model=_OriginalRanking_ model=powpularityS-model}");
      }
      query.add("fl", "*,score");
      query.add("sort", "description desc");

      /*
    Doc1 = "popularity=1", ScorePowpularityS(1)
    Doc5 = "popularity=5", ScorePowpularityS(25)
    Doc7 = "popularity=7", ScorePowpularityS(49)
    Doc8 = "popularity=8", ScorePowpularityS(64)

    PowpularitySRerankedList = [8,7,5,1]
    OriginalRanking = [1,5,8,7]

    Random Boolean Choices Generation from Seed: [1,0]
       */

      final int[] expectedInterleaved;
      if (originalRankingLast) {
        expectedInterleaved = new int[]{1, 8, 7, 5};
      } else {
        expectedInterleaved = new int[]{8, 1, 5, 7};
      }

      String[] tests = new String[5];
      tests[0] = "/response/numFound/==8";
      for (int i = 1; i <= 4; i++) {
        tests[i] = "/response/docs/[" + (i - 1) + "]/id==\"" + expectedInterleaved[(i - 1)] + "\"";
      }
      assertJQ("/query" + query.toQueryString(), tests);
    }

  }

}
