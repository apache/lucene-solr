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

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestJoinQueryPerformance extends SolrTestCaseJ4 {
  // Dictionary used to load String data
  private static final String DICT_PATH = "/usr/share/dict/words";
  private static final int NUM_DICT_TERMS = 235886;
  private static final String[] LOADED_DICTIONARY = new String[NUM_DICT_TERMS];

  // Performance run parameters: Indexing
  private static final String FROM_COLLECTION_NAME = "user_acls";
  private static final int NUM_FROM_DOCS = 5050; // 1 + 2 + 3 + 4 + ...  + 100
  private static final String TO_COLLECTION_NAME = "products";
  private static final int NUM_TO_DOCS = 500000;
  private static final int PERMISSION_CARDINALITY = 50000; // 50K unique groups/roles/whatever
  private static int BATCH_SIZE = 500;
  private static int NUM_COMMITS = 500;
  private static final int VAL_MAX = 1000;
  private static final int USER_MAX = 100;

  private static String COLLECTION_NAME= "foo";

  /*
   * As I start out here, I think I'll want a few different axes.
   *  - "from" collection matches (with "to" matches held constant)
   *  - "to" collection matches (with "from" matches held constant)
   *
   * So I think I should index a finite number of docs
   */

  @BeforeClass
  public static void setUpCluster() throws Exception {
    loadDictionary();
    //loadCollectionData(DType.USER);
    //loadCollectionData(DType.DATA);
  }

  private static void loadDictionary() throws Exception {
    try (BufferedReader reader = new BufferedReader(new FileReader(DICT_PATH))) {
      for (int i = 0; i < NUM_DICT_TERMS; i++) {
        LOADED_DICTIONARY[i] = reader.readLine();
      }
    }
  }

  public enum DType {
    USER(NUM_FROM_DOCS, FROM_COLLECTION_NAME) {
      // id - unique string
      // userid_s - username (user# from 1-100)...each user appears in # entries
      // permissions_ss - set of 300 string permissions (cardinality 50K)
      @Override
      SolrInputDocument buildDoc() {
        if (userRecordCounts[currentUser - 1] == currentUser) {
          currentUser++;
        } else {
          userRecordCounts[currentUser -1]++;
        }

        final SolrInputDocument newDoc = new SolrInputDocument("id", UUID.randomUUID().toString());
        final String userString = "user" + currentUser;
        final String[] permissions = getAFewDictionaryWords(300, PERMISSION_CARDINALITY);

        newDoc.addField("userid_s", userString);
        newDoc.addField("permissions_ss", permissions);

        return newDoc;
      }
    },
    DATA(NUM_TO_DOCS, TO_COLLECTION_NAME) {
      // id - unique string
      // val_i - random int between 1-1000
      // cost_d - random cost between 1-1000
      // body_txt - random text string between 100 - 10000 words
      // acl_ss - set of 100-3000 string permissions (cardinality 50K)
      @Override
      SolrInputDocument buildDoc() {
        final SolrInputDocument newDoc = new SolrInputDocument("id", UUID.randomUUID().toString());
        final int val = random().nextInt(1000) + 1;
        final double cost = random().nextDouble() * 1000d;
        final String body = String.join(" ", getAFewDictionaryWords(random().nextInt(9900) + 100));
        final String[] acls = getAFewDictionaryWords(random().nextInt(2900) + 100, PERMISSION_CARDINALITY);

        newDoc.addField("val_i", val);
        newDoc.addField("cost_d", cost);
        newDoc.addField("body_txt", body);
        newDoc.addField("acl_ss", acls);

        return newDoc;
      }
    };

    private int numDocs;
    private String collName;
    private static int[] userRecordCounts = new int[100];
    private static int currentUser = 1;

    private DType(int numDocs, String collectionName) {
      this.numDocs = numDocs;
      this.collName = collectionName;
    }

    abstract SolrInputDocument buildDoc();
  }

  private static void loadCollectionData(DType type) throws Exception {
    int numDocs = type.numDocs;
    String collectionName = type.collName;
    int numLoaded = 0;
    try (HttpSolrClient client = new HttpSolrClient.Builder("http://localhost:8983/solr").build()) {
      final int numBatches = numDocs / BATCH_SIZE + 1;
      final int commitEveryBatches = NUM_COMMITS > 0 ? numBatches / NUM_COMMITS : Integer.MAX_VALUE;
      int batchCount = 0;
      while (numLoaded < numDocs) {
        final int sizeOfBatch = numLoaded + BATCH_SIZE > numDocs ? numDocs - numLoaded : BATCH_SIZE;
        final Collection<SolrInputDocument> batch = buildBatch(type, sizeOfBatch);
        client.add(collectionName, batch);
        batchCount++;
        numLoaded+=sizeOfBatch;

        if (batchCount == commitEveryBatches) {
          client.commit(collectionName);
          batchCount = 0;
        }
      }
      client.commit(collectionName);
    }

  }

  private static Collection<SolrInputDocument> buildBatch(DType type, int sizeOfBatch) {
    final List<SolrInputDocument> batch = Lists.newArrayList();
    for (int i = 0; i < sizeOfBatch; i++) {
      batch.add(type.buildDoc());
    }
    return batch;
  }

  private static String[] getAFewDictionaryWords(int numWords) {
    return getAFewDictionaryWords(numWords, NUM_DICT_TERMS);
  }

  private static String[] getAFewDictionaryWords(int numWords, int onlyFirstN) {
    final String[] words = new String[numWords];
    for (int i = 0; i < numWords; i++) {
      words[i] = LOADED_DICTIONARY[random().nextInt(onlyFirstN)];
    }

    return words;
  }


  @Test
  public void testJoinPerformanceAsMainQueryHitsIncrease() throws Exception {
    final String joinQueryBase = "{!join fromIndex=" + FROM_COLLECTION_NAME + " from=permissions_ss to=acl_ss cache=false";
    final String fromQuery = "userid_s:user25"; // The higher the user number, the more permissions he has attached to his name (1-100)
    final String standardJoin = joinQueryBase + "}" + fromQuery;
    final String noScoreJoin = joinQueryBase + " score=none}" + fromQuery;
    final String tpiJoin = joinQueryBase + " toplevel=true}" + fromQuery;

    try (HttpSolrClient client = new HttpSolrClient.Builder("http://localhost:8983/solr").build()) {
      for ( int i = 0; i < VAL_MAX; i+=20) {
        final String mainQuery = "val_i:[1 TO " + (i+1) + "]";
        final QueryResponse standardJoinRsp = client.query(TO_COLLECTION_NAME, new SolrQuery("q", mainQuery, "fq", standardJoin), SolrRequest.METHOD.POST);
        final QueryResponse tpiJoinRsp = client.query(TO_COLLECTION_NAME, new SolrQuery("q", mainQuery, "fq", tpiJoin), SolrRequest.METHOD.POST);
        final QueryResponse noScoreJoinRsp = client.query(TO_COLLECTION_NAME, new SolrQuery("q", mainQuery, "fq", noScoreJoin), SolrRequest.METHOD.POST);
        final long numFound = tpiJoinRsp.getResults().getNumFound();

        System.out.println(i + "," + numFound + "," + standardJoinRsp.getQTime() + "," + noScoreJoinRsp.getQTime() + "," + tpiJoinRsp.getQTime());
      }
    }
  }

  @Test
  public void testJoinPerformanceAsFromQueryHitsIncrease() throws Exception {
    final String mainQuery = "val_i:[1 TO 250]"; // Half the docs match the query (250K)

    final String joinQueryBase = "{!join fromIndex=" + FROM_COLLECTION_NAME + " from=permissions_ss to=acl_ss cache=false";


    try (HttpSolrClient client = new HttpSolrClient.Builder("http://localhost:8983/solr").build()) {
      for ( int i = 1; i <= USER_MAX; i++) {
        final String fromQuery = "userid_s:user" + i;
        final String standardJoin = joinQueryBase + "}" + fromQuery;
        final String noScoreJoin = joinQueryBase + " score=none}" + fromQuery;
        final String tpiJoin = joinQueryBase + " toplevel=true}" + fromQuery;

        final QueryResponse standardJoinRsp = client.query(TO_COLLECTION_NAME, new SolrQuery("q", mainQuery, "fq", standardJoin), SolrRequest.METHOD.POST);
        final QueryResponse tpiJoinRsp = client.query(TO_COLLECTION_NAME, new SolrQuery("q", mainQuery, "fq", tpiJoin), SolrRequest.METHOD.POST);
        final QueryResponse noScoreJoinRsp = client.query(TO_COLLECTION_NAME, new SolrQuery("q", mainQuery, "fq", noScoreJoin), SolrRequest.METHOD.POST);
        final long numFound = tpiJoinRsp.getResults().getNumFound();

        System.out.println(i + "," + numFound + "," + standardJoinRsp.getQTime() + "," + noScoreJoinRsp.getQTime() + "," + tpiJoinRsp.getQTime());
      }
    }
  }

  private static String buildTermsQueryString(int numTerms) {
    final String[] terms = getAFewDictionaryWords(numTerms);
    return String.join(",", terms);
  }
}
