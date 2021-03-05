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
package org.apache.solr.search.mlt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CloudMLTQParserTest extends SolrCloudTestCase {
  
  @Before
  public void setupCluster() throws Exception {
    configureCluster(2)
    .addConfig("conf", configset("cloud-dynamic"))
    .configure();
    
    final CloudSolrClient client = cluster.getSolrClient();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1)
        .processAndWait(client, DEFAULT_TIMEOUT);

    cluster.waitForActiveCollection(COLLECTION, 2, 2);

    String id = "id";
    String FIELD1 = "lowerfilt_u" ;
    String FIELD2 = "lowerfilt1_u" ;

    new UpdateRequest()
        .add(sdoc(id, "1", FIELD1, "toyota"))
        .add(sdoc(id, "2", FIELD1, "chevrolet"))
        .add(sdoc(id, "3", FIELD1, "bmw usa"))
        .add(sdoc(id, "4", FIELD1, "ford"))
        .add(sdoc(id, "5", FIELD1, "ferrari"))
        .add(sdoc(id, "6", FIELD1, "jaguar"))
        .add(sdoc(id, "7", FIELD1, "mclaren moon or the moon and moon moon shine and the moon but moon was good foxes too"))
        .add(sdoc(id, "8", FIELD1, "sonata"))
        .add(sdoc(id, "9", FIELD1, "The quick red fox jumped over the lazy big and large brown dogs."))
        .add(sdoc(id, "10", FIELD1, "blue"))
        .add(sdoc(id, "12", FIELD1, "glue"))
        .add(sdoc(id, "13", FIELD1, "The quote red fox jumped over the lazy brown dogs."))
        .add(sdoc(id, "14", FIELD1, "The quote red fox jumped over the lazy brown dogs."))
        .add(sdoc(id, "15", FIELD1, "The fat red fox jumped over the lazy brown dogs."))
        .add(sdoc(id, "16", FIELD1, "The slim red fox jumped over the lazy brown dogs."))
        .add(sdoc(id, "17", FIELD1,
            "The quote red fox jumped moon over the lazy brown dogs moon. Of course moon. Foxes and moon come back to the foxes and moon"))
        .add(sdoc(id, "18", FIELD1, "The quote red fox jumped over the lazy brown dogs."))
        .add(sdoc(id, "19", FIELD1, "The hose red fox jumped over the lazy brown dogs."))
        .add(sdoc(id, "20", FIELD1, "The quote red fox jumped over the lazy brown dogs."))
        .add(sdoc(id, "21", FIELD1, "The court red fox jumped over the lazy brown dogs."))
        .add(sdoc(id, "22", FIELD1, "The quote red fox jumped over the lazy brown dogs."))
        .add(sdoc(id, "23", FIELD1, "The quote red fox jumped over the lazy brown dogs."))
        .add(sdoc(id, "24", FIELD1, "The file red fox jumped over the lazy brown dogs."))
        .add(sdoc(id, "25", FIELD1, "rod fix"))
        .add(sdoc(id, "26", FIELD1, "bmw usa 328i"))
        .add(sdoc(id, "27", FIELD1, "bmw usa 535i"))
        .add(sdoc(id, "28", FIELD1, "bmw 750Li"))
        .add(sdoc(id, "29", FIELD1, "bmw usa", FIELD2, "red green blue"))
        .add(sdoc(id, "30", FIELD1, "The quote red fox jumped over the lazy brown dogs.", FIELD2, "red green yellow"))
        .add(sdoc(id, "31", FIELD1, "The fat red fox jumped over the lazy brown dogs.", FIELD2, "green blue yellow"))
        .add(sdoc(id, "32", FIELD1, "The slim red fox jumped over the lazy brown dogs.", FIELD2, "yellow white black"))
        .commit(client, COLLECTION);
  }
  
  @After
  public void cleanCluster() throws Exception {
    if (null != cluster) {
      cluster.shutdown();
    }
  }

  public static final String COLLECTION = "mlt-collection";

  @Test
  public void testMLTQParser() throws Exception {

    QueryResponse queryResponse = cluster.getSolrClient()
        .query(COLLECTION, new SolrQuery("{!mlt qf=lowerfilt_u}17").setShowDebugInfo(true));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    int[] expectedIds = new int[]{7, 9, 13, 14, 15, 16, 20, 22, 24, 32};
    int[] actualIds = new int[10];
    int i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] = Integer.parseInt(String.valueOf(solrDocument.getFieldValue("id")));
    }
    
    Arrays.sort(actualIds);
    Arrays.sort(expectedIds);
    assertArrayEquals(expectedIds, actualIds);

  }

  @Test
  public void testBoost() throws Exception {

    QueryResponse queryResponse = cluster.getSolrClient().query(COLLECTION, new SolrQuery("{!mlt qf=lowerfilt_u boost=true}17"));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    int[] expectedIds = new int[]{7, 9, 13, 14, 15, 16, 20, 22, 24, 32};
    int[] actualIds = new int[solrDocuments.size()];
    int i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] = Integer.parseInt(String.valueOf(solrDocument.getFieldValue("id")));
    }
    
    Arrays.sort(actualIds);
    Arrays.sort(expectedIds);
    assertArrayEquals(expectedIds, actualIds);

    queryResponse = cluster.getSolrClient().query(COLLECTION, new SolrQuery("{!mlt qf=lowerfilt_u^10,lowerfilt1_u^1000 boost=false mintf=0 mindf=0}30"));
    solrDocuments = queryResponse.getResults();
    expectedIds = new int[]{31, 18, 23, 13, 14, 20, 22, 32, 19, 21};
    actualIds = new int[solrDocuments.size()];
    i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] = Integer.parseInt(String.valueOf(solrDocument.getFieldValue("id")));
    }
    
    Arrays.sort(actualIds);
    Arrays.sort(expectedIds);
    System.out.println("DEBUG ACTUAL IDS 1: " + Arrays.toString(actualIds));
    assertArrayEquals(expectedIds, actualIds);

    queryResponse = cluster.getSolrClient().query(COLLECTION, new SolrQuery("{!mlt qf=lowerfilt_u^10,lowerfilt1_u^1000 boost=true mintf=0 mindf=0}30"));
    solrDocuments = queryResponse.getResults();
    expectedIds = new int[]{29, 31, 32, 18, 23, 13, 14, 20, 22, 19};
    actualIds = new int[solrDocuments.size()];
    i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] = Integer.parseInt(String.valueOf(solrDocument.getFieldValue("id")));
    }
    
    Arrays.sort(actualIds);
    Arrays.sort(expectedIds);
    System.out.println("DEBUG ACTUAL IDS 2: " + Arrays.toString(actualIds));
    assertArrayEquals(Arrays.toString(expectedIds) + " " + Arrays.toString(actualIds), expectedIds, actualIds);
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testMinDF() throws Exception {

    QueryResponse queryResponse = cluster.getSolrClient().query(COLLECTION,
        new SolrQuery("{!mlt qf=lowerfilt_u mindf=0 mintf=1}3").setShowDebugInfo(true));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    int[] expectedIds = new int[]{29, 27, 26, 28};
    int[] actualIds = new int[solrDocuments.size()];
    int i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] = Integer.parseInt(String.valueOf(solrDocument.getFieldValue("id")));
    }
    
    Arrays.sort(actualIds);
    Arrays.sort(expectedIds);
    assertArrayEquals(Arrays.toString(expectedIds) + " " + Arrays.toString(actualIds), expectedIds, actualIds);

    String[] expectedQueryStrings = new String[]{
        "+(lowerfilt_u:bmw lowerfilt_u:usa) -id:3",
        "+(lowerfilt_u:usa lowerfilt_u:bmw) -id:3"};

    String[] actualParsedQueries;
    if (queryResponse.getDebugMap().get("parsedquery") instanceof String) {
      String parsedQueryString = (String) queryResponse.getDebugMap().get("parsedquery");
      assertTrue(parsedQueryString.equals(expectedQueryStrings[0]) || parsedQueryString.equals(expectedQueryStrings[1]));
    } else {
      actualParsedQueries = ((ArrayList<String>) queryResponse
          .getDebugMap().get("parsedquery")).toArray(new String[0]);
      Arrays.sort(actualParsedQueries);
      assertArrayEquals(expectedQueryStrings, actualParsedQueries);
    }
  }

  @Test
  public void testMultipleFields() throws Exception {

    QueryResponse queryResponse = cluster.getSolrClient().query(COLLECTION,
        new SolrQuery("{!mlt qf=lowerfilt_u,lowerfilt1_u mindf=0 mintf=1}26"));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    int[] expectedIds = new int[]{3, 29, 27, 28};
    int[] actualIds = new int[solrDocuments.size()];
    int i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] = Integer.parseInt(String.valueOf(solrDocument.getFieldValue("id")));
    }

    Arrays.sort(actualIds);
    Arrays.sort(expectedIds);
    assertArrayEquals(Arrays.toString(expectedIds) + " " + Arrays.toString(actualIds), expectedIds, actualIds);

  }

  @Test
  public void testHighDFValue() throws Exception {

    // Test out a high value of df and make sure nothing matches.
    QueryResponse queryResponse = cluster.getSolrClient().query(COLLECTION,
        new SolrQuery("{!mlt qf=lowerfilt_u mindf=20 mintf=1}3"));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    assertEquals("Expected to match 0 documents with a mindf of 20 but found more", solrDocuments.size(), 0);

  }

  @Test
  public void testHighWLValue() throws Exception {

    // Test out a high value of wl and make sure nothing matches.
    QueryResponse queryResponse = cluster.getSolrClient().query(COLLECTION,
        new SolrQuery("{!mlt qf=lowerfilt_u minwl=4 mintf=1}3"));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    assertEquals("Expected to match 0 documents with a minwl of 4 but found more", solrDocuments.size(), 0);

  }

  @Test
  public void testLowMinWLValue() throws Exception {

    // Test out a low enough value of minwl and make sure we get the expected matches.
    QueryResponse queryResponse = cluster.getSolrClient().query(COLLECTION,
        new SolrQuery("{!mlt qf=lowerfilt_u minwl=3 mintf=1}3"));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    assertEquals("Expected to match 4 documents with a minwl of 3 but found more", 4, solrDocuments.size());

  }

  @Test
  public void testUnstoredAndUnanalyzedFieldsAreIgnored() throws Exception {

    // Assert that {!mlt}id does not throw an exception i.e. implicitly, only fields that are stored + have explicit
    // analyzer are used for MLT Query construction.
    QueryResponse queryResponse = cluster.getSolrClient().query(COLLECTION, new SolrQuery("{!mlt}20"));
    SolrDocumentList solrDocuments = queryResponse.getResults();
    int[] actualIds = new int[solrDocuments.size()];
    int[] expectedIds = new int[]{13, 14, 15, 16, 22, 24, 32, 18, 19, 21};
    int i = 0;
    StringBuilder sb = new StringBuilder();
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] =  Integer.parseInt(String.valueOf(solrDocument.getFieldValue("id")));
      sb.append(actualIds[i-1]).append(", ");
    }
    
    Arrays.sort(actualIds);
    Arrays.sort(expectedIds);
    assertArrayEquals(expectedIds, actualIds);
  }

  public void testInvalidSourceDocument() throws IOException {
    SolrException e = expectThrows(SolrException.class, () -> {
      cluster.getSolrClient().query(COLLECTION, new SolrQuery("{!mlt qf=lowerfilt_u}999999"));
    });
  }
}
