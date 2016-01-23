package org.apache.solr.search.mlt;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

public class CloudMLTQParserTest extends AbstractFullDistribZkTestBase {

  public CloudMLTQParserTest() {
    sliceCount = 2;
    
    configString = "solrconfig.xml";
    schemaString = "schema.xml";
  }

  @Override
  protected String getCloudSolrConfig() {
    return configString;
  }

  @Test
  @ShardsFixed(num = 2)
  public void test() throws Exception {
    
    waitForRecoveriesToFinish(false);

    String id = "id";
    delQ("*:*");
    String FIELD1 = "lowerfilt" ;
    String FIELD2 = "lowerfilt1" ;
    
    indexDoc(sdoc(id, "1", FIELD1, "toyota"));
    indexDoc(sdoc(id, "2", FIELD1, "chevrolet"));
    indexDoc(sdoc(id, "3", FIELD1, "bmw usa"));
    indexDoc(sdoc(id, "4", FIELD1, "ford"));
    indexDoc(sdoc(id, "5", FIELD1, "ferrari"));
    indexDoc(sdoc(id, "6", FIELD1, "jaguar"));
    indexDoc(sdoc(id, "7", FIELD1, "mclaren moon or the moon and moon moon shine and the moon but moon was good foxes too"));
    indexDoc(sdoc(id, "8", FIELD1, "sonata"));
    indexDoc(sdoc(id, "9", FIELD1, "The quick red fox jumped over the lazy big and large brown dogs."));
    indexDoc(sdoc(id, "10", FIELD1, "blue"));
    indexDoc(sdoc(id, "12", FIELD1, "glue"));
    indexDoc(sdoc(id, "13", FIELD1, "The quote red fox jumped over the lazy brown dogs."));
    indexDoc(sdoc(id, "14", FIELD1, "The quote red fox jumped over the lazy brown dogs."));
    indexDoc(sdoc(id, "15", FIELD1, "The fat red fox jumped over the lazy brown dogs."));
    indexDoc(sdoc(id, "16", FIELD1, "The slim red fox jumped over the lazy brown dogs."));
    indexDoc(sdoc(id, "17", FIELD1, "The quote red fox jumped moon over the lazy brown dogs moon. Of course moon. Foxes and moon come back to the foxes and moon"));
    indexDoc(sdoc(id, "18", FIELD1, "The quote red fox jumped over the lazy brown dogs."));
    indexDoc(sdoc(id, "19", FIELD1, "The hose red fox jumped over the lazy brown dogs."));
    indexDoc(sdoc(id, "20", FIELD1, "The quote red fox jumped over the lazy brown dogs."));
    indexDoc(sdoc(id, "21", FIELD1, "The court red fox jumped over the lazy brown dogs."));
    indexDoc(sdoc(id, "22", FIELD1, "The quote red fox jumped over the lazy brown dogs."));
    indexDoc(sdoc(id, "23", FIELD1, "The quote red fox jumped over the lazy brown dogs."));
    indexDoc(sdoc(id, "24", FIELD1, "The file red fox jumped over the lazy brown dogs."));
    indexDoc(sdoc(id, "25", FIELD1, "rod fix"));
    indexDoc(sdoc(id, "26", FIELD1, "bmw usa 328i"));
    indexDoc(sdoc(id, "27", FIELD1, "bmw usa 535i"));
    indexDoc(sdoc(id, "28", FIELD1, "bmw 750Li"));
    indexDoc(sdoc(id, "29", FIELD1, "bmw usa",
        FIELD2, "red green blue"));
    indexDoc(sdoc(id, "30", FIELD1, "The quote red fox jumped over the lazy brown dogs.",
        FIELD2, "red green yellow"));
    indexDoc(sdoc(id, "31", FIELD1, "The fat red fox jumped over the lazy brown dogs.",
        FIELD2, "green blue yellow"));
    indexDoc(sdoc(id, "32", FIELD1, "The slim red fox jumped over the lazy brown dogs.",
        FIELD2, "yellow white black"));

    commit();

    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);

    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(CommonParams.Q, "{!mlt qf=lowerfilt}17");
    QueryResponse queryResponse = cloudClient.query(params);
    SolrDocumentList solrDocuments = queryResponse.getResults();
    int[] expectedIds = new int[]{7, 13, 14, 15, 16, 20, 22, 24, 32, 9};
    int[] actualIds = new int[10];
    int i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] =  Integer.valueOf(String.valueOf(solrDocument.getFieldValue("id")));
    }
    assertArrayEquals(expectedIds, actualIds);

    params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt boost=true}17");
    queryResponse = queryServer(params);
    solrDocuments = queryResponse.getResults();
    expectedIds = new int[]{7, 13, 14, 15, 16, 20, 22, 24, 32, 9};
    actualIds = new int[solrDocuments.size()];
    i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] =  Integer.valueOf(String.valueOf(solrDocument.getFieldValue("id")));
    }
    assertArrayEquals(expectedIds, actualIds);
    
    params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt mindf=0 mintf=1}3");
    params.set(CommonParams.DEBUG, "true");
    queryResponse = queryServer(params);
    solrDocuments = queryResponse.getResults();
    expectedIds = new int[]{29, 27, 26, 28};
    actualIds = new int[solrDocuments.size()];
    i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] =  Integer.valueOf(String.valueOf(solrDocument.getFieldValue("id")));
    }
    assertArrayEquals(expectedIds, actualIds);

    String[] expectedQueryStrings = new String[]{
      "(+(lowerfilt:bmw lowerfilt:usa) -id:3)/no_coord",
      "(+(lowerfilt:usa lowerfilt:bmw) -id:3)/no_coord"};

    String[] actualParsedQueries;
    if(queryResponse.getDebugMap().get("parsedquery") instanceof  String) {
      String parsedQueryString = (String) queryResponse.getDebugMap().get("parsedquery");
      assertTrue(parsedQueryString.equals(expectedQueryStrings[0]) || parsedQueryString.equals(expectedQueryStrings[1]));
    } else {
      actualParsedQueries = ((ArrayList<String>) queryResponse
          .getDebugMap().get("parsedquery")).toArray(new String[0]);
      Arrays.sort(actualParsedQueries);
      assertArrayEquals(expectedQueryStrings, actualParsedQueries);
    }


    params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt,lowerfilt1 mindf=0 mintf=1}26");
    params.set(CommonParams.DEBUG, "true");
    queryResponse = queryServer(params);
    solrDocuments = queryResponse.getResults();
    expectedIds = new int[]{27, 3, 29, 28};
    actualIds = new int[solrDocuments.size()];
    i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] =  Integer.valueOf(String.valueOf(solrDocument.getFieldValue("id")));
    }
    
    assertArrayEquals(expectedIds, actualIds);

    params = new ModifiableSolrParams();
    // Test out a high value of df and make sure nothing matches.
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt mindf=20 mintf=1}3");
    params.set(CommonParams.DEBUG, "true");
    queryResponse = queryServer(params);
    solrDocuments = queryResponse.getResults();
    assertEquals("Expected to match 0 documents with a mindf of 20 but found more", solrDocuments.size(), 0);

    params = new ModifiableSolrParams();
    // Test out a high value of wl and make sure nothing matches.
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt minwl=4 mintf=1}3");
    params.set(CommonParams.DEBUG, "true");
    queryResponse = queryServer(params);
    solrDocuments = queryResponse.getResults();
    assertEquals("Expected to match 0 documents with a minwl of 4 but found more", solrDocuments.size(), 0);

    params = new ModifiableSolrParams();
    // Test out a low enough value of minwl and make sure we get the expected matches.
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt minwl=3 mintf=1}3");
    params.set(CommonParams.DEBUG, "true");
    queryResponse = queryServer(params);
    solrDocuments = queryResponse.getResults();
    assertEquals("Expected to match 4 documents with a minwl of 3 but found more", 4, solrDocuments.size());

    // Assert that {!mlt}id does not throw an exception i.e. implicitly, only fields that are stored + have explicit
    // analyzer are used for MLT Query construction.
    params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt}20");

    queryResponse = queryServer(params);
    solrDocuments = queryResponse.getResults();
    actualIds = new int[solrDocuments.size()];
    expectedIds = new int[]{13, 14, 15, 16, 22, 24, 32, 18, 19, 21};
    i = 0;
    StringBuilder sb = new StringBuilder();
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] =  Integer.valueOf(String.valueOf(solrDocument.getFieldValue("id")));
      sb.append(actualIds[i-1]).append(", ");
    }
    assertArrayEquals(expectedIds, actualIds);
  }
  
  @Test(expected=SolrException.class)
  public void testInvalidDocument() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt}999999");
    try {
      cloudClient.query(params);
      fail("The above query is supposed to throw an exception.");
    } catch (SolrServerException e) {
      // Do nothing.
    }
  }
}
