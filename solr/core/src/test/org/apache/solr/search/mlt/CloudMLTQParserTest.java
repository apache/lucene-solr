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

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class CloudMLTQParserTest extends AbstractFullDistribZkTestBase {

  static Logger log = LoggerFactory.getLogger(CloudMLTQParserTest.class);
  
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

    commit();

    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);

    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(CommonParams.Q, "{!mlt qf=lowerfilt}17");
    QueryResponse queryResponse = cloudClient.query(params);
    SolrDocumentList solrDocuments = queryResponse.getResults();
    int[] expectedIds = new int[]{17, 13, 14, 20, 22, 15, 16, 24, 18, 23};
    int[] actualIds = new int[10];
    int i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] =  Integer.valueOf(String.valueOf(solrDocument.getFieldValue("id")));
    }
    assertArrayEquals(expectedIds, actualIds);
    
    params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt}3");
    queryResponse = queryServer(params);
    solrDocuments = queryResponse.getResults();
    expectedIds = new int[]{3, 27, 26, 28};
    actualIds = new int[4];
    i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] =  Integer.valueOf(String.valueOf(solrDocument.getFieldValue("id")));
    }
    assertArrayEquals(expectedIds, actualIds);

    params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt}20");
    params.set("debug" , "query");
    queryResponse = queryServer(params);
    solrDocuments = queryResponse.getResults();
    expectedIds = new int[]{18, 23, 13, 14, 20, 22, 19, 21, 15, 16};
    actualIds = new int[10];
    i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] =  Integer.valueOf(String.valueOf(solrDocument.getFieldValue("id")));
    }
    assertArrayEquals(expectedIds, actualIds);

    String expectedQueryString = "lowerfilt:over lowerfilt:fox lowerfilt:lazy lowerfilt:brown "
        + "lowerfilt:jumped lowerfilt:red lowerfilt:dogs. lowerfilt:quote lowerfilt:the";
    
    ArrayList<String> actualParsedQueries = (ArrayList<String>) queryResponse
        .getDebugMap().get("parsedquery");

    for (int counter = 0; counter < actualParsedQueries.size(); counter++) {
      assertTrue("Parsed queries aren't equal",
          compareParsedQueryStrings(expectedQueryString,
              actualParsedQueries.get(counter)));
    }
  
    // Assert that {!mlt}id does not throw an exception i.e. implicitly, only fields that are stored + have explicit
    // analyzer are used for MLT Query construction.
    params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt}20");

    queryResponse = queryServer(params);
    solrDocuments = queryResponse.getResults();
    expectedIds = new int[]{18, 23, 13, 14, 20, 22, 19, 21, 15, 16};
    i = 0;
    for (SolrDocument solrDocument : solrDocuments) {
      actualIds[i++] =  Integer.valueOf(String.valueOf(solrDocument.getFieldValue("id")));
    }
    assertArrayEquals(expectedIds, actualIds);
  }
  
  @Test
  public void testInvalidDocument() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "{!mlt qf=lowerfilt}nonexistentdocid");
    try {
      cloudClient.query(params);
      fail("The above query is supposed to throw an exception.");
    } catch (SolrServerException e) {
      // Do nothing.
    }
  }
  
  private boolean compareParsedQueryStrings(String expected, String actual) {
    HashSet<String> expectedQueryParts = new HashSet<>();
    expectedQueryParts.addAll(Arrays.asList(expected.split("\\s+")));
    HashSet<String> actualQueryParts = new HashSet();
    actualQueryParts.addAll(Arrays.asList(actual.split("\\s+")));
    return expectedQueryParts.containsAll(actualQueryParts);
  }
}
