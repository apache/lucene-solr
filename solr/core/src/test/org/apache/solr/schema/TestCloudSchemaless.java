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
package org.apache.solr.schema;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.cloud.SolrCloudBridgeTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.BaseTestHarness;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Tests a schemaless collection configuration with SolrCloud
 */
@SolrTestCase.SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows machines occasionally
public class TestCloudSchemaless extends SolrCloudBridgeTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String SUCCESS_XPATH = "/response/lst[@name='responseHeader']/int[@name='status'][.='0']";

  @After
  public void teardDown() throws Exception {
    super.tearDown();
    closeRestTestHarnesses();
  }

  public TestCloudSchemaless() {
    schemaString = "schema-add-schema-fields-update-processor.xml";
    solrconfigString = "solrconfig-schemaless.xml";
    sliceCount = 2;
    numJettys = 2;
    extraServlets = getExtraServlets();
    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "true");
  }

  public SortedMap<ServletHolder,String> getExtraServlets() {
    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<>();
    return extraServlets;
  }

  private String[] getExpectedFieldResponses(int numberOfDocs) {
    String[] expectedAddFields = new String[1 + numberOfDocs];
    expectedAddFields[0] = SUCCESS_XPATH;

    for (int i = 0; i < numberOfDocs; ++i) {
      String newFieldName = "newTestFieldInt" + i;
      expectedAddFields[1 + i] =
        "/response/arr[@name='fields']/lst/str[@name='name'][.='" + newFieldName + "']";
    }
    return expectedAddFields;
  }

  @Test
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 04-May-2018
  public void test() throws Exception {
    setupRestTestHarnesses();

    // First, add a bunch of documents in a single update with the same new field.
    // This tests that the replicas properly handle schema additions.

    int slices = cloudClient.getZkStateReader().getClusterState()
      .getCollection(COLLECTION).getActiveSlices().size();
    int trials = TEST_NIGHTLY ? 50 : 15;
    // generate enough docs so that we can expect at least a doc per slice
    int numDocsPerTrial = (int)(slices * (Math.log(slices) + 1)) ;
    SolrClient randomClient = clients.get(random().nextInt(clients.size()));
    int docNumber = 0;
    for (int i = 0; i < trials; ++i) {
      List<SolrInputDocument> docs = new ArrayList<>();
      for (int j =0; j < numDocsPerTrial; ++j) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", Long.toHexString(Double.doubleToLongBits(random().nextDouble())));
        doc.addField("newTestFieldInt" + docNumber++, "123");
        doc.addField("constantField", "3.14159");
        docs.add(doc);
      }

      randomClient.add(docs);
    }
    randomClient.commit();

    String [] expectedFields = getExpectedFieldResponses(docNumber);
    // Check that all the fields were added

    forAllRestTestHarnesses(client -> {
      try {
        String request = "/schema/fields?wt=xml";
        String response = client.query(request);
        String result = BaseTestHarness.validateXPath(cluster.getJettySolrRunners().get(0).getCoreContainer().getCores().iterator().next().getResourceLoader(), response, expectedFields);
        if (result != null) {
          String msg = "QUERY FAILED: xpath=" + result + "  request=" + request + "  response=" + response;
          log.error(msg);

          fail(msg);
        }
      } catch (Exception ex) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Caught exception: " + ex.getMessage(), ex);
      }
    });

    // Now, let's ensure that writing the same field with two different types fails
    int failTrials = TEST_NIGHTLY ? 50 : 15;
    for (int i = 0; i < failTrials; ++i) {
      SolrInputDocument intDoc = new SolrInputDocument();
      intDoc.addField("id", Long.toHexString(Double.doubleToLongBits(random().nextDouble())));
      intDoc.addField("longOrDateField" + i, "123");

      SolrInputDocument dateDoc = new SolrInputDocument();
      dateDoc.addField("id", Long.toHexString(Double.doubleToLongBits(random().nextDouble())));
      dateDoc.addField("longOrDateField" + i, "1995-12-31T23:59:59Z");

      // randomize the order of the docs
      List<SolrInputDocument> docs = random().nextBoolean()? Arrays.asList(intDoc, dateDoc): Arrays.asList(dateDoc, intDoc);

//      SolrException ex = expectThrows(SolrException.class,  () -> {
//        randomClient.add(docs);
//        randomClient.commit();
//      });
//      assertEquals(ErrorCode.BAD_REQUEST, ErrorCode.getErrorCode(ex.code()));

//      SolrException ex = expectThrows(SolrException.class, () -> {
//        CloudHttp2SolrClient cloudSolrClient = cloudClient;
//        cloudSolrClient.add(docs);
//        cloudSolrClient.commit();
//      });
//      assertEquals(ErrorCode.BAD_REQUEST, ErrorCode.getErrorCode(ex.code()));
    }
  }
}
