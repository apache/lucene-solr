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
package org.apache.solr.update;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

import java.io.IOException;
import java.util.Arrays;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import org.junit.Assert;
import org.junit.Test;


/**
 * This test is deliberately kept in different class as we don't want segment merging to kick in after deleting documents.
 * This ensures that first check the cached IndexFingerprint and 
 * recompute it only if any documents in the segment were deleted since caching the fingerprint first time around  
 *   
 *  
 */
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class PeerSyncWithIndexFingerprintCachingTest extends BaseDistributedSearchTestCase {
  private static int numVersions = 100;  // number of versions to use when syncing
  private final String FROM_LEADER = DistribPhase.FROMLEADER.toString();

  private ModifiableSolrParams seenLeader = 
    params(DISTRIB_UPDATE_PARAM, FROM_LEADER);
  
  public PeerSyncWithIndexFingerprintCachingTest() {
    stress = 0;

    // TODO: a better way to do this?
    configString = "solrconfig-tlog.xml";
    schemaString = "schema.xml";
  }

  @Test
  @ShardsFixed(num = 3)
  public void test() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    handle.put("score", SKIPVAL);
    handle.put("maxScore", SKIPVAL);

    SolrClient client0 = clients.get(0);
    SolrClient client1 = clients.get(1);

    long v =1;
    for(; v < 8; ++v) {
      add(client0, seenLeader, sdoc("id", ""+v,"_version_",v));
      add(client1, seenLeader, sdoc("id",""+v,"_version_",v));
      
    }
    client0.commit(); client1.commit();
    
    IndexFingerprint before = getFingerprint(client0, Long.MAX_VALUE);
    
    del(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_",Long.toString(-++v)), "2");
    client0.commit(); 
    
    IndexFingerprint after = getFingerprint(client0, Long.MAX_VALUE);
   
    // make sure fingerprint before and after deleting are not the same
    Assert.assertTrue(IndexFingerprint.compare(before, after) != 0);
    
    // replica which missed the delete should be able to sync
    assertSync(client1, numVersions, true, shardsArr[0]);
    client0.commit(); client1.commit();  

    queryAndCompare(params("q", "*:*", "sort","_version_ desc"), client0, client1);
  }

  IndexFingerprint getFingerprint(SolrClient client, long maxVersion) throws IOException, SolrServerException {
    QueryRequest qr = new QueryRequest(params("qt","/get", "getFingerprint",Long.toString(maxVersion)));
    @SuppressWarnings({"rawtypes"})
    NamedList rsp = client.request(qr);
    return IndexFingerprint.fromObject(rsp.get("fingerprint"));
  }

  void assertSync(SolrClient client, int numVersions, boolean expectedResult, String... syncWith) throws IOException, SolrServerException {
    QueryRequest qr = new QueryRequest(params("qt","/get", "getVersions",Integer.toString(numVersions), "sync", StrUtils.join(Arrays.asList(syncWith), ',')));
    @SuppressWarnings({"rawtypes"})
    NamedList rsp = client.request(qr);
    assertEquals(expectedResult, (Boolean) rsp.get("sync"));
  }

}
