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

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;

@SolrTestCaseJ4.SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class PeerSyncWithLeaderTest extends PeerSyncTest {

  @Override
  protected void testOverlap(Set<Integer> docsAdded, SolrClient client0, SolrClient client1, long v) throws IOException, SolrServerException {
    for (int i=0; i<numVersions; i++) {
      add(client0, seenLeader, sdoc("id",Integer.toString(i+11),"_version_",v+i+1));
      docsAdded.add(i+11);
    }

    // sync should fail since we are too far with the leader
    assertSync(client1, numVersions, false, shardsArr[0]);

    // add a doc that was missing... just enough to give enough overlap
    add(client1, seenLeader, sdoc("id",Integer.toString(11),"_version_",v+1));

    assertSync(client1, numVersions, true, shardsArr[0]);
    validateDocs(docsAdded, client0, client1);
  }

  @Override
  void assertSync(SolrClient client, int numVersions, boolean expectedResult, String... syncWith) throws IOException, SolrServerException {
    QueryRequest qr = new QueryRequest(params("qt","/get", "getVersions",Integer.toString(numVersions), "syncWithLeader", StrUtils.join(Arrays.asList(syncWith), ',')));
    @SuppressWarnings({"rawtypes"})
    NamedList rsp = client.request(qr);
    assertEquals(expectedResult, (Boolean) rsp.get("syncWithLeader"));
  }
}
