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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;

public class PeerSyncWithLeaderAndIndexFingerprintCachingTest extends PeerSyncWithIndexFingerprintCachingTest {
  @Override
  void assertSync(SolrClient client, int numVersions, boolean expectedResult, String... syncWith) throws IOException, SolrServerException {
    QueryRequest qr = new QueryRequest(params("qt","/get", "getVersions",Integer.toString(numVersions), "syncWithLeader", StrUtils.join(Arrays.asList(syncWith), ',')));
    @SuppressWarnings({"rawtypes"})
    NamedList rsp = client.request(qr);
    assertEquals(expectedResult, (Boolean) rsp.get("syncWithLeader"));
  }
}
