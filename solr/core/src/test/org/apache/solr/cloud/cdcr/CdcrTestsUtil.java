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

package org.apache.solr.cloud.cdcr;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.CdcrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdcrTestsUtil extends SolrTestCaseJ4{
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void cdcrStart(CloudSolrClient client) throws SolrServerException, IOException {
    QueryResponse response = invokeCdcrAction(client, CdcrParams.CdcrAction.START);
    assertEquals("started", ((NamedList) response.getResponse().get("status")).get("process"));
  }

  public static void cdcrStop(CloudSolrClient client) throws SolrServerException, IOException {
    QueryResponse response = invokeCdcrAction(client, CdcrParams.CdcrAction.STOP);
    assertEquals("stopped", ((NamedList) response.getResponse().get("status")).get("process"));
  }

  public static void cdcrEnableBuffer(CloudSolrClient client) throws IOException, SolrServerException {
    QueryResponse response = invokeCdcrAction(client, CdcrParams.CdcrAction.ENABLEBUFFER);
    assertEquals("enabled", ((NamedList) response.getResponse().get("status")).get("buffer"));
  }

  public static void cdcrDisableBuffer(CloudSolrClient client) throws IOException, SolrServerException {
    QueryResponse response = invokeCdcrAction(client, CdcrParams.CdcrAction.DISABLEBUFFER);
    assertEquals("disabled", ((NamedList) response.getResponse().get("status")).get("buffer"));
  }

  public static QueryResponse invokeCdcrAction(CloudSolrClient client, CdcrParams.CdcrAction action) throws IOException, SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.QT, "/cdcr");
    params.set(CommonParams.ACTION, action.toLower());
    return client.query(params);
  }

  public static QueryResponse getCdcrQueue(CloudSolrClient client) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.QT, "/cdcr");
    params.set(CommonParams.ACTION, CdcrParams.QUEUES);
    return client.query(params);
  }

  public static long waitForClusterToSync(int numDocs, CloudSolrClient clusterSolrClient) throws SolrServerException, IOException, InterruptedException {
    return waitForClusterToSync(numDocs, clusterSolrClient, "*:*");
  }

  public static long waitForClusterToSync(int numDocs, CloudSolrClient clusterSolrClient, String query) throws SolrServerException, IOException, InterruptedException {
    long start = System.nanoTime();
    QueryResponse response = null;
    while (System.nanoTime() - start <= TimeUnit.NANOSECONDS.convert(120, TimeUnit.SECONDS)) {
      try {
        clusterSolrClient.commit();
        response = clusterSolrClient.query(new SolrQuery(query));
        if (response.getResults().getNumFound() == numDocs) {
          break;
        }
      } catch (Exception e) {
        log.warn("Exception trying to commit on cluster. This is expected and safe to ignore.", e);
      }
      Thread.sleep(1000);
    }
    return response != null ? response.getResults().getNumFound() : 0;
  }
}
