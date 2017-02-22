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
package org.apache.solr.handler.admin;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

public class StatsReloadRaceTest extends SolrTestCaseJ4 {

  // to support many times repeating
  static AtomicInteger taskNum = new AtomicInteger();

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml", "schema.xml");

    XmlDoc docs = new XmlDoc();
    for (int i = 0; i < atLeast(10); i++) {
      docs.xml += doc("id", "" + i,
          "name_s", "" + i);
    }
    assertU(add(docs));
    assertU(commit());
  }

  @Test
  public void testParallelReloadAndStats() throws Exception {

    Random random = random();
    
    for (int i = 0; i < atLeast(random, 2); i++) {

      int asyncId = taskNum.incrementAndGet();

     
      h.getCoreContainer().getMultiCoreHandler().handleRequest(req(
          CommonParams.QT, "/admin/cores",
          CoreAdminParams.ACTION,
          CoreAdminParams.CoreAdminAction.RELOAD.toString(),
          CoreAdminParams.CORE, DEFAULT_TEST_CORENAME,
          "async", "" + asyncId), new SolrQueryResponse());

      boolean isCompleted;
      do {
        if (random.nextBoolean()) {
          requestMbeans();
        } else {
          requestCoreStatus();
        }

        isCompleted = checkReloadComlpetion(asyncId);
      } while (!isCompleted);
    }
  }

  private void requestCoreStatus() throws Exception {
    SolrQueryResponse rsp = new SolrQueryResponse();
    h.getCoreContainer().getMultiCoreHandler().handleRequest(req(
        CoreAdminParams.ACTION,
        CoreAdminParams.CoreAdminAction.STATUS.toString(),
        "core", DEFAULT_TEST_CORENAME), rsp);
    assertNull(""+rsp.getException(),rsp.getException());

  }

  private boolean checkReloadComlpetion(int asyncId) {
    boolean isCompleted;
    SolrQueryResponse rsp = new SolrQueryResponse();
    h.getCoreContainer().getMultiCoreHandler().handleRequest(req(
        CoreAdminParams.ACTION,
        CoreAdminParams.CoreAdminAction.REQUESTSTATUS.toString(),
        CoreAdminParams.REQUESTID, "" + asyncId), rsp);
    
    @SuppressWarnings("unchecked")
    List<Object> statusLog = rsp.getValues().getAll(CoreAdminAction.STATUS.name());

    assertFalse("expect status check w/o error, got:" + statusLog,
                              statusLog.contains(CoreAdminHandler.FAILED));

    isCompleted = statusLog.contains(CoreAdminHandler.COMPLETED);
    return isCompleted;
  }

  private void requestMbeans() throws Exception {
    String stats = h.query(req(
        CommonParams.QT, "/admin/mbeans",
        "stats", "true"));

    NamedList<NamedList<Object>> actualStats = SolrInfoMBeanHandler.fromXML(stats).get("CORE");
    
    for (Map.Entry<String, NamedList<Object>> tuple : actualStats) {
      if (tuple.getKey().contains("earcher")) { // catches "searcher" and "Searcher@345345 blah"
        NamedList<Object> searcherStats = tuple.getValue();
        @SuppressWarnings("unchecked")
        NamedList<Object> statsList = (NamedList<Object>)searcherStats.get("stats");
        assertEquals("expect to have exactly one indexVersion at "+statsList, 1, statsList.getAll("indexVersion").size());
        assertTrue(statsList.get("indexVersion") instanceof Long); 
      }
    }
  }

}
