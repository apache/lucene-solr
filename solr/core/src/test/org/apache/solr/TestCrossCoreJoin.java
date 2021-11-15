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
package org.apache.solr;

import java.io.StringWriter;
import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.join.TestScoreJoinQPNoScore;
import org.apache.solr.servlet.DirectSolrConnection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCrossCoreJoin extends SolrTestCaseJ4 {

  private static SolrCore fromCore;

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    System.setProperty("solr.filterCache.async", "true");
//    initCore("solrconfig.xml","schema12.xml");

    // File testHome = createTempDir().toFile();
    // FileUtils.copyDirectory(getFile("solrj/solr"), testHome);
    initCore("solrconfig.xml", "schema12.xml", TEST_HOME(), "collection1");
    final CoreContainer coreContainer = h.getCoreContainer();

    fromCore = coreContainer.create("fromCore", ImmutableMap.of("configSet", "minimal"));

    assertU(add(doc("id", "1", "id_s_dv", "1", "name", "john", "title", "Director", "dept_s", "Engineering")));
    assertU(add(doc("id", "2", "id_s_dv", "2", "name", "mark", "title", "VP", "dept_s", "Marketing")));
    assertU(add(doc("id", "3", "id_s_dv", "3", "name", "nancy", "title", "MTS", "dept_s", "Sales")));
    assertU(add(doc("id", "4", "id_s_dv", "4", "name", "dave", "title", "MTS", "dept_s", "Support", "dept_s", "Engineering")));
    assertU(add(doc("id", "5", "id_s_dv", "5", "name", "tina", "title", "VP", "dept_s", "Engineering")));
    assertU(commit());

    update(fromCore, add(doc("id", "10", "id_s_dv", "10", "dept_id_s", "Engineering", "text", "These guys develop stuff", "cat", "dev")));
    update(fromCore, add(doc("id", "11", "id_s_dv", "11", "dept_id_s", "Marketing", "text", "These guys make you look good")));
    update(fromCore, add(doc("id", "12", "id_s_dv", "12", "dept_id_s", "Sales", "text", "These guys sell stuff")));
    update(fromCore, add(doc("id", "13", "id_s_dv", "13", "dept_id_s", "Support", "text", "These guys help customers")));
    update(fromCore, commit());

  }


  public static String update(SolrCore core, String xml) throws Exception {
    DirectSolrConnection connection = new DirectSolrConnection(core);
    SolrRequestHandler handler = core.getRequestHandler("/update");
    return connection.request(handler, null, xml);
  }

  @Test
  public void testJoin() throws Exception {
    doTestJoin("{!join");
  }

  @Test
  public void testScoreJoin() throws Exception {
    doTestJoin("{!join " + TestScoreJoinQPNoScore.whateverScore());
  }

  void doTestJoin(String joinPrefix) throws Exception {
    assertJQ(req("q", joinPrefix + " from=dept_id_s to=dept_s fromIndex=fromCore}cat:dev", "fl", "id",
        "debugQuery", random().nextBoolean() ? "true":"false")
        , "/response=={'numFound':3,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'4'},{'id':'5'}]}"
    );

    assertJQ(req( "qt", "/export", 
            "q", joinPrefix + " from=dept_id_s to=dept_s fromIndex=fromCore}cat:dev", "fl", "id_s_dv",
            "sort", "id_s_dv asc",
            "debugQuery", random().nextBoolean() ? "true":"false")
            , "/response=={'numFound':3,'docs':[{'id_s_dv':'1'},{'id_s_dv':'4'},{'id_s_dv':'5'}]}"
    );
    assertFalse(fromCore.isClosed());
    assertFalse(h.getCore().isClosed());

    // find people that develop stuff - but limit via filter query to a name of "john"
    // this tests filters being pushed down to queries (SOLR-3062)
    assertJQ(req("q", joinPrefix + " from=dept_id_s to=dept_s fromIndex=fromCore}cat:dev", "fl", "id", "fq", "name:john",
        "debugQuery", random().nextBoolean() ? "true":"false")
        , "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'1'}]}"
    );
  }

  @Test
  public void testCoresAreDifferent() throws Exception {
    assertQEx("schema12.xml" + " has no \"cat\" field", req("cat:*"), ErrorCode.BAD_REQUEST);
    final LocalSolrQueryRequest req = new LocalSolrQueryRequest(fromCore, "cat:*", "/select", 0, 100, Collections.emptyMap());
    final String resp = query(fromCore, req);
    assertTrue(resp, resp.contains("numFound=\"1\""));
    assertTrue(resp, resp.contains("<str name=\"id\">10</str>"));

  }

  public String query(SolrCore core, SolrQueryRequest req) throws Exception {
    String handler = "standard";
    if (req.getParams().get("qt") != null) {
      handler = req.getParams().get("qt");
    }
    if (req.getParams().get("wt") == null){
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set("wt", "xml");
      req.setParams(params);
    }
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
    core.execute(core.getRequestHandler(handler), req, rsp);
    if (rsp.getException() != null) {
      throw rsp.getException();
    }
    StringWriter sw = new StringWriter(32000);
    QueryResponseWriter responseWriter = core.getQueryResponseWriter(req);
    responseWriter.write(sw, req, rsp);
    req.close();
    SolrRequestInfo.clearRequestInfo();
    return sw.toString();
  }

  @AfterClass
  public static void nukeAll() {
    fromCore = null;
  }
}
