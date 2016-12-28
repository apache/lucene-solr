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
import java.nio.file.FileSystems;
import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.DirectSolrConnection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.search.join.TestScoreJoinQPNoScore.whateverScore;

public class TestCrossCoreJoin extends SolrTestCaseJ4 {

  private static final String rel_from_long = "rel_from_long";
  private static final String rel_int = "rel_int";
  private static final String rel_long = "rel_long";
  private static final String name = "name";
  private static final String cat = "cat";
  private static SolrCore fromCore;

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_

    initCore("solrconfig-basic.xml", "schema-docValuesJoin.xml", TEST_HOME(), "collection1");
    final CoreContainer coreContainer = h.getCoreContainer();

    fromCore = coreContainer.create("fromCore",
        FileSystems.getDefault().getPath(TEST_HOME(),DEFAULT_TEST_CORENAME),
        ImmutableMap.of("config", "solrconfig-basic.xml",
            "schema", "schema-minimal-numeric-join.xml"),
        true);

    assertU(add(doc("id", "1", "uid_l_dv", "1", "name_s", "john",
        "title_s", "Director", "dept_ss", "Engineering", "u_role_i_dv", "1")));
    assertU(add(doc("id", "2", "uid_l_dv", "2", "name_s", "mark",
        "title_s", "VP", "dept_ss", "Marketing", "u_role_i_dv", "1")));
    assertU(add(doc("id", "3", "uid_l_dv", "3", "name_s", "nancy",
        "title_s", "MTS", "dept_ss", "Sales", "u_role_i_dv", "2")));
    assertU(add(doc("id", "4", "uid_l_dv", "4", "name_s", "dave",
        "title_s", "MTS", "dept_ss", "Support", "dept_ss", "Engineering", "u_role_i_dv", "3")));
    assertU(add(doc("id", "5", "uid_l_dv", "5", "name_s", "tina",
        "title_s", "VP", "dept_ss", "Engineering", "u_role_i_dv", "3")));

    assertU(commit());

    update(fromCore, add(doc("id", "15", rel_from_long, "1",rel_long, "2")));
    update(fromCore, add(doc("id", "16", rel_from_long, "2",rel_long, "1")));

    update(fromCore, add(doc("id", "20", rel_int, "1", "name", "admin")));
    update(fromCore, add(doc("id", "21", rel_int, "2", "name", "mod")));
    update(fromCore, add(doc("id", "22", rel_int, "3", "name", "user")));

    update(fromCore, add(doc("id", "10", "dept_id", "Engineering", "text", "These guys develop stuff", cat, "dev")));
    update(fromCore, add(doc("id", "11", "dept_id", "Marketing", "text", "These guys make you look good")));
    update(fromCore, add(doc("id", "12", "dept_id", "Sales", "text", "These guys sell stuff")));
    update(fromCore, add(doc("id", "13", "dept_id", "Support", "text", "These guys help customers")));
    update(fromCore, commit());

  }

  @Test
  public void testJoinNumeric() throws Exception {
    // Test join long field
    assertJQ(req("q","{!join fromIndex=fromCore " +
        "from="+rel_long+" to=uid_l_dv"+ whateverScore()+"}"+rel_from_long+":1", "fl","id")
        ,"/response=={'numFound':1,'start':0,'docs':[{'id':'2'}]}"
    );

    // Test join int field
    assertJQ(req("q","{!join fromIndex=fromCore " +
        "from="+rel_int+" to=u_role_i_dv"+whateverScore()+"}name:admin", "fl","id")
        ,"/response=={'numFound':2,'start':0,'docs':[{'id':'1'},{'id':'2'}]}"
    );

    // Test join int field
    assertQEx("From and to field must have same numeric type",req("q","{!join fromIndex=fromCore " +
            "from="+rel_int+" to=uid_l_dv"+whateverScore()+"}name:admin", "fl","id")
        ,ErrorCode.BAD_REQUEST
    );
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
    doTestJoin("{!join " + whateverScore());
  }

  void doTestJoin(String joinPrefix) throws Exception {
    assertJQ(req("q", joinPrefix + " to=dept_ss from=dept_id fromIndex=fromCore}"+cat+":dev", "fl", "id",
        "debugQuery", random().nextBoolean() ? "true":"false")
        , "/response=={'numFound':3,'start':0,'docs':[{'id':'1'},{'id':'4'},{'id':'5'}]}"
    );

    // find people that develop stuff - but limit via filter query to a name of "john"
    // this tests filters being pushed down to queries (SOLR-3062)
    assertJQ(req("q", joinPrefix + " to=dept_ss from=dept_id fromIndex=fromCore}"+cat+":dev", "fl", "id", "fq", "name_s:john",
        "debugQuery", random().nextBoolean() ? "true":"false")
        , "/response=={'numFound':1,'start':0,'docs':[{'id':'1'}]}"
    );
  }

  @Test
  public void testSchemasAreDifferent() throws Exception {
    for (String field: new String[]{cat, name, rel_long, rel_int}) {
      assertQEx("'to' schema" + " has no \""+field+"\" field", req(field+":*"), ErrorCode.BAD_REQUEST);
    }

    final LocalSolrQueryRequest req = new LocalSolrQueryRequest(fromCore, cat+":*", "standard", 0, 100, Collections.emptyMap());
    final String resp = query(fromCore, req);
    assertTrue(resp, resp.contains("numFound=\"1\""));
    assertTrue(resp, resp.contains("<str name=\"id\">10</str>"));

  }

  public String query(SolrCore core, SolrQueryRequest req) throws Exception {
    String handler = "standard";
    if (req.getParams().get("qt") != null)
      handler = req.getParams().get("qt");
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
