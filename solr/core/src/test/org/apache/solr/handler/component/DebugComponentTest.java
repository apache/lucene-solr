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
package org.apache.solr.handler.component;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 *
 *
 **/
public class DebugComponentTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    assertU(adoc("id", "1", "title", "this is a title.", "inStock_b1", "true"));
    assertU(adoc("id", "2", "title", "this is another title.", "inStock_b1", "true"));
    assertU(adoc("id", "3", "title", "Mary had a little lamb.", "inStock_b1", "false"));
    assertU(commit());

  }

  @Test
  public void testBasicInterface() throws Exception {
    //make sure the basics are in place
    assertQ(req("q", "*:*", CommonParams.DEBUG_QUERY, "true"),
            "//str[@name='rawquerystring']='*:*'",
            "//str[@name='querystring']='*:*'",
            "//str[@name='parsedquery']='MatchAllDocsQuery(*:*)'",
            "//str[@name='parsedquery_toString']='*:*'",
            "count(//lst[@name='explain']/*)=3",
            "//lst[@name='explain']/str[@name='1']",
            "//lst[@name='explain']/str[@name='2']",
            "//lst[@name='explain']/str[@name='3']",
            "//str[@name='QParser']",// make sure the QParser is specified
            "count(//lst[@name='timing']/*)=3", //should be three pieces to timings
            "//lst[@name='timing']/double[@name='time']", //make sure we have a time value, but don't specify its result
            "count(//lst[@name='prepare']/*)>0",
            "//lst[@name='prepare']/double[@name='time']",
            "count(//lst[@name='process']/*)>0",
            "//lst[@name='process']/double[@name='time']"
    );
  }

  // Test the ability to specify which pieces to include

  @Test
  public void testPerItemInterface() throws Exception {
    //Same as debugQuery = true
    assertQ(req("q", "*:*", "debug", "true"),
            "//str[@name='rawquerystring']='*:*'",
            "//str[@name='querystring']='*:*'",
            "//str[@name='parsedquery']='MatchAllDocsQuery(*:*)'",
            "//str[@name='parsedquery_toString']='*:*'",
            "//str[@name='QParser']",// make sure the QParser is specified
            "count(//lst[@name='explain']/*)=3",
            "//lst[@name='explain']/str[@name='1']",
            "//lst[@name='explain']/str[@name='2']",
            "//lst[@name='explain']/str[@name='3']",
            "count(//lst[@name='timing']/*)=3", //should be three pieces to timings
            "//lst[@name='timing']/double[@name='time']", //make sure we have a time value, but don't specify its result
            "count(//lst[@name='prepare']/*)>0",
            "//lst[@name='prepare']/double[@name='time']",
            "count(//lst[@name='process']/*)>0",
            "//lst[@name='process']/double[@name='time']"
    );
    //timing only
    assertQ(req("q", "*:*", "debug", CommonParams.TIMING),
            "count(//str[@name='rawquerystring'])=0",
            "count(//str[@name='querystring'])=0",
            "count(//str[@name='parsedquery'])=0",
            "count(//str[@name='parsedquery_toString'])=0",
            "count(//lst[@name='explain']/*)=0",
            "count(//str[@name='QParser'])=0",// make sure the QParser is specified
            "count(//lst[@name='timing']/*)=3", //should be three pieces to timings
            "//lst[@name='timing']/double[@name='time']", //make sure we have a time value, but don't specify its result
            "count(//lst[@name='prepare']/*)>0",
            "//lst[@name='prepare']/double[@name='time']",
            "count(//lst[@name='process']/*)>0",
            "//lst[@name='process']/double[@name='time']"
    );
    //query only
    assertQ(req("q", "*:*", "debug", CommonParams.QUERY),
            "//str[@name='rawquerystring']='*:*'",
            "//str[@name='querystring']='*:*'",
            "//str[@name='parsedquery']='MatchAllDocsQuery(*:*)'",
            "//str[@name='parsedquery_toString']='*:*'",
            "count(//lst[@name='explain']/*)=0",
            "//str[@name='QParser']",// make sure the QParser is specified
            "count(//lst[@name='timing']/*)=0"

    );

    //explains
    assertQ(req("q", "*:*", "debug", CommonParams.RESULTS),
            "count(//str[@name='rawquerystring'])=0",
            "count(//str[@name='querystring'])=0",
            "count(//str[@name='parsedquery'])=0",
            "count(//str[@name='parsedquery_toString'])=0",
            "count(//lst[@name='explain']/*)=3",
            "//lst[@name='explain']/str[@name='1']",
            "//lst[@name='explain']/str[@name='2']",
            "//lst[@name='explain']/str[@name='3']",
            "count(//str[@name='QParser'])=0",// make sure the QParser is specified
            "count(//lst[@name='timing']/*)=0"
    );

    assertQ(req("q", "*:*", "debug", CommonParams.RESULTS,
            "debug", CommonParams.QUERY),
            "//str[@name='rawquerystring']='*:*'",
            "//str[@name='querystring']='*:*'",
            "//str[@name='parsedquery']='MatchAllDocsQuery(*:*)'",
            "//str[@name='parsedquery_toString']='*:*'",
            "//str[@name='QParser']",// make sure the QParser is specified

            "count(//lst[@name='explain']/*)=3",
            "//lst[@name='explain']/str[@name='1']",
            "//lst[@name='explain']/str[@name='2']",
            "//lst[@name='explain']/str[@name='3']",

            "count(//lst[@name='timing']/*)=0"
    );
    
    //Grouping
    assertQ(req("q", "*:*", "debug", CommonParams.RESULTS,
        "group", CommonParams.TRUE,
        "group.field", "inStock_b1",
        "debug", CommonParams.TRUE), 
        "//str[@name='rawquerystring']='*:*'",
        "count(//lst[@name='explain']/*)=2"
    );
  }
  
  @Test
  public void testModifyRequestTrack() {
    DebugComponent component = new DebugComponent();
    List<SearchComponent> components = new ArrayList<>(1);
    components.add(component);
    for(int i = 0; i < 10; i++) {
      SolrQueryRequest req = req("q", "test query", "distrib", "true", CommonParams.REQUEST_ID, "123456-my_rid");
      SolrQueryResponse resp = new SolrQueryResponse();
      ResponseBuilder rb = new ResponseBuilder(req, resp, components);
      ShardRequest sreq = new ShardRequest();
      sreq.params = new ModifiableSolrParams();
      sreq.purpose = ShardRequest.PURPOSE_GET_FIELDS;
      sreq.purpose |= ShardRequest.PURPOSE_GET_DEBUG;
      //expecting the same results with debugQuery=true or debug=track
      if(random().nextBoolean()) {
        rb.setDebug(true);
      } else {
        rb.setDebug(false);
        rb.setDebugTrack(true);
        //should not depend on other debug options
        rb.setDebugQuery(random().nextBoolean());
        rb.setDebugTimings(random().nextBoolean());
        rb.setDebugResults(random().nextBoolean());
      }
      component.modifyRequest(rb, null, sreq);
      //if the request has debugQuery=true or debug=track, the sreq should get debug=track always
      assertTrue(Arrays.asList(sreq.params.getParams(CommonParams.DEBUG)).contains(CommonParams.TRACK));
      //the purpose must be added as readable param to be included in the shard logs
      assertEquals("GET_FIELDS,GET_DEBUG,SET_TERM_STATS", sreq.params.get(CommonParams.REQUEST_PURPOSE));
      //the rid must be added to be included in the shard logs
      assertEquals("123456-my_rid", sreq.params.get(CommonParams.REQUEST_ID));
      // close requests - this method obtains a searcher in order to access its StatsCache
      req.close();
    }
    
  }
  
  @Test
  public void testPrepare() throws IOException {
    DebugComponent component = new DebugComponent();
    List<SearchComponent> components = new ArrayList<>(1);
    components.add(component);
    SolrQueryRequest req;
    ResponseBuilder rb;
    for(int i = 0; i < 10; i++) {
      req = req("q", "test query", "distrib", "true");
      rb = new ResponseBuilder(req, new SolrQueryResponse(), components);
      rb.isDistrib = true;
      //expecting the same results with debugQuery=true or debug=track
      if(random().nextBoolean()) {
        rb.setDebug(true);
      } else {
        rb.setDebug(false);
        rb.setDebugTrack(true);
        //should not depend on other debug options
        rb.setDebugQuery(random().nextBoolean());
        rb.setDebugTimings(random().nextBoolean());
        rb.setDebugResults(random().nextBoolean());
      }
      component.prepare(rb);
      ensureRidPresent(rb, null);
    }
   
    req = req("q", "test query", "distrib", "true", CommonParams.REQUEST_ID, "123");
    rb = new ResponseBuilder(req, new SolrQueryResponse(), components);
    rb.isDistrib = true;
    rb.setDebug(true);
    component.prepare(rb);
    ensureRidPresent(rb, "123");
  }
  
  @SuppressWarnings("unchecked")
  private void ensureRidPresent(ResponseBuilder rb, String expectedRid) {
    SolrQueryRequest req = rb.req;
    SolrQueryResponse resp = rb.rsp;
    //a generated request ID should be added to the request
    String rid = req.getParams().get(CommonParams.REQUEST_ID);
    if(expectedRid == null) {
      assertTrue(rid + " Doesn't match expected pattern.", Pattern.matches(".*-collection1-[0-9]*-[0-9]+", rid));
    } else {
      assertEquals("Expecting " + expectedRid + " but found " + rid, expectedRid, rid);
    }
    //The request ID is added to the debug/track section
    assertEquals(rid, ((NamedList<Object>) rb.getDebugInfo().get("track")).get(CommonParams.REQUEST_ID));
    //RID must be added to the toLog, so that it's included in the main request log
    assertEquals(rid, resp.getToLog().get(CommonParams.REQUEST_ID));
  }

  //
  // NOTE: String representations are not meant to be exact or backward compatible.
  // For example, foo:bar^3, foo:bar^3.0 and (foo:bar)^3 are equivalent.  Use your
  // judgement when modifying these tests.
  //
  @Test
  public void testQueryToString() throws Exception {

    // test that both boosts are represented in a double-boost scenario
    assertQ(req("debugQuery", "true", "indent","true", "rows","0", "q", "(foo_s:aaa^3)^4"),
        "//str[@name='parsedquery'][.='foo_s:aaa^3.0^4.0']"
    );

    // test to see that extra parens are avoided
    assertQ(req("debugQuery", "true", "indent","true", "rows","0", "q", "+foo_s:aaa^3 -bar_s:bbb^0"),
        "//str[@name='parsedquery'][.='+foo_s:aaa^3.0 -bar_s:bbb^0.0']"
    );

    // test that parens are added when needed
    assertQ(req("debugQuery", "true", "indent", "true", "rows", "0", "q", "foo_s:aaa (bar_s:bbb baz_s:ccc)"),
        "//str[@name='parsedquery'][.='foo_s:aaa (bar_s:bbb baz_s:ccc)']"
    );

    // test boosts on subqueries
    assertQ(req("debugQuery", "true", "indent", "true", "rows", "0", "q", "foo_s:aaa^3 (bar_s:bbb baz_s:ccc)^4"),
        "//str[@name='parsedquery'][.='foo_s:aaa^3.0 (bar_s:bbb baz_s:ccc)^4.0']"
    );

    // test constant score query boost exists
    assertQ(req("debugQuery", "true", "indent", "true", "rows", "0", "q", "foo_s:aaa^=3"),
        "//str[@name='parsedquery'][contains(.,'3.0')]"
    );

  }
}
