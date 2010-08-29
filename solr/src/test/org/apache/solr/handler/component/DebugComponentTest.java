package org.apache.solr.handler.component;
/**
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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
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
    assertU(adoc("id", "1", "title", "this is a title."));
    assertU(adoc("id", "2", "title", "this is another title."));
    assertU(adoc("id", "3", "title", "Mary had a little lamb."));
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
            "//lst[@name='timing']/double[@name='time']", //make sure we have a time value, but don't specify it's result
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
            "//lst[@name='timing']/double[@name='time']", //make sure we have a time value, but don't specify it's result
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
            "//lst[@name='timing']/double[@name='time']", //make sure we have a time value, but don't specify it's result
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

  }
}
