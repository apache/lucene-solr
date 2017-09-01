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
package org.apache.solr.search.json;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseHS;

import org.apache.solr.common.params.CommonParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@LuceneTestCase.SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Lucene45","Appending"})
public class TestJsonRequest extends SolrTestCaseHS {

  private static SolrInstances servers;  // for distributed testing

  @BeforeClass
  public static void beforeTests() throws Exception {
    JSONTestUtil.failRepeatedKeys = true;
    initCore("solrconfig-tlog.xml","schema_latest.xml");
  }

  public static void initServers() throws Exception {
    if (servers == null) {
      servers = new SolrInstances(3, "solrconfig-tlog.xml","schema_latest.xml");
    }
  }

  @AfterClass
  public static void afterTests() throws Exception {
    JSONTestUtil.failRepeatedKeys = false;
    if (servers != null) {
      servers.stop();
      servers = null;
    }
  }

  @Test
  public void testLocalJsonRequest() throws Exception {
    doJsonRequest(Client.localClient);
  }

  @Test
  public void testDistribJsonRequest() throws Exception {
    initServers();
    initServers();
    Client client = servers.getClient( random().nextInt() );
    client.queryDefaults().set( "shards", servers.getShards() );
    doJsonRequest(client);
  }


  public static void doJsonRequest(Client client) throws Exception {
    client.deleteByQuery("*:*", null);
    client.add(sdoc("id", "1", "cat_s", "A", "where_s", "NY"), null);
    client.add(sdoc("id", "2", "cat_s", "B", "where_s", "NJ"), null);
    client.add(sdoc("id", "3"), null);
    client.commit();
    client.add(sdoc("id", "4", "cat_s", "A", "where_s", "NJ"), null);
    client.add(sdoc("id", "5", "cat_s", "B", "where_s", "NJ"), null);
    client.commit();
    client.add(sdoc("id", "6", "cat_s", "B", "where_s", "NY"), null);
    client.commit();


    // test json param
    client.testJQ( params("json","{query:'cat_s:A'}")
        , "response/numFound==2"
    );

    // test multiple json params
    client.testJQ( params("json","{query:'cat_s:A'}", "json","{filter:'where_s:NY'}")
        , "response/numFound==1"
    );

    // test multiple json params with one being zero length
    client.testJQ( params("json","{query:'cat_s:A'}", "json","{filter:'where_s:NY'}", "json","")
        , "response/numFound==1"
    );

    // test multiple json params with one being a comment
    client.testJQ( params("json","{query:'cat_s:A'}", "json","{filter:'where_s:NY'}", "json","/* */")
        , "response/numFound==1"
    );

    // test merging multi-valued params into list
    client.testJQ( params("json","{query:'*:*'}", "json","{filter:'where_s:NY'}", "json","{filter:'cat_s:A'}")
        , "response/numFound==1"
    );

    // test merging multi-valued params into list, second value is already list
    client.testJQ( params("json","{query:'*:*'}", "json","{filter:'where_s:NY'}", "json","{filter:['cat_s:A']}")
        , "response/numFound==1"
    );

    // test merging multi-valued params into list, first value is already list
    client.testJQ( params("json","{query:'*:*'}", "json","{filter:['where_s:NY']}", "json","{filter:'cat_s:A'}")
        , "response/numFound==1"
    );

    // test merging multi-valued params into list, both values are already list
    client.testJQ( params("json","{query:'*:*'}", "json","{filter:['where_s:NY']}", "json","{filter:['cat_s:A']}")
        , "response/numFound==1"
    );

    // test inserting and merging with paths
    client.testJQ( params("json.query","'*:*'", "json.filter","'where_s:NY'", "json.filter","'cat_s:A'")
        , "response/numFound==1"
    );

    // test inserting and merging with paths with an empty string and a comment
    client.testJQ( params("json.query","'*:*'", "json.filter","'where_s:NY'", "json.filter","'cat_s:A'", "json.filter","", "json.filter","/* */")
        , "response/numFound==1"
    );

    // test overwriting of non-multivalued params
    client.testJQ( params("json.query","'foo_s:NONE'", "json.filter","'where_s:NY'", "json.filter","'cat_s:A'", "json.query","'*:*'")
        , "response/numFound==1"
    );

    // normal parameter specified in the params block, including numeric params cast back to string
    client.testJQ( params("json","{params:{q:'*:*', fq:['cat_s:A','where_s:NY'], start:0, rows:5, fl:id}}")
        , "response/docs==[{id:'1'}]"
    );
    client.testJQ( params("json","{params:{q:'*:*', fq:['cat_s:A','where_s:(NY OR NJ)'], start:0, rows:1, fl:id, sort:'where_s asc'}}")
        , "response/numFound==2"
        , "response/docs==[{id:'4'}]"
    );
    client.testJQ( params("json","{params:{q:'*:*', fq:['cat_s:A','where_s:(NY OR NJ)'], start:0, rows:1, fl:[id,'x:5.5'], sort:'where_s asc'}}")
        , "response/numFound==2"
        , "response/docs==[{id:'4', x:5.5}]"
    );
    // test merge params
    client.testJQ( params("json","{params:{q:'*:*'}}", "json","{params:{fq:['cat_s:A','where_s:(NY OR NJ)'], start:0, rows:1, fl:[id,'x:5.5']}}", "json","{params:{sort:'where_s asc'}}")
        , "response/numFound==2"
        , "response/docs==[{id:'4', x:5.5}]"
    );


    // test offset/limit/sort/fields
    client.testJQ( params("json.query","'*:*'",  "json.offset","1", "json.limit","2", "json.sort","'id desc'", "json.fields","'id'")
        , "response/docs==[{id:'5'},{id:'4'}]"
    );
    // test offset/limit/sort/fields, multi-valued json.fields
    client.testJQ( params("json.query","'*:*'",  "json.offset","1", "json.limit","2", "json.sort","'id desc'", "json.fields","'id'", "json.fields","'x:5.5'")
        , "response/docs==[{id:'5', x:5.5},{id:'4', x:5.5}]"
    );
    // test offset/limit/sort/fields, overwriting non-multivalued params
    client.testJQ( params("json.query","'*:*'",  "json.offset","17", "json.offset","1", "json.limit","42", "json.limit","2", "json.sort","'id asc'", "json.sort","'id desc'", "json.fields","'id'", "json.fields","'x:5.5'")
        , "response/docs==[{id:'5', x:5.5},{id:'4', x:5.5}]"
    );



    // test templating before parsing JSON
    client.testJQ( params("json","${OPENBRACE} query:'cat_s:A' ${CLOSEBRACE}", "json","${OPENBRACE} filter:'where_s:NY'${CLOSEBRACE}",  "OPENBRACE","{", "CLOSEBRACE","}")
        , "response/numFound==1"
    );

    // test templating with params defined in the JSON itself!  Do we want to keep this functionality?
    client.testJQ( params("json","{params:{V1:A,V2:NY}, query:'cat_s:${V1}'}", "json","{filter:'where_s:${V2}'}")
        , "response/numFound==1"
    );


    //
    // with body
    //
    client.testJQ(params(CommonParams.STREAM_BODY, "{query:'cat_s:A'}", "stream.contentType", "application/json")
        , "response/numFound==2"
    );

    // test body in conjunction with query params
    client.testJQ(params(CommonParams.STREAM_BODY, "{query:'cat_s:A'}", "stream.contentType", "application/json", "json.filter", "'where_s:NY'")
        , "response/numFound==1"
    );

    // test that json body in params come "after" (will overwrite)
    client.testJQ(params(CommonParams.STREAM_BODY, "{query:'*:*', filter:'where_s:NY'}", "stream.contentType", "application/json", "json","{query:'cat_s:A'}")
        , "response/numFound==1"
    );

    // test that json.x params come after body
    client.testJQ(params(CommonParams.STREAM_BODY, "{query:'*:*', filter:'where_s:NY'}", "stream.contentType", "application/json", "json.query","'cat_s:A'")
        , "response/numFound==1"
    );


    // test facet with json body
    client.testJQ(params(CommonParams.STREAM_BODY, "{query:'*:*', facet:{x:'unique(where_s)'}}", "stream.contentType", "application/json")
        , "facets=={count:6,x:2}"
    );

    // test facet with json body, insert additional facets via query parameter
    client.testJQ(params(CommonParams.STREAM_BODY, "{query:'*:*', facet:{x:'unique(where_s)'}}", "stream.contentType", "application/json", "json.facet.y","{terms:{field:where_s}}", "json.facet.z","'unique(where_s)'")
        , "facets=={count:6,x:2, y:{buckets:[{val:NJ,count:3},{val:NY,count:2}]}, z:2}"
    );

    // test debug
    client.testJQ( params("json","{query:'cat_s:A'}", "json.filter","'where_s:NY'", "debug","true")
        , "debug/json=={query:'cat_s:A', filter:'where_s:NY'}"
    );


    try {
      // test failure on unknown parameter
      client.testJQ(params("json", "{query:'cat_s:A', foobar_ignore_exception:5}")
          , "response/numFound==2"
      );
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("foobar"));
    }

  }

}
