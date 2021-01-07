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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseHS;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.CaffeineCache;
import org.apache.solr.search.DocSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.StringContains.containsString;


@SuppressWarnings("deprecation")
@LuceneTestCase.SuppressCodecs({"Lucene3x","Lucene40","Lucene41","Lucene42","Lucene45","Appending"})
public class TestJsonRequest extends SolrTestCaseHS {

  private static SolrInstances servers;  // for distributed testing

  @BeforeClass
  public static void beforeTests() throws Exception {
    systemSetPropertySolrDisableShardsWhitelist("true");
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
    systemClearPropertySolrDisableShardsWhitelist();
  }

  @Test
  public void testLocalJsonRequest() throws Exception {
    doJsonRequest(Client.localClient, false);
  }

  @Test
  public void testLocalJsonRequestWithTags() throws Exception {
    doJsonRequestWithTag(Client.localClient);
  }

  @Test
  public void testDistribJsonRequest() throws Exception {
    initServers();
    Client client = servers.getClient( random().nextInt() );
    client.queryDefaults().set( "shards", servers.getShards() );
    doJsonRequest(client, true);
  }

  public static void doJsonRequest(Client client, boolean isDistrib) throws Exception {
    addDocs(client);

    ignoreException("Expected JSON");

    // test json param
    client.testJQ( params("json","{query:'cat_s:A'}")
        , "response/numFound==2"
    );

    // invalid value
    SolrException ex = expectThrows(SolrException.class, () -> client.testJQ(params("q", "*:*", "json", "5")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertThat(ex.getMessage(), containsString("Expected JSON Object but got Long=5"));

    // this is to verify other json params are not affected
    client.testJQ( params("q", "cat_s:A", "json.limit", "1"),
        "response/numFound==2"
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

    doParamRefDslTest(client);

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

    // test query dsl
    client.testJQ( params("json", "{'query':'{!lucene}id:1'}")
        , "response/numFound==1"
    );

    client.testJQ( params("json", "{" +
            "  'query': {" +
            "    'bool' : {" +
            "      'should' : [" +
            "        {'lucene' : {'query' : 'id:1'}}," +
            "        'id:2'" +
            "      ]" +
            "    }" +
            "  }" +
            "}")
        , "response/numFound==2"
    );

    client.testJQ( params("json", "{" +
            "  'query': {" +
            "    'bool' : {" +
            "      'should' : [" +
            "        {'#MYTAG' : 'id:1'}," +  // tagged query (the tag should do nothing here)
            "        'id:2'" +
            "      ]" +
            "    }" +
            "  }" +
            "}")
        , "response/numFound==2"
    );

    client.testJQ( params("json", "{   " +
            " query : {" +
            "  boost : {" +
            "   query : {" +
            "    lucene : {      " +
            "     df : cat_s,      " +
            "     query : A     " +
            "    }" +
            "   },   " +
            "   b : 1.5 " +
            "  }  " +
            " } " +
            "}")
        , "response/numFound==2"
    );

    client.testJQ( params("json","{ " +
            " query : {" +
            "  bool : {" +
            "   must : {" +
            "    lucene : {" +
            "     q.op : AND," +
            "     df : cat_s," +
            "     query : A" +
            "    }" +
            "   }" +
            "   must_not : {'#NOT':{lucene : {query:'id: 1'}}}" +  // testing tagging syntax at the same time (the tag should do nothing here)
            "  }" +
            " }" +
            "}")
        , "response/numFound==1"
    );

    client.testJQ( params("json","{ " +
            " query : {" +
            "  bool : {" +
            "   must : {" +
            "    lucene : {" +
            "     q.op : AND," +
            "     df : cat_s," +
            "     query : A" +
            "    }" +
            "   }" +
            "   must_not : [{lucene : {query:'id: 1'}}]" +
            "  }" +
            " }" +
            "}")
        , "response/numFound==1"
    );

    assertCatANot1(client, "must");
    
    testFilterCachingLocally(client);

    client.testJQ( params("json","{" +
            " query : '*:*'," +
            " filter : {" +
            "  collapse : {" +
            "   field : cat_s" +
            "  } " +
            " } " +
            "}")
        , isDistrib ? "" : "response/numFound==2"
    );

    client.testJQ( params("json","{" +
            " query : {" +
            "  edismax : {" +
            "   query : 'A'," +
            "   qf : 'cat_s'," +
            "   bq : {" +
            "    edismax : {" +
            "     query : 'NJ'" +
            "     qf : 'where_s'" +
            "    }" +
            "   }" +
            "  }" +
            " }, " +
            " fields : id" +
            "}")
        , "response/numFound==2", isDistrib? "" : "response/docs==[{id:'4'},{id:'1'}]"
    );

    client.testJQ( params("json","{" +
            " query : {" +
            "  edismax : {" +
            "   query : 'A'," +
            "   qf : 'cat_s'," +
            "   bq : {" +
            "    edismax : {" +
            "     query : 'NY'" +
            "     qf : 'where_s'" +
            "    }" +
            "   }" +
            "  }" +
            " }, " +
            " fields : id" +
            "}")
        , "response/numFound==2", isDistrib? "" : "response/docs==[{id:'1'},{id:'4'}]"
    );

    client.testJQ( params("json","{" +
            " query : {" +
            "  dismax : {" +
            "   query : 'A NJ'" +
            "   qf : 'cat_s^0.1 where_s^100'" +
            "  } " +
            " }, " +
            " filter : '-id:2'," +
            " fields : id" +
            "}")
        , "response/numFound==3", isDistrib? "" : "response/docs==[{id:'4'},{id:'5'},{id:'1'}]"
    );

    client.testJQ( params("json","{" +
            " query : {" +
            "  dismax : {" +
            "   query : 'A NJ'" +
            "   qf : ['cat_s^100', 'where_s^0.1']" +
            "  } " +
            " }, " +
            " filter : '-id:2'," +
            " fields : id" +
            "}")
        , "response/numFound==3", isDistrib? "" :  "response/docs==[{id:'4'},{id:'1'},{id:'5'}]"
    );

    // TODO: this seems like a reasonable capability that we would want to support in the future.  It should be OK to make this pass.
    Exception e = expectThrows(Exception.class, () -> {
      client.testJQ(params("json", "{query:{'lucene':'foo_s:ignore_exception'}}"));
    });
    assertThat(e.getMessage(), containsString("foo_s"));

    // test failure on unknown parameter
    e = expectThrows(Exception.class, () -> {
      client.testJQ(params("json", "{query:'cat_s:A', foobar_ignore_exception:5}"), "response/numFound==2");
    });
    assertThat(e.getMessage(), containsString("foobar"));

    resetExceptionIgnores();
  }

  private static void doParamRefDslTest(Client client) throws Exception {
    // referencing in dsl                //nestedqp
    client.testJQ( params("json","{query: {query:  {param:'ref1'}}}", "ref1","{!field f=cat_s}A")
        , "response/numFound==2"
    );   
    // referencing json string param
    client.testJQ( params("json", random().nextBoolean()  ? 
            "{query:{query:{param:'ref1'}}}"  // nestedqp
           : "{query: {query: {query:{param:'ref1'}}}}",  // nestedqp, v local param  
          "json",random().nextBoolean() 
              ? "{params:{ref1:'{!field f=cat_s}A'}}" // string param  
              : "{queries:{ref1:{field:{f:cat_s,query:A}}}}" ) // qdsl
        , "response/numFound==2"
    );
    {                                                     // shortest top level ref
      final ModifiableSolrParams params = params("json","{query:{param:'ref1'}}");
      if (random().nextBoolean()) {
        params.add("ref1","cat_s:A"); // either to plain string
      } else {
        params.add("json","{queries:{ref1:{field:{f:cat_s,query:A}}}}");// or to qdsl
      }
      client.testJQ( params, "response/numFound==2");
    }  // ref in bool must
    client.testJQ( params("json","{query:{bool: {must:[{param:fq1},{param:fq2}]}}}",
        "json","{params:{fq1:'cat_s:A', fq2:'where_s:NY'}}", "json.fields", "id")
        , "response/docs==[{id:'1'}]"
    );// referencing dsl&strings from filters objs&array
    client.testJQ( params("json.filter","{param:fq1}","json.filter","{param:fq2}",
        "json", random().nextBoolean() ?
             "{queries:{fq1:{lucene:{query:'cat_s:A'}}, fq2:{lucene:{query:'where_s:NY'}}}}" : 
             "{params:{fq1:'cat_s:A', fq2:'where_s:NY'}}", 
        "json.fields", "id", "q", "*:*")
        , "response/docs==[{id:'1'}]"
    );
  }

  private static void testFilterCachingLocally(Client client) throws Exception {
    if(client.getClientProvider()==null) {
      final SolrQueryRequest request = req();
      try {
        final CaffeineCache<Query,DocSet> filterCache = (CaffeineCache<Query,DocSet>) request.getSearcher().getFilterCache();
        filterCache.clear();
        final TermQuery catA = new TermQuery(new Term("cat_s", "A"));
        assertNull("cache is empty",filterCache.get(catA));

        if(random().nextBoolean()) {
          if(random().nextBoolean()) {
            if(random().nextBoolean()) {
              assertCatANot1(client, "must");
            }else {
              assertCatANot1(client, "must", "cat_s:A");
            }
          } else {
            assertCatANot1(client, "must","{!lucene q.op=AND df=cat_s "+"cache="+random().nextBoolean()+"}A" );
          }   
        } else {
          assertCatANot1(client, "filter", "{!lucene q.op=AND df=cat_s cache=false}A");
        }
        assertNull("no cache still",filterCache.get(catA));

        if (random().nextBoolean()) {
          if (random().nextBoolean()) {
            assertCatANot1(client, "filter", "cat_s:A");
          } else {
            assertCatANot1(client, "filter");
          }
        } else {
          assertCatANot1(client, "filter","{!lucene q.op=AND df=cat_s cache=true}A");
        }
        assertNotNull("got cached ",filterCache.get(catA));

      } finally {
        request.close();
      }
    }
  }

  private static void assertCatANot1(Client client, final String occur) throws Exception {
    assertCatANot1(client, occur,  "{!lucene q.op=AND df=cat_s}A");
  }

  private static void assertCatANot1(Client client, final String occur, String catAclause) throws Exception {
    client.testJQ( params("json","{ " +
            " query : {" +
            "  bool : {" +
            "   " + occur + " : '"+ catAclause+ "'" +
            "   must_not : '{!lucene v=\\'id:1\\'}'" +
            "  }" +
            " }" +
            "}")
        , "response/numFound==1"
    );
  }

  public static void doJsonRequestWithTag(Client client) throws Exception {
    addDocs(client);

    try {
      client.testJQ( params("json","{" +
          " query : '*:*'," +
          " filter : { \"RCAT\" : \"cat_s:A OR ignore_exception\" }" + // without the pound, the tag would be interpreted as a query type
          "}", "json.facet", "{" +
          "categories:{ type:terms, field:cat_s, domain:{excludeTags:\"RCAT\"} }  " +
          "}"), "facets=={ count:2, " +
          " categories:{ buckets:[ {val:B, count:3}, {val:A, count:2} ]  }" +
          "}"
      );
      fail("no # no tag");
    } catch (Exception e) {
      // This is just the current mode of failure.  It's fine if it fails a different way (with a 400 error) in the future.
      assertTrue(e.getMessage().contains("expect a json object"));
    }

    final String taggedQ = "{" +
            " \"#RCAT\" : " + (random().nextBoolean() ? 
                "{" +
                "     term : {" +
                "       f : cat_s," +
                "       v : A" +
                "     } " +
                "   } " 
            : "\"cat_s:A\"")+
            " } ";
    boolean queryAndFilter = random().nextBoolean() ;
    client.testJQ(params("json", "{" +
        " query :" + ( queryAndFilter ? " '*:*', filter : " : "")
        + (!queryAndFilter || random().nextBoolean() ? taggedQ : "["+taggedQ+"]" )+
        "}", "json.facet", "{" +
        "categories:{ type:terms, field:cat_s, domain:{excludeTags:\"RCAT\"} }  " +
        "}"), "facets=={ count:2, " +
        " categories:{ buckets:[ {val:B, count:3}, {val:A, count:2} ]  }" +
        "}"
    );

    client.testJQ( params("json","{" +
        " query : '*:*'," +
        " filter : {" +
        "  term : {" +
        "   f : cat_s," +
        "   v : A" +
        "  } " +
        " } " +
        "}", "json.facet", "{" +
        "categories:{ type:terms, field:cat_s"
        +( random().nextBoolean() ? ", domain:{excludeTags:\"RCAT\"} ": " ")
        + "}  " +
        "}"), "facets=={ count:2, " +
        " categories:{ buckets:[ {val:A, count:2} ] }" +
        "}"
    );

    client.testJQ( params("json","{" +
        " query : '*:*'," +
        " filter : {" +
        "   \"#RCAT\" : {" +
        "     term : {" +
        "       f : cat_s," +
        "       v : A" +
        "     } " +
        "   } " +
        " } " +
        "}", "json.facet", "{" +
        "categories:{ type:terms, field:cat_s }  " +
        "}"), "facets=={ count:2, " +
        " categories:{ buckets:[ {val:A, count:2} ] }" +
        "}"
    );

    boolean multiTag = random().nextBoolean();
    client.testJQ(params("json", "{" +
            " query : '*:*'," +
            " filter : [" +
            "{ \"#RCAT"+(multiTag ? ",RCATSECONDTAG":"") + "\" :  \"cat_s:A\" }," +
            "{ \"#RWHERE\" : {" +
            "     term : {" +
            "       f : where_s," +
            "       v : NY" +
            "     } " +
            "   }" +
            "}]}"
        , "json.facet", "{" +
            "categories:{ type:terms, field:cat_s, domain:{excludeTags:\"RCAT\"} }  " +
            "countries:{ type:terms, field:where_s, domain:{excludeTags:\"RWHERE\"} }  " +
            "ids:{ type:terms, field:id, domain:{excludeTags:[\""+ (multiTag ? "RCATSECONDTAG":"RCAT")+ "\", \"RWHERE\"]} }  " +
            "}"), "facets==" + "{\n" +
        "    \"count\":1,\n" +
        "    \"categories\":{\n" +
        "      \"buckets\":[{\n" +
        "          \"val\":\"A\",\n" +
        "          \"count\":1},\n" +
        "        {\n" +
        "          \"val\":\"B\",\n" +
        "          \"count\":1}]},\n" +
        "    \"countries\":{\n" +
        "      \"buckets\":[{\n" +
        "          \"val\":\"NJ\",\n" +
        "          \"count\":1},\n" +
        "        {\n" +
        "          \"val\":\"NY\",\n" +
        "          \"count\":1}]},\n" +
        "    \"ids\":{\n" +
        "      \"buckets\":[{\n" +
        "          \"val\":\"1\",\n" +
        "          \"count\":1},\n" +
        "        {\n" +
        "          \"val\":\"2\",\n" +
        "          \"count\":1},\n" +
        "        {\n" +
        "          \"val\":\"3\",\n" +
        "          \"count\":1},\n" +
        "        {\n" +
        "          \"val\":\"4\",\n" +
        "          \"count\":1},\n" +
        "        {\n" +
        "          \"val\":\"5\",\n" +
        "          \"count\":1},\n" +
        "        {\n" +
        "          \"val\":\"6\",\n" +
        "          \"count\":1}]}}}"
    );
  }

  private static void addDocs(Client client) throws Exception {
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
  }

}
