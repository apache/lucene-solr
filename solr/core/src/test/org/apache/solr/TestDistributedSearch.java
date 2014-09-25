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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RangeFacet;
import org.apache.solr.cloud.ChaosMonkey;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;

/**
 * TODO? perhaps use:
 *  http://docs.codehaus.org/display/JETTY/ServletTester
 * rather then open a real connection?
 *
 *
 * @since solr 1.3
 */
@Slow
public class TestDistributedSearch extends BaseDistributedSearchTestCase {

  String t1="a_t";
  String i1="a_i1";
  String nint = "n_i";
  String tint = "n_ti";
  String tlong = "other_tl1";
  String tdate_a = "a_n_tdt";
  String tdate_b = "b_n_tdt";
  
  String oddField="oddField_s";
  String missingField="ignore_exception__missing_but_valid_field_t";
  String invalidField="ignore_exception__invalid_field_not_in_schema";

  @Override
  public void doTest() throws Exception {
    QueryResponse rsp = null;
    int backupStress = stress; // make a copy so we can restore


    del("*:*");
    indexr(id,1, i1, 100, tlong, 100,t1,"now is the time for all good men",
           tdate_a, "2010-04-20T11:00:00Z",
           tdate_b, "2009-08-20T11:00:00Z",
           "foo_f", 1.414f, "foo_b", "true", "foo_d", 1.414d);
    indexr(id,2, i1, 50 , tlong, 50,t1,"to come to the aid of their country.",
           tdate_a, "2010-05-02T11:00:00Z",
           tdate_b, "2009-11-02T11:00:00Z");
    indexr(id,3, i1, 2, tlong, 2,t1,"how now brown cow",
           tdate_a, "2010-05-03T11:00:00Z");
    indexr(id,4, i1, -100 ,tlong, 101,
           t1,"the quick fox jumped over the lazy dog", 
           tdate_a, "2010-05-03T11:00:00Z",
           tdate_b, "2010-05-03T11:00:00Z");
    indexr(id,5, i1, 500, tlong, 500 ,
           t1,"the quick fox jumped way over the lazy dog", 
           tdate_a, "2010-05-05T11:00:00Z");
    indexr(id,6, i1, -600, tlong, 600 ,t1,"humpty dumpy sat on a wall");
    indexr(id,7, i1, 123, tlong, 123 ,t1,"humpty dumpy had a great fall");
    indexr(id,8, i1, 876, tlong, 876,
           tdate_b, "2010-01-05T11:00:00Z",
           t1,"all the kings horses and all the kings men");
    indexr(id,9, i1, 7, tlong, 7,t1,"couldn't put humpty together again");

    commit();  // try to ensure there's more than one segment

    indexr(id,10, i1, 4321, tlong, 4321,t1,"this too shall pass");
    indexr(id,11, i1, -987, tlong, 987,
           t1,"An eye for eye only ends up making the whole world blind.");
    indexr(id,12, i1, 379, tlong, 379,
           t1,"Great works are performed, not by strength, but by perseverance.");
    indexr(id,13, i1, 232, tlong, 232,
           t1,"no eggs on wall, lesson learned", 
           oddField, "odd man out");

    indexr(id, "1001", "lowerfilt", "toyota"); // for spellcheck

    indexr(id, 14, "SubjectTerms_mfacet", new String[]  {"mathematical models", "mathematical analysis"});
    indexr(id, 15, "SubjectTerms_mfacet", new String[]  {"test 1", "test 2", "test3"});
    indexr(id, 16, "SubjectTerms_mfacet", new String[]  {"test 1", "test 2", "test3"});
    String[] vals = new String[100];
    for (int i=0; i<100; i++) {
      vals[i] = "test " + i;
    }
    indexr(id, 17, "SubjectTerms_mfacet", vals);
    
    

    for (int i=100; i<150; i++) {
      indexr(id, i);      
    }

    commit();

    handle.clear();
    handle.put("timestamp", SKIPVAL);
    handle.put("_version_", SKIPVAL); // not a cloud test, but may use updateLog

    // random value sort
    for (String f : fieldNames) {
      query("q","*:*", "sort",f+" desc");
      query("q","*:*", "sort",f+" asc");
    }

    // these queries should be exactly ordered and scores should exactly match
    query("q","*:*", "sort",i1+" desc");
    query("q","*:*", "sort","{!func}testfunc(add("+i1+",5))"+" desc");
    query("q","*:*", "sort",i1+" asc");
    query("q","*:*", "sort",i1+" desc", "fl","*,score");
    query("q","*:*", "sort","n_tl1 asc", "fl","*,score"); 
    query("q","*:*", "sort","n_tl1 desc");
    handle.put("maxScore", SKIPVAL);
    query("q","{!func}"+i1);// does not expect maxScore. So if it comes ,ignore it. JavaBinCodec.writeSolrDocumentList()
    //is agnostic of request params.
    handle.remove("maxScore");
    query("q","{!func}"+i1, "fl","*,score");  // even scores should match exactly here

    handle.put("highlighting", UNORDERED);
    handle.put("response", UNORDERED);

    handle.put("maxScore", SKIPVAL);
    query("q","quick");
    query("q","all","fl","id","start","0");
    query("q","all","fl","foofoofoo","start","0");  // no fields in returned docs
    query("q","all","fl","id","start","100");

    handle.put("score", SKIPVAL);
    query("q","quick","fl","*,score");
    query("q","all","fl","*,score","start","1");
    query("q","all","fl","*,score","start","100");

    query("q","now their fox sat had put","fl","*,score",
            "hl","true","hl.fl",t1);

    query("q","now their fox sat had put","fl","foofoofoo",
            "hl","true","hl.fl",t1);

    query("q","matchesnothing","fl","*,score");  

    // test that a single NOW value is propagated to all shards... if that is true
    // then the primary sort should always be a tie and then the secondary should always decide
    query("q","{!func}ms(NOW)", "sort","score desc,"+i1+" desc","fl","id");    

    query("q","*:*", "rows",0, "facet","true", "facet.field",t1, "facet.field",t1);
    query("q","*:*", "rows",0, "facet","true", "facet.field",t1,"facet.limit",1);
    query("q","*:*", "rows",0, "facet","true", "facet.query","quick", "facet.query","quick", "facet.query","all", "facet.query","*:*");
    query("q","*:*", "rows",0, "facet","true", "facet.field",t1, "facet.mincount",2);

    // a facet query to test out chars out of the ascii range
    query("q","*:*", "rows",0, "facet","true", "facet.query","{!term f=foo_s}international\u00ff\u01ff\u2222\u3333");

    // simple field facet on date fields
    rsp = query("q","*:*", "rows", 0,
                "facet","true", "facet.limit", 1, // TODO: limit shouldn't be needed: SOLR-6386
                "facet.field", tdate_a);
    assertEquals(1, rsp.getFacetFields().size());
    rsp = query("q","*:*", "rows", 0,
                "facet","true", "facet.limit", 1, // TODO: limit shouldn't be needed: SOLR-6386
                "facet.field", tdate_b, "facet.field", tdate_a);
    assertEquals(2, rsp.getFacetFields().size());

    // simple date facet on one field
    query("q","*:*", "rows",100, "facet","true", 
          "facet.date",tdate_a,
          "facet.date",tdate_a,
          "facet.date.other", "all", 
          "facet.date.start","2010-05-01T11:00:00Z", 
          "facet.date.gap","+1DAY", 
          "facet.date.end","2010-05-20T11:00:00Z");

    // date facet on multiple fields
    query("q","*:*", "rows",100, "facet","true", 
          "facet.date",tdate_a,
          "facet.date",tdate_b,
          "facet.date",tdate_a,
          "facet.date.other", "all", 
          "f."+tdate_b+".facet.date.start","2009-05-01T11:00:00Z", 
          "f."+tdate_b+".facet.date.gap","+3MONTHS", 
          "facet.date.start","2010-05-01T11:00:00Z", 
          "facet.date.gap","+1DAY", 
          "facet.date.end","2010-05-20T11:00:00Z");

    // simple range facet on one field
    query("q","*:*", "rows",100, "facet","true", 
          "facet.range",tlong,
          "facet.range",tlong,
          "facet.range.start",200, 
          "facet.range.gap",100, 
          "facet.range.end",900);

    // range facet on multiple fields
    query("q","*:*", "rows",100, "facet","true", 
          "facet.range",tlong, 
          "facet.range",i1, 
          "f."+i1+".facet.range.start",300, 
          "f."+i1+".facet.range.gap",87, 
          "facet.range.end",900,
          "facet.range.start",200, 
          "facet.range.gap",100, 
          "f."+tlong+".facet.range.end",900);

    // Test mincounts. Do NOT want to go through all the stuff where with validateControlData in query() method
    // Purposely packing a _bunch_ of stuff together here to insure that the proper level of mincount is used for
    // each
    ModifiableSolrParams minParams = new ModifiableSolrParams();
    minParams.set("q","*:*");
    minParams.set("rows", 1);
    minParams.set("facet", "true");
    minParams.set("facet.missing", "true");
    minParams.set("facet.field", i1);
    minParams.set("facet.missing", "true");
    minParams.set("facet.mincount", 2);

    // Return a separate section of ranges over i1. Should respect global range mincount
    minParams.set("facet.range", i1);
    minParams.set("f." + i1 + ".facet.range.start", 0);
    minParams.set("f." + i1 + ".facet.range.gap", 200);
    minParams.set("f." + i1 + ".facet.range.end", 1200);
    minParams.set("f." + i1 + ".facet.mincount", 4);


    // Return a separate section of ranges over tlong Should respect facet.mincount
    minParams.add("facet.range", tlong);
    minParams.set("f." + tlong + ".facet.range.start", 0);
    minParams.set("f." + tlong + ".facet.range.gap", 100);
    minParams.set("f." + tlong + ".facet.range.end", 1200);
    // Repeat with a range type of date
    minParams.add("facet.range", tdate_b);
    minParams.set("f." + tdate_b + ".facet.range.start", "2009-02-01T00:00:00Z");
    minParams.set("f." + tdate_b + ".facet.range.gap", "+1YEAR");
    minParams.set("f." + tdate_b + ".facet.range.end", "2011-01-01T00:00:00Z");
    minParams.set("f." + tdate_b + ".facet.mincount", 3);

    // Insure that global mincount is respected for facet queries
    minParams.set("facet.query", tdate_a + ":[2010-01-01T00:00:00Z TO 2011-01-01T00:00:00Z]"); // Should return some counts
    //minParams.set("facet.query", tdate_a + ":[* TO *]"); // Should be removed
    minParams.add("facet.query", tdate_b + ":[2008-01-01T00:00:00Z TO 2009-09-01T00:00:00Z]"); // Should be removed from response


    setDistributedParams(minParams);
    QueryResponse minResp = queryServer(minParams);

    ModifiableSolrParams eParams = new ModifiableSolrParams();
    eParams.set("q",tdate_b + ":[* TO *]");
    eParams.set("rows", 1000);
    eParams.set("fl", tdate_b);
    setDistributedParams(eParams);
    QueryResponse eResp = queryServer(eParams);

    // Check that exactly the right numbers of counts came through
    assertEquals("Should be exactly 2 range facets returned after minCounts taken into account ", 3, minResp.getFacetRanges().size());
    assertEquals("Should only be 1 query facets returned after minCounts taken into account ", 1, minResp.getFacetQuery().size());

    checkMinCountsField(minResp.getFacetField(i1).getValues(), new Object[]{null, 55L}); // Should just be the null entries for field

    checkMinCountsRange(minResp.getFacetRanges().get(0).getCounts(), new Object[]{"0", 5L}); // range on i1
    checkMinCountsRange(minResp.getFacetRanges().get(1).getCounts(), new Object[]{"0", 3L, "100", 3L}); // range on tlong
    checkMinCountsRange(minResp.getFacetRanges().get(2).getCounts(), new Object[]{"2009-02-01T00:00:00Z",  3L}); // date (range) on tvh

    assertTrue("Should have a facet for tdate_a", minResp.getFacetQuery().containsKey("a_n_tdt:[2010-01-01T00:00:00Z TO 2011-01-01T00:00:00Z]"));
    int qCount = minResp.getFacetQuery().get("a_n_tdt:[2010-01-01T00:00:00Z TO 2011-01-01T00:00:00Z]");
    assertEquals("tdate_a should be 5", qCount, 5);

    // Now let's do some queries, the above is getting too complex
    minParams = new ModifiableSolrParams();
    minParams.set("q","*:*");
    minParams.set("rows", 1);
    minParams.set("facet", "true");
    minParams.set("facet.mincount", 3);

    minParams.set("facet.query", tdate_a + ":[2010-01-01T00:00:00Z TO 2010-05-04T00:00:00Z]");
    minParams.add("facet.query", tdate_b + ":[2009-01-01T00:00:00Z TO 2010-01-01T00:00:00Z]"); // Should be removed
    setDistributedParams(minParams);
    minResp = queryServer(minParams);

    assertEquals("Should only be 1 query facets returned after minCounts taken into account ", 1, minResp.getFacetQuery().size());
    assertTrue("Should be an entry for a_n_tdt", minResp.getFacetQuery().containsKey("a_n_tdt:[2010-01-01T00:00:00Z TO 2010-05-04T00:00:00Z]"));
    qCount = minResp.getFacetQuery().get("a_n_tdt:[2010-01-01T00:00:00Z TO 2010-05-04T00:00:00Z]");
    assertEquals("a_n_tdt should have a count of 4 ", qCount, 4);
    //  variations of fl
    query("q","*:*", "fl","score","sort",i1 + " desc");
    query("q","*:*", "fl",i1 + ",score","sort",i1 + " desc");
    query("q","*:*", "fl", i1, "fl","score","sort",i1 + " desc");
    query("q","*:*", "fl", "id," + i1,"sort",i1 + " desc");
    query("q","*:*", "fl", "id", "fl",i1,"sort",i1 + " desc");
    query("q","*:*", "fl",i1, "fl", "id","sort",i1 + " desc");
    query("q","*:*", "fl", "id", "fl",nint, "fl",tint,"sort",i1 + " desc");
    query("q","*:*", "fl",nint, "fl", "id", "fl",tint,"sort",i1 + " desc");

    // basic spellcheck testing
    query("q", "toyata", "fl", "id,lowerfilt", "spellcheck", true, "spellcheck.q", "toyata", "qt", "spellCheckCompRH_Direct", "shards.qt", "spellCheckCompRH_Direct");

    stress=0;  // turn off stress... we want to tex max combos in min time
    for (int i=0; i<25*RANDOM_MULTIPLIER; i++) {
      String f = fieldNames[random().nextInt(fieldNames.length)];
      if (random().nextBoolean()) f = t1;  // the text field is a really interesting one to facet on (and it's multi-valued too)

      // we want a random query and not just *:* so we'll get zero counts in facets also
      // TODO: do a better random query
      String q = random().nextBoolean() ? "*:*" : "id:(1 3 5 7 9 11 13) OR id:[100 TO " + random().nextInt(50) + "]";

      int nolimit = random().nextBoolean() ? -1 : 10000;  // these should be equivalent

      // if limit==-1, we should always get exact matches
      query("q",q, "rows",0, "facet","true", "facet.field",f, "facet.limit",nolimit, "facet.sort","count", "facet.mincount",random().nextInt(5), "facet.offset",random().nextInt(10));
      query("q",q, "rows",0, "facet","true", "facet.field",f, "facet.limit",nolimit, "facet.sort","index", "facet.mincount",random().nextInt(5), "facet.offset",random().nextInt(10));
      // for index sort, we should get exact results for mincount <= 1
      query("q",q, "rows",0, "facet","true", "facet.field",f, "facet.sort","index", "facet.mincount",random().nextInt(2), "facet.offset",random().nextInt(10), "facet.limit",random().nextInt(11)-1);
    }
    stress = backupStress;  // restore stress

    // test faceting multiple things at once
    query("q","*:*", "rows",0, "facet","true", "facet.query","quick", "facet.query","all", "facet.query","*:*"
    ,"facet.field",t1);

    // test filter tagging, facet exclusion, and naming (multi-select facet support)
    query("q","*:*", "rows",0, "facet","true", "facet.query","{!key=myquick}quick", "facet.query","{!key=myall ex=a}all", "facet.query","*:*"
    ,"facet.field","{!key=mykey ex=a}"+t1
    ,"facet.field","{!key=other ex=b}"+t1
    ,"facet.field","{!key=again ex=a,b}"+t1
    ,"facet.field",t1
    ,"fq","{!tag=a}id:[1 TO 7]", "fq","{!tag=b}id:[3 TO 9]"
    );
    query("q", "*:*", "facet", "true", "facet.field", "{!ex=t1}SubjectTerms_mfacet", "fq", "{!tag=t1}SubjectTerms_mfacet:(test 1)", "facet.limit", "10", "facet.mincount", "1");

    // test field that is valid in schema but missing in all shards
    query("q","*:*", "rows",100, "facet","true", "facet.field",missingField, "facet.mincount",2);
    // test field that is valid in schema and missing in some shards
    query("q","*:*", "rows",100, "facet","true", "facet.field",oddField, "facet.mincount",2);

    query("q","*:*", "sort",i1+" desc", "stats", "true", "stats.field", "stats_dt");
    query("q","*:*", "sort",i1+" desc", "stats", "true", "stats.field", i1);
    query("q","*:*", "sort",i1+" desc", "stats", "true", "stats.field", tdate_a);
    query("q","*:*", "sort",i1+" desc", "stats", "true", "stats.field", tdate_b);

    query("q","*:*", "sort",i1+" desc", "stats", "true", 
          "fq", "{!tag=nothing}-*:*",
          "stats.field", "{!key=special_key ex=nothing}stats_dt");
    query("q","*:*", "sort",i1+" desc", "stats", "true", 
          "f.stats_dt.stats.calcdistinct", "true",
          "stats.field", "{!key=special_key}stats_dt");
    query("q","*:*", "sort",i1+" desc", "stats", "true", 
          "f.stats_dt.stats.calcdistinct", "true",
          "fq", "{!tag=xxx}id:[3 TO 9]",
          "stats.field", "{!key=special_key}stats_dt",
          "stats.field", "{!ex=xxx}stats_dt");

    query("q","*:*", "sort",i1+" desc", "stats", "true",
          // do a really simple query so distributed IDF doesn't cause problems
          // when comparing with control collection
          "stats.field", "{!lucene key=q_key}" + i1 + "foo_b:true",
          "stats.field", "{!func key=f_key}sum(" + tlong +","+i1+")");

    query("q","*:*", "sort",i1+" desc", "stats", "true",
          "stats.field", "stats_dt",
          "stats.field", i1,
          "stats.field", tdate_a,
          "stats.field", tdate_b);

    /*** TODO: the failure may come back in "exception"
    try {
      // test error produced for field that is invalid for schema
      query("q","*:*", "rows",100, "facet","true", "facet.field",invalidField, "facet.mincount",2);
      TestCase.fail("SolrServerException expected for invalid field that is not in schema");
    } catch (SolrServerException ex) {
      // expected
    }
    ***/

    // Try to get better coverage for refinement queries by turning off over requesting.
    // This makes it much more likely that we may not get the top facet values and hence
    // we turn of that checking.
    handle.put("facet_fields", SKIPVAL);    
    query("q","*:*", "rows",0, "facet","true", "facet.field",t1,"facet.limit",5, "facet.shard.limit",5);
    // check a complex key name
    query("q","*:*", "rows",0, "facet","true", "facet.field","{!key='$a b/c \\' \\} foo'}"+t1,"facet.limit",5, "facet.shard.limit",5);
    query("q","*:*", "rows",0, "facet","true", "facet.field","{!key='$a'}"+t1,"facet.limit",5, "facet.shard.limit",5);
    handle.remove("facet_fields");


    // index the same document to two servers and make sure things
    // don't blow up.
    if (clients.size()>=2) {
      index(id,100, i1, 107 ,t1,"oh no, a duplicate!");
      for (int i=0; i<clients.size(); i++) {
        index_specific(i, id,100, i1, 107 ,t1,"oh no, a duplicate!");
      }
      commit();
      query("q","duplicate", "hl","true", "hl.fl", t1);
      query("q","fox duplicate horses", "hl","true", "hl.fl", t1);
      query("q","*:*", "rows",100);
    }

    //SOLR 3161 ensure shards.qt=/update fails (anything but search handler really)
    // Also see TestRemoteStreaming#testQtUpdateFails()
    try {
      ignoreException("isShard is only acceptable");
      // query("q","*:*","shards.qt","/update","stream.body","<delete><query>*:*</query></delete>");
      // fail();
    } catch (SolrException e) {
      //expected
    }
    unIgnoreException("isShard is only acceptable");

    // test debugging
    // handle.put("explain", UNORDERED);
    handle.put("explain", SKIPVAL);  // internal docids differ, idf differs w/o global idf
    handle.put("debug", UNORDERED);
    handle.put("time", SKIPVAL);
    handle.put("track", SKIP); //track is not included in single node search
    query("q","now their fox sat had put","fl","*,score",CommonParams.DEBUG_QUERY, "true");
    query("q", "id:[1 TO 5]", CommonParams.DEBUG_QUERY, "true");
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.TIMING);
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.RESULTS);
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.QUERY);

    // Check Info is added to for each shard
    ModifiableSolrParams q = new ModifiableSolrParams();
    q.set("q", "*:*");
    q.set(ShardParams.SHARDS_INFO, true);
    setDistributedParams(q);
    rsp = queryServer(q);
    NamedList<?> sinfo = (NamedList<?>) rsp.getResponse().get(ShardParams.SHARDS_INFO);
    String shards = getShardsString();
    int cnt = StringUtils.countMatches(shards, ",")+1;
    
    assertNotNull("missing shard info", sinfo);
    assertEquals("should have an entry for each shard ["+sinfo+"] "+shards, cnt, sinfo.size());

    // test shards.tolerant=true
    for(int numDownServers = 0; numDownServers < jettys.size()-1; numDownServers++)
    {
      List<JettySolrRunner> upJettys = new ArrayList<>(jettys);
      List<SolrServer> upClients = new ArrayList<>(clients);
      List<JettySolrRunner> downJettys = new ArrayList<>();
      List<String> upShards = new ArrayList<>(Arrays.asList(shardsArr));
      for(int i=0; i<numDownServers; i++)
      {
        // shut down some of the jettys
        int indexToRemove = r.nextInt(upJettys.size());
        JettySolrRunner downJetty = upJettys.remove(indexToRemove);
        upClients.remove(indexToRemove);
        upShards.remove(indexToRemove);
        ChaosMonkey.stop(downJetty);
        downJettys.add(downJetty);
      }

      queryPartialResults(upShards, upClients, 
          "q","*:*",
          "facet","true", 
          "facet.field",t1,
          "facet.field",t1,
          "facet.limit",5,
          ShardParams.SHARDS_INFO,"true",
          ShardParams.SHARDS_TOLERANT,"true");

      queryPartialResults(upShards, upClients,
          "q", "*:*",
          "facet", "true",
          "facet.query", i1 + ":[1 TO 50]",
          "facet.query", i1 + ":[1 TO 50]",
          ShardParams.SHARDS_INFO, "true",
          ShardParams.SHARDS_TOLERANT, "true");

      // test group query
      // TODO: Remove this? This doesn't make any real sense now that timeAllowed might trigger early
      //       termination of the request during Terms enumeration/Query expansion.
      //       During such an exit, partial results isn't supported as it wouldn't make any sense.
      // Increasing the timeAllowed from 1 to 100 for now.
      queryPartialResults(upShards, upClients,
          "q", "*:*",
          "rows", 100,
          "fl", "id," + i1,
          "group", "true",
          "group.query", t1 + ":kings OR " + t1 + ":eggs",
          "group.limit", 10,
          "sort", i1 + " asc, id asc",
          CommonParams.TIME_ALLOWED, 100,
          ShardParams.SHARDS_INFO, "true",
          ShardParams.SHARDS_TOLERANT, "true");

      queryPartialResults(upShards, upClients,
          "q", "*:*",
          "stats", "true",
          "stats.field", i1,
          ShardParams.SHARDS_INFO, "true",
          ShardParams.SHARDS_TOLERANT, "true");

      queryPartialResults(upShards, upClients,
          "q", "toyata",
          "spellcheck", "true",
          "spellcheck.q", "toyata",
          "qt", "spellCheckCompRH_Direct",
          "shards.qt", "spellCheckCompRH_Direct",
          ShardParams.SHARDS_INFO, "true",
          ShardParams.SHARDS_TOLERANT, "true");

      // restart the jettys
      for (JettySolrRunner downJetty : downJettys) {
        downJetty.start();
      }
    }

    // This index has the same number for every field
    
    // TODO: This test currently fails because debug info is obtained only
    // on shards with matches.
    // query("q","matchesnothing","fl","*,score", "debugQuery", "true");
    
    // Thread.sleep(10000000000L);

    del("*:*"); // delete all docs and test stats request
    commit();
    try {
      query("q", "*:*", "stats", "true", 
            "stats.field", "stats_dt", 
            "stats.field", i1, 
            "stats.field", tdate_a, 
            "stats.field", tdate_b,
            "stats.calcdistinct", "true");
    } catch (HttpSolrServer.RemoteSolrException e) {
      if (e.getMessage().startsWith("java.lang.NullPointerException"))  {
        fail("NullPointerException with stats request on empty index");
      } else  {
        throw e;
      }
    }
  }

  protected void checkMinCountsField(List<FacetField.Count> counts, Object[] pairs) {
    assertEquals("There should be exactly " + pairs.length / 2 + " returned counts. There were: " + counts.size(), counts.size(), pairs.length / 2);
    assertTrue("Variable len param must be an even number, it was: " + pairs.length, (pairs.length % 2) == 0);
    for (int pairs_idx = 0, counts_idx = 0; pairs_idx < pairs.length; pairs_idx += 2, counts_idx++) {
      String act_name = counts.get(counts_idx).getName();
      long act_count = counts.get(counts_idx).getCount();
      String exp_name = (String) pairs[pairs_idx];
      long exp_count = (long) pairs[pairs_idx + 1];
      assertEquals("Expected ordered entry " + exp_name + " at position " + counts_idx + " got " + act_name, act_name, exp_name);
      assertEquals("Expected count for entry: " + exp_name + " at position " + counts_idx + " got " + act_count, act_count, exp_count);
    }
  }

  protected void checkMinCountsRange(List<RangeFacet.Count> counts, Object[] pairs) {
    assertEquals("There should be exactly " + pairs.length / 2 + " returned counts. There were: " + counts.size(), counts.size(), pairs.length / 2);
    assertTrue("Variable len param must be an even number, it was: " + pairs.length, (pairs.length % 2) == 0);
    for (int pairs_idx = 0, counts_idx = 0; pairs_idx < pairs.length; pairs_idx += 2, counts_idx++) {
      String act_name = counts.get(counts_idx).getValue();
      long act_count = counts.get(counts_idx).getCount();
      String exp_name = (String) pairs[pairs_idx];
      long exp_count = (long) pairs[pairs_idx + 1];
      assertEquals("Expected ordered entry " + exp_name + " at position " + counts_idx + " got " + act_name, act_name, exp_name);
      assertEquals("Expected count for entry: " + exp_name + " at position " + counts_idx + " got " + act_count, act_count, exp_count);
    }
  }

  protected void queryPartialResults(final List<String> upShards,
                                     final List<SolrServer> upClients, 
                                     Object... q) throws Exception {
    
    final ModifiableSolrParams params = new ModifiableSolrParams();

    for (int i = 0; i < q.length; i += 2) {
      params.add(q[i].toString(), q[i + 1].toString());
    }
    // TODO: look into why passing true causes fails
    params.set("distrib", "false");
    final QueryResponse controlRsp = controlClient.query(params);
    // if time.allowed is specified then even a control response can return a partialResults header
    if (params.get(CommonParams.TIME_ALLOWED) == null)  {
      validateControlData(controlRsp);
    }

    params.remove("distrib");
    setDistributedParams(params);

    QueryResponse rsp = queryRandomUpServer(params,upClients);

    comparePartialResponses(rsp, controlRsp, upShards);

    if (stress > 0) {
      log.info("starting stress...");
      Thread[] threads = new Thread[nThreads];
      for (int i = 0; i < threads.length; i++) {
        threads[i] = new Thread() {
          @Override
          public void run() {
            for (int j = 0; j < stress; j++) {
              int which = r.nextInt(upClients.size());
              SolrServer client = upClients.get(which);
              try {
                QueryResponse rsp = client.query(new ModifiableSolrParams(params));
                if (verifyStress) {
                  comparePartialResponses(rsp, controlRsp, upShards);
                }
              } catch (SolrServerException e) {
                throw new RuntimeException(e);
              }
            }
          }
        };
        threads[i].start();
      }

      for (Thread thread : threads) {
        thread.join();
      }
    }
  }

  protected QueryResponse queryRandomUpServer(ModifiableSolrParams params, List<SolrServer> upClients) throws SolrServerException {
    // query a random "up" server
    int which = r.nextInt(upClients.size());
    SolrServer client = upClients.get(which);
    QueryResponse rsp = client.query(params);
    return rsp;
  }

  protected void comparePartialResponses(QueryResponse rsp, QueryResponse controlRsp, List<String> upShards)
  {
    NamedList<?> sinfo = (NamedList<?>) rsp.getResponse().get(ShardParams.SHARDS_INFO);
    
    assertNotNull("missing shard info", sinfo);
    assertEquals("should have an entry for each shard ["+sinfo+"] "+shards, shardsArr.length, sinfo.size());
    // identify each one
    for (Map.Entry<String,?> entry : sinfo) {
      String shard = entry.getKey();
      NamedList<?> info = (NamedList<?>) entry.getValue();
      boolean found = false;
      for(int i=0; i<shardsArr.length; i++) {
        String s = shardsArr[i];
        if (shard.contains(s)) {
          found = true;
          // make sure that it responded if it's up
          if (upShards.contains(s)) {
            assertTrue("Expected to find numFound in the up shard info",info.get("numFound") != null);
            assertTrue("Expected to find shardAddress in the up shard info",info.get("shardAddress") != null);
          }
          else {
            assertEquals("Expected to find the partialResults header set if a shard is down", Boolean.TRUE, rsp.getHeader().get("partialResults"));
            assertTrue("Expected to find error in the down shard info",info.get("error") != null);
          }
        }
      }
      assertTrue("Couldn't find shard " + shard + " represented in shards info", found);
    }
  }
  
  @Override
  public void validateControlData(QueryResponse control) throws Exception {
    super.validateControlData(control);
    assertNull("Expected the partialResults header to be null", control.getHeader().get("partialResults"));
  }
}
