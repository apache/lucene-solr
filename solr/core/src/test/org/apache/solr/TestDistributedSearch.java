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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RangeFacet;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams.FacetRangeMethod;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.StatsParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.handler.component.StatsComponentTest.StatSetCombinations;
import org.apache.solr.handler.component.StatsField.Stat;
import org.apache.solr.handler.component.TrackingShardHandlerFactory;
import org.apache.solr.handler.component.TrackingShardHandlerFactory.RequestTrackingQueue;
import org.apache.solr.handler.component.TrackingShardHandlerFactory.ShardRequestAndParams;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO? perhaps use:
 *  http://docs.codehaus.org/display/JETTY/ServletTester
 * rather then open a real connection?
 *
 *
 * @since solr 1.3
 */
@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-9061")
public class TestDistributedSearch extends BaseDistributedSearchTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  String t1="a_t";
  String i1 = pickRandom("a_i1", "a_i_p", "a_i_ni_p");
  String nint = pickRandom("n_i", "n_is_p", "n_is_ni_p");
  String tint = "n_ti";
  String tlong = "other_tl1";
  String tdate_a = "a_n_tdt";
  String tdate_b = "b_n_tdt";
  
  String oddField="oddField_s";
  String s1="a_s";
  String missingField="ignore_exception__missing_but_valid_field_t";
  String invalidField="ignore_exception__invalid_field_not_in_schema";

  @Override
  protected String getSolrXml() {
    return "solr-trackingshardhandler.xml";
  }

  @BeforeClass
  public static void beforeClass() {
    // we shutdown a jetty and start it and try to use
    // the same http client pretty fast - this lowered setting makes sure
    // we validate the connection before use on the restarted
    // server so that we don't use a bad one
    System.setProperty("validateAfterInactivity", "200");
    
    System.setProperty("solr.httpclient.retries", "0");
    System.setProperty("distribUpdateSoTimeout", "5000");
    

  }

  public TestDistributedSearch() {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
  }
  
  @Test
  @SuppressWarnings({"unchecked"})
  public void test() throws Exception {
    
    assertEquals(clients.size(), jettys.size());
    
    QueryResponse rsp = null;
    int backupStress = stress; // make a copy so we can restore

    del("*:*");
    indexr(id,1, i1, 100, tlong, 100,t1,"now is the time for all good men",
           "foo_sev_enum", "Medium",
           tdate_a, "2010-04-20T11:00:00Z",
           tdate_b, "2009-08-20T11:00:00Z",
           "foo_f", 1.414f, "foo_b", "true", "foo_d", 1.414d, 
           s1, "z${foo}");
    indexr(id,2, i1, 50 , tlong, 50,t1,"to come to the aid of their country.",
           "foo_sev_enum", "Medium",
           "foo_sev_enum", "High",
           tdate_a, "2010-05-02T11:00:00Z",
           tdate_b, "2009-11-02T11:00:00Z",
           s1, "z${foo}");
    indexr(id,3, i1, 2, tlong, 2,t1,"how now brown cow",
           tdate_a, "2010-05-03T11:00:00Z",
           s1, "z${foo}");
    indexr(id,4, i1, -100 ,tlong, 101,
           t1,"the quick fox jumped over the lazy dog", 
           tdate_a, "2010-05-03T11:00:00Z",
           tdate_b, "2010-05-03T11:00:00Z",
           s1, "a");
    indexr(id,5, i1, 500, tlong, 500 ,
           t1,"the quick fox jumped way over the lazy dog", 
           tdate_a, "2010-05-05T11:00:00Z",
           s1, "b");
    indexr(id,6, i1, -600, tlong, 600 ,t1,"humpty dumpy sat on a wall", s1, "c");
    indexr(id,7, i1, 123, tlong, 123 ,t1,"humpty dumpy had a great fall", s1, "d");
    indexr(id,8, i1, 876, tlong, 876,
           tdate_b, "2010-01-05T11:00:00Z",
           "foo_sev_enum", "High",
           t1,"all the kings horses and all the kings men", s1, "e");
    indexr(id,9, i1, 7, tlong, 7,t1,"couldn't put humpty together again", s1, "f");

    commit();  // try to ensure there's more than one segment

    indexr(id,10, i1, 4321, tlong, 4321,t1,"this too shall pass", s1, "g");
    indexr(id,11, i1, -987, tlong, 987,
           "foo_sev_enum", "Medium",
           t1,"An eye for eye only ends up making the whole world blind.", s1, "h");
    indexr(id,12, i1, 379, tlong, 379,
           t1,"Great works are performed, not by strength, but by perseverance.", s1, "i");
    indexr(id,13, i1, 232, tlong, 232,
           t1,"no eggs on wall, lesson learned", 
           oddField, "odd man out", s1, "j");

    indexr(id, "1001", "lowerfilt", "toyota", s1, "k"); // for spellcheck

    indexr(id, 14, "SubjectTerms_mfacet", new String[]  {"mathematical models", "mathematical analysis"}, s1, "l");
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

    //Test common query parameters.
    validateCommonQueryParameters();

    // random value sort
    for (String f : fieldNames) {
      query("q","*:*", "sort",f+" desc");
      query("q","*:*", "sort",f+" asc");
    }

    // these queries should be exactly ordered and scores should exactly match
    query("q","*:*", "sort",i1+" desc");
    query("q","*:*", "sort","{!func}testfunc(add("+i1+",5))"+" desc");
    query("q",i1 + "[* TO *]", "sort",i1+" asc");
    query("q","*:*", "sort",i1+" asc, id desc");
    query("q","*:*", "sort",i1+" desc", "fl","*,score");
    query("q","*:*", "sort","n_tl1 asc", "fl","*,score"); 
    query("q","*:*", "sort","n_tl1 desc");
    
    handle.put("maxScore", SKIPVAL);
    testMinExactCount();
    
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
    
    String facetQuery = "id_i1:[1 TO 15]";

    // simple range facet on one field
    query("q",facetQuery, "rows",100, "facet","true", 
          "facet.range",tlong,
          "facet.range",tlong,
          "facet.range.start",200, 
          "facet.range.gap",100, 
          "facet.range.end",900,
          "facet.range.method", FacetRangeMethod.FILTER);
    
    // simple range facet on one field using dv method
    query("q",facetQuery, "rows",100, "facet","true", 
          "facet.range",tlong,
          "facet.range",tlong,
          "facet.range.start",200, 
          "facet.range.gap",100, 
          "facet.range.end",900,
          "facet.range.method", FacetRangeMethod.DV);

    // range facet on multiple fields
    query("q",facetQuery, "rows",100, "facet","true", 
          "facet.range",tlong, 
          "facet.range",i1, 
          "f."+i1+".facet.range.start",300, 
          "f."+i1+".facet.range.gap",87, 
          "facet.range.end",900,
          "facet.range.start",200, 
          "facet.range.gap",100, 
          "f."+tlong+".facet.range.end",900,
          "f."+i1+".facet.range.method", FacetRangeMethod.FILTER,
          "f."+tlong+".facet.range.method", FacetRangeMethod.DV);
    
    // range facet with "other" param
    QueryResponse response = query("q",facetQuery, "rows",100, "facet","true", 
          "facet.range",tlong,
          "facet.range.start",200, 
          "facet.range.gap",100, 
          "facet.range.end",900,
          "facet.range.other","all");
    assertEquals(tlong, response.getFacetRanges().get(0).getName());
    assertEquals(6, response.getFacetRanges().get(0).getBefore());
    assertEquals(5, response.getFacetRanges().get(0).getBetween());
    assertEquals(2, response.getFacetRanges().get(0).getAfter());

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
    handle.put("did", SKIPVAL);
    query("q","*:*", "fl","did:[docid]","sort",i1 + " desc");
    handle.remove("did");
    query("q","*:*", "fl","log(" + tlong + "),abs(" + tlong + "),score","sort",i1 + " desc");
    query("q","*:*", "fl","n_*","sort",i1 + " desc");

    // basic spellcheck testing
    query("q", "toyata", "fl", "id,lowerfilt", "spellcheck", true, "spellcheck.q", "toyata", "qt", "/spellCheckCompRH_Direct", "shards.qt", "/spellCheckCompRH_Direct");

    stress=0;  // turn off stress... we want to tex max combos in min time
    for (int i=0; i<25*RANDOM_MULTIPLIER; i++) {
      String f = fieldNames[random().nextInt(fieldNames.length)];
      if (random().nextBoolean()) f = t1;  // the text field is a really interesting one to facet on (and it's multi-valued too)

      // we want a random query and not just *:* so we'll get zero counts in facets also
      // TODO: do a better random query
      String q = random().nextBoolean() ? "*:*" : "id:(1 3 5 7 9 11 13) OR id_i1:[100 TO " + random().nextInt(50) + "]";

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
    queryAndCompareUIF("q","*:*", "rows",0, "facet","true", "facet.query","{!key=myquick}quick", "facet.query","{!key=myall ex=a}all", "facet.query","*:*"
    ,"facet.field","{!key=mykey ex=a}"+t1
    ,"facet.field","{!key=other ex=b}"+t1
    ,"facet.field","{!key=again ex=a,b}"+t1
    ,"facet.field",t1
    ,"fq","{!tag=a}id_i1:[1 TO 7]", "fq","{!tag=b}id_i1:[3 TO 9]"
    );
    queryAndCompareUIF("q", "*:*", "facet", "true", "facet.field", "{!ex=t1}SubjectTerms_mfacet", "fq", "{!tag=t1}SubjectTerms_mfacet:(test 1)", "facet.limit", "10", "facet.mincount", "1");

    // test field that is valid in schema but missing in all shards
    query("q","*:*", "rows",100, "facet","true", "facet.field",missingField, "facet.mincount",2);
    // test field that is valid in schema and missing in some shards
    query("q","*:*", "rows",100, "facet","true", "facet.field",oddField, "facet.mincount",2);
    
    query("q","*:*", "sort",i1+" desc", "stats", "true", "stats.field", "stats_dt");
    query("q","*:*", "sort",i1+" desc", "stats", "true", "stats.field", i1);
    query("q","*:*", "sort",i1+" desc", "stats", "true", "stats.field", nint);

    handle.put("stddev", FUZZY);
    handle.put("sumOfSquares", FUZZY);
    query("q","*:*", "sort",i1+" desc", "stats", "true", "stats.field", tdate_a);
    query("q","*:*", "sort",i1+" desc", "stats", "true", "stats.field", tdate_b);
    handle.remove("stddev");
    handle.remove("sumOfSquares");


    rsp = query("q", "*:*", "sort", i1 + " desc", "stats", "true", 
                "stats.field", "{!cardinality='true'}" + oddField,
                "stats.field", "{!cardinality='true'}" + tlong);

    { // don't leak variabls

      // long
      FieldStatsInfo s = rsp.getFieldStatsInfo().get(tlong);
      assertNotNull("missing stats", s);
      assertEquals("wrong cardinality", Long.valueOf(13), s.getCardinality());
      //
      assertNull("expected null for min", s.getMin());
      assertNull("expected null for mean", s.getMean());
      assertNull("expected null for count", s.getCount());
      assertNull("expected null for calcDistinct", s.getCountDistinct());
      assertNull("expected null for distinct vals", s.getDistinctValues());
      assertNull("expected null for max", s.getMax());
      assertNull("expected null for missing", s.getMissing());
      assertNull("expected null for stddev", s.getStddev());
      assertNull("expected null for sum", s.getSum());
      assertNull("expected null for percentiles", s.getSum());

      // string
      s = rsp.getFieldStatsInfo().get(oddField);
      assertNotNull("missing stats", s);
      assertEquals("wrong cardinality", Long.valueOf(1), s.getCardinality());
      //
      assertNull("expected null for min", s.getMin());
      assertNull("expected null for mean", s.getMean());
      assertNull("expected null for count", s.getCount());
      assertNull("expected null for calcDistinct", s.getCountDistinct());
      assertNull("expected null for distinct vals", s.getDistinctValues());
      assertNull("expected null for max", s.getMax());
      assertNull("expected null for missing", s.getMissing());
      assertNull("expected null for stddev", s.getStddev());
      assertNull("expected null for sum", s.getSum());
      assertNull("expected null for percentiles", s.getSum());
    }

    query("q", "*:*", "sort", i1 + " desc", "stats", "true", "stats.field",
        "{!percentiles='1,2,3,4,5'}" + i1);
    
    query("q", "*:*", "sort", i1 + " desc", "stats", "true", "stats.field",
        "{!percentiles='1,20,30,40,98,99,99.9'}" + i1);
    
    rsp = query("q", "*:*", "sort", i1 + " desc", "stats", "true", "stats.field",
                "{!percentiles='1.0,99.999,0.001'}" + tlong);
    { // don't leak variabls
      Double[] expectedKeys = new Double[] { 1.0D, 99.999D, 0.001D };
      Double[] expectedVals = new Double[] { 2.0D, 4320.0D, 2.0D }; 
      FieldStatsInfo s = rsp.getFieldStatsInfo().get(tlong);
      assertNotNull("no stats for " + tlong, s);

      Map<Double,Double> p = s.getPercentiles();
      assertNotNull("no percentils", p);
      assertEquals("insufficient percentiles", expectedKeys.length, p.size());
      Iterator<Double> actualKeys = p.keySet().iterator();
      for (int i = 0; i < expectedKeys.length; i++) {
        Double expectedKey = expectedKeys[i];
        assertTrue("Ran out of actual keys as of : "+ i + "->" +expectedKey,
                   actualKeys.hasNext());
        assertEquals(expectedKey, actualKeys.next());
        assertEquals("percentiles are off: " + p.toString(),
                     expectedVals[i], p.get(expectedKey), 1.0D);
      }

      //
      assertNull("expected null for count", s.getMin());
      assertNull("expected null for count", s.getMean());
      assertNull("expected null for count", s.getCount());
      assertNull("expected null for calcDistinct", s.getCountDistinct());
      assertNull("expected null for distinct vals", s.getDistinctValues());
      assertNull("expected null for max", s.getMax());
      assertNull("expected null for missing", s.getMissing());
      assertNull("expected null for stddev", s.getStddev());
      assertNull("expected null for sum", s.getSum());
    }
    
    query("q", "*:*", "sort", i1 + " desc", "stats", "true", "stats.field",
        "{!percentiles='1,20,50,80,99'}" + tdate_a);

    query("q","*:*", "sort",i1+" desc", "stats", "true", 
          "fq", "{!tag=nothing}-*:*",
          "stats.field", "{!key=special_key ex=nothing}stats_dt");
    query("q","*:*", "sort",i1+" desc", "stats", "true", 
          "f.stats_dt.stats.calcdistinct", "true",
          "stats.field", "{!key=special_key}stats_dt");
    query("q","*:*", "sort",i1+" desc", "stats", "true", 
          "f.stats_dt.stats.calcdistinct", "true",
          "fq", "{!tag=xxx}id_i1:[3 TO 9]",
          "stats.field", "{!key=special_key}stats_dt",
          "stats.field", "{!ex=xxx}stats_dt");

    handle.put("stddev", FUZZY);
    handle.put("sumOfSquares", FUZZY);
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
    
    // only ask for "min" and "mean", explicitly exclude deps of mean, whitebox check shard responses
    try {
      RequestTrackingQueue trackingQueue = new RequestTrackingQueue();
      TrackingShardHandlerFactory.setTrackingQueue(jettys, trackingQueue);

      rsp = query("q","*:*", "sort",i1+" desc", "stats", "true",
                  "stats.field", "{!min=true sum=false mean=true count=false}" + i1);
      FieldStatsInfo s = rsp.getFieldStatsInfo().get(i1);
      assertNotNull("no stats for " + i1, s);
      //
      assertEquals("wrong min", -987.0D, (Double)s.getMin(), 0.0001D );
      assertEquals("wrong mean", 377.153846D, (Double)s.getMean(), 0.0001D );
      //
      assertNull("expected null for count", s.getCount());
      assertNull("expected null for calcDistinct", s.getCountDistinct());
      assertNull("expected null for distinct vals", s.getDistinctValues());
      assertNull("expected null for max", s.getMax());
      assertNull("expected null for missing", s.getMissing());
      assertNull("expected null for stddev", s.getStddev());
      assertNull("expected null for sum", s.getSum());
      assertNull("expected null for percentiles", s.getPercentiles());
      assertNull("expected null for cardinality", s.getCardinality());

      // sanity check deps relationship
      for (Stat dep : EnumSet.of(Stat.sum, Stat.count)) {
        assertTrue("Purpose of this test is to ensure that asking for some stats works even when the deps " +
                   "of those stats are explicitly excluded -- but the expected dep relationshp is no longer valid. " +
                   "ie: who changed the code and didn't change this test?, expected: " + dep,
                   Stat.mean.getDistribDeps().contains(dep));
      }

      // check our shard requests & responses - ensure we didn't get unneccessary stats from every shard
      int numStatsShardRequests = 0;
      EnumSet<Stat> shardStatsExpected = EnumSet.of(Stat.min, Stat.sum, Stat.count);
      for (List<ShardRequestAndParams> shard : trackingQueue.getAllRequests().values()) {
        for (ShardRequestAndParams shardReq : shard) {
          if (shardReq.params.getBool(StatsParams.STATS, false)) {
            numStatsShardRequests++;
            for (ShardResponse shardRsp : shardReq.sreq.responses) {
              NamedList<Object> shardStats =
                ((NamedList<NamedList<NamedList<Object>>>)
                 shardRsp.getSolrResponse().getResponse().get("stats")).get("stats_fields").get(i1);

              assertNotNull("no stard stats for " + i1, shardStats);
              //
              for (Map.Entry<String,Object> entry : shardStats) {
                Stat found = Stat.forName(entry.getKey());
                assertNotNull("found shardRsp stat key we were not expecting: " + entry, found);
                assertTrue("found stat we were not expecting: " + entry, shardStatsExpected.contains(found));
                
              }
            }
          }
        }
      }
      assertTrue("did't see any stats=true shard requests", 0 < numStatsShardRequests);
    } finally {
      TrackingShardHandlerFactory.setTrackingQueue(jettys, null);
    }
    
    // only ask for "min", "mean" and "stddev",
    rsp = query("q","*:*", "sort",i1+" desc", "stats", "true",
                "stats.field", "{!min=true mean=true stddev=true}" + i1);
    { // don't leak variables 
      FieldStatsInfo s = rsp.getFieldStatsInfo().get(i1);
      assertNotNull("no stats for " + i1, s);
      //
      assertEquals("wrong min", -987.0D, (Double)s.getMin(), 0.0001D );
      assertEquals("wrong mean", 377.153846D, (Double)s.getMean(), 0.0001D );
      assertEquals("wrong stddev", 1271.76215D, s.getStddev(), 0.0001D );
      //
      assertNull("expected null for count", s.getCount());
      assertNull("expected null for calcDistinct", s.getCountDistinct());
      assertNull("expected null for distinct vals", s.getDistinctValues());
      assertNull("expected null for max", s.getMax());
      assertNull("expected null for missing", s.getMissing());
      assertNull("expected null for sum", s.getSum());
      assertNull("expected null for percentiles", s.getPercentiles());
      assertNull("expected null for cardinality", s.getCardinality());
    }

    // request stats, but disable them all via param refs
    rsp = query("q","*:*", "sort",i1+" desc", "stats", "true", "doMin", "false",
                "stats.field", "{!min=$doMin}" + i1);
    { // don't leak variables 
      FieldStatsInfo s = rsp.getFieldStatsInfo().get(i1);
      // stats section should exist, even though stats should be null
      assertNotNull("no stats for " + i1, s);
      //
      assertNull("expected null for min", s.getMin() );
      assertNull("expected null for mean", s.getMean() );
      assertNull("expected null for stddev", s.getStddev() );
      //
      assertNull("expected null for count", s.getCount());
      assertNull("expected null for calcDistinct", s.getCountDistinct());
      assertNull("expected null for distinct vals", s.getDistinctValues());
      assertNull("expected null for max", s.getMax());
      assertNull("expected null for missing", s.getMissing());
      assertNull("expected null for sum", s.getSum());
      assertNull("expected null for percentiles", s.getPercentiles());
      assertNull("expected null for cardinality", s.getCardinality());
    }

    final String[] stats = new String[] {
      "min", "max", "sum", "sumOfSquares", "stddev", "mean", "missing", "count"
    };
    
    // ask for arbitrary pairs of stats
    for (String stat1 : stats) {
      for (String stat2 : stats) {
        // NOTE: stat1 might equal stat2 - good edge case to test for

        rsp = query("q","*:*", "sort",i1+" desc", "stats", "true",
                    "stats.field", "{!" + stat1 + "=true " + stat2 + "=true}" + i1);

        final List<String> statsExpected = new ArrayList<String>(2);
        statsExpected.add(stat1);
        if ( ! stat1.equals(stat2) ) {
          statsExpected.add(stat2);
        }

        // ignore the FieldStatsInfo convinience class, and look directly at the NamedList
        // so we don't need any sort of crazy reflection
        NamedList<Object> svals =
          ((NamedList<NamedList<NamedList<Object>>>)
           rsp.getResponse().get("stats")).get("stats_fields").get(i1);

        assertNotNull("no stats for field " + i1, svals);
        assertEquals("wrong quantity of stats", statsExpected.size(), svals.size());

        
        for (String s : statsExpected) {
          assertNotNull("stat shouldn't be null: " + s, svals.get(s));
          assertTrue("stat should be a Number: " + s + " -> " + svals.get(s).getClass(),
                     svals.get(s) instanceof Number);
          // some loose assertions since we're iterating over various stats
          if (svals.get(s) instanceof Double) {
            Double val = (Double) svals.get(s);
            assertFalse("stat shouldn't be NaN: " + s, val.isNaN());
            assertFalse("stat shouldn't be Inf: " + s, val.isInfinite());
            assertFalse("stat shouldn't be 0: " + s, val.equals(0.0D));
          } else {
            // count or missing
            assertTrue("stat should be count of missing: " + s,
                       ("count".equals(s) || "missing".equals(s)));
            assertTrue("stat should be a Long: " + s + " -> " + svals.get(s).getClass(),
                       svals.get(s) instanceof Long);
            Long val = (Long) svals.get(s);
            assertFalse("stat shouldn't be 0: " + s, val.equals(0L));
          }
        }
      }
    }
    
    // all of these diff ways of asking for min & calcdistinct should have the same result
    for (SolrParams p : new SolrParams[] {
        params("stats.field", "{!min=true calcdistinct=true}" + i1),
        params("stats.calcdistinct", "true",
               "stats.field", "{!min=true}" + i1),
        params("f."+i1+".stats.calcdistinct", "true",
               "stats.field", "{!min=true}" + i1),
        params("stats.calcdistinct", "false",
               "f."+i1+".stats.calcdistinct", "true",
               "stats.field", "{!min=true}" + i1),
        params("stats.calcdistinct", "false",
               "f."+i1+".stats.calcdistinct", "false",
               "stats.field", "{!min=true calcdistinct=true}" + i1),
        params("stats.calcdistinct", "false",
               "f."+i1+".stats.calcdistinct", "false",
               "stats.field", "{!min=true countDistinct=true distinctValues=true}" + i1),
        params("stats.field", "{!min=true countDistinct=true distinctValues=true}" + i1),
        params("yes", "true",
               "stats.field", "{!min=$yes countDistinct=$yes distinctValues=$yes}" + i1),
      }) {
      
      rsp = query(SolrParams.wrapDefaults
                  (p, params("q","*:*", "sort",i1+" desc", "stats", "true")));
      FieldStatsInfo s = rsp.getFieldStatsInfo().get(i1);
      assertNotNull(p+" no stats for " + i1, s);
      //
      assertEquals(p+" wrong min", -987.0D, (Double)s.getMin(), 0.0001D );
      assertEquals(p+" wrong calcDistinct", Long.valueOf(13), s.getCountDistinct());
      assertNotNull(p+" expected non-null list for distinct vals", s.getDistinctValues());
      assertEquals(p+" expected list for distinct vals", 13, s.getDistinctValues().size());
      //
      assertNull(p+" expected null for mean", s.getMean() );
      assertNull(p+" expected null for count", s.getCount());
      assertNull(p+" expected null for max", s.getMax());
      assertNull(p+" expected null for missing", s.getMissing());
      assertNull(p+" expected null for stddev", s.getStddev());
      assertNull(p+" expected null for sum", s.getSum());
      assertNull(p+" expected null for percentiles", s.getPercentiles());
      assertNull(p+" expected null for cardinality", s.getCardinality());
      
    }

    // all of these diff ways of excluding calcdistinct should have the same result
    for (SolrParams p : new SolrParams[] {
        params("stats.field", "{!min=true calcdistinct=false}" + i1),
        params("stats.calcdistinct", "false",
               "stats.field", "{!min=true}" + i1),
        params("f."+i1+".stats.calcdistinct", "false",
               "stats.field", "{!min=true}" + i1),
        params("stats.calcdistinct", "true",
               "f."+i1+".stats.calcdistinct", "false",
               "stats.field", "{!min=true}" + i1),
        params("stats.calcdistinct", "true",
               "f."+i1+".stats.calcdistinct", "true",
               "stats.field", "{!min=true calcdistinct=false}" + i1),
        params("stats.calcdistinct", "true",
               "f."+i1+".stats.calcdistinct", "true",
               "stats.field", "{!min=true countDistinct=false distinctValues=false}" + i1),
      }) {
      
      rsp = query(SolrParams.wrapDefaults
                  (p, params("q","*:*", "sort",i1+" desc", "stats", "true")));
      FieldStatsInfo s = rsp.getFieldStatsInfo().get(i1);
      assertNotNull(p+" no stats for " + i1, s);
      //
      assertEquals(p+" wrong min", -987.0D, (Double)s.getMin(), 0.0001D );
      //
      assertNull(p+" expected null for calcDistinct", s.getCountDistinct());
      assertNull(p+" expected null for distinct vals", s.getDistinctValues());
      //
      assertNull(p+" expected null for mean", s.getMean() );
      assertNull(p+" expected null for count", s.getCount());
      assertNull(p+" expected null for max", s.getMax());
      assertNull(p+" expected null for missing", s.getMissing());
      assertNull(p+" expected null for stddev", s.getStddev());
      assertNull(p+" expected null for sum", s.getSum());
      assertNull(p+" expected null for percentiles", s.getPercentiles());
      assertNull(p+" expected null for cardinality", s.getCardinality());
    }

    // this field doesn't exist in any doc in the result set.
    // ensure we get expected values for the stats we ask for, but null for the stats
    rsp = query("q","*:*", "sort",i1+" desc", "stats", "true",
                "stats.field", "{!min=true mean=true stddev=true}does_not_exist_i");
    { // don't leak variables 
      FieldStatsInfo s = rsp.getFieldStatsInfo().get("does_not_exist_i");
      assertNotNull("no stats for bogus field", s);

      // things we explicit expect because we asked for them
      // NOTE: min is expected to be null even though requested because of no values
      assertEquals("wrong min", null, s.getMin()); 
      assertTrue("mean should be NaN", ((Double)s.getMean()).isNaN());
      assertEquals("wrong stddev", 0.0D, s.getStddev(), 0.0D );

      // things that we didn't ask for, so they better be null
      assertNull("expected null for count", s.getCount());
      assertNull("expected null for calcDistinct", s.getCountDistinct());
      assertNull("expected null for distinct vals", s.getDistinctValues());
      assertNull("expected null for max", s.getMax());
      assertNull("expected null for missing", s.getMissing());
      assertNull("expected null for sum", s.getSum());
      assertNull("expected null for percentiles", s.getPercentiles());
      assertNull("expected null for cardinality", s.getCardinality());
    }

    // look at stats on non numeric fields
    //
    // not all stats are supported on every field type, so some of these permutations will 
    // result in no stats being computed but this at least lets us sanity check that for each 
    // of these field+stats(s) combinations we get consistent results between the distribted 
    // request and the single node situation.
    //
    // NOTE: percentiles excluded because it doesn't support simple 'true/false' syntax
    // (and since it doesn't work for non-numerics anyway, we aren't missing any coverage here)
    EnumSet<Stat> allStats = EnumSet.complementOf(EnumSet.of(Stat.percentiles));

    int numTotalStatQueries = 0;
    // don't go overboard, just do all permutations of 1 or 2 stat params, for each field & query
    final int numStatParamsAtOnce = 2; 
    for (int numParams = 1; numParams <= numStatParamsAtOnce; numParams++) {
      for (EnumSet<Stat> set : new StatSetCombinations(numParams, allStats)) {

        for (String field : new String[] {
            "foo_f", i1, tlong, tdate_a, oddField, "foo_sev_enum",
            // fields that no doc has any value in
            "bogus___s", "bogus___f", "bogus___i", "bogus___tdt", "bogus___sev_enum"
          }) {

          for ( String q : new String[] {
              "*:*",                         // all docs
              "bogus___s:bogus",             // no docs
              "id:" + random().nextInt(50 ), // 0 or 1 doc...
              "id:" + random().nextInt(50 ), 
              "id:" + random().nextInt(100), 
              "id:" + random().nextInt(100), 
              "id:" + random().nextInt(200) 
            }) {

            // EnumSets use natural ordering, we want to randomize the order of the params
            List<Stat> combo = new ArrayList<Stat>(set);
            Collections.shuffle(combo, random());
            
            StringBuilder paras = new StringBuilder("{!key=k ");
            
            for (Stat stat : combo) {
              paras.append(stat + "=true ");
            }
            
            paras.append("}").append(field);
            numTotalStatQueries++;
            rsp = query("q", q, "rows", "0", "stats", "true",
                        "stats.field", paras.toString());
            // simple assert, mostly relying on comparison with single shard
            FieldStatsInfo s = rsp.getFieldStatsInfo().get("k");
            assertNotNull(s);

            // TODO: if we had a programatic way to determine what stats are supported 
            // by what field types, we could make more confident asserts here.
          }
        }
      }
    }
    handle.remove("stddev");
    handle.remove("sumOfSquares");
    assertEquals("Sanity check failed: either test broke, or test changed, or you adjusted Stat enum" + 
                 " (adjust constant accordingly if intentional)",
                 5082, numTotalStatQueries);

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
    // Make sure there is no macro expansion for field values
    query("q","*:*", "rows",0, "facet","true", "facet.field",s1,"facet.limit",5, "facet.shard.limit",5);
    query("q","*:*", "rows",0, "facet","true", "facet.field",s1,"facet.limit",5, "facet.shard.limit",5, "expandMacros", "true");
    query("q","*:*", "rows",0, "facet","true", "facet.field",s1,"facet.limit",5, "facet.shard.limit",5, "expandMacros", "false");
    // Macro expansion should still work for the parameters
    query("q","*:*", "rows",0, "facet","true", "facet.field","${foo}", "f.${foo}.mincount", 1, "foo", s1);
    query("q","*:*", "rows",0, "facet","true", "facet.field","${foo}", "f.${foo}.mincount", 1, "foo", s1, "expandMacros", "true");

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

    //SolrException e = expectThrows(SolrException.class, () -> {
    //  ignoreException("isShard is only acceptable");
    //  query("q","*:*","shards.qt","/update","stream.body","<delete><query>*:*</query></delete>");
    //});
    unIgnoreException("isShard is only acceptable");

    // test debugging
    // handle.put("explain", UNORDERED);
    handle.put("explain", SKIPVAL);  // internal docids differ, idf differs w/o global idf
    handle.put("debug", UNORDERED);
    handle.put("time", SKIPVAL);
    handle.put("track", SKIP); //track is not included in single node search
    query("q","now their fox sat had put","fl","*,score",CommonParams.DEBUG_QUERY, "true");
    query("q", "id_i1:[1 TO 5]", CommonParams.DEBUG_QUERY, "true");
    query("q", "id_i1:[1 TO 5]", CommonParams.DEBUG, CommonParams.TIMING);
    query("q", "id_i1:[1 TO 5]", CommonParams.DEBUG, CommonParams.RESULTS);
    query("q", "id_i1:[1 TO 5]", CommonParams.DEBUG, CommonParams.QUERY);

    // SOLR-6545, wild card field list
    indexr(id, "19", "text", "d", "cat_a_sS", "1" ,t1, "2");
    commit();

    rsp = query("q", "id:19", "fl", "id", "fl", "*a_sS");
    assertFieldValues(rsp.getResults(), "id", "19");

    rsp = query("q", "id:19", "fl", "id," + t1 + ",cat*");
    assertFieldValues(rsp.getResults(), "id", "19");

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

    List<JettySolrRunner> upJettys = Collections.synchronizedList(new ArrayList<>(jettys));
    List<SolrClient> upClients = Collections.synchronizedList(new ArrayList<>(clients));
    List<JettySolrRunner> downJettys = Collections.synchronizedList(new ArrayList<>());
    List<String> upShards = Collections.synchronizedList(new ArrayList<>(Arrays.asList(shardsArr)));
    
    int cap =  Math.max(upJettys.size() - 1, 1);

    int numDownServers = random().nextInt(cap);
    for (int i = 0; i < numDownServers; i++) {
      if (upJettys.size() == 1) {
        continue;
      }
      // shut down some of the jettys
      int indexToRemove = r.nextInt(upJettys.size() - 1);
      JettySolrRunner downJetty = upJettys.remove(indexToRemove);
      upClients.remove(indexToRemove);
      upShards.remove(indexToRemove);
      downJetty.stop();
      downJettys.add(downJetty);
    }
    
    Thread.sleep(100);

    queryPartialResults(upShards, upClients,
        "q", "*:*",
        "facet", "true",
        "facet.field", t1,
        "facet.field", t1,
        "facet.limit", 5,
        ShardParams.SHARDS_INFO, "true",
        ShardParams.SHARDS_TOLERANT, "true");

    queryPartialResults(upShards, upClients,
        "q", "*:*",
        "facet", "true",
        "facet.query", i1 + ":[1 TO 50]",
        "facet.query", i1 + ":[1 TO 50]",
        ShardParams.SHARDS_INFO, "true",
        ShardParams.SHARDS_TOLERANT, "true");

    // test group query
    queryPartialResults(upShards, upClients,
        "q", "*:*",
        "rows", 100,
        "fl", "id," + i1,
        "group", "true",
        "group.query", t1 + ":kings OR " + t1 + ":eggs",
        "group.limit", 10,
        "sort", i1 + " asc, id asc",
        CommonParams.TIME_ALLOWED, 10000,
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
        "qt", "/spellCheckCompRH_Direct",
        "shards.qt", "/spellCheckCompRH_Direct",
        ShardParams.SHARDS_INFO, "true",
        ShardParams.SHARDS_TOLERANT, "true");

    // restart the jettys
    for (JettySolrRunner downJetty : downJettys) {
      downJetty.start();
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
    } catch (HttpSolrClient.RemoteSolrException e) {
      if (e.getMessage().startsWith("java.lang.NullPointerException"))  {
        fail("NullPointerException with stats request on empty index");
      } else  {
        throw e;
      }
    }
    
    String fieldName = "severity";
    indexr("id", "1", fieldName, "Not Available");
    indexr("id", "2", fieldName, "Low");
    indexr("id", "3", fieldName, "Medium");
    indexr("id", "4", fieldName, "High");
    indexr("id", "5", fieldName, "Critical");
    
    commit();
    
    rsp = query("q", "*:*", "stats", "true", "stats.field", fieldName);
    assertEquals(new EnumFieldValue(0, "Not Available"),
                 rsp.getFieldStatsInfo().get(fieldName).getMin());
    query("q", "*:*", "stats", "true", "stats.field", fieldName,  
          StatsParams.STATS_CALC_DISTINCT, "true");
    assertEquals(new EnumFieldValue(11, "Critical"),
                 rsp.getFieldStatsInfo().get(fieldName).getMax());

    handle.put("severity", UNORDERED); // this is stupid, but stats.facet doesn't guarantee order
    query("q", "*:*", "stats", "true", "stats.field", fieldName, 
          "stats.facet", fieldName);
  }

  private void testMinExactCount() throws Exception {
    assertIsExactHitCount("q","{!cache=false}dog OR men OR cow OR country OR dumpty", CommonParams.MIN_EXACT_COUNT, "200", CommonParams.ROWS, "2", CommonParams.SORT, "score desc, id asc");
    assertIsExactHitCount("q","{!cache=false}dog OR men OR cow OR country OR dumpty", CommonParams.MIN_EXACT_COUNT, "-1", CommonParams.ROWS, "2", CommonParams.SORT, "score desc, id asc");
    assertIsExactHitCount("q","{!cache=false}dog OR men OR cow OR country OR dumpty", CommonParams.MIN_EXACT_COUNT, "1", CommonParams.ROWS, "200", CommonParams.SORT, "score desc, id asc");
    assertIsExactHitCount("q","{!cache=false}dog OR men OR cow OR country OR dumpty", "facet", "true", "facet.field", s1, CommonParams.MIN_EXACT_COUNT,"1", CommonParams.ROWS, "200", CommonParams.SORT, "score desc, id asc");
    assertIsExactHitCount("q","{!cache=false}id:1", CommonParams.MIN_EXACT_COUNT,"1", CommonParams.ROWS, "1");
    assertApproximatedHitCount("q","{!cache=false}dog OR men OR cow OR country OR dumpty", CommonParams.MIN_EXACT_COUNT,"2", CommonParams.ROWS, "2", CommonParams.SORT, "score desc, id asc");
  }
  
  private void assertIsExactHitCount(Object... requestParams) throws Exception {
    QueryResponse response = query(requestParams);
    assertNotNull("Expecting exact hit count in response: " + response.getResults().toString(),
        response.getResults().getNumFoundExact());
    assertTrue("Expecting exact hit count in response: " + response.getResults().toString(),
        response.getResults().getNumFoundExact());
  }
  
  private void assertApproximatedHitCount(Object...requestParams) throws Exception {
    handle.put("numFound", SKIPVAL);
    QueryResponse response = query(requestParams);
    assertNotNull("Expecting numFoundExact in response: " + response.getResults().toString(),
        response.getResults().getNumFoundExact());
    assertFalse("Expecting aproximated results in response: " + response.getResults().toString(),
        response.getResults().getNumFoundExact());
    handle.remove("numFound", SKIPVAL);
  }

  /** comparing results with facet.method=uif */
  private void queryAndCompareUIF(Object ... params) throws Exception {
    final QueryResponse expect = query(params);
    
    final Object[] newParams = Arrays.copyOf(params, params.length+2);
    newParams[newParams.length-2] = "facet.method";
    newParams[newParams.length-1] = "uif";
    final QueryResponse uifResult = query(newParams);
    compareResponses(expect, uifResult);
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

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected void queryPartialResults(final List<String> upShards,
                                     final List<SolrClient> upClients,
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

    if (upClients.size() == 0) {
      return;
    }
    QueryResponse rsp = queryRandomUpServer(params, upClients);

    comparePartialResponses(rsp, controlRsp, upShards);

    if (stress > 0) {
      log.info("starting stress...");
      Set<Future<Object>> pending = new HashSet<>();;
      ExecutorCompletionService<Object> cs = new ExecutorCompletionService<>(executor);
      Callable[] threads = new Callable[nThreads];
      for (int i = 0; i < threads.length; i++) {
        threads[i] = new Callable() {
          @Override
          public Object call() {
            for (int j = 0; j < stress; j++) {
              int which = r.nextInt(upClients.size());
              SolrClient client = upClients.get(which);
              try {
                QueryResponse rsp = client.query(new ModifiableSolrParams(params));
                if (verifyStress) {
                  comparePartialResponses(rsp, controlRsp, upShards);
                }
              } catch (SolrServerException | IOException e) {
                throw new RuntimeException(e);
              }
            }
            return null;
          }
        };
        pending.add(cs.submit(threads[i]));
      }
      
      while (pending.size() > 0) {
        Future<Object> future = cs.take();
        pending.remove(future);
        future.get();
      }

    }
  }

  protected QueryResponse queryRandomUpServer(ModifiableSolrParams params, List<SolrClient> upClients)
      throws SolrServerException, IOException {
    // query a random "up" server
    SolrClient client;
    if (upClients.size() == 1) {
      client = upClients.get(0);
    } else {
      int which = r.nextInt(upClients.size() - 1);
      client = upClients.get(which);
    }

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
          // make sure that it responded if it's up and the landing node didn't error before sending the request to the shard
          if (upShards.contains(s)) {
            // this is no longer true if there was a query timeout on an up shard
            // assertTrue("Expected to find numFound in the up shard info",info.get("numFound") != null);
            boolean timeAllowedError = info.get("error") != null && info.get("error").toString().contains("Time allowed to handle this request");
            if (timeAllowedError) {
              assertEquals("Expected to find the " + SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY + " header set if a shard is down",
                  Boolean.TRUE, rsp.getHeader().get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
              assertTrue("Expected to find error in the down shard info: " + info.toString(), info.get("error") != null);
            } else {
              assertTrue("Expected timeAllowedError or to find shardAddress in the up shard info: " + info.toString(), info.get("shardAddress") != null);
            }
          } else {
            assertEquals("Expected to find the " + SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY + " header set if a shard is down. Response: " + rsp,
                Boolean.TRUE, rsp.getHeader().get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
            assertTrue("Expected to find error in the down shard info: " + info.toString(), info.get("error") != null);
          }
        }
      }
      assertTrue("Couldn't find shard " + shard + " represented in shards info", found);
    }
  }
  
  @Override
  public void validateControlData(QueryResponse control) throws Exception {
    super.validateControlData(control);
    assertNull("Expected the "+SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY+" header to be null",
        control.getHeader().get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
  }

  private void validateCommonQueryParameters() throws Exception {
    ignoreException("parameter cannot be negative");

    SolrException e1 = expectThrows(SolrException.class, () -> {
      SolrQuery query = new SolrQuery();
      query.setParam("start", "non_numeric_value").setQuery("*");
      QueryResponse resp = query(query);
    });
    assertEquals(ErrorCode.BAD_REQUEST.code, e1.code());

    SolrException e2 = expectThrows(SolrException.class, () -> {
      SolrQuery query = new SolrQuery();
      query.setStart(-1).setQuery("*");
      QueryResponse resp = query(query);
    });
    assertEquals(ErrorCode.BAD_REQUEST.code, e2.code());

    SolrException e3 = expectThrows(SolrException.class, () -> {
      SolrQuery query = new SolrQuery();
      query.setRows(-1).setStart(0).setQuery("*");
      QueryResponse resp = query(query);
    });
    assertEquals(ErrorCode.BAD_REQUEST.code, e3.code());

    SolrException e4 = expectThrows(SolrException.class, () -> {
      SolrQuery query = new SolrQuery();
      query.setParam("rows", "non_numeric_value").setQuery("*");
      QueryResponse resp = query(query);
    });
    assertEquals(ErrorCode.BAD_REQUEST.code, e4.code());

    resetExceptionIgnores();
  }
}
