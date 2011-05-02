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

package org.apache.solr;

import org.apache.solr.common.params.CommonParams;

/**
 * TODO? perhaps use:
 *  http://docs.codehaus.org/display/JETTY/ServletTester
 * rather then open a real connection?
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestDistributedSearch extends BaseDistributedSearchTestCase {

  String t1="a_t";
  String i1="a_si";
  String nint = "n_i";
  String tint = "n_ti";
  String nfloat = "n_f";
  String tfloat = "n_tf";
  String ndouble = "n_d";
  String tdouble = "n_td";
  String nlong = "n_l";
  String tlong = "other_tl1";
  String ndate = "n_dt";
  String tdate_a = "a_n_tdt";
  String tdate_b = "b_n_tdt";
  
  String oddField="oddField_s";
  String missingField="ignore_exception__missing_but_valid_field_t";
  String invalidField="ignore_exception__invalid_field_not_in_schema";

  @Override
  public void doTest() throws Exception {
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
    indexr(id,10, i1, 4321, tlong, 4321,t1,"this too shall pass");
    indexr(id,11, i1, -987, tlong, 987,
           t1,"An eye for eye only ends up making the whole world blind.");
    indexr(id,12, i1, 379, tlong, 379,
           t1,"Great works are performed, not by strength, but by perseverance.");
    indexr(id,13, i1, 232, tlong, 232,
           t1,"no eggs on wall, lesson learned", 
           oddField, "odd man out");

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
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);

    // random value sort
    for (String f : fieldNames) {
      query("q","*:*", "sort",f+" desc");
      query("q","*:*", "sort",f+" asc");
    }

    // these queries should be exactly ordered and scores should exactly match
    query("q","*:*", "sort",i1+" desc");
    query("q","*:*", "sort","{!func}add("+i1+",5)"+" desc");
    query("q","*:*", "sort",i1+" asc");
    query("q","*:*", "sort",i1+" desc", "fl","*,score");
    query("q","*:*", "sort","n_tl1 asc", "fl","score");  // test legacy behavior - "score"=="*,score"
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

    query("q","*:*", "rows",0, "facet","true", "facet.field",t1);
    query("q","*:*", "rows",0, "facet","true", "facet.field",t1,"facet.limit",1);
    query("q","*:*", "rows",0, "facet","true", "facet.query","quick", "facet.query","all", "facet.query","*:*");
    query("q","*:*", "rows",0, "facet","true", "facet.field",t1, "facet.mincount",2);

    // simple date facet on one field
    query("q","*:*", "rows",100, "facet","true", 
          "facet.date",tdate_a, 
          "facet.date.other", "all", 
          "facet.date.start","2010-05-01T11:00:00Z", 
          "facet.date.gap","+1DAY", 
          "facet.date.end","2010-05-20T11:00:00Z");

    // date facet on multiple fields
    query("q","*:*", "rows",100, "facet","true", 
          "facet.date",tdate_a, 
          "facet.date",tdate_b, 
          "facet.date.other", "all", 
          "f."+tdate_b+".facet.date.start","2009-05-01T11:00:00Z", 
          "f."+tdate_b+".facet.date.gap","+3MONTHS", 
          "facet.date.start","2010-05-01T11:00:00Z", 
          "facet.date.gap","+1DAY", 
          "facet.date.end","2010-05-20T11:00:00Z");

    // simple range facet on one field
    query("q","*:*", "rows",100, "facet","true", 
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

    stress=0;  // turn off stress... we want to tex max combos in min time
    for (int i=0; i<25*RANDOM_MULTIPLIER; i++) {
      String f = fieldNames[random.nextInt(fieldNames.length)];
      if (random.nextBoolean()) f = t1;  // the text field is a really interesting one to facet on (and it's multi-valued too)

      // we want a random query and not just *:* so we'll get zero counts in facets also
      // TODO: do a better random query
      String q = random.nextBoolean() ? "*:*" : "id:(1 3 5 7 9 11 13) OR id:[100 TO " + random.nextInt(50) + "]";

      int nolimit = random.nextBoolean() ? -1 : 10000;  // these should be equivalent

      // if limit==-1, we should always get exact matches
      query("q",q, "rows",0, "facet","true", "facet.field",f, "facet.limit",nolimit, "facet.sort","count", "facet.mincount",random.nextInt(5), "facet.offset",random.nextInt(10));
      query("q",q, "rows",0, "facet","true", "facet.field",f, "facet.limit",nolimit, "facet.sort","index", "facet.mincount",random.nextInt(5), "facet.offset",random.nextInt(10));
      // for index sort, we should get exact results for mincount <= 1
      query("q",q, "rows",0, "facet","true", "facet.field",f, "facet.sort","index", "facet.mincount",random.nextInt(2), "facet.offset",random.nextInt(10), "facet.limit",random.nextInt(11)-1);
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

    query("q","*:*", "sort",i1+" desc", "stats", "true", "stats.field", i1);

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

    // test debugging
    handle.put("explain", UNORDERED);
    handle.put("debug", UNORDERED);
    handle.put("time", SKIPVAL);
    query("q","now their fox sat had put","fl","*,score",CommonParams.DEBUG_QUERY, "true");
    query("q", "id:[1 TO 5]", CommonParams.DEBUG_QUERY, "true");
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.TIMING);
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.RESULTS);
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.QUERY);

    // TODO: This test currently fails because debug info is obtained only
    // on shards with matches.
    // query("q","matchesnothing","fl","*,score", "debugQuery", "true");

    // Thread.sleep(10000000000L);
  }

}
