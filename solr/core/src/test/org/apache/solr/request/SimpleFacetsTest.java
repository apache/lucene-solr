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
package org.apache.solr.request;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.params.FacetParams.FacetRangeMethod;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.TimeZoneUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.util.Utils.fromJSONString;


public class SimpleFacetsTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() throws Exception {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
    initCore("solrconfig.xml","schema.xml");
    createIndex();
  }

  static int random_commit_percent = 30;
  static int random_dupe_percent = 25;   // some duplicates in the index to create deleted docs

  static void randomCommit(int percent_chance) {
    if (random().nextInt(100) <= percent_chance)
      assertU(commit());
  }

  static ArrayList<String[]> pendingDocs = new ArrayList<>();

  // committing randomly gives different looking segments each time
  static void add_doc(String... fieldsAndValues) {
    do {
      //do our own copy-field:
      List<String> fieldsAndValuesList = new ArrayList<>(Arrays.asList(fieldsAndValues));
      int idx = fieldsAndValuesList.indexOf("a_tdt");
      if (idx >= 0) {
        fieldsAndValuesList.add("a_drf");
        fieldsAndValuesList.add(fieldsAndValuesList.get(idx + 1));//copy
      }
      idx = fieldsAndValuesList.indexOf("bday");
      if (idx >= 0) {
        fieldsAndValuesList.add("bday_drf");
        fieldsAndValuesList.add(fieldsAndValuesList.get(idx + 1));//copy
      }
      fieldsAndValues = fieldsAndValuesList.toArray(new String[fieldsAndValuesList.size()]);

      pendingDocs.add(fieldsAndValues);      
    } while (random().nextInt(100) <= random_dupe_percent);

    // assertU(adoc(fieldsAndValues));
    // randomCommit(random_commit_percent);
  }


  static void createIndex() throws Exception {
    doEmptyFacetCounts();   // try on empty index

    indexSimpleFacetCounts();
    indexDateFacets();
    indexFacetSingleValued();
    indexFacetPrefixMultiValued();
    indexFacetPrefixSingleValued();
    indexFacetContains();
    indexSimpleGroupedFacetCounts();

    Collections.shuffle(pendingDocs, random());
    for (String[] doc : pendingDocs) {
      assertU(adoc(doc));
      randomCommit(random_commit_percent);
    }
    assertU(commit());
  }

  static void indexSimpleFacetCounts() {
    add_doc("id", "42",
            "range_facet_f", "35.3",
            "range_facet_f1", "35.3",
            "trait_s", "Tool", "trait_s", "Obnoxious",
            "name", "Zapp Brannigan",
             "foo_s","A", "foo_s","B",
             "range_facet_mv_f", "1.0",
             "range_facet_mv_f", "2.5",
             "range_facet_mv_f", "3.7",
             "range_facet_mv_f", "3.3"
    );
    add_doc("id", "43" ,
            "range_facet_f", "28.789",
            "range_facet_f1", "28.789",
            "title", "Democratic Order of Planets",
            "foo_s","A", "foo_s","B",
            "range_facet_mv_f", "3.0",
            "range_facet_mv_f", "7.5",
            "range_facet_mv_f", "12.0"
    );
    add_doc("id", "44",
            "range_facet_f", "15.97",
            "range_facet_f1", "15.97",
            "trait_s", "Tool",
            "name", "The Zapper",
            "foo_s","A", "foo_s","B", "foo_s","C",
            "range_facet_mv_f", "0.0",
            "range_facet_mv_f", "5",
            "range_facet_mv_f", "74"
    );
    add_doc("id", "45",
            "range_facet_f", "30.0",
            "range_facet_f1", "30.0",
            "trait_s", "Chauvinist",
            "title", "25 star General",
            "foo_s","A", "foo_s","B",
            "range_facet_mv_f_f", "12.0",
            "range_facet_mv_f", "212.452",
            "range_facet_mv_f", "32.77",
            "range_facet_mv_f", "0.123"
    );
    add_doc("id", "46",
            "range_facet_f", "20.0",
            "range_facet_f1", "20.0",
            "trait_s", "Obnoxious",
            "subject", "Defeated the pacifists of the Gandhi nebula",
            "foo_s","A", "foo_s","B",
            "range_facet_mv_f", "123.0",
            "range_facet_mv_f", "2.0",
            "range_facet_mv_f", "7.3",
            "range_facet_mv_f", "0.123"
    );
    add_doc("id", "47",
            "range_facet_f", "28.62",
            "range_facet_f1", "28.62",
            "trait_s", "Pig",
            "text", "line up and fly directly at the enemy death cannons, clogging them with wreckage!",
            "zerolen_s","",
            "foo_s","A", "foo_s","B", "foo_s","C"
    );
    add_doc("id", "101", "myfield_s", "foo");
    add_doc("id", "102", "myfield_s", "bar");
  }

  static void indexSimpleGroupedFacetCounts() {
    add_doc("id", "2000", "hotel_s1", "a", "airport_s1", "ams", "duration_i1", "5");
    add_doc("id", "2001", "hotel_s1", "a", "airport_s1", "dus", "duration_i1", "10");
    add_doc("id", "2002", "hotel_s1", "b", "airport_s1", "ams", "duration_i1", "10");
    add_doc("id", "2003", "hotel_s1", "b", "airport_s1", "ams", "duration_i1", "5");
    add_doc("id", "2004", "hotel_s1", "b", "airport_s1", "ams", "duration_i1", "5");
  }

  public void testDvMethodNegativeFloatRangeFacet() throws Exception {
    String field = "negative_num_f1_dv";
    assertTrue("Unexpected schema configuration", h.getCore().getLatestSchema().getField(field).hasDocValues());
    assertEquals("Unexpected schema configuration", NumberType.FLOAT, h.getCore().getLatestSchema().getField(field).getType().getNumberType());
    assertFalse("Unexpected schema configuration", h.getCore().getLatestSchema().getField(field).multiValued());

    final String[] commonParams = { 
        "q", "*:*", "facet", "true", "facet.range.start", "-2", "facet.range.end", "0", "facet.range.gap", "2"
    };
    final String countAssertion
    = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='%s']/lst[@name='counts']/int[@name='-2.0'][.='1']";

    assertU(adoc("id", "10001", field, "-1.0"));
    assertU(commit());

    assertQ(req(commonParams, "facet.range", field, "facet.range.method", "filter"),
        String.format(Locale.ROOT, countAssertion, field)
        );
    assertQ(req(commonParams, "facet.range", field, "facet.range.method", "dv"),
        String.format(Locale.ROOT, countAssertion, field)
        );
  }


  public void testDefaultsAndAppends() throws Exception {
    // all defaults
    assertQ( req("indent","true", "q","*:*", "rows","0", "facet","true", "qt","/search-facet-def")
             // only one default facet.field
             ,"//lst[@name='facet_fields']/lst[@name='foo_s']"
             ,"count(//lst[@name='facet_fields']/lst[@name='foo_s'])=1"
             ,"count(//lst[@name='facet_fields']/lst)=1"
             // only one default facet.query
             ,"//lst[@name='facet_queries']/int[@name='foo_s:bar']"
             ,"count(//lst[@name='facet_queries']/int[@name='foo_s:bar'])=1"
             ,"count(//lst[@name='facet_queries']/int)=1"
             );

    // override default & pre-pend to appends
    assertQ( req("indent","true", "q","*:*", "rows","0", "facet","true", "qt","/search-facet-def",
                 "facet.field", "bar_s",
                 "facet.query", "bar_s:yak"
                 )
             // override single default facet.field
             ,"//lst[@name='facet_fields']/lst[@name='bar_s']"
             ,"count(//lst[@name='facet_fields']/lst[@name='bar_s'])=1"
             ,"count(//lst[@name='facet_fields']/lst)=1"
             // add an additional facet.query
             ,"//lst[@name='facet_queries']/int[@name='foo_s:bar']"
             ,"//lst[@name='facet_queries']/int[@name='bar_s:yak']"
             ,"count(//lst[@name='facet_queries']/int[@name='foo_s:bar'])=1"
             ,"count(//lst[@name='facet_queries']/int[@name='bar_s:yak'])=1"
             ,"count(//lst[@name='facet_queries']/int)=2"
             );
  }

  public void testInvariants() throws Exception {
    // no matter if we try to use facet.field or facet.query, results shouldn't change
    for (String ff : new String[] { "facet.field", "bogus" }) {
      for (String fq : new String[] { "facet.query", "bogus" }) {
        assertQ( req("indent","true", "q", "*:*", "rows","0", "facet","true", 
                     "qt","/search-facet-invariants",
                     ff, "bar_s",
                     fq, "bar_s:yak")
                 // only one invariant facet.field
                 ,"//lst[@name='facet_fields']/lst[@name='foo_s']"
                 ,"count(//lst[@name='facet_fields']/lst[@name='foo_s'])=1"
                 ,"count(//lst[@name='facet_fields']/lst)=1"
                 // only one invariant facet.query
                 ,"//lst[@name='facet_queries']/int[@name='foo_s:bar']"
                 ,"count(//lst[@name='facet_queries']/int[@name='foo_s:bar'])=1"
                 ,"count(//lst[@name='facet_queries']/int)=1"
                 );
      }
    }
  }

  @Test
  public void testCachingBigTerms() throws Exception {
    assertQ( req("indent","true", "q", "id_i1:[42 TO 47]",
            "facet", "true",
            "facet.field", "foo_s"  // big terms should cause foo_s:A to be cached
             ),
        "*[count(//doc)=6]"
    );

    // now use the cached term as a filter to make sure deleted docs are accounted for
    assertQ( req("indent","true", "fl","id", "q", "foo_s:B",
        "facet", "true",
        "facet.field", "foo_s",
        "fq","foo_s:A"
    ),
        "*[count(//doc)=6]"
    );


  }


  @Test
  public void testSimpleGroupedQueryRangeFacets() throws Exception {
    // for the purposes of our test data, it shouldn't matter 
    // if we use facet.limit -100, -1, or 100 ...
    // our set of values is small enough either way
    testSimpleGroupedQueryRangeFacets("-100");
    testSimpleGroupedQueryRangeFacets("-1");
    testSimpleGroupedQueryRangeFacets("100");
  }

  private void testSimpleGroupedQueryRangeFacets(String facetLimit) {
    assertQ(
        req(
            "q", "*:*",
            "fq", "id_i1:[2000 TO 2004]",
            "group", "true",
            "group.facet", "true",
            "group.field", "hotel_s1",
            "facet", "true",
            "facet.limit", facetLimit,
            "facet.query", "airport_s1:ams"
        ),
        "//lst[@name='facet_queries']/int[@name='airport_s1:ams'][.='2']"
    );
    /* Testing facet.query using tagged filter query and exclusion */
    assertQ(
        req(
            "q", "*:*",
            "fq", "id_i1:[2000 TO 2004]",
            "fq", "{!tag=dus}airport_s1:dus",
            "group", "true",
            "group.facet", "true",
            "group.field", "hotel_s1",
            "facet", "true",
            "facet.limit", facetLimit,
            "facet.query", "{!ex=dus}airport_s1:ams"
        ),
        "//lst[@name='facet_queries']/int[@name='{!ex=dus}airport_s1:ams'][.='2']"
    );
    assertQ(
        req(
            "q", "*:*",
            "fq", "id_i1:[2000 TO 2004]",
            "group", "true",
            "group.facet", "true",
            "group.field", "hotel_s1",
            "facet", "true",
            "facet.limit", facetLimit,
            "facet.range", "duration_i1",
            "facet.range.start", "5",
            "facet.range.end", "11",
            "facet.range.gap", "1"
        ),
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='5'][.='2']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='6'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='7'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='8'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='9'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='10'][.='2']"
    );
    /* Testing facet.range using tagged filter query and exclusion */
    assertQ(
        req(
            "q", "*:*",
            "fq", "id_i1:[2000 TO 2004]",
            "fq", "{!tag=dus}airport_s1:dus",
            "group", "true",
            "group.facet", "true",
            "group.field", "hotel_s1",
            "facet", "true",
            "facet.limit", facetLimit,
            "facet.range", "{!ex=dus}duration_i1",
            "facet.range.start", "5",
            "facet.range.end", "11",
            "facet.range.gap", "1"
        ),
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='5'][.='2']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='6'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='7'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='8'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='9'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='10'][.='2']"
    );
    
    // repeat the same query using DV method. This is not supported and the query should use filter method instead
    assertQ(
        req(
            "q", "*:*",
            "fq", "id_i1:[2000 TO 2004]",
            "fq", "{!tag=dus}airport_s1:dus",
            "group", "true",
            "group.facet", "true",
            "group.field", "hotel_s1",
            "facet", "true",
            "facet.limit", facetLimit,
            "facet.range", "{!ex=dus}duration_i1",
            "facet.range.start", "5",
            "facet.range.end", "11",
            "facet.range.gap", "1",
            "facet.range.method", FacetRangeMethod.DV.toString()
        ),
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='5'][.='2']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='6'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='7'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='8'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='9'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='10'][.='2']"
    );
  }

  @Test
  public void testSimpleGroupedFacets() throws Exception {
    assumeFalse("SOLR-10844: group.facet doesn't play nice with points *OR* DocValues",
                Boolean.getBoolean(NUMERIC_DOCVALUES_SYSPROP) || Boolean.getBoolean(NUMERIC_POINTS_SYSPROP));
                
    
    // for the purposes of our test data, it shouldn't matter 
    // if we use facet.limit -100, -1, or 100 ...
    // our set of values is small enough either way
    testSimpleGroupedFacets("100");
    testSimpleGroupedFacets("-100");
    testSimpleGroupedFacets("-5");
    testSimpleGroupedFacets("-1");
  }
  
  private void testSimpleGroupedFacets(String facetLimit) throws Exception {
    assertQ(
        "Return 5 docs with id range 1937 till 1940",
         req("id_i1:[2000 TO 2004]"),
        "*[count(//doc)=5]"
    );
    assertQ(
        "Return two facet counts for field airport_a and duration_i1",
         req(
             "q", "*:*",
             "fq", "id_i1:[2000 TO 2004]",
             "group", "true",
             "group.facet", "true",
             "group.field", "hotel_s1",
             "facet", "true",
             "facet.limit", facetLimit,
             "facet.field", "airport_s1",
             "facet.field", "duration_i1"
         ),
        "//lst[@name='facet_fields']/lst[@name='airport_s1']",
        "*[count(//lst[@name='airport_s1']/int)=2]",
        "//lst[@name='airport_s1']/int[@name='ams'][.='2']",
        "//lst[@name='airport_s1']/int[@name='dus'][.='1']",

        "//lst[@name='facet_fields']/lst[@name='duration_i1']",
        "*[count(//lst[@name='duration_i1']/int)=2]",
        "//lst[@name='duration_i1']/int[@name='5'][.='2']",
        "//lst[@name='duration_i1']/int[@name='10'][.='2']"
    );
    assertQ(
        "Return one facet count for field airport_a using facet.offset",
         req(
             "q", "*:*",
             "fq", "id_i1:[2000 TO 2004]",
             "group", "true",
             "group.facet", "true",
             "group.field", "hotel_s1",
             "facet", "true",
             "facet.offset", "1",
             "facet.limit", facetLimit,
             "facet.field", "airport_s1"
         ),
        "//lst[@name='facet_fields']/lst[@name='airport_s1']",
        "*[count(//lst[@name='airport_s1']/int)=1]",
        "//lst[@name='airport_s1']/int[@name='dus'][.='1']"
    );
    assertQ(
        "Return two facet counts for field airport_a with fq",
         req(
             "q", "*:*",
             "fq", "id_i1:[2000 TO 2004]",
             "fq", "duration_i1:5",
             "group", "true",
             "group.facet", "true",
             "group.field", "hotel_s1",
             "facet", "true",
             "facet.limit", facetLimit,
             "facet.field", "airport_s1"
         ),
        "//lst[@name='facet_fields']/lst[@name='airport_s1']",
        "*[count(//lst[@name='airport_s1']/int)=2]",
        "//lst[@name='airport_s1']/int[@name='ams'][.='2']",
        "//lst[@name='airport_s1']/int[@name='dus'][.='0']"
    );
    assertQ(
        "Return one facet count for field airport_s1 with prefix a",
         req(
             "q", "*:*",
             "fq", "id_i1:[2000 TO 2004]",
             "group", "true",
             "group.facet", "true",
             "group.field", "hotel_s1",
             "facet", "true",
             "facet.field", "airport_s1",
             "facet.limit", facetLimit,
             "facet.prefix", "a"
         ),
        "//lst[@name='facet_fields']/lst[@name='airport_s1']",
        "*[count(//lst[@name='airport_s1']/int)=1]",
        "//lst[@name='airport_s1']/int[@name='ams'][.='2']"
    );

    SolrException e = expectThrows(SolrException.class, () -> {
      h.query(
          req(
              "q", "*:*",
              "fq", "id_i1:[2000 TO 2004]",
              "group.facet", "true",
              "facet", "true",
              "facet.field", "airport_s1",
              "facet.prefix", "a"
          )
      );
    });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
  }

  @Test
  public void testEmptyFacetCounts() throws Exception {
    doEmptyFacetCounts();
  }

  // static so we can try both with and without an empty index
  static void doEmptyFacetCounts() throws Exception {
    doEmptyFacetCounts("empty_t", new String[]{null, "myprefix",""});
    doEmptyFacetCounts("empty_i", new String[]{null});
    doEmptyFacetCounts("empty_f", new String[]{null});
    doEmptyFacetCounts("empty_s", new String[]{null, "myprefix",""});
    doEmptyFacetCounts("empty_d", new String[]{null});
  }
  
  static void doEmptyFacetCounts(String field, String[] prefixes) throws Exception {
    SchemaField sf = h.getCore().getLatestSchema().getField(field);

    String response = JQ(req("q", "*:*"));
    @SuppressWarnings({"rawtypes"})
    Map rsp = (Map) fromJSONString(response);
    Long numFound  = (Long)(((Map)rsp.get("response")).get("numFound"));

    ModifiableSolrParams params = params("q","*:*", "facet.mincount","1","rows","0", "facet","true", "facet.field","{!key=myalias}"+field);
    
    String[] methods = {null, "fc","enum","fcs", "uif"};
    if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
      methods = new String[]{null, "fc","enum", "uif"};
    }

    prefixes = prefixes==null ? new String[]{null} : prefixes;


    for (String method : methods) {
      if (method == null) {
        params.remove("facet.method");
      } else {
        params.set("facet.method", method);
      }
      for (String prefix : prefixes) {
        if (prefix == null) {
          params.remove("facet.prefix");
        } else {
          params.set("facet.prefix", prefix);
        }

        for (String missing : new String[] {null, "true"}) {
          if (missing == null) {
            params.remove("facet.missing");
          } else {
            params.set("facet.missing", missing);
          }
          
          String expected = missing==null ? "[]" : "[null," + numFound + "]";
          
          assertJQ(req(params),
              "/facet_counts/facet_fields/myalias==" + expected);
        }
      }
    }
  }


  @Test
  public void testFacetMatches() {
    final String[][] uifSwitch = new String[][] {
        new String[]{"f.trait_s.facet.method", "uif"},
        new String[]{"facet.method", "uif"}
    };
    final String[] none = new String[]{};
    for (String[] aSwitch : uifSwitch) {
      for(String[] methodParam : new String[][]{ none, aSwitch}) {
        assertQ("check facet.match filters facets returned",
            req(methodParam
                , "q", "id:[42 TO 47]"
                , "facet", "true"
                , "facet.field", "trait_s"
                , "facet.matches", ".*o.*"
            )
            , "*[count(//doc)=6]"

            , "//lst[@name='facet_counts']/lst[@name='facet_queries']"

            , "//lst[@name='facet_counts']/lst[@name='facet_fields']"
            , "//lst[@name='facet_fields']/lst[@name='trait_s']"
            , "*[count(//lst[@name='trait_s']/int)=2]"
            , "//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            , "//lst[@name='trait_s']/int[@name='Obnoxious'][.='2']"
        );
      }
    }
  }

  @Test
  public void testFacetMissing() {
    SolrParams commonParams = params("q", "foo_s:A", "rows", "0", "facet", "true", "facet.missing", "true");

    // with facet.limit!=0 and facet.missing=true
    assertQ(
        req(commonParams, "facet.field", "trait_s", "facet.limit", "1"),
        "//lst[@name='facet_counts']/lst[@name='facet_fields']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='trait_s']",
        "*[count(//lst[@name='trait_s']/int)=2]",
        "//lst[@name='trait_s']/int[@name='Obnoxious'][.='2']",
        "//lst[@name='trait_s']/int[.='1']"
    );

    // with facet.limit=0 and facet.missing=true
    assertQ(
        req(commonParams, "facet.field", "trait_s", "facet.limit", "0"),
        "//lst[@name='facet_counts']/lst[@name='facet_fields']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='trait_s']",
        "*[count(//lst[@name='trait_s']/int)=1]",
        "//lst[@name='trait_s']/int[.='1']"
    );

    // facet.method=enum
    assertQ(
        req(commonParams, "facet.field", "trait_s", "facet.limit", "0", "facet.method", "enum"),
        "//lst[@name='facet_counts']/lst[@name='facet_fields']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='trait_s']",
        "*[count(//lst[@name='trait_s']/int)=1]",
        "//lst[@name='trait_s']/int[.='1']"
    );

    assertQ(
        req(commonParams, "facet.field", "trait_s", "facet.limit", "0", "facet.mincount", "1",
            "facet.method", "uif"),
        "//lst[@name='facet_counts']/lst[@name='facet_fields']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='trait_s']",
        "*[count(//lst[@name='trait_s']/int)=1]",
        "//lst[@name='trait_s']/int[.='1']"
    );

    // facet.method=fcs
    assertQ(
        req(commonParams, "facet.field", "trait_s", "facet.limit", "0", "facet.method", "fcs"),
        "//lst[@name='facet_counts']/lst[@name='facet_fields']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='trait_s']",
        "*[count(//lst[@name='trait_s']/int)=1]",
        "//lst[@name='trait_s']/int[.='1']"
    );

    // facet.missing=true on numeric field
    assertQ(
        req(commonParams, "facet.field", "range_facet_f", "facet.limit", "1", "facet.mincount", "1"),
        "//lst[@name='facet_counts']/lst[@name='facet_fields']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='range_facet_f']",
        "*[count(//lst[@name='range_facet_f']/int)=2]",
        "//lst[@name='range_facet_f']/int[.='0']"
    );

    // facet.limit=0
    assertQ(
        req(commonParams, "facet.field", "range_facet_f", "facet.limit", "0", "facet.mincount", "1"),
        "//lst[@name='facet_counts']/lst[@name='facet_fields']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='range_facet_f']",
        "*[count(//lst[@name='range_facet_f']/int)=1]",
        "//lst[@name='range_facet_f']/int[.='0']"
    );
  }

  @Test
  public void testSimpleFacetCounts() {
 
    assertQ("standard request handler returns all matches",
            req("id_i1:[42 TO 47]"),
            "*[count(//doc)=6]"
            );
 
    assertQ("filter results using fq",
            req("q","id_i1:[42 TO 46]",
                "fq", "id_i1:[43 TO 47]"),
            "*[count(//doc)=4]"
            );
    
    assertQ("don't filter results using blank fq",
            req("q","id_i1:[42 TO 46]",
                "fq", " "),
            "*[count(//doc)=5]"
            );
     
    assertQ("filter results using multiple fq params",
            req("q","id_i1:[42 TO 46]",
                "fq", "trait_s:Obnoxious",
                "fq", "id_i1:[43 TO 47]"),
            "*[count(//doc)=1]"
            );
 
    final String[] uifSwitch = new String[]{(random().nextBoolean() ? "":"f.trait_s.")+"facet.method", "uif"};
    final String[] none = new String[]{};
    
    for(String[] methodParam : new String[][]{ none, uifSwitch}){
      assertQ("check counts for facet queries",
          req(methodParam
              ,"q", "id_i1:[42 TO 47]"
              ,"facet", "true"
              ,"facet.query", "trait_s:Obnoxious"
              ,"facet.query", "id_i1:[42 TO 45]"
              ,"facet.query", "id_i1:[43 TO 47]"
              ,"facet.field", "trait_s"
              )
          ,"*[count(//doc)=6]"

          ,"//lst[@name='facet_counts']/lst[@name='facet_queries']"
          ,"//lst[@name='facet_queries']/int[@name='trait_s:Obnoxious'][.='2']"
          ,"//lst[@name='facet_queries']/int[@name='id_i1:[42 TO 45]'][.='4']"
          ,"//lst[@name='facet_queries']/int[@name='id_i1:[43 TO 47]'][.='5']"

          ,"//lst[@name='facet_counts']/lst[@name='facet_fields']"
          ,"//lst[@name='facet_fields']/lst[@name='trait_s']"
          ,"*[count(//lst[@name='trait_s']/int)=4]"
          ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
          ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='2']"
          ,"//lst[@name='trait_s']/int[@name='Pig'][.='1']"
          );
      
      assertQ("check multi-select facets with naming",
            req(methodParam, "q", "id_i1:[42 TO 47]"
                ,"facet", "true"
                ,"facet.query", "{!ex=1}trait_s:Obnoxious"
                ,"facet.query", "{!ex=2 key=foo}id_i1:[42 TO 45]"    // tag=2 same as 1
                ,"facet.query", "{!ex=3,4 key=bar}id_i1:[43 TO 47]"  // tag=3,4 don't exist
                ,"facet.field", "{!ex=3,1}trait_s"                // 3,1 same as 1
                ,"fq", "{!tag=1,2}id:47"                          // tagged as 1 and 2
                )
            ,"*[count(//doc)=1]"

            ,"//lst[@name='facet_counts']/lst[@name='facet_queries']"
            ,"//lst[@name='facet_queries']/int[@name='{!ex=1}trait_s:Obnoxious'][.='2']"
            ,"//lst[@name='facet_queries']/int[@name='foo'][.='4']"
            ,"//lst[@name='facet_queries']/int[@name='bar'][.='1']"

            ,"//lst[@name='facet_counts']/lst[@name='facet_fields']"
            ,"//lst[@name='facet_fields']/lst[@name='trait_s']"
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Pig'][.='1']"
            );
    }
    // test excluding main query
    assertQ(req("q", "{!tag=main}id:43"
                 ,"facet", "true"
                 ,"facet.query", "{!key=foo}id:42"
                 ,"facet.query", "{!ex=main key=bar}id:42"    // only matches when we exclude main query
                 )
             ,"//lst[@name='facet_queries']/int[@name='foo'][.='0']"
             ,"//lst[@name='facet_queries']/int[@name='bar'][.='1']"
             );

    for(String[] methodParam : new String[][]{ none, uifSwitch}){
      assertQ("check counts for applied facet queries using filtering (fq)",
            req(methodParam
                ,"q", "id_i1:[42 TO 47]"
                ,"facet", "true"
                ,"fq", "id_i1:[42 TO 45]"
                ,"facet.field", "trait_s"
                ,"facet.query", "id_i1:[42 TO 45]"
                ,"facet.query", "id_i1:[43 TO 47]"
                )
            ,"*[count(//doc)=4]"
            ,"//lst[@name='facet_counts']/lst[@name='facet_queries']"
            ,"//lst[@name='facet_queries']/int[@name='id_i1:[42 TO 45]'][.='4']"
            ,"//lst[@name='facet_queries']/int[@name='id_i1:[43 TO 47]'][.='3']"
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Chauvinist'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Pig'][.='0']"
            );
 
      assertQ("check counts with facet.zero=false&facet.missing=true using fq",
            req(methodParam
                ,"q", "id_i1:[42 TO 47]"
                ,"facet", "true"
                ,"facet.zeros", "false"
                ,"f.trait_s.facet.missing", "true"
                ,"fq", "id_i1:[42 TO 45]"
                ,"facet.field", "trait_s"
                )
            ,"*[count(//doc)=4]"
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Chauvinist'][.='1']"
            ,"//lst[@name='trait_s']/int[not(@name)][.='1']"
            );

      assertQ("check counts with facet.mincount=1&facet.missing=true using fq",
            req(methodParam
                ,"q", "id_i1:[42 TO 47]"
                ,"facet", "true"
                ,"facet.mincount", "1"
                ,"f.trait_s.facet.missing", "true"
                ,"fq", "id_i1:[42 TO 45]"
                ,"facet.field", "trait_s"
                )
            ,"*[count(//doc)=4]"
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Chauvinist'][.='1']"
            ,"//lst[@name='trait_s']/int[not(@name)][.='1']"
            );

      assertQ("check counts with facet.mincount=2&facet.missing=true using fq",
            req(methodParam
                ,"q", "id_i1:[42 TO 47]"
                ,"facet", "true"
                ,"facet.mincount", "2"
                ,"f.trait_s.facet.missing", "true"
                ,"fq", "id_i1:[42 TO 45]"
                ,"facet.field", "trait_s"
                )
            ,"*[count(//doc)=4]"
            ,"*[count(//lst[@name='trait_s']/int)=2]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[not(@name)][.='1']"               
            );

      assertQ("check sorted paging",
            req(methodParam
                ,"q", "id_i1:[42 TO 47]"
                ,"facet", "true"
                ,"fq", "id_i1:[42 TO 45]"
                ,"facet.field", "trait_s"
                ,"facet.mincount","0"
                ,"facet.offset","0"
                ,"facet.limit","4"
                )
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Chauvinist'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Pig'][.='0']"
            );

      // check that the default sort is by count
      assertQ("check sorted paging",
            req(methodParam, "q", "id_i1:[42 TO 47]"
                ,"facet", "true"
                ,"fq", "id_i1:[42 TO 45]"
                ,"facet.field", "trait_s"
                ,"facet.mincount","0"
                ,"facet.offset","0"
                ,"facet.limit","3"
                )
            ,"*[count(//lst[@name='trait_s']/int)=3]"
            ,"//int[1][@name='Tool'][.='2']"
            ,"//int[2][@name='Chauvinist'][.='1']"
            ,"//int[3][@name='Obnoxious'][.='1']"
            );

      //
      // check that legacy facet.sort=true/false works
      //
      assertQ(req(methodParam, "q", "id_i1:[42 TO 47]"
                ,"facet", "true"
                ,"fq", "id_i1:[42 TO 45]"
                ,"facet.field", "trait_s"
                ,"facet.mincount","0"
                ,"facet.offset","0"
                ,"facet.limit","3"
                ,"facet.sort","true"  // true means sort-by-count
                )
            ,"*[count(//lst[@name='trait_s']/int)=3]"
            ,"//int[1][@name='Tool'][.='2']"
            ,"//int[2][@name='Chauvinist'][.='1']"
            ,"//int[3][@name='Obnoxious'][.='1']"
            );

       assertQ(req(methodParam, "q", "id_i1:[42 TO 47]"
                ,"facet", "true"
                ,"fq", "id_i1:[42 TO 45]"
                ,"facet.field", "trait_s"
                ,"facet.mincount","1"
                ,"facet.offset","0"
                ,"facet.limit","3"
                ,"facet.sort","false"  // false means sort by index order
                )
            ,"*[count(//lst[@name='trait_s']/int)=3]"
            ,"//int[1][@name='Chauvinist'][.='1']"
            ,"//int[2][@name='Obnoxious'][.='1']"
            ,"//int[3][@name='Tool'][.='2']"
            );
    }

    for(String method : new String[]{ "fc","uif"}){
       assertQ(req("q", "id_i1:[42 TO 47]"
                ,"facet", "true"
                ,"fq", "id_i1:[42 TO 45]"
                ,"facet.field", "zerolen_s"
                ,(random().nextBoolean() ? "":"f.zerolen_s.")+"facet.method", method
                )
            ,"*[count(//lst[@name='zerolen_s']/int[@name=''])=1]"
       );
    }

    assertQ("a facet.query that analyzes to no query shoud not NPE",
        req("q", "*:*",
            "facet", "true",
            "facet.query", "{!field key=k f=lengthfilt}a"),//2 char minimum
        "//lst[@name='facet_queries']/int[@name='k'][.='0']"
    );
  }

  public void testBehaviorEquivilenceOfUninvertibleFalse() throws Exception {
    // NOTE: mincount=0 affects method detection/coercion, so we include permutations of it
    
    { 
      // an "uninvertible=false" field is not be facetable using the "default" method,
      // or any explicit method other then "enum".
      //
      // it should behave the same as any attempt (using any method) at faceting on
      // and "indexed=false docValues=false" field -- returning no buckets.
      
      final List<SolrParams> paramSets = new ArrayList<>();
      for (String min : Arrays.asList("0", "1")) {
        for (String f : Arrays.asList("trait_s_not_uninvert", "trait_s_not_indexed_sS")) {
          paramSets.add(params("facet.field", "{!key=x}" + f));
          for (String method : Arrays.asList("fc", "fcs", "uif")) {
            paramSets.add(params("facet.field", "{!key=x}" + f,
                                 "facet.mincount", min,
                                 "facet.method", method));
            paramSets.add(params("facet.field", "{!key=x}" + f,
                                 "facet.mincount", min,
                                 "facet.method", method));
          }
        }
        paramSets.add(params("facet.field", "{!key=x}trait_s_not_indexed_sS",
                             "facet.mincount", min,
                             "facet.method", "enum"));
      }
      for (SolrParams p : paramSets) {
        // "empty" results should be the same regardless of mincount
        assertQ("expect no buckets when field is not-indexed or not-uninvertible",
                req(p
                    ,"rows","0"
                    ,"q", "id_i1:[42 TO 47]"
                    ,"fq", "id_i1:[42 TO 45]"
                    ,"facet", "true"
                    )
                ,"//*[@numFound='4']"
                ,"*[count(//lst[@name='x'])=1]"
                ,"*[count(//lst[@name='x']/int)=0]"
                );
      }
      
    }
    
    { 
      // the only way to facet on an "uninvertible=false" field is to explicitly request facet.method=enum
      // in which case it should behave consistently with it's copyField source & equivilent docValues field
      // (using any method for either of them)

      final List<SolrParams> paramSets = new ArrayList<>();
      for (String min : Arrays.asList("0", "1")) {
        paramSets.add(params("facet.field", "{!key=x}trait_s_not_uninvert",
                             "facet.method", "enum"));
        for (String okField : Arrays.asList("trait_s", "trait_s_not_uninvert_dv")) {
          paramSets.add(params("facet.field", "{!key=x}" + okField));
          for (String method : Arrays.asList("enum","fc", "fcs", "uif")) {
            paramSets.add(params("facet.field", "{!key=x}" + okField,
                                 "facet.method", method));
          }
        }
        for (SolrParams p : paramSets) {
          assertQ("check counts for applied facet queries using filtering (fq)",
                  req(p
                      ,"rows","0"
                      ,"q", "id_i1:[42 TO 47]"
                      ,"fq", "id_i1:[42 TO 45]"
                      ,"facet", "true"
                      ,"facet.mincount", min
                      )
                  ,"//*[@numFound='4']"
                  ,"*[count(//lst[@name='x'])=1]"
                  ,"*[count(//lst[@name='x']/int)="+("0".equals(min) ? "4]" : "3]")
                  ,"//lst[@name='x']/int[@name='Tool'][.='2']"
                  ,"//lst[@name='x']/int[@name='Obnoxious'][.='1']"
                  ,"//lst[@name='x']/int[@name='Chauvinist'][.='1']"
                  ,"count(//lst[@name='x']/int[@name='Pig'][.='0'])=" + ("0".equals(min) ? "1" : "0")
                  );
        }
      }
    }
  }

  
  public static void indexDateFacets() {
    final String i = "id";
    final String f = "bday";
    final String ff = "a_tdt";
    final String ooo = "00:00:00.000Z";
    final String xxx = "15:15:15.155Z";

    //note: add_doc duplicates bday to bday_drf and a_tdt to a_drf (date range field)
    add_doc(i, "201",  f, "1976-07-04T12:08:56.235Z", ff, "1900-01-01T"+ooo);
    add_doc(i, "202",  f, "1976-07-05T00:00:00.000Z", ff, "1976-07-01T"+ooo);
    add_doc(i, "203",  f, "1976-07-15T00:07:57.890Z", ff, "1976-07-04T"+ooo);
    add_doc(i, "204",  f, "1976-07-21T00:07:57.890Z", ff, "1976-07-05T"+ooo);
    add_doc(i, "205",  f, "1976-07-13T12:12:25.255Z", ff, "1976-07-05T"+xxx);
    add_doc(i, "206",  f, "1976-07-03T17:01:23.456Z", ff, "1976-07-07T"+ooo);
    add_doc(i, "207",  f, "1976-07-12T12:12:25.255Z", ff, "1976-07-13T"+ooo);
    add_doc(i, "208",  f, "1976-07-15T15:15:15.155Z", ff, "1976-07-13T"+xxx);
    add_doc(i, "209",  f, "1907-07-12T13:13:23.235Z", ff, "1976-07-15T"+xxx);
    add_doc(i, "2010", f, "1976-07-03T11:02:45.678Z", ff, "2000-01-01T"+ooo);
    add_doc(i, "2011", f, "1907-07-12T12:12:25.255Z");
    add_doc(i, "2012", f, "2007-07-30T07:07:07.070Z");
    add_doc(i, "2013", f, "1976-07-30T22:22:22.222Z");
    add_doc(i, "2014", f, "1976-07-05T22:22:22.222Z");
  }

  @Test
  public void testTrieDateRangeFacets() {
    helpTestDateFacets("bday", FacetRangeMethod.FILTER);
  }
  
  @Test
  public void testTrieDateRangeFacetsDocValues() {
    helpTestDateFacets("bday", FacetRangeMethod.DV);
  }

  @Test
  public void testDateRangeFieldFacets() {
    helpTestDateFacets("bday_drf", FacetRangeMethod.FILTER);
  }

  private void helpTestDateFacets(final String fieldName, final FacetRangeMethod rangeFacetMethod) {
    final String p = "facet.range";
    final String b = "facet_ranges";
    final String f = fieldName;
    final String c = "/lst[@name='counts']";
    final String pre = "//lst[@name='"+b+"']/lst[@name='"+f+"']" + c;
    final String meta = pre + "/../";

    
    // range faceting defaults to including only lower endpoint
    // doc exists with value @ 00:00:00.000 on July5
    final String jul4 = "[.='1'  ]";

    assertQ("check counts for month of facet by day",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-01T00:00:00.000Z+1MONTH"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".method", rangeFacetMethod.toString()  //This only applies to range faceting, won't be use for date faceting
                )
            ,"*[count("+pre+"/int)=31]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z']" + jul4
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-13T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-14T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-15T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-16T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-17T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-18T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-19T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-21T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-22T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-23T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-24T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-25T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-26T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-27T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-28T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-29T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-30T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-31T00:00:00Z'][.='0']"
            
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='11']"
            
            );

    assertQ("check counts for month of facet by day with global mincount = 1",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-01T00:00:00.000Z+1MONTH"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,"facet.mincount", "1"
                )
            ,"*[count("+pre+"/int)=8]"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z']" + jul4
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-13T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-15T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-21T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-30T00:00:00Z'][.='1'  ]"
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='11']"
            );

    assertQ("check counts for month of facet by day with field mincount = 1",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-01T00:00:00.000Z+1MONTH"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,"f." + f + ".facet.mincount", "2"
                )
            ,"*[count("+pre+"/int)=3]"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='2'  ]"
            ,pre
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-15T00:00:00Z'][.='2'  ]"
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='11']"
            );

    assertQ("check before is not inclusive of upper bound by default",
            req("q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start",  "1976-07-05T00:00:00.000Z"
                ,p+".end",    "1976-07-07T00:00:00.000Z"
                ,p+".gap",    "+1DAY"
                ,p+".other",  "all"
                )
            ,"*[count("+pre+"/int)=2]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0']"

            ,meta+"/int[@name='before' ][.='5']"
            );
    assertQ("check after is not inclusive of lower bound by default (for dates)",
            req("q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start",  "1976-07-03T00:00:00.000Z"
                ,p+".end",    "1976-07-05T00:00:00.000Z"
                ,p+".gap",    "+1DAY"
                ,p+".other",  "all"
                )
            ,"*[count("+pre+"/int)=2]"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z']" + jul4

            ,meta+"/int[@name='after' ][.='9']"
            );


    assertQ("check hardend=false",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start",  "1976-07-01T00:00:00.000Z"
                ,p+".end",    "1976-07-13T00:00:00.000Z"
                ,p+".gap",    "+5DAYS"
                ,p+".other",  "all"
                ,p+".hardend","false"
                )
            ,"*[count("+pre+"/int)=3]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='5'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0'  ]"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='4'  ]"

            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='3']"
            ,meta+"/int[@name='between'][.='9']"
            );

    assertQ("check hardend=true",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start",  "1976-07-01T00:00:00.000Z"
                ,p+".end",    "1976-07-13T00:00:00.000Z"
                ,p+".gap",    "+5DAYS"
                ,p+".other",  "all"
                ,p+".hardend","true"
                )
            ,"*[count("+pre+"/int)=3]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='5'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0'  ]"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='1'  ]"

            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='6']"
            ,meta+"/int[@name='between'][.='6']"
            );

    //Fixed by SOLR-9080 related to the Gregorian Change Date
    assertQ("check BC era",
        req( "q", "*:*"
            ,"rows", "0"
            ,"facet", "true"
            ,p, f
            ,p+".start", "-0200-01-01T00:00:00Z" // BC
            ,p+".end",   "+0200-01-01T00:00:00Z" // AD
            ,p+".gap",   "+100YEARS"
            ,p+".other", "all"
        )
        ,pre+"/int[@name='-0200-01-01T00:00:00Z'][.='0']"
        ,pre+"/int[@name='-0100-01-01T00:00:00Z'][.='0']"
        ,pre+"/int[@name='0000-01-01T00:00:00Z'][.='0']"
        ,pre+"/int[@name='0100-01-01T00:00:00Z'][.='0']"
        ,meta+"/int[@name='before' ][.='0']"
        ,meta+"/int[@name='after'  ][.='14']"
        ,meta+"/int[@name='between'][.='0']"

    );

  }

  @Test
  public void testTrieDateRangeFacetsWithIncludeOption() {
    helpTestDateRangeFacetsWithIncludeOption("a_tdt");
  }

  @Test
  public void testDateRangeFieldDateRangeFacetsWithIncludeOption() {
    helpTestDateRangeFacetsWithIncludeOption("a_drf");
  }

  /** Similar to helpTestDateFacets, but for different fields with test data
      exactly on boundary marks */
  private void helpTestDateRangeFacetsWithIncludeOption(final String fieldName) {
    final String p = "facet.range";
    final String b = "facet_ranges";
    final String f = fieldName;
    final String c = "/lst[@name='counts']";
    final String pre = "//lst[@name='"+b+"']/lst[@name='"+f+"']" + c;
    final String meta = pre + "/../";

    assertQ("checking counts for lower",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-16T00:00:00.000Z"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "lower"
                )
            // 15 days + pre+post+inner = 18
            ,"*[count("+pre+"/int)=15]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-13T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-14T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-15T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='1']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='8']"
            );

    assertQ("checking counts for upper",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-16T00:00:00.000Z"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "upper"
                )
            // 15 days + pre+post+inner = 18
            ,"*[count("+pre+"/int)=15]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-13T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-14T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-15T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='7']"
            );

    assertQ("checking counts for lower & upper",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-16T00:00:00.000Z"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "lower"
                ,p+".include", "upper"
                )
            // 15 days + pre+post+inner = 18
            ,"*[count("+pre+"/int)=15]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-13T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-14T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-15T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='1']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='8']"
            );

    assertQ("checking counts for upper & edge",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-16T00:00:00.000Z"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "upper"
                ,p+".include", "edge"
                )
            // 15 days + pre+post+inner = 18
            ,"*[count("+pre+"/int)=15]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-13T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-14T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-15T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='1']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='8']"
            );

    assertQ("checking counts for upper & outer",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-13T00:00:00.000Z" // smaller now
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "upper"
                ,p+".include", "outer"
                )
            // 12 days + pre+post+inner = 15
            ,"*[count("+pre+"/int)=12]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='4']"
            ,meta+"/int[@name='between'][.='5']"
            );

    assertQ("checking counts for lower & edge",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-13T00:00:00.000Z" // smaller now
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "lower"
                ,p+".include", "edge"
                )
            // 12 days + pre+post+inner = 15
            ,"*[count("+pre+"/int)=12]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='1']"
            ,meta+"/int[@name='after'  ][.='3']"
            ,meta+"/int[@name='between'][.='6']"
            );

    assertQ("checking counts for lower & outer",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-13T00:00:00.000Z" // smaller now
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "lower"
                ,p+".include", "outer"
                )
            // 12 days + pre+post+inner = 15
            ,"*[count("+pre+"/int)=12]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='0']"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='4']"
            ,meta+"/int[@name='between'][.='5']"
            );

    assertQ("checking counts for lower & edge & outer",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-13T00:00:00.000Z" // smaller now
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "lower"
                ,p+".include", "edge"
                ,p+".include", "outer"
                )
            // 12 days + pre+post+inner = 15
            ,"*[count("+pre+"/int)=12]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='4']"
            ,meta+"/int[@name='between'][.='6']"
            );

    assertQ("checking counts for all",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-13T00:00:00.000Z" // smaller now
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "all"
                )
            // 12 days + pre+post+inner = 15
            ,"*[count("+pre+"/int)=12]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='4']"
            ,meta+"/int[@name='between'][.='6']"
            );
  }

  @Test
  public void testDateRangeFacetsWithTz() {
    helpTestDateRangeFacetsWithTz("a_tdt");
  }

  private void helpTestDateRangeFacetsWithTz(final String fieldName) {
    final String p = "facet.range";
    final String b = "facet_ranges";
    final String f = fieldName;
    final String c = "/lst[@name='counts']";
    final String pre = "//lst[@name='"+b+"']/lst[@name='"+f+"']" + c;
    final String meta = pre + "/../";

    final String TZ = "America/Los_Angeles";
    assumeTrue("Test requires JVM to know about about TZ: " + TZ,
               TimeZoneUtils.KNOWN_TIMEZONE_IDS.contains(TZ)); 

    assertQ("checking facet counts for fixed now, using TZ: " + TZ,
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"NOW", "205078333000" // 1976-07-01T14:12:13.000Z
                ,"TZ", TZ
                ,p, f
                ,p+".start", "NOW/MONTH"
                ,p+".end",   "NOW/MONTH+15DAYS"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "lower"
                )
            // 15 days + pre+post+inner = 18
            ,"*[count("+pre+"/int)=15]"
            ,pre+"/int[@name='1976-07-01T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-02T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T07:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-04T07:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T07:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-06T07:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-07T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-08T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T07:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-13T07:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-14T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-15T07:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='7']"
            );

    // NOTE: the counts should all be zero, what we really care about
    // is that the computed lower bounds take into account DST change
    assertQ("checking facet counts arround DST change for TZ: " + TZ,
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"NOW", "1288606136000" // 2010-11-01T10:08:56.235Z
                ,"TZ", TZ
                ,p, f
                ,p+".start", "NOW/MONTH"
                ,p+".end",   "NOW/MONTH+15DAYS"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "lower"
                )
            // 15 days + pre+post+inner = 18
            ,"*[count("+pre+"/int)=15]"
            ,pre+"/int[@name='2010-11-01T07:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-02T07:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-03T07:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-04T07:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-05T07:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-06T07:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-07T07:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-08T08:00:00Z'][.='0']" // BOOM!
            ,pre+"/int[@name='2010-11-09T08:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-10T08:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-11T08:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-12T08:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-13T08:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-14T08:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-15T08:00:00Z'][.='0']"
            );
    
  }

  @Test
  public void testNumericRangeFacetsTrieFloat() {
    helpTestFractionalNumberRangeFacets("range_facet_f");
  }
  @Test
  public void testNumericRangeFacetsTrieDouble() {
    helpTestFractionalNumberRangeFacets("range_facet_d");
  }
  
  @Test
  public void testNumericRangeFacetsTrieFloatDocValues() {
    helpTestFractionalNumberRangeFacets("range_facet_f", FacetRangeMethod.DV);
  }
  @Test
  public void testNumericRangeFacetsTrieDoubleDocValues() {
    helpTestFractionalNumberRangeFacets("range_facet_d", FacetRangeMethod.DV);
  }

  @Test
  public void testNumericRangeFacetsOverflowTrieDouble() {
    helpTestNumericRangeFacetsDoubleOverflow("range_facet_d", FacetRangeMethod.FILTER);
  }
  
  @Test
  public void testNumericRangeFacetsOverflowTrieDoubleDocValue() {
    helpTestNumericRangeFacetsDoubleOverflow("range_facet_d", FacetRangeMethod.DV);
  }

  private void helpTestNumericRangeFacetsDoubleOverflow(final String fieldName, final FacetRangeMethod method) {
    final String f = fieldName;
    final String pre = "//lst[@name='facet_ranges']/lst[@name='"+f+"']/lst[@name='counts']";
    final String meta = pre + "/../";

    String start = "0.0";
    String gap = Double.toString(Float.MAX_VALUE );
    String end = Double.toString(((double) Float.MAX_VALUE) * 3D);
    String mid = Double.toString(((double) Float.MAX_VALUE) * 2D);

    assertQ(f+": checking counts for lower",
            req( "q", "id_i1:[30 TO 60]"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.method", method.toString()
                ,"facet.range.start", start
                ,"facet.range.end",   end
                ,"facet.range.gap",   gap
                ,"facet.range.other", "all"
                ,"facet.range.include", "lower"
                )
            ,"*[count("+pre+"/int)=3]"
            ,pre+"/int[@name='"+start+"'][.='6'  ]"
            ,pre+"/int[@name='"+mid+"'][.='0'  ]"
            //
            ,meta+"/double[@name='end' ][.='"+end+"']"
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='6']"
            );
  }
  
  private void helpTestFractionalNumberRangeFacets(final String fieldName) {
    helpTestFractionalNumberRangeFacets(fieldName, FacetRangeMethod.FILTER);
  }
   private void helpTestFractionalNumberRangeFacets(final String fieldName, FacetRangeMethod method) {

    final String f = fieldName;
    final String pre = "//lst[@name='facet_ranges']/lst[@name='"+f+"']/lst[@name='counts']";
    final String meta = pre + "/../";

    assertQ(f+": checking counts for lower",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.method", method.toString()
                ,"facet.range.start", "10"
                ,"facet.range.end",   "50"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.include", "lower"
                )
            ,"*[count("+pre+"/int)=4]"
            ,pre+"/int[@name='10.0'][.='1'  ]"
            ,pre+"/int[@name='20.0'][.='3'  ]"
            ,pre+"/int[@name='30.0'][.='2'  ]"
            ,pre+"/int[@name='40.0'][.='0'  ]"
            //
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='6']"
            );

    assertQ(f + ":checking counts for upper",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.method", method.toString()
                ,"facet.range.start", "10"
                ,"facet.range.end",   "50"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.include", "upper"
                )
            ,"*[count("+pre+"/int)=4]"
            ,pre+"/int[@name='10.0'][.='2'  ]"
            ,pre+"/int[@name='20.0'][.='3'  ]"
            ,pre+"/int[@name='30.0'][.='1'  ]"
            ,pre+"/int[@name='40.0'][.='0'  ]"
            //
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='6']"
            );

    assertQ(f + ":checking counts for lower & upper",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.method", method.toString()
                ,"facet.range.start", "10"
                ,"facet.range.end",   "50"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.include", "upper"
                ,"facet.range.include", "lower"
                )
            ,"*[count("+pre+"/int)=4]"
            ,pre+"/int[@name='10.0'][.='2'  ]"
            ,pre+"/int[@name='20.0'][.='4'  ]"
            ,pre+"/int[@name='30.0'][.='2'  ]"
            ,pre+"/int[@name='40.0'][.='0'  ]"
            //
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='6']"
            );

    assertQ(f + ": checking counts for upper & edge",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.method", method.toString()
                ,"facet.range.start", "20"
                ,"facet.range.end",   "50"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.include", "upper"
                ,"facet.range.include", "edge"
                )
            ,"*[count("+pre+"/int)=3]"
            ,pre+"/int[@name='20.0'][.='4'  ]"
            ,pre+"/int[@name='30.0'][.='1'  ]"
            ,pre+"/int[@name='40.0'][.='0'  ]"
            //
            ,meta+"/int[@name='before' ][.='1']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='5']"
            );

    assertQ(f + ": checking counts for upper & outer",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.method", method.toString()
                ,"facet.range.start", "10"
                ,"facet.range.end",   "30"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.include", "upper"
                ,"facet.range.include", "outer"
                )
            ,"*[count("+pre+"/int)=2]"
            ,pre+"/int[@name='10.0'][.='2'  ]"
            ,pre+"/int[@name='20.0'][.='3'  ]"
            //
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='2']"
            ,meta+"/int[@name='between'][.='5']"
            );

    assertQ(f + ": checking counts for lower & edge",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.method", method.toString()
                ,"facet.range.start", "10"
                ,"facet.range.end",   "30"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.include", "lower"
                ,"facet.range.include", "edge"
                )
            ,"*[count("+pre+"/int)=2]"
            ,pre+"/int[@name='10.0'][.='1'  ]"
            ,pre+"/int[@name='20.0'][.='4'  ]"
            //
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='5']"
            );

    assertQ(f + ": checking counts for lower & outer",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.method", method.toString()
                ,"facet.range.start", "20"
                ,"facet.range.end",   "40"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.include", "lower"
                ,"facet.range.include", "outer"
                )
            ,"*[count("+pre+"/int)=2]"
            ,pre+"/int[@name='20.0'][.='3'  ]"
            ,pre+"/int[@name='30.0'][.='2'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='5']"
            );

    assertQ(f + ": checking counts for lower & edge & outer",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.method", method.toString()
                ,"facet.range.start", "20"
                ,"facet.range.end",   "35.3"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.hardend", "true"
                ,"facet.range.include", "lower"
                ,"facet.range.include", "edge"
                ,"facet.range.include", "outer"
                )
            ,"*[count("+pre+"/int)=2]"
            ,pre+"/int[@name='20.0'][.='3'  ]"
            ,pre+"/int[@name='30.0'][.='2'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='5']"
            );

    assertQ(f + ": checking counts for include all",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.method", method.toString()
                ,"facet.range.start", "20"
                ,"facet.range.end",   "35.3"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.hardend", "true"
                ,"facet.range.include", "all"
                )
            ,"*[count("+pre+"/int)=2]"
            ,pre+"/int[@name='20.0'][.='4'  ]"
            ,pre+"/int[@name='30.0'][.='2'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='5']"
            );
  }

  @Test
  public void testNumericRangeFacetsTrieInt() {
    helpTestWholeNumberRangeFacets("id_i1");
  }
  @Test
  public void testNumericRangeFacetsTrieLong() {
    helpTestWholeNumberRangeFacets("range_facet_l");
  }
  
  @Test
  public void testNumericRangeFacetsTrieIntDocValues() {
    helpTestWholeNumberRangeFacets("id_i1", FacetRangeMethod.DV);
  }
  
  @Test
  public void testNumericRangeFacetsTrieLongDocValues() {
    helpTestWholeNumberRangeFacets("range_facet_l", FacetRangeMethod.DV);
  }

  @Test
  public void testNumericRangeFacetsOverflowTrieLong() {
    helpTestNumericRangeFacetsLongOverflow("range_facet_l", FacetRangeMethod.FILTER);
  }
  
  @Test
  public void testNumericRangeFacetsOverflowTrieLongDocValues() {
    helpTestNumericRangeFacetsLongOverflow("range_facet_l", FacetRangeMethod.DV);
  }

  private void helpTestNumericRangeFacetsLongOverflow(final String fieldName, final FacetRangeMethod method) {
    final String f = fieldName;
    final String pre = "//lst[@name='facet_ranges']/lst[@name='"+f+"']/lst[@name='counts']";
    final String meta = pre + "/../";

    String start = "0";
    String gap = Long.toString(Integer.MAX_VALUE );
    String end = Long.toString( ((long)Integer.MAX_VALUE) * 3L );
    String mid = Long.toString(((long)Integer.MAX_VALUE) * 2L );

    assertQ(f+": checking counts for lower",
            req( "q", "id_i1:[30 TO 60]"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.method", method.toString()
                ,"facet.range.start", start
                ,"facet.range.end",   end
                ,"facet.range.gap",   gap
                ,"facet.range.other", "all"
                ,"facet.range.include", "lower"
                )
            ,"*[count("+pre+"/int)=3]"
            ,pre+"/int[@name='"+start+"'][.='6'  ]"
            ,pre+"/int[@name='"+mid+"'][.='0'  ]"
            //
            ,meta+"/long[@name='end'   ][.='"+end+"']"
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='6']"
            );
  }
  
  private void helpTestWholeNumberRangeFacets(final String fieldName) {
    helpTestWholeNumberRangeFacets(fieldName, FacetRangeMethod.FILTER);
  }
  
  private void helpTestWholeNumberRangeFacets(final String fieldName, FacetRangeMethod method) {

    // the float test covers a lot of the weird edge cases
    // here we just need some basic sanity checking of the parsing

    final String f = fieldName;
    final String pre = "//lst[@name='facet_ranges']/lst[@name='"+f+"']/lst[@name='counts']";
    final String meta = pre + "/../";

    assertQ(f+": checking counts for lower",
            req( "q", "id_i1:[30 TO 60]"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.method", method.toString()
                ,"facet.range.start", "35"
                ,"facet.range.end",   "50"
                ,"facet.range.gap",   "5"
                ,"facet.range.other", "all"
                ,"facet.range.include", "lower"
                )
            ,"*[count("+pre+"/int)=3]"
            ,pre+"/int[@name='35'][.='0'  ]"
            ,pre+"/int[@name='40'][.='3'  ]"
            ,pre+"/int[@name='45'][.='3'  ]"
            //
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='6']"
            );

    assertQ(f + ":checking counts for upper",
            req( "q", "id_i1:[30 TO 60]"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.method", method.toString()
                ,"facet.range.start", "35"
                ,"facet.range.end",   "50"
                ,"facet.range.gap",   "5"
                ,"facet.range.other", "all"
                ,"facet.range.include", "upper"
                )
            ,"*[count("+pre+"/int)=3]"
            ,pre+"/int[@name='35'][.='0'  ]"
            ,pre+"/int[@name='40'][.='4'  ]"
            ,pre+"/int[@name='45'][.='2'  ]"
            //
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='6']"
            );
    
  }

  static void indexFacetSingleValued() {
    indexFacets("40","t_s1");
  }

  @Test
  public void testFacetSingleValued() {
    doFacets("t_s1");
  }
  @Test
  public void testFacetSingleValuedFcs() {
    doFacets("t_s1","facet.method","fcs");
  }

  static void indexFacets(String idPrefix, String f) {
    add_doc("id", idPrefix+"1",  f, "A");
    add_doc("id", idPrefix+"2",  f, "B");
    add_doc("id", idPrefix+"3",  f, "C");
    add_doc("id", idPrefix+"4",  f, "C");
    add_doc("id", idPrefix+"5",  f, "D");
    add_doc("id", idPrefix+"6",  f, "E");
    add_doc("id", idPrefix+"7",  f, "E");
    add_doc("id", idPrefix+"8",  f, "E");
    add_doc("id", idPrefix+"9",  f, "F");
    add_doc("id", idPrefix+"10", f, "G");
    add_doc("id", idPrefix+"11", f, "G");
    add_doc("id", idPrefix+"12", f, "G");
    add_doc("id", idPrefix+"13", f, "G");
    add_doc("id", idPrefix+"14", f, "G");
  }

  public void doFacets(String f, String... params) {
    String pre = "//lst[@name='"+f+"']";
    String notc = "id:[* TO *] -"+f+":C";


    assertQ("check counts for unlimited facet",
            req(params, "q", "id:[* TO *]", "indent","true"
                ,"facet", "true"
                ,"facet.field", f
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=7]"

            ,pre+"/int[@name='G'][.='5']"
            ,pre+"/int[@name='E'][.='3']"
            ,pre+"/int[@name='C'][.='2']"

            ,pre+"/int[@name='A'][.='1']"
            ,pre+"/int[@name='B'][.='1']"
            ,pre+"/int[@name='D'][.='1']"
            ,pre+"/int[@name='F'][.='1']"
            );

    assertQ("check counts for facet with generous limit",
            req(params, "q", "id:[* TO *]"
                ,"facet", "true"
                ,"facet.limit", "100"
                ,"facet.field", f
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=7]"

            ,pre+"/int[1][@name='G'][.='5']"
            ,pre+"/int[2][@name='E'][.='3']"
            ,pre+"/int[3][@name='C'][.='2']"

            ,pre+"/int[@name='A'][.='1']"
            ,pre+"/int[@name='B'][.='1']"
            ,pre+"/int[@name='D'][.='1']"
            ,pre+"/int[@name='F'][.='1']"
            );

    assertQ("check counts for limited facet",
            req(params, "q", "id:[* TO *]"
                ,"facet", "true"
                ,"facet.limit", "2"
                ,"facet.field", f
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"

            ,pre+"/int[1][@name='G'][.='5']"
            ,pre+"/int[2][@name='E'][.='3']"
            );

   assertQ("check offset",
            req(params, "q", "id:[* TO *]"
                ,"facet", "true"
                ,"facet.offset", "1"
                ,"facet.limit", "1"
                ,"facet.field", f
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"

            ,pre+"/int[1][@name='E'][.='3']"
            );

    assertQ("test sorted facet paging with zero (don't count in limit)",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","1"
                ,"facet.offset","0"
                ,"facet.limit","6"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=6]"
            ,pre+"/int[1][@name='G'][.='5']"
            ,pre+"/int[2][@name='E'][.='3']"
            ,pre+"/int[3][@name='A'][.='1']"
            ,pre+"/int[4][@name='B'][.='1']"
            ,pre+"/int[5][@name='D'][.='1']"
            ,pre+"/int[6][@name='F'][.='1']"
            );

    assertQ("test sorted facet paging with zero (test offset correctness)",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","1"
                ,"facet.offset","3"
                ,"facet.limit","2"
                ,"facet.sort","count"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='B'][.='1']"
            ,pre+"/int[2][@name='D'][.='1']"
            );

   assertQ("test facet unsorted paging",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","1"
                ,"facet.offset","0"
                ,"facet.limit","6"
                ,"facet.sort","index"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=6]"
            ,pre+"/int[1][@name='A'][.='1']"
            ,pre+"/int[2][@name='B'][.='1']"
            ,pre+"/int[3][@name='D'][.='1']"
            ,pre+"/int[4][@name='E'][.='3']"
            ,pre+"/int[5][@name='F'][.='1']"
            ,pre+"/int[6][@name='G'][.='5']"
            );

   assertQ("test facet unsorted paging",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","1"
                ,"facet.offset","3"
                ,"facet.limit","2"
                ,"facet.sort","index"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='E'][.='3']"
            ,pre+"/int[2][@name='F'][.='1']"
            );

    assertQ("test facet unsorted paging, mincount=2",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","2"
                ,"facet.offset","1"
                ,"facet.limit","2"
                ,"facet.sort","index"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='G'][.='5']"
            );
  }


  static void indexFacetPrefixMultiValued() {
    indexFacetPrefix("50","t_s","","ignore_s");
  }

  @Test
  public void testFacetPrefixMultiValued() {
    doFacetPrefix("t_s", null, "", "facet.method","enum");
    doFacetPrefix("t_s", null, "", "facet.method", "enum", "facet.enum.cache.minDf", "3");
    doFacetPrefix("t_s", null, "", "facet.method", "enum", "facet.enum.cache.minDf", "100");
    doFacetPrefix("t_s", null, "", "facet.method", "fc");
    doFacetExistsPrefix("t_s", null, "");
    doFacetExistsPrefix("t_s", null, "", "facet.enum.cache.minDf", "3");
    doFacetExistsPrefix("t_s", null, "", "facet.enum.cache.minDf", "100");
  }

  @Test
  public void testFacetExistsShouldThrowExceptionForMincountGreaterThanOne () throws Exception {
    final String f = "t_s";
    final List<String> msg = Arrays.asList("facet.mincount", "facet.exists", f);
    Collections.shuffle(msg, random());
    assertQEx("checking global method or per field", msg.get(0), 
        req("q", "id:[* TO *]"
            ,"indent","on"
            ,"facet","true"
            , random().nextBoolean() ? "facet.exists": "f."+f+".facet.exists", "true"
            ,"facet.field", f
            , random().nextBoolean() ? "facet.mincount" : "f."+f+".facet.mincount" ,
                 "" + (2+random().nextInt(Integer.MAX_VALUE-2))
        )
        , ErrorCode.BAD_REQUEST);
    
    assertQ("overriding per field",
        req("q", "id:[* TO *]"
            ,"indent","on"
            ,"facet","true"
            ,"facet.exists", "true"
            ,"f."+f+".facet.exists", "false"
            ,"facet.field", f
            ,"facet.mincount",""+(2+random().nextInt(Integer.MAX_VALUE-2))
        ),
        "//lst[@name='facet_fields']/lst[@name='"+f+"']");
    
    assertQ("overriding per field",
        req("q", "id:[* TO *]"
            ,"indent","on"
            ,"facet","true"
            ,"facet.exists", "true"
            ,"facet.field", f
            ,"facet.mincount",""+(2+random().nextInt(Integer.MAX_VALUE-2))
            ,"f."+f+".facet.mincount", random().nextBoolean() ? "0":"1"
        ),
        "//lst[@name='facet_fields']/lst[@name='"+f+"']");
    
  }

  static void indexFacetPrefixSingleValued() {
    indexFacetPrefix("60","tt_s1","","ignore_s");
  }

  @Test
  public void testFacetPrefixSingleValued() {
    doFacetPrefix("tt_s1", null, "");
  }
  
  @Test
  public void testFacetPrefixSingleValuedFcs() {
    doFacetPrefix("tt_s1", null, "", "facet.method","fcs");
    doFacetPrefix("tt_s1", "{!threads=0}", "", "facet.method","fcs");   // direct execution
    doFacetPrefix("tt_s1", "{!threads=-1}", "", "facet.method","fcs");  // default / unlimited threads
    doFacetPrefix("tt_s1", "{!threads=2}", "", "facet.method","fcs");   // specific number of threads
  }

  @Test
  public void testFacetExclude() {
    for (String method : new String[] {"enum", "fcs", "fc", "uif"}) {
      doFacetExclude("contains_s1", "contains_group_s1", "Astra", "facet.method", method);
    }
  }

  private void doFacetExclude(String f, String g, String termSuffix, String... params) {
    String indent="on";
    String pre = "//lst[@name='"+f+"']";

    final SolrQueryRequest req = req(params, "q", "id:[* TO *]"
        ,"indent",indent
        ,"facet","true"
        ,"facet.field", f
        ,"facet.mincount","0"
        ,"facet.offset","0"
        ,"facet.limit","100"
        ,"facet.sort","count"
        ,"facet.excludeTerms","B,BBB"+termSuffix
    );

    assertQ("test facet.exclude",
        req
        ,"*[count(//lst[@name='facet_fields']/lst/int)=10]"
        ,pre+"/int[1][@name='BBB'][.='3']"
        ,pre+"/int[2][@name='CCC'][.='3']"
        ,pre+"/int[3][@name='CCC"+termSuffix+"'][.='3']"
        ,pre+"/int[4][@name='BB'][.='2']"
        ,pre+"/int[5][@name='BB"+termSuffix+"'][.='2']"
        ,pre+"/int[6][@name='CC'][.='2']"
        ,pre+"/int[7][@name='CC"+termSuffix+"'][.='2']"
        ,pre+"/int[8][@name='AAA'][.='1']"
        ,pre+"/int[9][@name='AAA"+termSuffix+"'][.='1']"
        ,pre+"/int[10][@name='B"+termSuffix+"'][.='1']"
    );

    final SolrQueryRequest groupReq = req(params, "q", "id:[* TO *]"
        ,"indent",indent
        ,"facet","true"
        ,"facet.field", f
        ,"facet.mincount","0"
        ,"facet.offset","0"
        ,"facet.limit","100"
        ,"facet.sort","count"
        ,"facet.excludeTerms","B,BBB"+termSuffix
        ,"group","true"
        ,"group.field",g
        ,"group.facet","true"
        ,"facet.missing","true"
    );

    assertQ("test facet.exclude for grouped facets",
        groupReq
        ,"*[count(//lst[@name='facet_fields']/lst/int)=11]"
        ,pre+"/int[1][@name='CCC'][.='3']"
        ,pre+"/int[2][@name='CCC"+termSuffix+"'][.='3']"
        ,pre+"/int[3][@name='BBB'][.='2']"
        ,pre+"/int[4][@name='AAA'][.='1']"
        ,pre+"/int[5][@name='AAA"+termSuffix+"'][.='1']"
        ,pre+"/int[6][@name='B"+termSuffix+"'][.='1']"
        ,pre+"/int[7][@name='BB'][.='1']"
        ,pre+"/int[8][@name='BB"+termSuffix+"'][.='1']"
        ,pre+"/int[9][@name='CC'][.='1']"
        ,pre+"/int[10][@name='CC"+termSuffix+"'][.='1']"
        ,pre+"/int[11][.='1']"
    );

    ModifiableSolrParams modifiableSolrParams = new ModifiableSolrParams(groupReq.getParams());
    modifiableSolrParams.set("facet.limit", "0");
    groupReq.setParams(modifiableSolrParams);

    assertQ("test facet.exclude for grouped facets with facet.limit=0, facet.missing=true",
        groupReq
        ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
        ,pre+"/int[.='1']"
    );
  }

  @Test
  public void testFacetContainsAndExclude() {
    for (String method : new String[] {"enum", "fcs", "fc", "uif"}) {
      String contains = "BAst";
      String groupContains = "Ast";
      final boolean ignoreCase = random().nextBoolean();
      if (ignoreCase) {
        contains = randomizeStringCasing(contains);
        groupContains = randomizeStringCasing(groupContains);
        doFacetContainsAndExclude("contains_s1", "contains_group_s1", "Astra", contains, groupContains, "facet.method", method, "facet.contains.ignoreCase", "true");
      } else {
        doFacetContainsAndExclude("contains_s1", "contains_group_s1", "Astra", contains, groupContains, "facet.method", method);
      }
    }
  }

  private String randomizeStringCasing(String str) {
    final char[] characters = str.toCharArray();

    for (int i = 0; i != characters.length; ++i) {
      final boolean switchCase = random().nextBoolean();
      if (!switchCase) {
        continue;
      }

      final char c = str.charAt(i);
      if (Character.isUpperCase(c)) {
        characters[i] = Character.toLowerCase(c);
      } else {
        characters[i] = Character.toUpperCase(c);
      }
    }

    return new String(characters);
  }

  private void doFacetContainsAndExclude(String f, String g, String termSuffix, String contains, String groupContains, String... params) {
    String indent="on";
    String pre = "//lst[@name='"+f+"']";

    final SolrQueryRequest req = req(params, "q", "id:[* TO *]"
        ,"indent",indent
        ,"facet","true"
        ,"facet.field", f
        ,"facet.mincount","0"
        ,"facet.offset","0"
        ,"facet.limit","100"
        ,"facet.sort","count"
        ,"facet.contains",contains
        ,"facet.excludeTerms","BBB"+termSuffix
    );

    assertQ("test facet.contains with facet.exclude",
        req
        ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
        ,pre+"/int[1][@name='BB"+termSuffix+"'][.='2']"
        ,pre+"/int[2][@name='B"+termSuffix+"'][.='1']"
    );

    final SolrQueryRequest groupReq = req(params, "q", "id:[* TO *]"
        ,"indent",indent
        ,"facet","true"
        ,"facet.field", f
        ,"facet.mincount","0"
        ,"facet.offset","0"
        ,"facet.limit","100"
        ,"facet.sort","count"
        ,"facet.contains",groupContains
        ,"facet.excludeTerms","AAA"+termSuffix
        ,"group","true"
        ,"group.field",g
        ,"group.facet","true"
    );

    assertQ("test facet.contains with facet.exclude for grouped facets",
        groupReq
        ,"*[count(//lst[@name='facet_fields']/lst/int)=5]"
        ,pre+"/int[1][@name='CCC"+termSuffix+"'][.='3']"
        ,pre+"/int[2][@name='BBB"+termSuffix+"'][.='2']"
        ,pre+"/int[3][@name='B"+termSuffix+"'][.='1']"
        ,pre+"/int[4][@name='BB"+termSuffix+"'][.='1']"
        ,pre+"/int[5][@name='CC"+termSuffix+"'][.='1']"
    );
  }
  
  @Test
  //@Ignore("SOLR-8466 - facet.method=uif ignores facet.contains")
  public void testFacetContainsUif() {
    doFacetContains("contains_s1", "contains_group_s1", "Astra", "BAst", "Ast", "facet.method", "uif");
    doFacetPrefix("contains_s1", null, "Astra", "facet.method", "uif", "facet.contains", "Ast");
    doFacetPrefix("contains_s1", null, "Astra", "facet.method", "uif", "facet.contains", "aST", "facet.contains.ignoreCase", "true");
  }

  static void indexFacetContains() {
    indexFacetPrefix("70","contains_s1","","contains_group_s1");
    indexFacetPrefix("80","contains_s1","Astra","contains_group_s1");
  }
  
  @Test
  public void testFacetContains() {
    doFacetContains("contains_s1", "contains_group_s1", "Astra", "BAst", "Ast", "facet.method", "enum");
    doFacetContains("contains_s1", "contains_group_s1", "Astra", "BAst", "Ast", "facet.method", "fcs");
    doFacetContains("contains_s1", "contains_group_s1", "Astra", "BAst", "Ast", "facet.method", "fc");
    doFacetContains("contains_s1", "contains_group_s1", "Astra", "bAst", "ast", "facet.method", "enum", "facet.contains.ignoreCase", "true");
    doFacetContains("contains_s1", "contains_group_s1", "Astra", "baSt", "ast", "facet.method", "fcs", "facet.contains.ignoreCase", "true");
    doFacetContains("contains_s1", "contains_group_s1", "Astra", "basT", "ast", "facet.method", "fc", "facet.contains.ignoreCase", "true");
    doFacetPrefix("contains_s1", null, "Astra", "facet.method", "enum", "facet.contains", "Ast");
    doFacetPrefix("contains_s1", null, "Astra", "facet.method", "fcs", "facet.contains", "Ast");
    doFacetPrefix("contains_s1", null, "Astra", "facet.method", "fc", "facet.contains", "Ast");
    doFacetPrefix("contains_s1", null, "Astra", "facet.method", "enum", "facet.contains", "aSt", "facet.contains.ignoreCase", "true");
    doFacetPrefix("contains_s1", null, "Astra", "facet.method", "fcs", "facet.contains", "asT", "facet.contains.ignoreCase", "true");
    doFacetPrefix("contains_s1", null, "Astra", "facet.method", "fc", "facet.contains", "aST", "facet.contains.ignoreCase", "true");
    doFacetExistsPrefix("contains_s1", null, "Astra", "facet.contains", "Ast");
  }

  static void indexFacetPrefix(String idPrefix, String f, String termSuffix, String g) {
    add_doc("id", idPrefix+"1",  f, "AAA"+termSuffix, g, "A");
    add_doc("id", idPrefix+"2",  f, "B"+termSuffix,   g, "A");
    add_doc("id", idPrefix+"3",  f, "BB"+termSuffix,  g, "B");
    add_doc("id", idPrefix+"4",  f, "BB"+termSuffix,  g, "B");
    add_doc("id", idPrefix+"5",  f, "BBB"+termSuffix, g, "B");
    add_doc("id", idPrefix+"6",  f, "BBB"+termSuffix, g, "B");
    add_doc("id", idPrefix+"7",  f, "BBB"+termSuffix, g, "C");
    add_doc("id", idPrefix+"8",  f, "CC"+termSuffix,  g, "C");
    add_doc("id", idPrefix+"9",  f, "CC"+termSuffix,  g, "C");
    add_doc("id", idPrefix+"10", f, "CCC"+termSuffix, g, "C");
    add_doc("id", idPrefix+"11", f, "CCC"+termSuffix, g, "D");
    add_doc("id", idPrefix+"12", f, "CCC"+termSuffix, g, "E");
    assertU(commit());
  }

  public void doFacetPrefix(String f, String local, String termSuffix, String... params) {
    String indent="on";
    String pre = "//lst[@name='"+f+"']";
    String lf = local==null ? f : local+f;


    assertQ("test facet.prefix middle, exact match first term",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=3]"
            ,pre+"/int[1][@name='BBB"+termSuffix+"'][.='3']"
            ,pre+"/int[2][@name='BB"+termSuffix+"'][.='2']"
            ,pre+"/int[3][@name='B"+termSuffix+"'][.='1']"
    );

    assertQ("test facet.prefix middle, exact match first term, unsorted",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","index"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=3]"
            ,pre+"/int[1][@name='B"+termSuffix+"'][.='1']"
            ,pre+"/int[2][@name='BB"+termSuffix+"'][.='2']"
            ,pre+"/int[3][@name='BBB"+termSuffix+"'][.='3']"
    );

    assertQ("test facet.prefix middle, paging",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='BB"+termSuffix+"'][.='2']"
            ,pre+"/int[2][@name='B"+termSuffix+"'][.='1']"
    );

    assertQ("test facet.prefix middle, paging",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","1"
                    ,"facet.sort","count"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='BB"+termSuffix+"'][.='2']"
    );

    assertQ("test facet.prefix middle, paging",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","1"
                    ,"facet.sort","count"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='BB"+termSuffix+"'][.='2']"
    );

    assertQ("test facet.prefix end, not exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","C"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='CCC"+termSuffix+"'][.='3']"
            ,pre+"/int[2][@name='CC"+termSuffix+"'][.='2']"
    );

    assertQ("test facet.prefix end, exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","CC"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='CCC"+termSuffix+"'][.='3']"
            ,pre+"/int[2][@name='CC"+termSuffix+"'][.='2']"
    );

    assertQ("test facet.prefix past end",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","X"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    assertQ("test facet.prefix past end",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","-1"
                    ,"facet.sort","count"
                    ,"facet.prefix","X"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    assertQ("test facet.prefix at start, exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","AAA"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='AAA"+termSuffix+"'][.='1']"
    );
    assertQ("test facet.prefix at Start, not exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","AA"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='AAA"+termSuffix+"'][.='1']"
    );
    assertQ("test facet.prefix at Start, not exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","AA"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='AAA"+termSuffix+"'][.='1']"
    );    
    assertQ("test facet.prefix before start",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","999"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    assertQ("test facet.prefix before start",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","2"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","999"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    // test offset beyond what is collected internally in queue
    assertQ(
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","3"
                    ,"facet.offset","5"
                    ,"facet.limit","10"
                    ,"facet.sort","count"
                    ,"facet.prefix","CC"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );
  }

  public void doFacetExistsPrefix(String f, String local, String termSuffix, String... params) {
    String indent="on";
    String pre = "//lst[@name='"+f+"']";
    String lf = local==null ? f : local+f;

    assertQ("test field facet.method",
        req(params, "q", "id:[* TO *]"
            ,"indent", indent
            ,"facet", "true"
            ,"f."+lf+".facet.exists", "true"
            ,"facet.field", lf
            ,"facet.mincount", "0"
            ,"facet.offset", "0"
            ,"facet.limit", "100"
            ,"facet.sort", "count"
            ,"facet.prefix", "B"
        )
        ,"*[count(//lst[@name='facet_fields']/lst/int)=3]"
        ,pre+"/int[1][@name='B"+termSuffix+"'][.='1']"
        ,pre+"/int[2][@name='BB"+termSuffix+"'][.='1']"
        ,pre+"/int[3][@name='BBB"+termSuffix+"'][.='1']"
    );

    assertQ("test facet.prefix middle, exact match first term",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.exists", "true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=3]"
            ,pre+"/int[1][@name='B"+termSuffix+"'][.='1']"
            ,pre+"/int[2][@name='BB"+termSuffix+"'][.='1']"
            ,pre+"/int[3][@name='BBB"+termSuffix+"'][.='1']"
    );

    assertQ("test facet.prefix middle, exact match first term, unsorted",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.exists", "true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","index"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=3]"
            ,pre+"/int[1][@name='B"+termSuffix+"'][.='1']"
            ,pre+"/int[2][@name='BB"+termSuffix+"'][.='1']"
            ,pre+"/int[3][@name='BBB"+termSuffix+"'][.='1']"
    );

    assertQ("test facet.prefix middle, paging",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.exists", "true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='BB"+termSuffix+"'][.='1']"
            ,pre+"/int[2][@name='BBB"+termSuffix+"'][.='1']"
    );

    assertQ("test facet.prefix middle, paging",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.exists", "true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","1"
                    ,"facet.sort","count"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='BB"+termSuffix+"'][.='1']"
    );

    assertQ("test facet.prefix end, not exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.exists", "true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","C"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='CC"+termSuffix+"'][.='1']"
            ,pre+"/int[2][@name='CCC"+termSuffix+"'][.='1']"
    );

    assertQ("test facet.prefix end, exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.exists", "true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","CC"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='CC"+termSuffix+"'][.='1']"
            ,pre+"/int[2][@name='CCC"+termSuffix+"'][.='1']"
    );

    assertQ("test facet.prefix past end",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.exists", "true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","X"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    assertQ("test facet.prefix past end",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.exists", "true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","-1"
                    ,"facet.sort","count"
                    ,"facet.prefix","X"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    assertQ("test facet.prefix at start, exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.exists", "true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","AAA"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='AAA"+termSuffix+"'][.='1']"
    );
    assertQ("test facet.prefix at Start, not exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.exists", "true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","AA"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='AAA"+termSuffix+"'][.='1']"
    );
    assertQ("test facet.prefix before start",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.exists", "true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","999"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    assertQ("test facet.prefix before start",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.exists", "true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","2"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","999"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    // test offset beyond what is collected internally in queue
    assertQ(
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.exists", "true"
                    ,"facet.field", lf
                    ,"facet.mincount","1"
                    ,"facet.offset","5"
                    ,"facet.limit","10"
                    ,"facet.sort","count"
                    ,"facet.prefix","CC"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );
  }

  public void doFacetContains(String f, String g, String termSuffix, String contains, String groupContains, String... params) {
    String indent="on";
    String pre = "//lst[@name='"+f+"']";

    assertQ("test facet.contains",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.contains",contains
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=3]"
            ,pre+"/int[1][@name='BBB"+termSuffix+"'][.='3']"
            ,pre+"/int[2][@name='BB"+termSuffix+"'][.='2']"
            ,pre+"/int[3][@name='B"+termSuffix+"'][.='1']"
    );

    assertQ("test facet.contains for grouped facets",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.contains",groupContains
                    ,"group","true"
                    ,"group.field",g
                    ,"group.facet","true"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=6]"
            ,pre+"/int[1][@name='CCC"+termSuffix+"'][.='3']"
            ,pre+"/int[2][@name='BBB"+termSuffix+"'][.='2']"
            ,pre+"/int[3][@name='AAA"+termSuffix+"'][.='1']"
            ,pre+"/int[4][@name='B"+termSuffix+"'][.='1']"
            ,pre+"/int[5][@name='BB"+termSuffix+"'][.='1']"
            ,pre+"/int[6][@name='CC"+termSuffix+"'][.='1']"
    );
  }

  /** 
   * kind of an absurd test because if there is an infinite loop, it 
   * would never finish -- but at least it ensures that <i>if</i> one of 
   * these requests return, they return an error 
   */
  public void testRangeFacetInfiniteLoopDetection() {

    for (String field : new String[] {"foo_f", "foo_d", "foo_i"}) {
      assertQEx("no zero gap error: " + field,
                req("q", "*:*",
                    "facet", "true",
                    "facet.range", field,
                    "facet.range.start", "23",
                    "facet.range.gap", "0",
                    "facet.range.end", "100"),
                400);
    }
    String field = "foo_dt";
    assertQEx("no zero gap error for facet.range: " + field,
                req("q", "*:*",
                    "facet", "true",
                    "facet.range", field,
                    "facet.range.start", "NOW",
                    "facet.range.gap", "+0DAYS",
                    "facet.range.end", "NOW+10DAY"),
                400);
    field = "foo_f";
    assertQEx("no float underflow error: " + field,
              req("q", "*:*",
                  "facet", "true",
                  "facet.range", field,
                  "facet.range.start", "100000000000",
                  "facet.range.end", "100000086200",
                  "facet.range.gap", "2160"),
              400);

    field = "foo_d";
    assertQEx("no double underflow error: " + field,
              req("q", "*:*",
                  "facet", "true",
                  "facet.range", field,
                  "facet.range.start", "9900000000000",
                  "facet.range.end", "9900000086200",
                  "facet.range.gap", "0.0003"),
              400);
  }
  
  public void testRangeQueryHardEndParamFilter() {
    doTestRangeQueryHardEndParam("range_facet_l", FacetRangeMethod.FILTER);
  }
  
  public void testRangeQueryHardEndParamDv() {
    doTestRangeQueryHardEndParam("range_facet_l", FacetRangeMethod.DV);
  }
  
  private void doTestRangeQueryHardEndParam(String field, FacetRangeMethod method) {
    assertQ("Test facet.range.hardend",
        req("q", "id_i1:[42 TO 47]"
                ,"facet","true"
                ,"fl","id," + field
                ,"facet.range", field
                ,"facet.range.method", method.toString()
                ,"facet.range.start","43"
                ,"facet.range.end","45"
                ,"facet.range.gap","5"
                ,"facet.range.hardend", "false"
                ,"facet.range.other", "after"
        )
        ,"*[count(//lst[@name='facet_ranges']/lst)=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int)=1]"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int[@name='43'][.='5']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/long[@name='end'][.='48']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='after'][.='0']"
    );
    
    assertQ("Test facet.range.hardend",
        req("q", "id_i1:[42 TO 47]"
                ,"facet","true"
                ,"fl","id," + field
                ,"facet.range", field
                ,"facet.range.method", method.toString()
                ,"facet.range.start","43"
                ,"facet.range.end","45"
                ,"facet.range.gap","5"
                ,"facet.range.hardend", "true"
                ,"facet.range.other", "after"
        )
        ,"*[count(//lst[@name='facet_ranges']/lst)=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int)=1]"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int[@name='43'][.='2']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/long[@name='end'][.='45']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='after'][.='3']"
    );
    
  }
  
  public void testRangeQueryOtherParamFilter() {
    doTestRangeQueryOtherParam("range_facet_l", FacetRangeMethod.FILTER);
  }
  
  public void testRangeQueryOtherParamDv() {
    doTestRangeQueryOtherParam("range_facet_l", FacetRangeMethod.DV);
  }
  
  private void doTestRangeQueryOtherParam(String field, FacetRangeMethod method) {
    
    assertQ("Test facet.range.other",
        req("q", "id_i1:[42 TO 47]"
                ,"facet","true"
                ,"fl","id," + field
                ,"facet.range", field
                ,"facet.range.method", method.toString()
                ,"facet.range.start","43"
                ,"facet.range.end","45"
                ,"facet.range.gap","1"
                ,"facet.range.other", FacetRangeOther.BEFORE.toString()
        )
        ,"*[count(//lst[@name='facet_ranges']/lst)=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int)=2]"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='before'][.='1']"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='after'])=0]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='between'])=0]"
    );
    
    assertQ("Test facet.range.other",
        req("q", "id_i1:[42 TO 47]"
                ,"facet","true"
                ,"fl","id," + field
                ,"facet.range", field
                ,"facet.range.method", method.toString()
                ,"facet.range.start","43"
                ,"facet.range.end","45"
                ,"facet.range.gap","1"
                ,"facet.range.other", FacetRangeOther.AFTER.toString()
        )
        ,"*[count(//lst[@name='facet_ranges']/lst)=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int)=2]"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='after'][.='3']"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='between'])=0]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='before'])=0]"
    );
    
    assertQ("Test facet.range.other",
        req("q", "id_i1:[42 TO 47]"
                ,"facet","true"
                ,"fl","id," + field
                ,"facet.range", field
                ,"facet.range.method", method.toString()
                ,"facet.range.start","43"
                ,"facet.range.end","45"
                ,"facet.range.gap","1"
                ,"facet.range.other",FacetRangeOther.BETWEEN.toString()
        )
        ,"*[count(//lst[@name='facet_ranges']/lst)=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int)=2]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='after'])=0]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='before'])=0]"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='between'][.='2']"
    );
    
    assertQ("Test facet.range.other",
        req("q", "id_i1:[42 TO 47]"
                ,"facet","true"
                ,"fl","id," + field
                ,"facet.range", field
                ,"facet.range.method", method.toString()
                ,"facet.range.start","43"
                ,"facet.range.end","45"
                ,"facet.range.gap","1"
                ,"facet.range.other",FacetRangeOther.NONE.toString()
        )
        ,"*[count(//lst[@name='facet_ranges']/lst)=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int)=2]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='after'])=0]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='before'])=0]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='between'])=0]"
    );

    // these should have equivalent behavior (multivalued 'other' param: top level vs local)
    for (SolrQueryRequest req : new SolrQueryRequest[] {
        req("q", "id_i1:[42 TO 47]"
            ,"facet","true"
            ,"fl","id," + field
            ,"facet.range", field
            ,"facet.range.method", method.toString()
            ,"facet.range.start","43"
            ,"facet.range.end","45"
            ,"facet.range.gap","1"
            ,"facet.range.other",FacetRangeOther.BEFORE.toString()
            ,"facet.range.other",FacetRangeOther.AFTER.toString()),
        req("q", "id_i1:[42 TO 47]"
            ,"facet","true"
            ,"fl","id," + field
            ,"facet.range", "{!facet.range.other=before facet.range.other=after}" + field
            ,"facet.range.method", method.toString()
            ,"facet.range.start","43"
            ,"facet.range.end","45"
            ,"facet.range.gap","1") }) {
            
      assertQ("Test facet.range.other: " + req.toString(), req
              ,"*[count(//lst[@name='facet_ranges']/lst)=1]"
              ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "'])=1]"
              ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts'])=1]"
              ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int)=2]"
              ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='between'])=0]"
              ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='after'][.='3']"
              ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='before'][.='1']"
              );
    }
    
    assertQ("Test facet.range.other",
        req("q", "id_i1:[42 TO 47]"
                ,"facet","true"
                ,"fl","id," + field
                ,"facet.range", field
                ,"facet.range.method", method.toString()
                ,"facet.range.start","43"
                ,"facet.range.end","45"
                ,"facet.range.gap","1"
                ,"facet.range.other",FacetRangeOther.BEFORE.toString()
                ,"facet.range.other",FacetRangeOther.AFTER.toString()
                ,"facet.range.other",FacetRangeOther.NONE.toString()
        )
        ,"*[count(//lst[@name='facet_ranges']/lst)=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int)=2]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='between'])=0]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='after'])=0]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='before'])=0]"
    );
    
    assertQ("Test facet.range.other",
        req("q", "id_i1:[42 TO 47]"
                ,"facet","true"
                ,"fl","id," + field
                ,"facet.range", field
                ,"facet.range.method", method.toString()
                ,"facet.range.start","43"
                ,"facet.range.end","45"
                ,"facet.range.gap","1"
                ,"facet.range.other",FacetRangeOther.ALL.toString()
                ,"facet.range.include", FacetRangeInclude.LOWER.toString()
        )
        ,"*[count(//lst[@name='facet_ranges']/lst)=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int)=2]"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='between'][.='2']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='after'][.='3']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='before'][.='1']"
    );
    
    assertQ("Test facet.range.other",
        req("q", "id_i1:[42 TO 47]"
                ,"facet","true"
                ,"fl","id," + field
                ,"facet.range", field
                ,"facet.range.method", method.toString()
                ,"facet.range.start","43"
                ,"facet.range.end","45"
                ,"facet.range.gap","1"
                ,"facet.range.other",FacetRangeOther.ALL.toString()
                ,"facet.range.include", FacetRangeInclude.UPPER.toString()
        )
        ,"*[count(//lst[@name='facet_ranges']/lst)=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int)=2]"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='between'][.='2']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='after'][.='2']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='before'][.='2']"
    );
    
    assertQ("Test facet.range.other",
        req("q", "id_i1:[42 TO 47]"
                ,"facet","true"
                ,"fl","id," + field
                ,"facet.range", field
                ,"facet.range.method", method.toString()
                ,"facet.range.start","43"
                ,"facet.range.end","45"
                ,"facet.range.gap","1"
                ,"facet.range.other",FacetRangeOther.ALL.toString()
                ,"facet.range.include", FacetRangeInclude.EDGE.toString()
        )
        ,"*[count(//lst[@name='facet_ranges']/lst)=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int)=2]"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='between'][.='3']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='after'][.='2']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='before'][.='1']"
    );
    
    assertQ("Test facet.range.other",
        req("q", "id_i1:[42 TO 47]"
                ,"facet","true"
                ,"fl","id," + field
                ,"facet.range", field
                ,"facet.range.method", method.toString()
                ,"facet.range.start","43"
                ,"facet.range.end","45"
                ,"facet.range.gap","1"
                ,"facet.range.other", FacetRangeOther.ALL.toString()
                ,"facet.range.include", FacetRangeInclude.OUTER.toString()
        )
        ,"*[count(//lst[@name='facet_ranges']/lst)=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int)=2]"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='between'][.='1']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='after'][.='3']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='before'][.='2']"
    );
    
    assertQ("Test facet.range.other",
        req("q", "id_i1:[12345 TO 12345]"
                ,"facet","true"
                ,"fl","id," + field
                ,"facet.range", field
                ,"facet.range.method", method.toString()
                ,"facet.range.start","43"
                ,"facet.range.end","45"
                ,"facet.range.gap","1"
                ,"facet.range.other", FacetRangeOther.ALL.toString()
        )
        ,"*[count(//lst[@name='facet_ranges']/lst)=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts'])=1]"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='between'][.='0']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='after'][.='0']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='before'][.='0']"
    );
    
    assertQ("Test facet.range.other",
        req("q", "id_i1:[42 TO 47]"
                ,"facet","true"
                ,"fl","id," + field
                ,"facet.range", field
                ,"facet.range.method", method.toString()
                ,"facet.range.start","43"
                ,"facet.range.end","45"
                ,"facet.range.gap","10"
                ,"facet.range.other", FacetRangeOther.ALL.toString()
        )
        ,"*[count(//lst[@name='facet_ranges']/lst)=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts'])=1]"
        ,"*[count(//lst[@name='facet_ranges']/lst[@name='" + field + "']/lst[@name='counts']/int)=1]"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='between'][.='5']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='after'][.='0']"
        ,"//lst[@name='facet_ranges']/lst[@name='" + field + "']/int[@name='before'][.='1']"
    );
    
  }

  public void testGroupFacetErrors() {
    ModifiableSolrParams params = params("q", "*:*", "group", "true", "group.query", "myfield_s:*",
        "facet", "true", "group.facet", "true");

    // with facet.field
    SolrException ex = expectThrows(SolrException.class, () -> {
      h.query(req(params, "facet.field", "myfield_s"));
    });
    assertEquals(ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("Specify the group.field as parameter or local parameter"));

    // with facet.query
    ex = expectThrows(SolrException.class, () -> {
      h.query(req(params, "facet.query", "myfield_s:*"));
    });
    assertEquals(ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("Specify the group.field as parameter or local parameter"));

    // with facet.range
    ex = expectThrows(SolrException.class, () -> h.query(req(params, "facet.range", "range_facet_l",
        "facet.range.start", "43", "facet.range.end", "450", "facet.range.gap", "10"))
    );
    assertEquals(ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("Specify the group.field as parameter or local parameter"));

    // with facet.interval
    ex = expectThrows(SolrException.class, () -> h.query(req(params, "facet.interval", "range_facet_l",
        "f.range_facet_l.facet.interval.set", "(43,60]"))
    );
    assertEquals(ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("Interval Faceting can't be used with group.facet"));
  }
  
  public void testRangeFacetingBadRequest() {
    String field = "range_facet_l";
    ignoreException(".");
    try {
      for (FacetRangeMethod method:FacetRangeMethod.values()) {
        assertQEx("Test facet.range bad requests",
            "range facet 'end' comes before 'start'",
            req("q", "*:*"
                    ,"facet","true"
                    ,"facet.range", field
                    ,"facet.range.method", method.toString()
                    ,"facet.range.start","45"
                    ,"facet.range.end","43"
                    ,"facet.range.gap","10"
            ),
            ErrorCode.BAD_REQUEST
        );
        
        assertQEx("Test facet.range bad requests",
            "range facet infinite loop (is gap negative? did the math overflow?)",
            req("q", "*:*"
                    ,"facet","true"
                    ,"facet.range", field
                    ,"facet.range.method", method.toString()
                    ,"facet.range.start","43"
                    ,"facet.range.end","45"
                    ,"facet.range.gap","-1"
            ),
            ErrorCode.BAD_REQUEST
        );
        
        assertQEx("Test facet.range bad requests",
            "range facet infinite loop: gap is either zero, or too small relative start/end and caused underflow",
            req("q", "*:*"
                    ,"facet","true"
                    ,"facet.range", field
                    ,"facet.range.method", method.toString()
                    ,"facet.range.start","43"
                    ,"facet.range.end","45"
                    ,"facet.range.gap","0"
            ),
            ErrorCode.BAD_REQUEST
        );
        
        assertQEx("Test facet.range bad requests",
            "Missing required parameter",
            req("q", "*:*"
                    ,"facet","true"
                    ,"facet.range", field
                    ,"facet.range.method", method.toString()
                    ,"facet.range.end","45"
                    ,"facet.range.gap","5"
            ),
            ErrorCode.BAD_REQUEST
        );
        assertQEx("Test facet.range bad requests",
            "Missing required parameter",
            req("q", "*:*"
                    ,"facet","true"
                    ,"facet.range", field
                    ,"facet.range.method", method.toString()
                    ,"facet.range.start","43"
                    ,"facet.range.gap","5"
            ),
            ErrorCode.BAD_REQUEST
        );
        assertQEx("Test facet.range bad requests",
            "Missing required parameter",
            req("q", "*:*"
                    ,"facet","true"
                    ,"facet.range", field
                    ,"facet.range.method", method.toString()
                    ,"facet.range.start","43"
                    ,"facet.range.end","45"
            ),
            ErrorCode.BAD_REQUEST
        );
        assertQEx("Test facet.range bad requests",
            "Unable to range facet on field",
            req("q", "*:*"
                    ,"facet","true"
                    ,"facet.range", "contains_s1"
                    ,"facet.range.method", method.toString()
                    ,"facet.range.start","43"
                    ,"facet.range.end","45"
                    ,"facet.range.gap","5"
            ),
            ErrorCode.BAD_REQUEST
        );
        assertQEx("Test facet.range bad requests",
            "foo is not a valid method for range faceting",
            req("q", "*:*"
                    ,"facet","true"
                    ,"facet.range", field
                    ,"facet.range.method", "foo"
                    ,"facet.range.start","43"
                    ,"facet.range.end","45"
                    ,"facet.range.gap","5"
            ),
            ErrorCode.BAD_REQUEST
        );
        
        assertQEx("Test facet.range bad requests",
            "foo is not a valid type of for range 'include' information",
            req("q", "*:*"
                    ,"facet","true"
                    ,"facet.range", field
                    ,"facet.range.method", method.toString()
                    ,"facet.range.start","43"
                    ,"facet.range.end","45"
                    ,"facet.range.gap","5"
                    ,"facet.range.include", "foo"
            ),
            ErrorCode.BAD_REQUEST
        );
      }
    } finally {
      resetExceptionIgnores();
    }
    
  }
  
  @SuppressWarnings("unchecked")
  public void testRangeFacetFilterVsDocValuesRandom() throws Exception {
    for (int i = 0; i < atLeast(100); i++) {
      ModifiableSolrParams params = null;
      int fieldType = i%3;
      switch (fieldType) {
        case 0: params = getRandomParamsDate(); break;
        case 1: params = getRandomParamsInt(); break;
        case 2: params = getRandomParamsFloat(); break;
      }
      String field = params.get("facet.range");
      params.add("q", getRandomQuery());
      
      
      params.add("facet", "true");
      if (random().nextBoolean()) {
        params.add("facet.range.method", FacetRangeMethod.FILTER.toString());
      }
      
      NamedList<Object> rangeFacetsFilter;
      NamedList<Object> rangeFacetsDv;
      
      SolrQueryRequest req = req(params);
      log.info("Using Params: {}", params);
      try {
        SolrQueryResponse rsp = h.queryAndResponse("", req);
        rangeFacetsFilter = (NamedList<Object>) ((NamedList<Object>) rsp.getValues().get("facet_counts")).get("facet_ranges");
      } finally {
        req.close();
      }
      params.add("facet.range.method", FacetRangeMethod.DV.toString());
      req = req(params);
      try {
        SolrQueryResponse rsp = h.queryAndResponse("", req);
        rangeFacetsDv = (NamedList<Object>) ((NamedList<Object>) rsp.getValues().get("facet_counts")).get("facet_ranges");
      } finally {
        req.close();
      }
      
      assertNotNull(rangeFacetsFilter.get(field));
      assertNotNull(rangeFacetsDv.get(field));
      
      assertSameResults("Different results obtained when using 'filter' and 'dv' methods for Range Facets using params."
          + params + "\n" + "Filter:" + rangeFacetsFilter + "\n DV: " + rangeFacetsDv, 
          (NamedList<Object>)rangeFacetsFilter.get(field), (NamedList<Object>)rangeFacetsDv.get(field));
    }
    
  }

  public void testFacetPrefixWithFacetThreads() throws Exception  {
    assertQ("Test facet.prefix with facet.thread",
        req("q", "id_i1:[101 TO 102]"
            ,"facet","true"
            ,"facet.field", "{!key=key1 facet.prefix=foo}myfield_s"
            ,"facet.field", "{!key=key2 facet.prefix=bar}myfield_s"
            ,"facet.threads", "1"
        )
        ,"*[count(//lst[@name='facet_fields']/lst[@name='key1']/int[@name='foo'])=1]"
        ,"*[count(//lst[@name='facet_fields']/lst[@name='key2']/int[@name='bar'])=1]"
    );

  }

  private String getRandomQuery() {
    if (rarely()) {
      return "*:*";
    }
    Integer[] values = new Integer[2];
    values[0] = random().nextInt(3000);
    values[1] = random().nextInt(3000);
    Arrays.sort(values);
    return String.format(Locale.ROOT,  "id_i1:[%d TO %d]", values[0], values[1]);
  }


  private void assertSameResults(String message,
      NamedList<Object> rangeFacetsFilter, NamedList<Object> rangeFacetsDv) {
    assertEquals(message + " Different number of elements.", rangeFacetsFilter.size(), rangeFacetsDv.size());
    for (Map.Entry<String, Object> entry:rangeFacetsFilter) {
      if (entry.getKey().equals("counts")) {
        continue;
      }
      Object value = rangeFacetsDv.get(entry.getKey());
      if (value == null) {
        fail(message + " Element not found with 'dv' method: " + entry.getKey());
      }
      assertEquals(message + "Different value for key " + entry.getKey(), entry.getValue(), value);
    }
    assertNotNull("Null counts: " + rangeFacetsFilter, rangeFacetsFilter.get("counts"));
    assertNotNull("Null counts: " + rangeFacetsDv, rangeFacetsDv.get("counts"));
    assertEquals(message + "Different counts", rangeFacetsFilter.get("counts"), rangeFacetsDv.get("counts"));
  }

  private ModifiableSolrParams getRandomParamsInt() {
    String field = new String[]{"range_facet_l_dv", "range_facet_i_dv", "range_facet_l", "duration_i1", "id_i1"}[random().nextInt(5)];
    ModifiableSolrParams params = new ModifiableSolrParams();
    Integer[] values = new Integer[2];
    do {
      values[0] = random().nextInt(3000) * (random().nextBoolean()?-1:1);
      values[1] = random().nextInt(3000) * (random().nextBoolean()?-1:1);
    } while (values[0].equals(values[1]));
    Arrays.sort(values);
    long gapNum = Math.max(1, random().nextInt(3000));
    
    params.add(FacetParams.FACET_RANGE_START, String.valueOf(values[0]));
    params.add(FacetParams.FACET_RANGE_END, String.valueOf(values[1]));
    params.add(FacetParams.FACET_RANGE_GAP, String.format(Locale.ROOT, "+%d", gapNum));
    addCommonRandomRangeParams(params);
    params.add(FacetParams.FACET_RANGE, field);
    return params;
  }
  
  private ModifiableSolrParams getRandomParamsFloat() {
    String field = new String[]{"range_facet_d_dv", "range_facet_f_dv", "range_facet_d", "range_facet_f", "range_facet_mv_f", "range_facet_f1", "range_facet_f1_dv"}[random().nextInt(7)];
    ModifiableSolrParams params = new ModifiableSolrParams();
    Float[] values = new Float[2];
    do {
      values[0] = random().nextFloat() * 3000 * (random().nextBoolean()?-1:1);
      values[1] = random().nextFloat() * 3000 * (random().nextBoolean()?-1:1);
    } while (values[0].equals(values[1]));
    Arrays.sort(values);
    float gapNum = Math.max(1, random().nextFloat() * 3000);
    
    params.add(FacetParams.FACET_RANGE_START, String.valueOf(values[0]));
    params.add(FacetParams.FACET_RANGE_END, String.valueOf(values[1]));
    params.add(FacetParams.FACET_RANGE_GAP, String.format(Locale.ROOT, "+%f", gapNum));
    addCommonRandomRangeParams(params);
    params.add(FacetParams.FACET_RANGE, field);
    return params;
  }
  
  private final static String[] DATE_GAP_UNITS = new String[]{"SECONDS", "MINUTES", "HOURS", "DAYS", "MONTHS", "YEARS"};
      
  private ModifiableSolrParams getRandomParamsDate() {
    String field = new String[]{"range_facet_dt_dv", "a_tdt", "bday"}[random().nextInt(3)];
    ModifiableSolrParams params = new ModifiableSolrParams();
    Date[] dates = new Date[2];
    do {
      dates[0] = new Date((long)(random().nextDouble()*(new Date().getTime()) * (random().nextBoolean()?-1:1)));
      dates[1] = new Date((long)(random().nextDouble()*(new Date().getTime()) * (random().nextBoolean()?-1:1)));
    } while (dates[0].equals(dates[1]));
    Arrays.sort(dates);
    long dateDiff = (dates[1].getTime() - dates[0].getTime())/1000;
    String gapUnit;
    if (dateDiff < 1000) {
      gapUnit = DATE_GAP_UNITS[random().nextInt(DATE_GAP_UNITS.length)];
    } else if (dateDiff < 10000){
      gapUnit = DATE_GAP_UNITS[1 + random().nextInt(DATE_GAP_UNITS.length - 1)];
    } else if (dateDiff < 100000){
      gapUnit = DATE_GAP_UNITS[2 + random().nextInt(DATE_GAP_UNITS.length - 2)];
    } else if (dateDiff < 1000000){
      gapUnit = DATE_GAP_UNITS[3 + random().nextInt(DATE_GAP_UNITS.length - 3)];
    } else {
      gapUnit = DATE_GAP_UNITS[4 + random().nextInt(DATE_GAP_UNITS.length - 4)];
    }
    int gapNum = random().nextInt(100) + 1;
    
    params.add(FacetParams.FACET_RANGE_START, dates[0].toInstant().toString());
    params.add(FacetParams.FACET_RANGE_END, dates[1].toInstant().toString());
    params.add(FacetParams.FACET_RANGE_GAP, String.format(Locale.ROOT, "+%d%s", gapNum, gapUnit));
    addCommonRandomRangeParams(params);
    params.add(FacetParams.FACET_RANGE, field);
    return params;
  }


  private void addCommonRandomRangeParams(ModifiableSolrParams params) {
    for (int i = 0; i < random().nextInt(2); i++) {
      params.add(FacetParams.FACET_RANGE_OTHER, FacetRangeOther.values()[random().nextInt(FacetRangeOther.values().length)].toString());
    }
    if (random().nextBoolean()) {
      params.add(FacetParams.FACET_RANGE_INCLUDE, FacetRangeInclude.values()[random().nextInt(FacetRangeInclude.values().length)].toString());
    }
    if (random().nextBoolean()) {
      params.add(FacetParams.FACET_MINCOUNT, String.valueOf(random().nextInt(10)));
    }
    params.add(FacetParams.FACET_RANGE_HARD_END, String.valueOf(random().nextBoolean()));
  }

}
