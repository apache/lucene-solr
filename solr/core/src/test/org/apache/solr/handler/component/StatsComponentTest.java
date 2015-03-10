package org.apache.solr.handler.component;
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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.StatsParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.StatsField.Stat;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.AbstractSolrTestCase;

import org.apache.commons.math3.util.Combinations;

import org.junit.BeforeClass;


/**
 * Statistics Component Test
 */
public class StatsComponentTest extends AbstractSolrTestCase {

  final static String XPRE = "/response/lst[@name='stats']/";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema11.xml");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
    lrf = h.getRequestFactory("standard", 0, 20);
  }

  public void testStats() throws Exception {
    for (String f : new String[] {
            "stats_i","stats_l","stats_f","stats_d",
            "stats_ti","stats_tl","stats_tf","stats_td",
            "stats_ti_dv","stats_tl_dv","stats_tf_dv","stats_td_dv", 
            "stats_ti_ni_dv","stats_tl_ni_dv","stats_tf_ni_dv","stats_td_ni_dv"
    }) {

      // all of our checks should work with all of these params
      // ie: with or w/o these excluded filters, results should be the same.
      SolrParams[] baseParamsSet = new SolrParams[] {
        // NOTE: doTestFieldStatisticsResult needs the full list of possible tags to exclude
        params("stats.field", f, "stats", "true"),
        params("stats.field", "{!ex=fq1,fq2}"+f, "stats", "true",
               "fq", "{!tag=fq1}-id:[0 TO 2]", 
               "fq", "{!tag=fq2}-id:[2 TO 1000]"), 
        params("stats.field", "{!ex=fq1}"+f, "stats", "true",
               "fq", "{!tag=fq1}id:1")
      };

      doTestFieldStatisticsResult(f, baseParamsSet);
      doTestFieldStatisticsMissingResult(f, baseParamsSet);
      doTestFacetStatisticsResult(f, baseParamsSet);
      doTestFacetStatisticsMissingResult(f, baseParamsSet);
      
      clearIndex();
      assertU(commit());
    }

    for (String f : new String[] {"stats_ii",
            "stats_tis","stats_tfs","stats_tls","stats_tds",  // trie fields
            "stats_tis_dv","stats_tfs_dv","stats_tls_dv","stats_tds_dv",  // Doc Values
            "stats_tis_ni_dv","stats_tfs_ni_dv","stats_tls_ni_dv","stats_tds_ni_dv"  // Doc Values Not indexed
                                  }) {

      doTestMVFieldStatisticsResult(f);
      clearIndex();
      assertU(commit());
    }
  }

  public void doTestFieldStatisticsResult(String f, SolrParams[] baseParamsSet) throws Exception {
    // used when doing key overrides in conjunction with the baseParamsSet
    //
    // even when these aren't included in the request, using them helps us
    // test the code path of an exclusion that refers to an fq that doesn't exist
    final String all_possible_ex = "fq1,fq2";

    assertU(adoc("id", "1", f, "-10"));
    assertU(adoc("id", "2", f, "-20"));
    assertU(commit());
    assertU(adoc("id", "3", f, "-30"));
    assertU(adoc("id", "4", f, "-40"));
    assertU(commit());

    final String fpre = XPRE + "lst[@name='stats_fields']/lst[@name='"+f+"']/";

    final String key = "key_key";
    final String kpre = XPRE + "lst[@name='stats_fields']/lst[@name='"+key+"']/";

    // status should be the same regardless of baseParams
    for (SolrParams baseParams : baseParamsSet) {
      for (String ct : new String[] {"stats.calcdistinct", "f."+f+".stats.calcdistinct"}) {
        assertQ("test statistics values using: " + ct, 
                req(baseParams, "q", "*:*", ct, "true")
                , fpre + "double[@name='min'][.='-40.0']"
                , fpre + "double[@name='max'][.='-10.0']"
                , fpre + "double[@name='sum'][.='-100.0']"
                , fpre + "long[@name='count'][.='4']"
                , fpre + "long[@name='missing'][.='0']"
                , fpre + "long[@name='countDistinct'][.='4']"
                , "count(" + fpre + "arr[@name='distinctValues']/*)=4"
                , fpre + "double[@name='sumOfSquares'][.='3000.0']"
                , fpre + "double[@name='mean'][.='-25.0']"
                , fpre + "double[@name='stddev'][.='12.909944487358056']"
                );  
        
        assertQ("test statistics w/fq using: " + ct, 
                req(baseParams, "q", "*:*", "fq", "-id:4", ct, "true")
                , fpre + "double[@name='min'][.='-30.0']"
                , fpre + "double[@name='max'][.='-10.0']"
                , fpre + "double[@name='sum'][.='-60.0']"
                , fpre + "long[@name='count'][.='3']"
                , fpre + "long[@name='missing'][.='0']"
                , fpre + "long[@name='countDistinct'][.='3']"
                , "count(" + fpre + "arr[@name='distinctValues']/*)=3"
                , fpre + "double[@name='sumOfSquares'][.='1400.0']"
                , fpre + "double[@name='mean'][.='-20.0']"
                , fpre + "double[@name='stddev'][.='10.0']"
                );  
        
        // now do both in a single query

        assertQ("test statistics w & w/fq via key override using: " + ct, 
                req(baseParams, "q", "*:*", ct, "true",
                    "fq", "{!tag=key_ex_tag}-id:4", 
                    "stats.field", "{!key="+key+" ex=key_ex_tag,"+all_possible_ex+"}"+f)

                // field name key, fq is applied
                , fpre + "double[@name='min'][.='-30.0']"
                , fpre + "double[@name='max'][.='-10.0']"
                , fpre + "double[@name='sum'][.='-60.0']"
                , fpre + "long[@name='count'][.='3']"
                , fpre + "long[@name='missing'][.='0']"
                , fpre + "long[@name='countDistinct'][.='3']"
                , "count(" + fpre + "arr[@name='distinctValues']/*)=3"
                , fpre + "double[@name='sumOfSquares'][.='1400.0']"
                , fpre + "double[@name='mean'][.='-20.0']"
                , fpre + "double[@name='stddev'][.='10.0']"

                // overridden key, fq is excluded
                , kpre + "double[@name='min'][.='-40.0']"
                , kpre + "double[@name='max'][.='-10.0']"
                , kpre + "double[@name='sum'][.='-100.0']"
                , kpre + "long[@name='count'][.='4']"
                , kpre + "long[@name='missing'][.='0']"
                , kpre + "long[@name='countDistinct'][.='4']"
                , "count(" + kpre + "arr[@name='distinctValues']/*)=4"
                , kpre + "double[@name='sumOfSquares'][.='3000.0']"
                , kpre + "double[@name='mean'][.='-25.0']"
                , kpre + "double[@name='stddev'][.='12.909944487358056']"

                );

      }
    }

    // we should be able to compute exact same stats for a field even
    // when we specify it using the "field()" function, or use other 
    // identify equivilent functions
    for (String param : new String[] {
        // bare
        "{!key="+key+" ex=key_ex_tag}" + f,
        "{!key="+key+" ex=key_ex_tag v="+f+"}",
        // field func
        "{!lucene key="+key+" ex=key_ex_tag}_val_:\"field("+f+")\"",
        "{!func key="+key+" ex=key_ex_tag}field("+f+")",
        "{!type=func key="+key+" ex=key_ex_tag}field("+f+")",
        "{!type=func key="+key+" ex=key_ex_tag v=field("+f+")}",
        "{!type=func key="+key+" ex=key_ex_tag v='field("+f+")'}",
        // identity math functions
        "{!type=func key="+key+" ex=key_ex_tag v='sum(0,"+f+")'}",
        "{!type=func key="+key+" ex=key_ex_tag v='product(1,"+f+")'}",
      }) {
      
      assertQ("test statistics over field specified as a function: " + param,
              // NOTE: baseParams aren't used, we're looking at the function
              req("q", "*:*", "stats", "true", "stats.calcdistinct", "true",
                  "fq", "{!tag=key_ex_tag}-id:4", 
                  "stats.field", param)
              
              , kpre + "double[@name='min'][.='-40.0']"
              , kpre + "double[@name='max'][.='-10.0']"
              , kpre + "double[@name='sum'][.='-100.0']"
              , kpre + "long[@name='count'][.='4']"
              , kpre + "long[@name='missing'][.='0']"
              , kpre + "long[@name='countDistinct'][.='4']"
              , "count(" + kpre + "arr[@name='distinctValues']/*)=4"
              , kpre + "double[@name='sumOfSquares'][.='3000.0']"
              , kpre + "double[@name='mean'][.='-25.0']"
              , kpre + "double[@name='stddev'][.='12.909944487358056']"
              
              );
    }
    
    // now get stats over a non-trivial function on our (single) field
    String func = "product(2, " + f + ")";
    assertQ("test function statistics & key override", 
            // NOTE: baseParams aren't used, we're looking at the function
            req("q", "*:*", "stats", "true", "stats.calcdistinct", "true",
                "fq", "{!tag=key_ex_tag}-id:4", 
                "stats.field", "{!func key="+key+" ex=key_ex_tag}"+func)

            , kpre + "double[@name='min'][.='-80.0']"
            , kpre + "double[@name='max'][.='-20.0']"
            , kpre + "double[@name='sum'][.='-200.0']"
            , kpre + "long[@name='count'][.='4']"
            , kpre + "long[@name='missing'][.='0']"
            , kpre + "long[@name='countDistinct'][.='4']"
            , "count(" + kpre + "arr[@name='distinctValues']/*)=4"
            , kpre + "double[@name='sumOfSquares'][.='12000.0']"
            , kpre + "double[@name='mean'][.='-50.0']" 
            , kpre + "double[@name='stddev'][.='25.81988897471611']"
            );
  }

  


  public void doTestMVFieldStatisticsResult(String f) throws Exception {
    assertU(adoc("id", "1", f, "-10", f, "-100", "active_s", "true"));
    assertU(adoc("id", "2", f, "-20", f, "200", "active_s", "true"));
    assertU(commit());
    assertU(adoc("id", "3", f, "-30", f, "-1", "active_s", "false"));
    assertU(adoc("id", "4", f, "-40", f, "10", "active_s", "false"));
    assertU(adoc("id", "5", "active_s", "false"));
    assertU(adoc("id", "6", "active_s", "false"));
    assertU(adoc("id", "7", "active_s", "true"));
    
    assertU(commit());

    // with or w/o these excluded filters, results should be the same
    for (SolrParams baseParams : new SolrParams[] {
        params("stats.field", f, "stats", "true"),
        params("stats.field", "{!ex=fq1}"+f, "stats", "true",
               "fq", "{!tag=fq1}id:1"),
        params("stats.field", "{!ex=fq1,fq2}"+f, "stats", "true",
               "fq", "{!tag=fq1}-id:[0 TO 2]", 
               "fq", "{!tag=fq2}-id:[2 TO 1000]")  }) {
      
      
      assertQ("test statistics values", 
              req(baseParams, "q", "*:*", "stats.calcdistinct", "true")
              , "//double[@name='min'][.='-100.0']"
              , "//double[@name='max'][.='200.0']"
              , "//double[@name='sum'][.='9.0']"
              , "//long[@name='count'][.='8']"
              , "//long[@name='missing'][.='3']"
              , "//long[@name='countDistinct'][.='8']"
              , "count(//arr[@name='distinctValues']/*)=8"
              , "//double[@name='sumOfSquares'][.='53101.0']"
              , "//double[@name='mean'][.='1.125']"
              , "//double[@name='stddev'][.='87.08852228787508']"
              );

      assertQ("test statistics values w/fq", 
              req(baseParams, "fq", "-id:1",
                  "q", "*:*", "stats.calcdistinct", "true")
              , "//double[@name='min'][.='-40.0']"
              , "//double[@name='max'][.='200.0']"
              , "//double[@name='sum'][.='119.0']"
              , "//long[@name='count'][.='6']"
              , "//long[@name='missing'][.='3']"
              , "//long[@name='countDistinct'][.='6']"
              , "count(//arr[@name='distinctValues']/*)=6"
              , "//double[@name='sumOfSquares'][.='43001.0']"
              , "//double[@name='mean'][.='19.833333333333332']"
              , "//double[@name='stddev'][.='90.15634568163611']"
              );
      
      // TODO: why are there 3 identical requests below?
      
      assertQ("test statistics values", 
              req(baseParams, "q", "*:*", "stats.calcdistinct", "true", "stats.facet", "active_s")
              , "//double[@name='min'][.='-100.0']"
              , "//double[@name='max'][.='200.0']"
              , "//double[@name='sum'][.='9.0']"
              , "//long[@name='count'][.='8']"
              , "//long[@name='missing'][.='3']"
              , "//long[@name='countDistinct'][.='8']"
              , "count(//lst[@name='" + f + "']/arr[@name='distinctValues']/*)=8"
              , "//double[@name='sumOfSquares'][.='53101.0']"
              , "//double[@name='mean'][.='1.125']"
              , "//double[@name='stddev'][.='87.08852228787508']"
              );
      
      assertQ("test value for active_s=true", 
              req(baseParams, "q", "*:*", "stats.calcdistinct", "true", "stats.facet", "active_s")
              , "//lst[@name='true']/double[@name='min'][.='-100.0']"
              , "//lst[@name='true']/double[@name='max'][.='200.0']"
              , "//lst[@name='true']/double[@name='sum'][.='70.0']"
              , "//lst[@name='true']/long[@name='count'][.='4']"
              , "//lst[@name='true']/long[@name='missing'][.='1']"
              , "//lst[@name='true']//long[@name='countDistinct'][.='4']"
              , "count(//lst[@name='true']/arr[@name='distinctValues']/*)=4"
              , "//lst[@name='true']/double[@name='sumOfSquares'][.='50500.0']"
              , "//lst[@name='true']/double[@name='mean'][.='17.5']"
              , "//lst[@name='true']/double[@name='stddev'][.='128.16005617976296']"
              );
      
      assertQ("test value for active_s=false", 
              req(baseParams, "q", "*:*", "stats.calcdistinct", "true", "stats.facet", "active_s")
              , "//lst[@name='false']/double[@name='min'][.='-40.0']"
              , "//lst[@name='false']/double[@name='max'][.='10.0']"
              , "//lst[@name='false']/double[@name='sum'][.='-61.0']"
              , "//lst[@name='false']/long[@name='count'][.='4']"
              , "//lst[@name='false']/long[@name='missing'][.='2']"
              , "//lst[@name='true']//long[@name='countDistinct'][.='4']"
              , "count(//lst[@name='true']/arr[@name='distinctValues']/*)=4"
              , "//lst[@name='false']/double[@name='sumOfSquares'][.='2601.0']"
              , "//lst[@name='false']/double[@name='mean'][.='-15.25']"
              , "//lst[@name='false']/double[@name='stddev'][.='23.59908190304586']"
              );
    }

  }

  public void testFieldStatisticsResultsStringField() throws Exception {
    SolrCore core = h.getCore();
    assertU(adoc("id", "1", "active_s", "string1"));
    assertU(adoc("id", "2", "active_s", "string2"));
    assertU(adoc("id", "3", "active_s", "string3"));
    assertU(adoc("id", "4"));
    assertU(commit());

    Map<String, String> args = new HashMap<>();
    args.put(CommonParams.Q, "*:*");
    args.put(StatsParams.STATS, "true");
    args.put(StatsParams.STATS_FIELD, "active_s");
    args.put("f.active_s.stats.calcdistinct","true");
    args.put("indent", "true");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(args));

    assertQ("test string statistics values", req,
            "//str[@name='min'][.='string1']",
            "//str[@name='max'][.='string3']",
            "//long[@name='count'][.='3']",
            "//long[@name='missing'][.='1']",
            "//long[@name='countDistinct'][.='3']",
            "count(//arr[@name='distinctValues']/str)=3");

    // stats over a string function
    assertQ("strdist func stats",
            req("q", "*:*",
                "stats","true",
                "stats.field","{!func}strdist('string22',active_s,edit)")
            , "//double[@name='min'][.='0.75']"
            , "//double[@name='max'][.='0.875']"
            , "//double[@name='sum'][.='2.375']"
            , "//long[@name='count'][.='3']"
            ,"//long[@name='missing'][.='1']"
            );

  }

  public void testFieldStatisticsResultsDateField() throws Exception {
    SolrCore core = h.getCore();

    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.ROOT);
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

    String date1 = dateFormat.format(new Date(123456789)) + "Z";
    String date2 = dateFormat.format(new Date(987654321)) + "Z";

    assertU(adoc("id", "1", "active_dt", date1));
    assertU(adoc("id", "2", "active_dt", date2));
    assertU(adoc("id", "3"));
    assertU(commit());

    Map<String, String> args = new HashMap<>();
    args.put(CommonParams.Q, "*:*");
    args.put(StatsParams.STATS, "true");
    args.put(StatsParams.STATS_FIELD, "active_dt");
    args.put("f.active_dt.stats.calcdistinct","true");
    args.put("indent", "true");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(args));

    assertQ("test date statistics values", req,
            "//long[@name='count'][.='2']",
            "//long[@name='missing'][.='1']",
            "//date[@name='min'][.='1970-01-02T10:17:36Z']",
            "//date[@name='max'][.='1970-01-12T10:20:54Z']",
            "//long[@name='countDistinct'][.='2']",
            "count(//arr[@name='distinctValues']/date)=2"
        //  "//date[@name='sum'][.='1970-01-13T20:38:30Z']",  // sometimes 29.999Z
        //  "//date[@name='mean'][.='1970-01-07T10:19:15Z']"  // sometiems 14.999Z
            );
  }


  public void doTestFieldStatisticsMissingResult(String f, SolrParams[] baseParamsSet) throws Exception {
    assertU(adoc("id", "1", f, "-10"));
    assertU(adoc("id", "2", f, "-20"));
    assertU(commit());
    assertU(adoc("id", "3"));
    assertU(adoc("id", "4", f, "-40"));
    assertU(commit());

    final String fpre = XPRE + "lst[@name='stats_fields']/lst[@name='"+f+"']/";
    final String key = "key_key";
    final String kpre = XPRE + "lst[@name='stats_fields']/lst[@name='"+key+"']/";

    // status should be the same regardless of baseParams
    for (SolrParams baseParams : baseParamsSet) {

      SolrQueryRequest request = req(baseParams, "q", "*:*", "stats.calcdistinct", "true");

      assertQ("test statistics values", request
              , "//double[@name='min'][.='-40.0']"
              , "//double[@name='max'][.='-10.0']"
              , "//double[@name='sum'][.='-70.0']"
              , "//long[@name='count'][.='3']"
              , "//long[@name='missing'][.='1']"
              , "//long[@name='countDistinct'][.='3']"
              , "count(//arr[@name='distinctValues']/*)=3"
              , "//double[@name='sumOfSquares'][.='2100.0']"
              , "//double[@name='mean'][.='-23.333333333333332']"
              , "//double[@name='stddev'][.='15.275252316519467']"
              );
    }

    // we should be able to compute exact same stats for a field even
    // when we specify it using the "field()" function, or use other 
    // identify equivilent functions
    for (String param : new String[] {
        // bare
        "{!key="+key+" ex=key_ex_tag}" + f,
        "{!key="+key+" ex=key_ex_tag v="+f+"}",
        // field func
        "{!lucene key="+key+" ex=key_ex_tag}_val_:\"field("+f+")\"",
        "{!func key="+key+" ex=key_ex_tag}field("+f+")",
        "{!type=func key="+key+" ex=key_ex_tag}field("+f+")",
        "{!type=func key="+key+" ex=key_ex_tag v=field("+f+")}",
        "{!type=func key="+key+" ex=key_ex_tag v='field("+f+")'}",
        // identity math functions
        "{!type=func key="+key+" ex=key_ex_tag v='sum(0,"+f+")'}",
        "{!type=func key="+key+" ex=key_ex_tag v='product(1,"+f+")'}",
      }) {
      
      assertQ("test statistics over field specified as a function: " + param,
              // NOTE: baseParams aren't used, we're looking at the function
              req("q", "*:*", "stats", "true", "stats.calcdistinct", "true",
                  "fq", "{!tag=key_ex_tag}-id:4", 
                  "stats.field", param)
              
              , kpre + "double[@name='min'][.='-40.0']"
              , kpre + "double[@name='max'][.='-10.0']"
              , kpre + "double[@name='sum'][.='-70.0']"
              , kpre + "long[@name='count'][.='3']"
              , kpre + "long[@name='missing'][.='1']"
              , kpre + "long[@name='countDistinct'][.='3']"
              , "count(" + kpre + "arr[@name='distinctValues']/*)=3"
              , kpre + "double[@name='sumOfSquares'][.='2100.0']"
              , kpre + "double[@name='mean'][.='-23.333333333333332']"
              , kpre + "double[@name='stddev'][.='15.275252316519467']"
              
              );
    }

  }

  public void doTestFacetStatisticsResult(String f, SolrParams[] baseParamsSet) throws Exception {
    assertU(adoc("id", "1", f, "10", "active_s", "true",  "other_s", "foo"));
    assertU(adoc("id", "2", f, "20", "active_s", "true",  "other_s", "bar"));
    assertU(commit());
    assertU(adoc("id", "3", f, "30", "active_s", "false", "other_s", "foo"));
    assertU(adoc("id", "4", f, "40", "active_s", "false", "other_s", "foo"));
    assertU(commit());
    
    final String pre = "//lst[@name='stats_fields']/lst[@name='"+f+"']/lst[@name='facets']/lst[@name='active_s']";

    // status should be the same regardless of baseParams
    for (SolrParams baseParams : baseParamsSet) {

      assertQ("test value for active_s=true", 
              req(baseParams, 
                  "q", "*:*", "stats.calcdistinct", "true",
                  "stats.facet", "active_s", "stats.facet", "other_s")
              , "*[count("+pre+")=1]"
              , pre+"/lst[@name='true']/double[@name='min'][.='10.0']"
              , pre+"/lst[@name='true']/double[@name='max'][.='20.0']"
              , pre+"/lst[@name='true']/double[@name='sum'][.='30.0']"
              , pre+"/lst[@name='true']/long[@name='count'][.='2']"
              , pre+"/lst[@name='true']/long[@name='missing'][.='0']"
              , pre + "/lst[@name='true']/long[@name='countDistinct'][.='2']"
              , "count(" + pre + "/lst[@name='true']/arr[@name='distinctValues']/*)=2"
              , pre+"/lst[@name='true']/double[@name='sumOfSquares'][.='500.0']"
              , pre+"/lst[@name='true']/double[@name='mean'][.='15.0']"
              , pre+"/lst[@name='true']/double[@name='stddev'][.='7.0710678118654755']"
              );

      assertQ("test value for active_s=false", 
              req(baseParams, "q", "*:*", "stats.calcdistinct", "true", "stats.facet", "active_s")
              , pre+"/lst[@name='false']/double[@name='min'][.='30.0']"
              , pre+"/lst[@name='false']/double[@name='max'][.='40.0']"
              , pre+"/lst[@name='false']/double[@name='sum'][.='70.0']"
              , pre+"/lst[@name='false']/long[@name='count'][.='2']"
              , pre+"/lst[@name='false']/long[@name='missing'][.='0']"
              , pre + "/lst[@name='true']/long[@name='countDistinct'][.='2']"
              , "count(" + pre + "/lst[@name='true']/arr[@name='distinctValues']/*)=2"
              , pre+"/lst[@name='false']/double[@name='sumOfSquares'][.='2500.0']"
              , pre+"/lst[@name='false']/double[@name='mean'][.='35.0']"
              , pre+"/lst[@name='false']/double[@name='stddev'][.='7.0710678118654755']"
              );
    }

    // we should be able to compute exact same stats & stats.facet for a field even
    // when we specify it using the "field()" function, or use other 
    // identify equivilent functions
    for (String param : new String[] {
        // bare
        "{!key="+f+" ex=key_ex_tag}" + f,
        "{!key="+f+" ex=key_ex_tag v="+f+"}",
        // field func
        "{!lucene key="+f+" ex=key_ex_tag}_val_:\"field("+f+")\"",
        "{!func key="+f+" ex=key_ex_tag}field("+f+")",
        "{!type=func key="+f+" ex=key_ex_tag}field("+f+")",
        "{!type=func key="+f+" ex=key_ex_tag v=field("+f+")}",
        "{!type=func key="+f+" ex=key_ex_tag v='field("+f+")'}",
        // identity math functions
        "{!type=func key="+f+" ex=key_ex_tag v='sum(0,"+f+")'}",
        "{!type=func key="+f+" ex=key_ex_tag v='product(1,"+f+")'}",
      }) {
      assertQ("test statis & stats.facet over field specified as a function: " + param,
              req("q", "*:*", "stats", "true", "stats.calcdistinct", "true",
                  "fq", "{!tag=key_ex_tag}-id:4", 
                  "stats.field", param,
                  "stats.facet", "active_s", "stats.facet", "other_s")
              , "*[count("+pre+")=1]"
              , pre+"/lst[@name='true']/double[@name='min'][.='10.0']"
              , pre+"/lst[@name='true']/double[@name='max'][.='20.0']"
              , pre+"/lst[@name='true']/double[@name='sum'][.='30.0']"
              , pre+"/lst[@name='true']/long[@name='count'][.='2']"
              , pre+"/lst[@name='true']/long[@name='missing'][.='0']"
              , pre + "/lst[@name='true']/long[@name='countDistinct'][.='2']"
              , "count(" + pre + "/lst[@name='true']/arr[@name='distinctValues']/*)=2"
              , pre+"/lst[@name='true']/double[@name='sumOfSquares'][.='500.0']"
              , pre+"/lst[@name='true']/double[@name='mean'][.='15.0']"
              , pre+"/lst[@name='true']/double[@name='stddev'][.='7.0710678118654755']"
              //
              , pre+"/lst[@name='false']/double[@name='min'][.='30.0']"
              , pre+"/lst[@name='false']/double[@name='max'][.='40.0']"
              , pre+"/lst[@name='false']/double[@name='sum'][.='70.0']"
              , pre+"/lst[@name='false']/long[@name='count'][.='2']"
              , pre+"/lst[@name='false']/long[@name='missing'][.='0']"
              , pre + "/lst[@name='true']/long[@name='countDistinct'][.='2']"
              , "count(" + pre + "/lst[@name='true']/arr[@name='distinctValues']/*)=2"
              , pre+"/lst[@name='false']/double[@name='sumOfSquares'][.='2500.0']"
              , pre+"/lst[@name='false']/double[@name='mean'][.='35.0']"
              , pre+"/lst[@name='false']/double[@name='stddev'][.='7.0710678118654755']"
              );
    }
  }
  
  public void doTestFacetStatisticsMissingResult(String f, SolrParams[] baseParamsSet) throws Exception {
    assertU(adoc("id", "1", f, "10", "active_s", "true"));
    assertU(adoc("id", "2", f, "20", "active_s", "true"));
    assertU(commit());
    assertU(adoc("id", "3", "active_s", "false"));
    assertU(adoc("id", "4", f, "40", "active_s", "false"));
    assertU(commit());
    
    // status should be the same regardless of baseParams
    for (SolrParams baseParams : baseParamsSet) {
      
      assertQ("test value for active_s=true", 
              req(baseParams, "q", "*:*", "stats.calcdistinct", "true", "stats.facet", "active_s")
              , "//lst[@name='true']/double[@name='min'][.='10.0']"
              , "//lst[@name='true']/double[@name='max'][.='20.0']"
              , "//lst[@name='true']/double[@name='sum'][.='30.0']"
              , "//lst[@name='true']/long[@name='count'][.='2']"
              , "//lst[@name='true']/long[@name='missing'][.='0']"
              , "//lst[@name='true']/long[@name='countDistinct'][.='2']"
              , "count(//lst[@name='true']/arr[@name='distinctValues']/*)=2"
              , "//lst[@name='true']/double[@name='sumOfSquares'][.='500.0']"
              , "//lst[@name='true']/double[@name='mean'][.='15.0']"
              , "//lst[@name='true']/double[@name='stddev'][.='7.0710678118654755']"
              );
      
      assertQ("test value for active_s=false", 
              req(baseParams, "q", "*:*", "stats.facet", "active_s", "stats.calcdistinct", "true")
              , "//lst[@name='false']/double[@name='min'][.='40.0']"
              , "//lst[@name='false']/double[@name='max'][.='40.0']"
              , "//lst[@name='false']/double[@name='sum'][.='40.0']"
              , "//lst[@name='false']/long[@name='count'][.='1']"
              , "//lst[@name='false']/long[@name='missing'][.='1']"
              , "//lst[@name='false']/long[@name='countDistinct'][.='1']"
              , "count(//lst[@name='false']/arr[@name='distinctValues']/*)=1"
              , "//lst[@name='false']/double[@name='sumOfSquares'][.='1600.0']"
              , "//lst[@name='false']/double[@name='mean'][.='40.0']"
              , "//lst[@name='false']/double[@name='stddev'][.='0.0']"
              );
    }

  }

  public void testFieldStatisticsResultsNumericFieldAlwaysMissing() throws Exception {
    SolrCore core = h.getCore();
    assertU(adoc("id", "1"));
    assertU(adoc("id", "2"));
    assertU(commit());
    assertU(adoc("id", "3"));
    assertU(adoc("id", "4"));
    assertU(commit());

    Map<String, String> args = new HashMap<>();
    args.put(CommonParams.Q, "*:*");
    args.put(StatsParams.STATS, "true");
    args.put(StatsParams.STATS_FIELD, "active_i");
    args.put("indent", "true");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(args));

    assertQ("test string statistics values", req
            ,"//lst[@name='active_i']/long[@name='count'][.='0']"
            ,"//lst[@name='active_i']/long[@name='missing'][.='4']"

            ,"//lst[@name='active_i']/null[@name='min']"
            ,"//lst[@name='active_i']/null[@name='max']"
            ,"//lst[@name='active_i']/double[@name='sum'][.='0.0']"
            ,"//lst[@name='active_i']/double[@name='sumOfSquares'][.='0.0']"
            ,"//lst[@name='active_i']/double[@name='stddev'][.='0.0']"
            ,"//lst[@name='active_i']/double[@name='mean'][.='NaN']"
            // if new stats are supported, this will break - update test to assert values for each
            ,"count(//lst[@name='active_i']/*)=8"
            
            );
  }

  public void testFieldStatisticsResultsStringFieldAlwaysMissing() throws Exception {
    SolrCore core = h.getCore();
    assertU(adoc("id", "1"));
    assertU(adoc("id", "2"));
    assertU(commit());
    assertU(adoc("id", "3"));
    assertU(adoc("id", "4"));
    assertU(commit());

    Map<String, String> args = new HashMap<>();
    args.put(CommonParams.Q, "*:*");
    args.put(StatsParams.STATS, "true");
    args.put(StatsParams.STATS_FIELD, "active_s");
    args.put("indent", "true");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(args));

    assertQ("test string statistics values", req
            ,"//lst[@name='active_s']/long[@name='count'][.='0']"
            ,"//lst[@name='active_s']/long[@name='missing'][.='4']"

            ,"//lst[@name='active_s']/null[@name='min']"
            ,"//lst[@name='active_s']/null[@name='max']"
            // if new stats are supported, this will break - update test to assert values for each
            ,"count(//lst[@name='active_s']/*)=4"
         );
  }

  //SOLR-3160
  public void testFieldStatisticsResultsDateFieldAlwaysMissing() throws Exception {
    SolrCore core = h.getCore();

    assertU(adoc("id", "1"));
    assertU(adoc("id", "2"));
    assertU(commit());
    assertU(adoc("id", "3"));
    assertU(commit());

    Map<String, String> args = new HashMap<>();
    args.put(CommonParams.Q, "*:*");
    args.put(StatsParams.STATS, "true");
    args.put(StatsParams.STATS_FIELD, "active_dt");
    args.put("indent", "true");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(args));

    assertQ("test string statistics values", req
            ,"//lst[@name='active_dt']/long[@name='count'][.='0']"
            ,"//lst[@name='active_dt']/long[@name='missing'][.='3']"

            ,"//lst[@name='active_dt']/null[@name='min']"
            ,"//lst[@name='active_dt']/null[@name='max']"
            ,"//lst[@name='active_dt']/null[@name='mean']"
            ,"//lst[@name='active_dt']/date[@name='sum'][.='1970-01-01T00:00:00Z']"
            ,"//lst[@name='active_dt']/double[@name='sumOfSquares'][.='0.0']"
            ,"//lst[@name='active_dt']/double[@name='stddev'][.='0.0']"

            // if new stats are supported, this will break - update test to assert values for each
            ,"count(//lst[@name='active_dt']/*)=8"
            );
  }

  public void testStatsFacetMultivaluedErrorHandling() throws Exception {
    SolrCore core = h.getCore();
    SchemaField foo_ss = core.getLatestSchema().getField("foo_ss");

    assertU(adoc("id", "1", "active_i", "1", "foo_ss", "aa" ));
    assertU(commit());
    assertU(adoc("id", "2", "active_i", "1", "foo_ss", "bb" ));
    assertU(adoc("id", "3", "active_i", "5", "foo_ss", "aa" ));
    assertU(commit());

    assertTrue("schema no longer satisfies test requirements: foo_ss no longer multivalued", foo_ss.multiValued());
    assertTrue("schema no longer satisfies test requirements: foo_ss's fieldtype no longer single valued", ! foo_ss.getType().isMultiValued());
    
    assertQEx("no failure trying to get stats facet on foo_ss",
              req("q", "*:*", 
                  "stats", "true",
                  "stats.field", "active_i",
                  "stats.facet", "foo_ss"),
              400);

  }

  //SOLR-3177
  public void testStatsExcludeFilterQuery() throws Exception {
    SolrCore core = h.getCore();
    assertU(adoc("id", "1"));
    assertU(adoc("id", "2"));
    assertU(adoc("id", "3"));
    assertU(adoc("id", "4"));
    assertU(commit());

    Map<String, String> args = new HashMap<String, String>();
    args.put(CommonParams.Q, "*:*");
    args.put(StatsParams.STATS, "true");
    args.put(StatsParams.STATS_FIELD, "{!ex=id}id");
    args.put("fq", "{!tag=id}id:[2 TO 3]");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(args));

    assertQ("test exluding filter query", req
            , "//lst[@name='id']/double[@name='min'][.='1.0']"
            , "//lst[@name='id']/double[@name='max'][.='4.0']");

    args = new HashMap<String, String>();
    args.put(CommonParams.Q, "*:*");
    args.put(StatsParams.STATS, "true");
    args.put(StatsParams.STATS_FIELD, "{!key=id2}id");
    args.put("fq", "{!tag=id}id:[2 TO 3]");
    req = new LocalSolrQueryRequest(core, new MapSolrParams(args));

    assertQ("test rename field", req
            , "//lst[@name='id2']/double[@name='min'][.='2.0']"
            , "//lst[@name='id2']/double[@name='max'][.='3.0']");
  }
  
  // SOLR-6024
  public void testFieldStatisticsDocValuesAndMultiValued() throws Exception {
    SolrCore core = h.getCore();
    
    // precondition for the test
    SchemaField catDocValues = core.getLatestSchema().getField("cat_docValues");
    assertTrue("schema no longer satisfies test requirements: cat_docValues no longer multivalued", catDocValues.multiValued());
    assertTrue("schema no longer satisfies test requirements: cat_docValues fieldtype no longer single valued", !catDocValues.getType().isMultiValued());
    assertTrue("schema no longer satisfies test requirements: cat_docValues no longer has docValues", catDocValues.hasDocValues());
    
    List<FldType> types = new ArrayList<>();
    types.add(new FldType("id", ONE_ONE, new SVal('A', 'Z', 4, 4)));
    types.add(new FldType("cat_docValues",new IRange(2,2),  new SVal('a','z',1, 30)));
    Doc d1 = createDoc(types);
    d1.getValues("id").set(0, "1");
    d1.getValues("cat_docValues").set(0, "test");
    d1.getValues("cat_docValues").set(1, "testtw");
    updateJ(toJSON(d1), null);
    Doc d2 = createDoc(types);
    d2.getValues("id").set(0, "2");
    d2.getValues("cat_docValues").set(0, "test");
    d2.getValues("cat_docValues").set(1, "testtt");
    updateJ(toJSON(d2), null);
    
    assertU(commit());
    
    Map<String, String> args = new HashMap<>();
    args.put(CommonParams.Q, "*:*");
    args.put(StatsParams.STATS, "true");
    args.put(StatsParams.STATS_FIELD, "cat_docValues");
    args.put("indent", "true");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(args));
    
    assertQ("test min/max on docValues and multiValued", req
        , "//lst[@name='cat_docValues']/str[@name='min'][.='test']"
        , "//lst[@name='cat_docValues']/str[@name='max'][.='testtw']");
    
  }

  public void testFieldStatisticsDocValuesAndMultiValuedInteger() throws Exception {
      SolrCore core = h.getCore();
      String fieldName = "cat_intDocValues";
      // precondition for the test
      SchemaField catDocValues = core.getLatestSchema().getField(fieldName);
      assertTrue("schema no longer satisfies test requirements: cat_docValues no longer multivalued", catDocValues.multiValued());
      assertTrue("schema no longer satisfies test requirements: cat_docValues fieldtype no longer single valued", !catDocValues.getType().isMultiValued());
      assertTrue("schema no longer satisfies test requirements: cat_docValues no longer has docValues", catDocValues.hasDocValues());

      List<FldType> types = new ArrayList<>();
      types.add(new FldType("id", ONE_ONE, new SVal('A', 'Z', 4, 4)));
      types.add(new FldType(fieldName, ONE_ONE, new IRange(0, 0)));

      Doc d1 = createDocValuesDocument(types, fieldName, "1", -1, 3, 5);
      updateJ(toJSON(d1), null);

      Doc d2 = createDocValuesDocument(types, fieldName, "2", 3, -2, 6);
      updateJ(toJSON(d2), null);

      Doc d3 = createDocValuesDocument(types, fieldName, "3", 16, -3, 11);
      updateJ(toJSON(d3), null);

      assertU(commit());

      Map<String, String> args = new HashMap<>();
      args.put(CommonParams.Q, "*:*");
      args.put(StatsParams.STATS, "true");
      args.put(StatsParams.STATS_FIELD, fieldName);
      args.put(StatsParams.STATS_CALC_DISTINCT, "true");
      args.put("indent", "true");

      SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(args));

      assertQ("test min/max on docValues and multiValued", req
          , "//lst[@name='" + fieldName + "']/double[@name='min'][.='-3.0']"
          , "//lst[@name='" + fieldName + "']/double[@name='max'][.='16.0']"
          , "//lst[@name='" + fieldName + "']/long[@name='count'][.='12']"
          , "//lst[@name='" + fieldName + "']/long[@name='countDistinct'][.='9']"
          , "//lst[@name='" + fieldName + "']/double[@name='sum'][.='38.0']"
          , "//lst[@name='" + fieldName + "']/double[@name='mean'][.='3.1666666666666665']"
          , "//lst[@name='" + fieldName + "']/double[@name='stddev'][.='5.638074031784151']"
          , "//lst[@name='" + fieldName + "']/double[@name='sumOfSquares'][.='470.0']"
          , "//lst[@name='" + fieldName + "']/long[@name='missing'][.='0']");

    }

  public void testFieldStatisticsDocValuesAndMultiValuedIntegerFacetStats() throws Exception {
       SolrCore core = h.getCore();
       String fieldName = "cat_intDocValues";
       // precondition for the test
       SchemaField catDocValues = core.getLatestSchema().getField(fieldName);
       assertTrue("schema no longer satisfies test requirements: cat_docValues no longer multivalued", catDocValues.multiValued());
       assertTrue("schema no longer satisfies test requirements: cat_docValues fieldtype no longer single valued", !catDocValues.getType().isMultiValued());
       assertTrue("schema no longer satisfies test requirements: cat_docValues no longer has docValues", catDocValues.hasDocValues());

       List<FldType> types = new ArrayList<>();
       types.add(new FldType("id", ONE_ONE, new SVal('A', 'Z', 4, 4)));
       types.add(new FldType(fieldName, ONE_ONE, new IRange(0, 0)));

       Doc d1 = createDocValuesDocument(types, fieldName, "1", -1, 3, 5);
       updateJ(toJSON(d1), null);

       Doc d2 = createDocValuesDocument(types, fieldName, "2", 3, -2, 6);
       updateJ(toJSON(d2), null);

       Doc d3 = createDocValuesDocument(types, fieldName, "3", 16, -3, 11);
       updateJ(toJSON(d3), null);

       assertU(commit());

       Map<String, String> args = new HashMap<>();
       args.put(CommonParams.Q, "*:*");
       args.put(StatsParams.STATS, "true");
       args.put(StatsParams.STATS_FIELD, fieldName);
       args.put(StatsParams.STATS_FACET, fieldName);
       args.put(StatsParams.STATS_CALC_DISTINCT, "true");
       args.put("indent", "true");

       SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(args));

       assertQEx("can not use FieldCache on multivalued field: cat_intDocValues", req, 400);

     }


  public void testMiscQueryStats() throws Exception {
    final String kpre = XPRE + "lst[@name='stats_fields']/lst[@name='k']/";

    assertU(adoc("id", "1", "a_f", "2.3", "b_f", "9.7", "foo_t", "how now brown cow"));
    assertU(adoc("id", "2", "a_f", "4.5", "b_f", "8.6", "foo_t", "cow cow cow cow"));
    assertU(adoc("id", "3", "a_f", "5.6", "b_f", "7.5", "foo_t", "red fox"));
    assertU(adoc("id", "4", "a_f", "6.7", "b_f", "6.3", "foo_t", "red cow"));
    assertU(commit());

    assertQ("functions over multiple fields",
            req("q","foo_t:cow", "stats", "true",
                "stats.field", "{!func key=k}product(a_f,b_f)")
            
            , kpre + "double[@name='min'][.='22.309999465942383']"
            , kpre + "double[@name='max'][.='42.209999084472656']"
            , kpre + "double[@name='sum'][.='103.21999931335449']"
            , kpre + "long[@name='count'][.='3']"
            , kpre + "long[@name='missing'][.='0']"
            , kpre + "double[@name='sumOfSquares'][.='3777.110157933046']"
            , kpre + "double[@name='mean'][.='34.40666643778483']"
            , kpre + "double[@name='stddev'][.='10.622007151430441']"
            );

    assertQ("functions over a query",
            req("q","*:*", "stats", "true",
                "stats.field", "{!lucene key=k}foo_t:cow")
            // scores are: 1.0, 0.625, 0.5, & "missing"
            , kpre + "double[@name='min'][.='0.5']"
            , kpre + "double[@name='max'][.='1.0']"
            , kpre + "double[@name='sum'][.='2.125']"
            , kpre + "long[@name='count'][.='3']"
            , kpre + "long[@name='missing'][.='1']"
            , kpre + "double[@name='sumOfSquares'][.='1.640625']"
            , kpre + "double[@name='mean'][.='0.7083333333333334']"
            , kpre + "double[@name='stddev'][.='0.2602082499332666']"
            );
    
  }

  /**
   * Whitebox test of {@link StatsField} parsing to ensure expected equivilence 
   * operations hold up
   */
  public void testStatsFieldWhitebox() throws Exception {
    StatsComponent component = new StatsComponent();
    List<SearchComponent> components = new ArrayList<>(1);
    components.add(component);
    SolrParams common = params("stats", "true", "q", "*:*", "nested","foo_t:cow");

    // all of these should produce the same SchemaField based StatsField
    for (String param : new String[] { 
        "foo_i", "{!func}field(\"foo_i\")", "{!lucene}_val_:\"field(foo_i)\""
      }) {
      SolrQueryRequest req = req(common);
      try {
        ResponseBuilder rb = new ResponseBuilder(req, new SolrQueryResponse(), components);
        
        StatsField sf = new StatsField(rb, param);
        
        assertNull("value source of: " + param, sf.getValueSource());
        assertNotNull("schema field of: " + param, sf.getSchemaField());

        assertEquals("field name of: " + param,
                     "foo_i", sf.getSchemaField().getName());
      } finally {
        req.close();
      }
    }

    // all of these should produce the same QueryValueSource based StatsField
    for (String param : new String[] { 
        "{!lucene}foo_t:cow", "{!func}query($nested)", "{!field f=foo_t}cow", 
      }) {
      SolrQueryRequest req = req(common);
      try {
        ResponseBuilder rb = new ResponseBuilder(req, new SolrQueryResponse(), components);
        
        StatsField sf = new StatsField(rb, param);
        
        assertNull("schema field of: " + param, sf.getSchemaField());
        assertNotNull("value source of: " + param, sf.getValueSource());
        assertTrue(sf.getValueSource().getClass() + " is vs type of: " + param,
                   sf.getValueSource() instanceof QueryValueSource);
        QueryValueSource qvs = (QueryValueSource) sf.getValueSource();
        assertEquals("query of :" + param,
                     new TermQuery(new Term("foo_t","cow")),
                     qvs.getQuery());
      } finally {
        req.close();
      }
    }
  }

  public void testFieldStatisticsDocValuesAndMultiValuedDouble() throws Exception {
    SolrCore core = h.getCore();
    String fieldName = "cat_floatDocValues";
    // precondition for the test
    SchemaField catDocValues = core.getLatestSchema().getField(fieldName);
    assertTrue("schema no longer satisfies test requirements: cat_docValues no longer multivalued", catDocValues.multiValued());
    assertTrue("schema no longer satisfies test requirements: cat_docValues fieldtype no longer single valued", !catDocValues.getType().isMultiValued());
    assertTrue("schema no longer satisfies test requirements: cat_docValues no longer has docValues", catDocValues.hasDocValues());

    List<FldType> types = new ArrayList<>();
    types.add(new FldType("id", ONE_ONE, new SVal('A', 'Z', 4, 4)));
    types.add(new FldType(fieldName, ONE_ONE, new FVal(0, 0)));

    Doc d1 = createDocValuesDocument(types, fieldName,  "1", -1, 3, 5);
    updateJ(toJSON(d1), null);

    Doc d2 = createDocValuesDocument(types, fieldName,  "2", 3, -2, 6);
    updateJ(toJSON(d2), null);

    Doc d3 = createDocValuesDocument(types, fieldName,  "3", 16, -3, 11);
    updateJ(toJSON(d3), null);

    assertU(commit());

    final SolrParams baseParams = params(CommonParams.Q, "*:*",
                                         "indent", "true",
                                         StatsParams.STATS, "true");

    SolrQueryRequest req1 = req(baseParams, 
                                StatsParams.STATS_CALC_DISTINCT, "true",
                                StatsParams.STATS_FIELD, fieldName);
    SolrQueryRequest req2 = req(baseParams, 
                                StatsParams.STATS_FIELD,
                                "{!min=true, max=true, count=true, sum=true, mean=true, stddev=true, sumOfSquares=true, missing=true, calcdistinct=true}" + fieldName);

    for (SolrQueryRequest req : new SolrQueryRequest[] { req1, req2 }) {
      assertQ("test status on docValues and multiValued: " + req.toString(), req
              , "//lst[@name='" + fieldName + "']/double[@name='min'][.='-3.0']"
              , "//lst[@name='" + fieldName + "']/double[@name='max'][.='16.0']"
              , "//lst[@name='" + fieldName + "']/long[@name='count'][.='12']"
              , "//lst[@name='" + fieldName + "']/double[@name='sum'][.='38.0']"
              , "//lst[@name='" + fieldName + "']/double[@name='mean'][.='3.1666666666666665']"
              , "//lst[@name='" + fieldName + "']/double[@name='stddev'][.='5.638074031784151']"
              , "//lst[@name='" + fieldName + "']/double[@name='sumOfSquares'][.='470.0']"
              , "//lst[@name='" + fieldName + "']/long[@name='missing'][.='0']"
              , "//lst[@name='" + fieldName + "']/long[@name='countDistinct'][.='9']"
              // always comes along with countDistinct
              , "count(//lst[@name='" + fieldName + "']/arr[@name='distinctValues']/float)=9"
              // if new default stats are added, this will break - update test to assert values for each
              ,"count(//lst[@name='" + fieldName + "']/*)=10"
              );
    }
  }

  public void testEnumFieldTypeStatus() throws Exception {
    clearIndex();
    
    String fieldName = "severity";    
    assertU(adoc("id", "0", fieldName, "Not Available"));
    assertU(adoc("id", "1", fieldName, "Not Available"));
    assertU(adoc("id", "2", fieldName, "Not Available"));
    assertU(adoc("id", "3", fieldName, "Not Available"));
    assertU(adoc("id", "4", fieldName, "Not Available"));
    assertU(adoc("id", "5", fieldName, "Low"));
    assertU(adoc("id", "6", fieldName, "Low"));
    assertU(adoc("id", "7", fieldName, "Low"));
    assertU(adoc("id", "8", fieldName, "Low"));
    assertU(adoc("id", "9", fieldName, "Medium"));
    assertU(adoc("id", "10", fieldName, "Medium"));
    assertU(adoc("id", "11", fieldName, "Medium"));
    assertU(adoc("id", "12", fieldName, "High"));
    assertU(adoc("id", "13", fieldName, "High"));
    assertU(adoc("id", "14", fieldName, "Critical"));
    
    
    for (int i = 20; i <= 30; i++) {
      assertU(adoc("id", "" + i));
    }

    assertU(commit());
    
    assertQ("enum", req("q","*:*", "stats", "true", "stats.field", fieldName)
            , "//lst[@name='" + fieldName + "']/str[@name='min'][.='Not Available']"
            , "//lst[@name='" + fieldName + "']/str[@name='max'][.='Critical']"
            , "//lst[@name='" + fieldName + "']/long[@name='count'][.='15']"
            , "//lst[@name='" + fieldName + "']/long[@name='missing'][.='11']");
    
    
    assertQ("enum calcdistinct", req("q","*:*", "stats", "true", "stats.field", fieldName, 
                                     StatsParams.STATS_CALC_DISTINCT, "true")
            , "//lst[@name='" + fieldName + "']/str[@name='min'][.='Not Available']"
            , "//lst[@name='" + fieldName + "']/str[@name='max'][.='Critical']"
            , "//lst[@name='" + fieldName + "']/long[@name='count'][.='15']"
            , "//lst[@name='" + fieldName + "']/long[@name='countDistinct'][.='5']"
            , "count(//lst[@name='" + fieldName + "']/arr[@name='distinctValues']/*)=5"
            , "//lst[@name='" + fieldName + "']/long[@name='missing'][.='11']");
    
    
    final String pre = "//lst[@name='stats_fields']/lst[@name='"+fieldName+"']/lst[@name='facets']/lst[@name='severity']";

    assertQ("enum + stats.facet", req("q","*:*", "stats", "true", "stats.field", fieldName, 
                                      "stats.facet", fieldName)
            , pre + "/lst[@name='High']/str[@name='min'][.='High']"
            , pre + "/lst[@name='High']/str[@name='max'][.='High']"
            , pre + "/lst[@name='High']/long[@name='count'][.='2']"
            , pre + "/lst[@name='High']/long[@name='missing'][.='0']"
            , pre + "/lst[@name='Low']/str[@name='min'][.='Low']"
            , pre + "/lst[@name='Low']/str[@name='max'][.='Low']"
            , pre + "/lst[@name='Low']/long[@name='count'][.='4']"
            , pre + "/lst[@name='Low']/long[@name='missing'][.='0']"
            , pre + "/lst[@name='Medium']/str[@name='min'][.='Medium']"
            , pre + "/lst[@name='Medium']/str[@name='max'][.='Medium']"
            , pre + "/lst[@name='Medium']/long[@name='count'][.='3']"
            , pre + "/lst[@name='Medium']/long[@name='missing'][.='0']"
            , pre + "/lst[@name='Not Available']/str[@name='min'][.='Not Available']"
            , pre + "/lst[@name='Not Available']/str[@name='max'][.='Not Available']"
            , pre + "/lst[@name='Not Available']/long[@name='count'][.='5']"
            , pre + "/lst[@name='Not Available']/long[@name='missing'][.='0']"
            , pre + "/lst[@name='Critical']/str[@name='min'][.='Critical']"
            , pre + "/lst[@name='Critical']/str[@name='max'][.='Critical']"
            , pre + "/lst[@name='Critical']/long[@name='count'][.='1']"
            , pre + "/lst[@name='Critical']/long[@name='missing'][.='0']"
            );
  }
  
  private Doc createDocValuesDocument(List<FldType> types, String fieldName,  String id, Comparable... values) throws Exception {
    Doc doc = createDoc(types);
    doc.getValues("id").set(0, id);
    initMultyValued(doc.getValues(fieldName), values);
    return doc;
  }

  private List<Comparable> initMultyValued(List<Comparable> cat_docValues, Comparable... comparables) {
    Collections.addAll(cat_docValues, comparables);
    return cat_docValues;
  }
  
  public void testIndividualStatLocalParams() throws Exception {
    final String kpre = XPRE + "lst[@name='stats_fields']/lst[@name='k']/";
    
    assertU(adoc("id", "1", "a_f", "2.3", "b_f", "9.7", "a_i", "9", "foo_t", "how now brown cow"));
    assertU(commit());
    
    // some quick sanity check assertions...

    // trivial check that we only get the exact 2 we ask for
    assertQ("ask for and get only 2 stats",
            req("q","*:*", "stats", "true",
                "stats.field", "{!key=k mean=true min=true}a_i")
            , kpre + "double[@name='mean'][.='9.0']"
            , kpre + "double[@name='min'][.='9.0']"
            , "count(" + kpre + "*)=2"
            );

    // for stats that are true/false, sanity check false does it's job
    assertQ("min=true & max=false: only min should come back",
            req("q","*:*", "stats", "true",
                "stats.field", "{!key=k max=false min=true}a_i")
            , kpre + "double[@name='min'][.='9.0']"
            , "count(" + kpre + "*)=1"
            );
    assertQ("min=false: localparam stat means ignore default set, "+
            "but since only local param is false no stats should be returned",
            req("q","*:*", "stats", "true",
                "stats.field", "{!key=k min=false}a_i")
            , "count(" + kpre + "*)=0"
            );

   double sum = 0;
   double sumOfSquares = 0;
   final int count = 20;
   for (int i = 0; i < count; i++) {
     assertU(adoc("id", String.valueOf(i), "a_f", "2.3", "b_f", "9.7", "a_i", String.valueOf(i%10), "foo_t", "how now brown cow"));
     sum+=i%10;
     sumOfSquares+=(i%10)*(i%10);
   }
   
   assertU(commit());

   EnumSet<Stat> allStats = EnumSet.allOf(Stat.class);

   Map<Stat, String> expectedStats = new HashMap<>();
   expectedStats.put(Stat.min, "0.0");
   expectedStats.put(Stat.max, "9.0");
   expectedStats.put(Stat.missing, "0");
   expectedStats.put(Stat.sum, String.valueOf(sum));
   expectedStats.put(Stat.count, String.valueOf(count));
   expectedStats.put(Stat.mean, String.valueOf(sum/count));
   expectedStats.put(Stat.sumOfSquares, String.valueOf(sumOfSquares));
   expectedStats.put(Stat.stddev, String.valueOf(Math.sqrt(((count * sumOfSquares) - (sum * sum)) / (20 * (count - 1.0D)))));
   expectedStats.put(Stat.calcdistinct, "10");
   
   Map<Stat, String> expectedType = new HashMap<>();
   expectedType.put(Stat.min, "double");
   expectedType.put(Stat.max, "double");
   expectedType.put(Stat.missing, "long");
   expectedType.put(Stat.sum, "double");
   expectedType.put(Stat.count, "long");
   expectedType.put(Stat.mean, "double");
   expectedType.put(Stat.sumOfSquares, "double");
   expectedType.put(Stat.stddev, "double");
   expectedType.put(Stat.calcdistinct, "long");

   // canary in the coal mine
   assertEquals("size of expectedStats doesn't match all known stats; " + 
                "enum was updated w/o updating test?",
                expectedStats.size(), allStats.size());
   assertEquals("size of expectedType doesn't match all known stats; " + 
                "enum was updated w/o updating test?",
                expectedType.size(), allStats.size());

   // whitebox test: explicitly ask for isShard=true with an individual stat
   for (Stat stat : expectedStats.keySet()) {
     EnumSet<Stat> distribDeps = stat.getDistribDeps();

     StringBuilder exclude = new StringBuilder();
     List<String> testParas = new ArrayList<String>(distribDeps.size() + 2);
     int calcdistinctFudge = 0;

     for (Stat perShardStat : distribDeps ){
       String key = perShardStat.toString();
       if (perShardStat.equals(Stat.calcdistinct)) {
         // this abomination breaks all the rules - uses a diff response key and triggers
         // the additional "distinctValues" stat
         key = "countDistinct";
         calcdistinctFudge++;
         testParas.add("count(" + kpre + "arr[@name='distinctValues']/*)=10");
       }
       testParas.add(kpre + expectedType.get(perShardStat) + 
                     "[@name='" + key + "'][.='" + expectedStats.get(perShardStat) + "']");
       // even if we go out of our way to exclude the dependent stats, 
       // the shard should return them since they are a dependency for the requested stat
       exclude.append(perShardStat + "=false ");
     }
     testParas.add("count(" + kpre + "*)=" + (distribDeps.size() + calcdistinctFudge));

     assertQ("ask for only "+stat+", with isShard=true, and expect only deps: " + distribDeps,
             req("q", "*:*", "isShard", "true", "stats", "true", 
                 "stats.field", "{!key=k " + exclude + stat + "=true}a_i")
             , testParas.toArray(new String[testParas.size()])
             );
   }
   
   // test all the possible combinations (of all possible sizes) of stats params
   for (int numParams = 1; numParams <= allStats.size(); numParams++) {
     for (EnumSet<Stat> set : new StatSetCombinations(numParams, allStats)) {

       // EnumSets use natural ordering, we want to randomize the order of the params
       List<Stat> combo = new ArrayList<Stat>(set);
       Collections.shuffle(combo, random());

       StringBuilder paras = new StringBuilder("{!key=k ");
       List<String> testParas = new ArrayList<String>(numParams + 2);

       int calcdistinctFudge = 0;
       for (Stat stat : combo) {
         String key = stat.toString();
         if (stat.equals(Stat.calcdistinct)) {
           // this abomination breaks all the rules - uses a diff response key and triggers
           // the additional "distinctValues" stat
           key = "countDistinct";
           calcdistinctFudge++; 
           testParas.add("count(" + kpre + "arr[@name='distinctValues']/*)=10");
         }
         paras.append(stat + "=true ");
         testParas.add(kpre + expectedType.get(stat) + "[@name='" + key + "'][.='" + expectedStats.get(stat) + "']");
       }

       paras.append("}a_i");
       testParas.add("count(" + kpre + "*)=" + (combo.size() + calcdistinctFudge));

       assertQ("ask for an get only: "+ combo,
               req("q","*:*", "stats", "true",
                   "stats.field", paras.toString())
               , testParas.toArray(new String[testParas.size()])
               );
     }
   }

  }
  
  // Test for Solr-6349
  public void testCalcDistinctStats() throws Exception {
    final String kpre = XPRE + "lst[@name='stats_fields']/lst[@name='k']/";
    final String min = "count(" + kpre +"/double[@name='min'])";
    final String countDistinct = "count(" + kpre +"/long[@name='countDistinct'])";
    final String distinctValues = "count(" + kpre +"/arr[@name='distinctValues'])";

    final int count = 20;
    for (int i = 0; i < count; i++) {
      assertU(adoc("id", String.valueOf(i), "a_f", "2.3", "b_f", "9.7", "a_i",
                   String.valueOf(i % 10), "foo_t", "how now brown cow"));
    }
    
    assertU(commit());
    
    String[] baseParams = new String[] { "q", "*:*", "stats", "true","indent", "true" };

    for (SolrParams p : new SolrParams[] { 
        params("stats.field", "{!key=k}a_i"),
        params(StatsParams.STATS_CALC_DISTINCT, "false", 
               "stats.field", "{!key=k}a_i"),
        params("f.a_i." + StatsParams.STATS_CALC_DISTINCT, "false", 
               "stats.field", "{!key=k}a_i"),
        params(StatsParams.STATS_CALC_DISTINCT, "true", 
               "f.a_i." + StatsParams.STATS_CALC_DISTINCT, "false", 
               "stats.field", "{!key=k}a_i"),
        params("stats.field", "{!key=k min='true'}a_i"),
        params(StatsParams.STATS_CALC_DISTINCT, "true", 
               "f.a_i." + StatsParams.STATS_CALC_DISTINCT, "true", 
               "stats.field", "{!key=k min='true' calcdistinct='false'}a_i"),
      }) {

      assertQ("min is either default or explicitly requested; "+
              "countDistinct & distinctValues either default or explicitly prevented"
              , req(p, baseParams)
              , min + "=1"
              , countDistinct + "=0"
              , distinctValues + "=0");
    }
    
    for (SolrParams p : new SolrParams[] { 
        params("stats.calcdistinct", "true",
               "stats.field", "{!key=k}a_i"),
        params("f.a_i." + StatsParams.STATS_CALC_DISTINCT, "true", 
               "stats.field", "{!key=k}a_i"),
        params("stats.calcdistinct", "false",
               "f.a_i." + StatsParams.STATS_CALC_DISTINCT, "true", 
               "stats.field", "{!key=k}a_i"),
        params("stats.calcdistinct", "false ", 
               "stats.field", "{!key=k min=true calcdistinct=true}a_i"),
        params("f.a_i." + StatsParams.STATS_CALC_DISTINCT, "false", 
               "stats.field", "{!key=k min=true calcdistinct=true}a_i"),
        params("stats.calcdistinct", "false ", 
               "f.a_i." + StatsParams.STATS_CALC_DISTINCT, "false", 
               "stats.field", "{!key=k min=true calcdistinct=true}a_i"),
      }) {

      assertQ("min is either default or explicitly requested; " +
              "countDistinct & distinctValues explicitly requested"
              , req(p, baseParams)
              , min + "=1"
              , countDistinct + "=1"
              , distinctValues + "=1");
    }
    
    for (SolrParams p : new SolrParams[] { 
        params("stats.field", "{!key=k calcdistinct=true}a_i"),

        params("stats.calcdistinct", "true",
               "stats.field", "{!key=k min='false'}a_i"),

        params("stats.calcdistinct", "true",
               "stats.field", "{!key=k max='true' min='false'}a_i"),
        
        params("stats.calcdistinct", "false",
               "stats.field", "{!key=k calcdistinct=true}a_i"),
        params("f.a_i." + StatsParams.STATS_CALC_DISTINCT, "false", 
               "stats.field", "{!key=k calcdistinct=true}a_i"),
        params("stats.calcdistinct", "false",
               "f.a_i." + StatsParams.STATS_CALC_DISTINCT, "false", 
               "stats.field", "{!key=k calcdistinct=true}a_i"),
        params("stats.calcdistinct", "false",
               "f.a_i." + StatsParams.STATS_CALC_DISTINCT, "false", 
               "stats.field", "{!key=k min='false' calcdistinct=true}a_i"),
      }) {

      assertQ("min is explicitly excluded; " +
              "countDistinct & distinctValues explicitly requested"
              , req(p, baseParams)
              , min + "=0"
              , countDistinct + "=1"
              , distinctValues + "=1");
    }
    
    for (SolrParams p : new SolrParams[] { 
        params(StatsParams.STATS_CALC_DISTINCT, "true", 
               "stats.field", "{!key=k min=true}a_i"),
        params("f.a_i.stats.calcdistinct", "true", 
               "stats.field", "{!key=k min=true}a_i"),
        params(StatsParams.STATS_CALC_DISTINCT, "false", 
               "f.a_i.stats.calcdistinct", "true", 
               "stats.field", "{!key=k min=true}a_i"),
        params("f.a_i.stats.calcdistinct", "false", 
               "stats.field", "{!key=k min=true calcdistinct=true}a_i"),
        params(StatsParams.STATS_CALC_DISTINCT, "false", 
               "stats.field", "{!key=k min=true calcdistinct=true}a_i"),
        params(StatsParams.STATS_CALC_DISTINCT, "false", 
               "f.a_i.stats.calcdistinct", "false", 
               "stats.field", "{!key=k min=true calcdistinct=true}a_i"),
      }) {

      assertQ("min is explicitly requested; " +
              "countDistinct & distinctValues explicitly requested"
              , req(p, baseParams)
              , min + "=1"
              , countDistinct + "=1"
              , distinctValues + "=1");
    }
  }

  /** 
   * given a comboSize and an EnumSet of Stats, generates iterators that produce every possible
   * enum combination of that size 
   */
  public static final class StatSetCombinations implements Iterable<EnumSet<Stat>> {
    // we need an array so we can do fixed index offset lookups
    private final Stat[] all;
    private final Combinations intCombos;
    public StatSetCombinations(int comboSize, EnumSet<Stat> universe) {
      // NOTE: should not need to sort, EnumSet uses natural ordering
      all = universe.toArray(new Stat[universe.size()]);
      intCombos = new Combinations(all.length, comboSize);
    }
    public Iterator<EnumSet<Stat>> iterator() {
      return new Iterator<EnumSet<Stat>>() {
        final Iterator<int[]> wrapped = intCombos.iterator();
        public void remove() {
          wrapped.remove();
        }
        public boolean hasNext() {
          return wrapped.hasNext();
        }
        public EnumSet<Stat> next() {
          EnumSet<Stat> result = EnumSet.noneOf(Stat.class);
          int[] indexes = wrapped.next();
          for (int i = 0; i < indexes.length; i++) {
            result.add(all[indexes[i]]);
          }
          return result;
        }
      };
    }
  }

}
