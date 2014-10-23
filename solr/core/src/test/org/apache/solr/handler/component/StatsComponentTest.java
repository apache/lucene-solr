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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.StatsParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.BeforeClass;


/**
 * Statistics Component Test
 */
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42"})
public class StatsComponentTest extends AbstractSolrTestCase {

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

    for (String f : new String[] {"stats_ii", // plain int
            "stats_is",    // sortable int
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
    assertU(adoc("id", "1", f, "-10"));
    assertU(adoc("id", "2", f, "-20"));
    assertU(commit());
    assertU(adoc("id", "3", f, "-30"));
    assertU(adoc("id", "4", f, "-40"));
    assertU(commit());

    // status should be the same regardless of baseParams
    for (SolrParams baseParams : baseParamsSet) {

      assertQ("test statistics values", 
              req(baseParams, "q", "*:*", "stats.calcdistinct", "true")
              , "//double[@name='min'][.='-40.0']"
              , "//double[@name='max'][.='-10.0']"
              , "//double[@name='sum'][.='-100.0']"
              , "//long[@name='count'][.='4']"
              , "//long[@name='missing'][.='0']"
              , "//long[@name='countDistinct'][.='4']"
              , "count(//arr[@name='distinctValues']/*)=4"
              , "//double[@name='sumOfSquares'][.='3000.0']"
              , "//double[@name='mean'][.='-25.0']"
              , "//double[@name='stddev'][.='12.909944487358056']"
              );  

      assertQ("test statistics w/fq", 
              req(baseParams, 
                  "q", "*:*", "fq", "-id:4",
                  "stats.calcdistinct", "true")
              , "//double[@name='min'][.='-30.0']"
              , "//double[@name='max'][.='-10.0']"
              , "//double[@name='sum'][.='-60.0']"
              , "//long[@name='count'][.='3']"
              , "//long[@name='missing'][.='0']"
              , "//long[@name='countDistinct'][.='3']"
              , "count(//arr[@name='distinctValues']/*)=3"
              , "//double[@name='sumOfSquares'][.='1400.0']"
              , "//double[@name='mean'][.='-20.0']"
              , "//double[@name='stddev'][.='10.0']"
              );  
    }
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

    assertQ("test string statistics values", req,
        "//null[@name='active_i'][.='']");
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

    assertQ("test string statistics values", req,
        "//null[@name='active_s'][.='']");
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

    assertQ("test string statistics values", req,
        "//null[@name='active_dt'][.='']");
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
      args.put("indent", "true");
      args.put(StatsParams.STATS_CALC_DISTINCT, "true");

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
       args.put("indent", "true");
       args.put(StatsParams.STATS_CALC_DISTINCT, "true");

       SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(args));

       assertQEx("can not use FieldCache on multivalued field: cat_intDocValues", req, 400);

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
        , "//lst[@name='" + fieldName + "']/double[@name='sum'][.='38.0']"
        , "//lst[@name='" + fieldName + "']/long[@name='countDistinct'][.='9']"
        , "//lst[@name='" + fieldName + "']/double[@name='mean'][.='3.1666666666666665']"
        , "//lst[@name='" + fieldName + "']/double[@name='stddev'][.='5.638074031784151']"
        , "//lst[@name='" + fieldName + "']/double[@name='sumOfSquares'][.='470.0']"
        , "//lst[@name='" + fieldName + "']/long[@name='missing'][.='0']");

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
  
  
//  public void testOtherFacetStatsResult() throws Exception {
//    
//    assertU(adoc("id", "1", "stats_tls_dv", "10", "active_i", "1"));
//    assertU(adoc("id", "2", "stats_tls_dv", "20", "active_i", "1"));
//    assertU(commit());
//    assertU(adoc("id", "3", "stats_tls_dv", "30", "active_i", "2"));
//    assertU(adoc("id", "4", "stats_tls_dv", "40", "active_i", "2"));
//    assertU(commit());
//    
//    final String pre = "//lst[@name='stats_fields']/lst[@name='stats_tls_dv']/lst[@name='facets']/lst[@name='active_i']";
//
//    assertQ("test value for active_s=true", req("q", "*:*", "stats", "true", "stats.field", "stats_tls_dv", "stats.facet", "active_i","indent", "true")
//            , "*[count("+pre+")=1]"
//            , pre+"/lst[@name='1']/double[@name='min'][.='10.0']"
//            , pre+"/lst[@name='1']/double[@name='max'][.='20.0']"
//            , pre+"/lst[@name='1']/double[@name='sum'][.='30.0']"
//            , pre+"/lst[@name='1']/long[@name='count'][.='2']"
//            , pre+"/lst[@name='1']/long[@name='missing'][.='0']"
//            , pre + "/lst[@name='true']/long[@name='countDistinct'][.='2']"
//            , "count(" + pre + "/lst[@name='true']/arr[@name='distinctValues']/*)=2"
//            , pre+"/lst[@name='1']/double[@name='sumOfSquares'][.='500.0']"
//            , pre+"/lst[@name='1']/double[@name='mean'][.='15.0']"
//            , pre+"/lst[@name='1']/double[@name='stddev'][.='7.0710678118654755']"
//    );
//  }
}
