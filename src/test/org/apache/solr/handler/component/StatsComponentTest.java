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

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.StatsParams;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.AbstractSolrTestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistics Component Test
 */
public class StatsComponentTest extends AbstractSolrTestCase {

  @Override
  public String getSchemaFile() {
    return "schema11.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    lrf = h.getRequestFactory("standard", 0, 20);
  }

  public void testFieldStatisticsResult() throws Exception {
    SolrCore core = h.getCore();
    assertU(adoc("id", "1", "stats_i", "-10"));
    assertU(adoc("id", "2", "stats_i", "-20"));
    assertU(adoc("id", "3", "stats_i", "-30"));
    assertU(adoc("id", "4", "stats_i", "-40"));
    assertU(commit());

    Map<String, String> args = new HashMap<String, String>();
    args.put(CommonParams.Q, "*:*");
    args.put(StatsParams.STATS, "true");
    args.put(StatsParams.STATS_FIELD, "stats_i");
    args.put("indent", "true");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(args));

    assertQ("test statistics values", req
            , "//double[@name='min'][.='-40.0']"
            , "//double[@name='max'][.='-10.0']"
            , "//double[@name='sum'][.='-100.0']"
            , "//long[@name='count'][.='4']"
            , "//long[@name='missing'][.='0']"
            , "//double[@name='sumOfSquares'][.='3000.0']"
            , "//double[@name='mean'][.='-25.0']"
            , "//double[@name='stddev'][.='12.909944487358056']"
    );
  }


  public void testMVFieldStatisticsResult() throws Exception {
    SolrCore core = h.getCore();

    assertU(adoc("id", "1", "stats_ii", "-10", "stats_ii", "-100", "active_s", "true"));
    assertU(adoc("id", "2", "stats_ii", "-20", "stats_ii", "200", "active_s", "true"));
    assertU(adoc("id", "3", "stats_ii", "-30", "stats_ii", "-1", "active_s", "false"));
    assertU(adoc("id", "4", "stats_ii", "-40", "stats_ii", "10", "active_s", "false"));
    assertU(commit());


    Map<String, String> args = new HashMap<String, String>();
    args.put(CommonParams.Q, "*:*");
    args.put(StatsParams.STATS, "true");
    args.put(StatsParams.STATS_FIELD, "stats_ii");
    args.put("indent", "true");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(args));


    assertQ("test statistics values", req
            , "//double[@name='min'][.='-100.0']"
            , "//double[@name='max'][.='200.0']"
            , "//double[@name='sum'][.='9.0']"
            , "//long[@name='count'][.='8']"
            , "//long[@name='missing'][.='0']"
            , "//double[@name='sumOfSquares'][.='53101.0']"
            , "//double[@name='mean'][.='1.125']"
            , "//double[@name='stddev'][.='87.08852228787508']"
    );



    args.put(StatsParams.STATS_FACET, "active_s");
    req = new LocalSolrQueryRequest(core, new MapSolrParams(args));

    assertQ("test statistics values", req
            , "//double[@name='min'][.='-100.0']"
            , "//double[@name='max'][.='200.0']"
            , "//double[@name='sum'][.='9.0']"
            , "//long[@name='count'][.='8']"
            , "//long[@name='missing'][.='0']"
            , "//double[@name='sumOfSquares'][.='53101.0']"
            , "//double[@name='mean'][.='1.125']"
            , "//double[@name='stddev'][.='87.08852228787508']"
    );




    assertQ("test value for active_s=true", req
            , "//lst[@name='true']/double[@name='min'][.='-100.0']"
            , "//lst[@name='true']/double[@name='max'][.='200.0']"
            , "//lst[@name='true']/double[@name='sum'][.='70.0']"
            , "//lst[@name='true']/long[@name='count'][.='4']"
            , "//lst[@name='true']/long[@name='missing'][.='0']"
            , "//lst[@name='true']/double[@name='sumOfSquares'][.='50500.0']"
            , "//lst[@name='true']/double[@name='mean'][.='17.5']"
            , "//lst[@name='true']/double[@name='stddev'][.='128.16005617976296']"
    );


  }


  public void testFieldStatisticsMissingResult() throws Exception {
    SolrCore core = h.getCore();
    assertU(adoc("id", "1", "stats_i", "-10"));
    assertU(adoc("id", "2", "stats_i", "-20"));
    assertU(adoc("id", "3"));
    assertU(adoc("id", "4", "stats_i", "-40"));
    assertU(commit());

    Map<String, String> args = new HashMap<String, String>();
    args.put(CommonParams.Q, "*:*");
    args.put(StatsParams.STATS, "true");
    args.put(StatsParams.STATS_FIELD, "stats_i");
    args.put("indent", "true");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(args));

    assertQ("test statistics values", req
            , "//double[@name='min'][.='-40.0']"
            , "//double[@name='max'][.='-10.0']"
            , "//double[@name='sum'][.='-70.0']"
            , "//long[@name='count'][.='3']"
            , "//long[@name='missing'][.='1']"
            , "//double[@name='sumOfSquares'][.='2100.0']"
            , "//double[@name='mean'][.='-23.333333333333332']"
            , "//double[@name='stddev'][.='15.275252316519467']"
    );
  }

  public void testFacetStatisticsResult() throws Exception {
    SolrCore core = h.getCore();
    assertU(adoc("id", "1", "stats_i", "10", "active_s", "true"));
    assertU(adoc("id", "2", "stats_i", "20", "active_s", "true"));
    assertU(adoc("id", "3", "stats_i", "30", "active_s", "false"));
    assertU(adoc("id", "4", "stats_i", "40", "active_s", "false"));
    assertU(commit());

    Map<String, String> args = new HashMap<String, String>();
    args.put(CommonParams.Q, "*:*");
    args.put(StatsParams.STATS, "true");
    args.put(StatsParams.STATS_FIELD, "stats_i");
    args.put(StatsParams.STATS_FACET, "active_s");
    args.put("indent", "true");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new MapSolrParams(args));

    assertQ("test value for active_s=true", req
            , "//lst[@name='true']/double[@name='min'][.='10.0']"
            , "//lst[@name='true']/double[@name='max'][.='20.0']"
            , "//lst[@name='true']/double[@name='sum'][.='30.0']"
            , "//lst[@name='true']/long[@name='count'][.='2']"
            , "//lst[@name='true']/long[@name='missing'][.='0']"
            , "//lst[@name='true']/double[@name='sumOfSquares'][.='500.0']"
            , "//lst[@name='true']/double[@name='mean'][.='15.0']"
            , "//lst[@name='true']/double[@name='stddev'][.='7.0710678118654755']"
    );

    assertQ("test value for active_s=false", req
            , "//lst[@name='false']/double[@name='min'][.='30.0']"
            , "//lst[@name='false']/double[@name='max'][.='40.0']"
            , "//lst[@name='false']/double[@name='sum'][.='70.0']"
            , "//lst[@name='false']/long[@name='count'][.='2']"
            , "//lst[@name='false']/long[@name='missing'][.='0']"
            , "//lst[@name='false']/double[@name='sumOfSquares'][.='2500.0']"
            , "//lst[@name='false']/double[@name='mean'][.='35.0']"
            , "//lst[@name='false']/double[@name='stddev'][.='7.0710678118654755']"
    );
  }
}
