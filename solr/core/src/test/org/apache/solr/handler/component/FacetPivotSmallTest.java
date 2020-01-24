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
package org.apache.solr.handler.component;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;

/**
 * Single node testing of pivot facets
 */
public class FacetPivotSmallTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
    initCore("solrconfig.xml", "schema11.xml");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  /**
   * we don't support comma's in the "stats" local param ... yet: SOLR-6663  
   */
  public void testStatsTagHasComma() throws Exception {

    if (random().nextBoolean()) {
      // behavior should be same either way
      index();
    }

    assertQEx("Can't use multiple tags in stats local param until SOLR-6663 is decided",
              req("q","*:*", "facet", "true",
                  "stats", "true",
                  "stats.field", "{!tag=foo}price_ti",
                  "stats.field", "{!tag=bar}id",
                  "facet.pivot", "{!stats=foo,bar}place_t,company_t"),
              400);
  }

  /**
   * if bogus stats are requested, the pivots should still work
   */
  public void testBogusStatsTag() throws Exception {
    index();

    assertQ(req("q","*:*", "facet", "true",
                "facet.pivot", "{!stats=bogus}place_t,company_t")
            // check we still get pivots...
            , "//arr[@name='place_t,company_t']/lst[str[@name='value'][.='dublin']]"
            // .. but sanity check we don't have any stats
            , "count(//arr[@name='place_t,company_t']/lst[str[@name='value'][.='dublin']]/lst[@name='stats'])=0");
  }

  public void testPivotFacetUnsorted() throws Exception {
    index();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("facet", "true");
    params.add("facet.pivot", "place_t,company_t");

    SolrQueryRequest req = req(params);
    final String facetPivotPrefix = "//lst[@name='facet_counts']/lst[@name='facet_pivot']/arr[@name='place_t,company_t']/lst";
    assertQ(req, facetPivotPrefix + "/str[@name='field'][.='place_t']",
        // dublin
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='microsoft']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=4]",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='polecat']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=4]",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[3]/str[@name='value'][.='null']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[3]/int[@name='count'][.=3]",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[4]/str[@name='value'][.='fujitsu']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[4]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[5]/str[@name='value'][.='bbc']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[5]/int[@name='count'][.=1]",
        // london
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='null']",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=3]",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='polecat']",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=3]",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[3]/str[@name='value'][.='bbc']",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[3]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[4]/str[@name='value'][.='fujitsu']",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[4]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[5]/str[@name='value'][.='microsoft']",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[5]/int[@name='count'][.=2]",
        // cardiff
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='polecat']",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=3]",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='bbc']",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[3]/str[@name='value'][.='microsoft']",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[3]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[4]/str[@name='value'][.='null']",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[4]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[5]/str[@name='value'][.='fujitsu']",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[5]/int[@name='count'][.=1]",
        // krakow
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='null']",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=3]",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='bbc']",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[3]/str[@name='value'][.='polecat']",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[3]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[4]/str[@name='value'][.='fujitsu']",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[4]/int[@name='count'][.=1]",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[5]/str[@name='value'][.='microsoft']",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[5]/int[@name='count'][.=1]",

        // la
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='fujitsu']",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='microsoft']",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[3]/str[@name='value'][.='null']",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[3]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[4]/str[@name='value'][.='polecat']",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[4]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[5]/str[@name='value'][.='bbc']",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[5]/int[@name='count'][.=1]",
        // cork
        facetPivotPrefix + "[str[@name='value'][.='cork']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='fujitsu']",
        facetPivotPrefix + "[str[@name='value'][.='cork']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=1]",
        facetPivotPrefix + "[str[@name='value'][.='cork']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='rte']",
        facetPivotPrefix + "[str[@name='value'][.='cork']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=1]"
    );
  }

  public void testPivotFacetStatsUnsortedTagged() throws Exception {
    index();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("facet", "true");
    params.add("facet.pivot", "{!stats=s1}place_t,company_t");
    params.add("stats", "true");
    params.add("stats.field", "{!key=avg_price tag=s1}price_ti");

    SolrQueryRequest req = req(params);
    final String statsPrefix = "//lst[@name='facet_counts']/lst[@name='facet_pivot']/arr[@name='place_t,company_t']/lst";
    String dublinMicrosoftStats = statsPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[str[@name='value'][.='microsoft']]/lst[@name='stats']/lst[@name='stats_fields']/lst[@name='avg_price']";
    String cardiffPolecatStats = statsPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[str[@name='value'][.='polecat']]/lst[@name='stats']/lst[@name='stats_fields']/lst[@name='avg_price']";
    String krakowFujitsuStats = statsPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[str[@name='value'][.='fujitsu']]/lst[@name='stats']/lst[@name='stats_fields']/lst[@name='avg_price']";
    assertQ(req,
        dublinMicrosoftStats + "/double[@name='min'][.=15.0]",
        dublinMicrosoftStats + "/double[@name='max'][.=29.0]",
        dublinMicrosoftStats + "/long[@name='count'][.=3]",
        dublinMicrosoftStats + "/long[@name='missing'][.=1]",
        dublinMicrosoftStats + "/double[@name='sum'][.=63.0]",
        dublinMicrosoftStats + "/double[@name='sumOfSquares'][.=1427.0]",
        dublinMicrosoftStats + "/double[@name='mean'][.=21.0]",
        dublinMicrosoftStats + "/double[@name='stddev'][.=7.211102550927978]",
        // if new stats are supported, this will break - update test to assert values for each
        "count(" + dublinMicrosoftStats + "/*)=8",

        cardiffPolecatStats + "/double[@name='min'][.=15.0]",
        cardiffPolecatStats + "/double[@name='max'][.=39.0]",
        cardiffPolecatStats + "/long[@name='count'][.=2]",
        cardiffPolecatStats + "/long[@name='missing'][.=1]",
        cardiffPolecatStats + "/double[@name='sum'][.=54.0]",
        cardiffPolecatStats + "/double[@name='sumOfSquares'][.=1746.0]",
        cardiffPolecatStats + "/double[@name='mean'][.=27.0]",
        cardiffPolecatStats + "/double[@name='stddev'][.=16.97056274847714]",
        // if new stats are supported, this will break - update test to assert values for each
        "count(" + cardiffPolecatStats + "/*)=8",

        krakowFujitsuStats + "/null[@name='min']",
        krakowFujitsuStats + "/null[@name='max']",
        krakowFujitsuStats + "/long[@name='count'][.=0]",
        krakowFujitsuStats + "/long[@name='missing'][.=1]",
        krakowFujitsuStats + "/double[@name='sum'][.=0.0]",
        krakowFujitsuStats + "/double[@name='sumOfSquares'][.=0.0]",
        krakowFujitsuStats + "/double[@name='mean'][.='NaN']",
        krakowFujitsuStats + "/double[@name='stddev'][.=0.0]",
        // if new stats are supported, this will break - update test to assert values for each
        "count(" + krakowFujitsuStats + "/*)=8"

    );
  }


  public void testPivotFacetSortedCount() throws Exception {
    index();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("facet", "true");
    params.add("facet.pivot", "place_t,company_t");

    // Test sorting by count
    //TODO clarify why facet count active by default
    // The default is count if facet.limit is greater than 0, index otherwise, but facet.limit was not defined
    params.set(FacetParams.FACET_SORT, FacetParams.FACET_SORT_COUNT);
    final String facetPivotPrefix = "//lst[@name='facet_counts']/lst[@name='facet_pivot']/arr[@name='place_t,company_t']/lst";
    SolrQueryRequest req = req(params);
    assertQ(req, facetPivotPrefix + "/str[@name='field'][.='place_t']",
        // dublin
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='microsoft']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=4]",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='polecat']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=4]",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[3]/str[@name='value'][.='null']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[3]/int[@name='count'][.=3]",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[4]/str[@name='value'][.='fujitsu']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[4]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[5]/str[@name='value'][.='bbc']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[5]/int[@name='count'][.=1]",
        // london
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='null']",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=3]",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='polecat']",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=3]",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[3]/str[@name='value'][.='bbc']",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[3]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[4]/str[@name='value'][.='fujitsu']",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[4]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[5]/str[@name='value'][.='microsoft']",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[5]/int[@name='count'][.=2]",
        // cardiff
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='polecat']",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=3]",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='bbc']",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[3]/str[@name='value'][.='microsoft']",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[3]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[4]/str[@name='value'][.='null']",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[4]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[5]/str[@name='value'][.='fujitsu']",
        facetPivotPrefix + "[str[@name='value'][.='cardiff']]/arr[@name='pivot']/lst[5]/int[@name='count'][.=1]",
        // krakow
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='null']",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=3]",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='bbc']",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[3]/str[@name='value'][.='polecat']",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[3]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[4]/str[@name='value'][.='fujitsu']",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[4]/int[@name='count'][.=1]",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[5]/str[@name='value'][.='microsoft']",
        facetPivotPrefix + "[str[@name='value'][.='krakow']]/arr[@name='pivot']/lst[5]/int[@name='count'][.=1]",

        // la
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='fujitsu']",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='microsoft']",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[3]/str[@name='value'][.='null']",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[3]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[4]/str[@name='value'][.='polecat']",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[4]/int[@name='count'][.=2]",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[5]/str[@name='value'][.='bbc']",
        facetPivotPrefix + "[str[@name='value'][.='la']]/arr[@name='pivot']/lst[5]/int[@name='count'][.=1]",
        // cork
        facetPivotPrefix + "[str[@name='value'][.='cork']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='fujitsu']",
        facetPivotPrefix + "[str[@name='value'][.='cork']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=1]",
        facetPivotPrefix + "[str[@name='value'][.='cork']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='rte']",
        facetPivotPrefix + "[str[@name='value'][.='cork']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=1]"
    );


  }


  public void testPivotFacetLimit() throws Exception {
    index();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("facet", "true");
    params.add("facet.pivot", "place_t,company_t");

    params.set(FacetParams.FACET_SORT, FacetParams.FACET_SORT_COUNT);
    params.set(FacetParams.FACET_LIMIT, 2);

    final String facetPivotPrefix = "//lst[@name='facet_counts']/lst[@name='facet_pivot']/arr[@name='place_t,company_t']/lst";
    SolrQueryRequest req = req(params);
    assertQ(req, facetPivotPrefix + "/str[@name='field'][.='place_t']",
        // dublin
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='microsoft']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=4]",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='polecat']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=4]",
        // london
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='null']",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=3]",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='polecat']",
        facetPivotPrefix + "[str[@name='value'][.='london']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=3]"
    );
  }

  public void testPivotIndividualFacetLimit() throws Exception {
    index();

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("facet", "true");
    params.add("facet.pivot", "place_t,company_t");

    params.set(FacetParams.FACET_SORT, FacetParams.FACET_SORT_COUNT);
    params.set("f.place_t." + FacetParams.FACET_LIMIT, 1);
    params.set("f.company_t." + FacetParams.FACET_LIMIT, 4);

    final String facetPivotPrefix = "//lst[@name='facet_counts']/lst[@name='facet_pivot']/arr[@name='place_t,company_t']/lst";
    SolrQueryRequest req = req(params);
    assertQ(req, facetPivotPrefix + "/str[@name='field'][.='place_t']",
        // dublin
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[1]/str[@name='value'][.='microsoft']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[1]/int[@name='count'][.=4]",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[2]/str[@name='value'][.='polecat']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[2]/int[@name='count'][.=4]",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[3]/str[@name='value'][.='null']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[3]/int[@name='count'][.=3]",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[4]/str[@name='value'][.='fujitsu']",
        facetPivotPrefix + "[str[@name='value'][.='dublin']]/arr[@name='pivot']/lst[4]/int[@name='count'][.=2]"
    );
  }

  public void testPivotFacetMissing() throws Exception {
    // Test facet.missing=true with diff sorts
    index();
    indexMissing();

    SolrParams missingA = params("q", "*:*",
        "rows", "0",
        "facet", "true",
        "facet.pivot", "place_t,company_t",
        // default facet.sort
        FacetParams.FACET_MISSING, "true");

    final String facetPivotPrefix = "//lst[@name='facet_counts']/lst[@name='facet_pivot']/arr[@name='place_t,company_t']/lst";
    SolrQueryRequest req = req(missingA);
    assertQ(req, facetPivotPrefix + "/arr[@name='pivot'][count(.) > 0]",   // not enough values for pivot
        facetPivotPrefix + "[7]/null[@name='value'][.='']",   // not the missing place value
        facetPivotPrefix + "[7]/int[@name='count'][.=2]",       // wrong missing place count
        facetPivotPrefix + "[7]/arr[@name='pivot'][count(.) > 0]", // not enough sub-pivots for missing place
        facetPivotPrefix + "[7]/arr[@name='pivot']/lst[6]/null[@name='value'][.='']", // not the missing company value
        facetPivotPrefix + "[7]/arr[@name='pivot']/lst[6]/int[@name='count'][.=1]", // wrong missing company count
        facetPivotPrefix + "[7]/arr[@name='pivot']/lst[6][not(arr[@name='pivot'])]" // company shouldn't have sub-pivots
    );

    SolrParams missingB = SolrParams.wrapDefaults(missingA,
        params(FacetParams.FACET_LIMIT, "4",
            "facet.sort", "index"));


    req = req(missingB);
    assertQ(req, facetPivotPrefix + "/arr[@name='pivot'][count(.) > 0]",   // not enough values for pivot
        facetPivotPrefix + "[5]/null[@name='value'][.='']",   // not the missing place value
        facetPivotPrefix + "[5]/int[@name='count'][.=2]",       // wrong missing place count
        facetPivotPrefix + "[5]/arr[@name='pivot'][count(.) > 0]", // not enough sub-pivots for missing place
        facetPivotPrefix + "[5]/arr[@name='pivot']/lst[5]/null[@name='value'][.='']", // not the missing company value
        facetPivotPrefix + "[5]/arr[@name='pivot']/lst[5]/int[@name='count'][.=1]", // wrong missing company count
        facetPivotPrefix + "[5]/arr[@name='pivot']/lst[5][not(arr[@name='pivot'])]" // company shouldn't have sub-pivots
    );

    SolrParams missingC = SolrParams.wrapDefaults(missingA,
        params(FacetParams.FACET_LIMIT, "0", "facet.sort", "index"));

    assertQ(req(missingC), facetPivotPrefix + "/arr[@name='pivot'][count(.) > 0]",   // not enough values for pivot
        facetPivotPrefix + "[1]/null[@name='value'][.='']",   // not the missing place value
        facetPivotPrefix + "[1]/int[@name='count'][.=2]",       // wrong missing place count
        facetPivotPrefix + "[1]/arr[@name='pivot'][count(.) > 0]", // not enough sub-pivots for missing place
        facetPivotPrefix + "[1]/arr[@name='pivot']/lst[1]/null[@name='value'][.='']", // not the missing company value
        facetPivotPrefix + "[1]/arr[@name='pivot']/lst[1]/int[@name='count'][.=1]", // wrong missing company count
        facetPivotPrefix + "[1]/arr[@name='pivot']/lst[1][not(arr[@name='pivot'])]" // company shouldn't have sub-pivots
    );
  }

  public void testPivotFacetIndexSortMincountAndLimit() throws Exception {
    // sort=index + mincount + limit
    index();
    indexMissing();

    for (SolrParams variableParams : new SolrParams[]{
        // we should get the same results regardless of overrequest
        params(),
        params()}) {
      SolrParams p = SolrParams.wrapDefaults(params("q", "*:*",
              "rows", "0",
              "facet", "true",
              "facet.pivot", "company_t",
              "facet.sort", "index",
              "facet.pivot.mincount", "4",
              "facet.limit", "4"),
          variableParams);
      final String facetPivotPrefix = "//lst[@name='facet_counts']/lst[@name='facet_pivot']/arr[@name='company_t']";
      SolrQueryRequest req = req(p);
      assertQ(req, facetPivotPrefix + "[count(./lst) = 4]",   // not enough values for pivot
          facetPivotPrefix + "/lst[1]/str[@name='value'][.='fujitsu']",
          facetPivotPrefix + "/lst[1]/int[@name='count'][.=4]",
          facetPivotPrefix + "/lst[2]/str[@name='value'][.='microsoft']",
          facetPivotPrefix + "/lst[2]/int[@name='count'][.=5]",
          facetPivotPrefix + "/lst[3]/str[@name='value'][.='null']",
          facetPivotPrefix + "/lst[3]/int[@name='count'][.=6]",
          facetPivotPrefix + "/lst[4]/str[@name='value'][.='polecat']",
          facetPivotPrefix + "/lst[4]/int[@name='count'][.=6]"
      );
    }
  }

  public void testPivotFacetIndexSortMincountLimitAndOffset() throws Exception {
    // sort=index + mincount + limit + offset
    index();
    indexMissing();

    for (SolrParams variableParams : new SolrParams[]{
        // we should get the same results regardless of overrequest
        params(),
        params()}) {
      SolrParams p = SolrParams.wrapDefaults(params("q", "*:*",
              "rows", "0",
              "facet", "true",
              "facet.pivot", "company_t",
              "facet.sort", "index",
              "facet.pivot.mincount", "4",
              "facet.offset", "1",
              "facet.limit", "4"),
          variableParams);
      final String facetPivotPrefix = "//lst[@name='facet_counts']/lst[@name='facet_pivot']/arr[@name='company_t']";
      SolrQueryRequest req = req(p);
      assertQ(req, facetPivotPrefix + "[count(./lst) = 3]", // asked for 4, but not enough meet the mincount
          facetPivotPrefix + "/lst[1]/str[@name='value'][.='microsoft']",
          facetPivotPrefix + "/lst[1]/int[@name='count'][.=5]",
          facetPivotPrefix + "/lst[2]/str[@name='value'][.='null']",
          facetPivotPrefix + "/lst[2]/int[@name='count'][.=6]",
          facetPivotPrefix + "/lst[3]/str[@name='value'][.='polecat']",
          facetPivotPrefix + "/lst[3]/int[@name='count'][.=6]"
      );
    }
  }


  public void testPivotFacetIndexSortMincountLimitAndOffsetPermutations() throws Exception {
    // sort=index + mincount + limit + offset (more permutations)
    index();
    indexMissing();

    for (SolrParams variableParams : new SolrParams[]{
        // all of these combinations should result in the same first value
        params("facet.pivot.mincount", "4",
            "facet.offset", "2"),
        params("facet.pivot.mincount", "5",
            "facet.offset", "1"),
        params("facet.pivot.mincount", "6",
            "facet.offset", "0")}) {
      SolrParams p = SolrParams.wrapDefaults(params("q", "*:*",
              "rows", "0",
              "facet", "true",
              "facet.limit", "1",
              "facet.sort", "index",
              "facet.overrequest.ratio", "0",
              "facet.pivot", "company_t"),
          variableParams);
      final String facetPivotPrefix = "//lst[@name='facet_counts']/lst[@name='facet_pivot']/arr[@name='company_t']";
      SolrQueryRequest req = req(p);
      assertQ(req, facetPivotPrefix + "[count(./lst) = 1]", // asked for 4, but not enough meet the mincount
          facetPivotPrefix + "/lst[1]/str[@name='value'][.='null']",
          facetPivotPrefix + "/lst[1]/int[@name='count'][.=6]"
      );
    }
  }

  private void indexMissing() {
    String[] missingDoc = {"id", "777"};
    assertU(adoc(missingDoc));
    assertU(commit());
  }

  private void index() {
    // NOTE: we use the literal (4 character) string "null" as a company name
    // to help ensure there isn't any bugs where the literal string is treated as if it
    // were a true NULL value.
    String[] doc = {"id", "19", "place_t", "cardiff dublin", "company_t", "microsoft polecat", "price_ti", "15"};
    assertU(adoc(doc));
    String[] doc1 = {"id", "20", "place_t", "dublin", "company_t", "polecat microsoft null", "price_ti", "19"};
    assertU(adoc(doc1));
    String[] doc2 = {"id", "21", "place_t", "london la dublin", "company_t",
        "microsoft fujitsu null polecat", "price_ti", "29"};
    assertU(adoc(doc2));
    String[] doc3 = {"id", "22", "place_t", "krakow london cardiff", "company_t",
        "polecat null bbc", "price_ti", "39"};
    assertU(adoc(doc3));
    String[] doc4 = {"id", "23", "place_t", "london", "company_t", "", "price_ti", "29"};
    assertU(adoc(doc4));
    String[] doc5 = {"id", "24", "place_t", "la", "company_t", ""};
    assertU(adoc(doc5));
    String[] doc6 = {"id", "25", "company_t", "microsoft polecat null fujitsu null bbc", "price_ti", "59"};
    assertU(adoc(doc6));
    String[] doc7 = {"id", "26", "place_t", "krakow", "company_t", "null"};
    assertU(adoc(doc7));
    String[] doc8 = {"id", "27", "place_t", "krakow cardiff dublin london la", "company_t",
        "null microsoft polecat bbc fujitsu"};
    assertU(adoc(doc8));
    String[] doc9 = {"id", "28", "place_t", "cork", "company_t",
        "fujitsu rte"};
    assertU(adoc(doc9));
    assertU(commit());
  }
}
