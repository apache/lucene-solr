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

package org.apache.solr.search.facet;

import java.util.Arrays;

import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseHS;
import org.apache.solr.common.params.SolrParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestJsonRangeFacets extends SolrTestCaseHS {

  private static SolrInstances servers;  // for distributed testing
  private static String cache;

  @SuppressWarnings("deprecation")
  @BeforeClass
  public static void beforeTests() throws Exception {
    systemSetPropertySolrDisableShardsWhitelist("true");
    JSONTestUtil.failRepeatedKeys = true;

    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");

    initCore("solrconfig-tlog.xml","schema_latest.xml");
    cache = Boolean.toString(random().nextBoolean());
  }

  /**
   * Start all servers for cluster if they don't already exist
   */
  public static void initServers() throws Exception {
    if (servers == null) {
      servers = new SolrInstances(3, "solrconfig-tlog.xml", "schema_latest.xml");
    }
  }

  @SuppressWarnings("deprecation")
  @AfterClass
  public static void afterTests() throws Exception {
    systemClearPropertySolrDisableShardsWhitelist();
    JSONTestUtil.failRepeatedKeys = false;
    if (servers != null) {
      servers.stop();
      servers = null;
    }
  }

  public void indexSimple(Client client) throws Exception {
    client.deleteByQuery("*:*", null);
    client.add(sdoc("id", "1", "cat_s", "A", "where_s", "NY", "num_d", "4", "num_i", "2",
        "num_is", "4", "num_is", "2",
        "val_b", "true", "sparse_s", "one"), null);
    client.add(sdoc("id", "2", "cat_s", "B", "where_s", "NJ", "num_d", "-9", "num_i", "-5",
        "num_is", "-9", "num_is", "-5",
        "val_b", "false"), null);
    client.add(sdoc("id", "3"), null);
    client.commit();
    client.add(sdoc("id", "4", "cat_s", "A", "where_s", "NJ", "num_d", "2", "num_i", "3",
        "num_is", "2", "num_is", "3"), null);
    client.add(sdoc("id", "5", "cat_s", "B", "where_s", "NJ", "num_d", "11", "num_i", "7",
        "num_is", "11", "num_is", "7",
        "sparse_s", "two"),null);
    client.commit();
    client.add(sdoc("id", "6", "cat_s", "B", "where_s", "NY", "num_d", "-5", "num_i", "-5",
        "num_is", "-5"),null);
    client.commit();
  }

  public void testRangeOtherWhiteboxDistrib() throws Exception {
    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set( "shards", servers.getShards()).set("debugQuery", Boolean.toString(random().nextBoolean()) );
  }

  public void testRangeOtherWhitebox() throws Exception {
    doRangeOtherWhitebox(Client.localClient());
  }

  /**
   * whitebox sanity checks that a shard request range facet that returns "between" or "after"
   * will cause the correct "actual_end" to be returned
   */
  private void doRangeOtherWhitebox(Client client) throws Exception {
    client.queryDefaults().set("cache", cache);
    indexSimple(client);

    // false is default, but randomly check explicit false as well
    final String nohardend = random().nextBoolean() ? "" : " hardend:false, ";

    { // first check some "phase #1" requests

      final SolrParams p = params("q", "*:*", "rows", "0", "isShard", "true", "distrib", "false",
          "_facet_", "{}", "shards.purpose", ""+FacetModule.PURPOSE_GET_JSON_FACETS);
      final String basic_opts = "type:range, field:num_d, start:-5, end:10, gap:7, ";
      final String buckets = "buckets:[ {val:-5.0,count:1}, {val:2.0,count:2}, {val:9.0,count:1} ], ";

      client.testJQ(params(p, "json.facet", "{f:{ " + basic_opts + nohardend + " other:before}}")
          , "facets=={count:6, f:{" + buckets
              // before doesn't need actual_end
              + "   before:{count:1}"
              + "} }"
      );
      client.testJQ(params(p, "json.facet", "{f:{" + basic_opts + nohardend + "other:after}}")
          , "facets=={count:6, f:{" + buckets
              + "   after:{count:0}, _actual_end:'16.0'"
              + "} }"
      );
      client.testJQ(params(p, "json.facet", "{f:{ " + basic_opts + nohardend + "other:between}}")
          , "facets=={count:6, f:{" + buckets
              + "   between:{count:4}, _actual_end:'16.0'"
              + "} }"
      );
      client.testJQ(params(p, "json.facet", "{f:{ " + basic_opts + nohardend + "other:all}}")
          , "facets=={count:6, f:{" + buckets
              + "   before:{count:1},"
              + "   after:{count:0},"
              + "   between:{count:4},"
              + "   _actual_end:'16.0'"
              + "} }"
      );
      // with hardend:true, not only do the buckets change, but actual_end should not need to be returned
      client.testJQ(params(p, "json.facet", "{f:{ " + basic_opts + " hardend:true, other:after}}")
          , "facets=={count:6, f:{"
              + "   buckets:[ {val:-5.0,count:1}, {val:2.0,count:2}, {val:9.0,count:0} ], "
              + "   after:{count:1}"
              + "} }"
      );
    }

    { // now check some "phase #2" requests with refinement buckets already specified

      final String facet
          = "{ top:{ type:range, field:num_i, start:-5, end:5, gap:7," + nohardend
          + "        other:all, facet:{ x:{ type:terms, field:cat_s, limit:1, refine:true } } } }";

      // the behavior should be the same, regardless of wether we pass actual_end to the shards
      // because in a "mixed mode" rolling update, the shards should be smart enough to re-compute if
      // the merging node is running an older version that doesn't send it
      for (String actual_end : Arrays.asList(", _actual_end:'9'", "")) {
        client.testJQ(params("q", "*:*", "rows", "0", "isShard", "true", "distrib", "false",
            "shards.purpose", ""+FacetModule.PURPOSE_REFINE_JSON_FACETS,
            "json.facet", facet,
            "_facet_", "{ refine: { top: { between:{ x:{ _l:[B] } }" + actual_end + "} } }")
            , "facets=={top:{ buckets:[], between:{x:{buckets:[{val:B,count:3}] }} } }");
      }
    }
  }

  @Test
  public void testDateFacetsDistrib() throws Exception {
    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set( "shards", servers.getShards()).set("debugQuery", Boolean.toString(random().nextBoolean()) );
    doDateFacets(client);
  }

  @Test
  public void testDateFacets() throws Exception {
    doDateFacets(Client.localClient());
  }

  private void doDateFacets(Client client) throws Exception {
    client.queryDefaults().set("cache", cache);
    client.deleteByQuery("*:*", null);
    boolean multiValue = random().nextBoolean();
    String dateField = multiValue? "b_dts": "b_dt";
    String dateRange = multiValue? "b_drfs": "b_drf";

    client.add(sdoc("id", "1", "cat_s", "A", dateField, "2014-03-15T12:00:00Z",
        dateRange, "2014-03-15T12:00:00Z"), null);
    client.add(sdoc("id", "2", "cat_s", "B", dateField, "2015-01-03T00:00:00Z",
        dateRange, "2015-01-03T00:00:00Z"), null);
    client.add(sdoc("id", "3"), null);
    client.commit();
    client.add(sdoc("id", "4", "cat_s", "A", dateField, "2014-03-15T12:00:00Z",
        dateRange, "2014-03-15T12:00:00Z"), null);
    client.add(sdoc("id", "5", "cat_s", "B", dateField, "2015-01-03T00:00:00Z",
        dateRange, "2015-01-03T00:00:00Z"),null);
    client.commit();
    client.add(sdoc("id", "6", "cat_s", "B", dateField, "2014-03-15T12:00:00Z",
        dateRange, "2014-03-15T12:00:00Z"),null);
    client.commit();

    SolrParams p = params("q", "*:*", "rows", "0");
    for (String s : new String[]{dateField, dateRange}) {
      client.testJQ(params(p, "json.facet"
          , "{date:{type : range, mincount:1, field :" + s +
              ",start:'2013-11-01T00:00:00Z',end:NOW,gap:'+90DAY'}}"),
          "facets=={count:6, date:{buckets:" +
              "[{val:\"2014-01-30T00:00:00Z\",count:3}, {val:\"2014-10-27T00:00:00Z\",count:2}]" +
              "}}");

      // with ranges
      client.testJQ(params(p, "json.facet"
          , "{date:{type : range, mincount:1, field :" + s +
              ",ranges:[{from:'2013-11-01T00:00:00Z', to:'2014-04-30T00:00:00Z'}," +
              "{from:'2015-01-01T00:00:00Z', to:'2020-01-30T00:00:00Z'}]}}"),
          "facets=={count:6, date:{buckets:" +
              "[{val:\"[2013-11-01T00:00:00Z,2014-04-30T00:00:00Z)\",count:3}," +
              " {val:\"[2015-01-01T00:00:00Z,2020-01-30T00:00:00Z)\",count:2}]" +
              "}}");
    }

    client.add(sdoc("id", "7", "cat_s", "B", dateRange, "[2010 TO 2014-05-21]"),null);
    client.commit();
    client.testJQ(params(p, "json.facet"
        , "{date:{type : range, other:'before', field :" + dateRange +
            ",start:'2011-11-01T00:00:00Z',end:'2016-01-30T00:00:00Z',gap:'+1YEAR'}}"),
        "facets=={count:7, date:{buckets:[" +
            "{val:\"2011-11-01T00:00:00Z\",count:1}, {val:\"2012-11-01T00:00:00Z\",count:1}," +
            "{val:\"2013-11-01T00:00:00Z\",count:4}, {val:\"2014-11-01T00:00:00Z\",count:2}," +
            "{val:\"2015-11-01T00:00:00Z\",count:0}" +
            "],before:{count:1}" +
            "}}");
  }

  @Test
  public void testRangeFacetWithRangesDistrib() throws Exception {
    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set( "shards", servers.getShards()).set("debugQuery", Boolean.toString(random().nextBoolean()) );
    doRangeFacetWithRanges(client);
  }

  public void testRangeFacetWithRanges() throws Exception {
    Client client = Client.localClient();
    doRangeFacetWithRanges(client);
  }

  private void doRangeFacetWithRanges(Client client) throws Exception {
    client.queryDefaults().set("cache", cache);
    client.deleteByQuery("*:*", null);
    indexSimple(client);

    final SolrParams p = params("q", "*:*", "rows", "0");
    // with lower and upper include
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i, ranges:[{range:\"  [-5,7] \"}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"[-5,7]\",count:5}]}}");

    // with lower include and upper exclude
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{range:\"[-5,7)\"}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"[-5,7)\",count:4}]}}");

    // with lower exclude and upper include
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{range:\"(-5,7]\"}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"(-5,7]\",count:3}]}}");

    // with lower and upper exclude
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{range:\"(-5,7)\"}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"(-5,7)\",count:2}]}}");

    // with other and include, they are not supported
    // but wouldn't throw any error as they are not consumed
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{range:\"(-5,7)\"}],include:\"lower\",other:[\"after\"]}}"),
        "facets=={count:6, price:{buckets:[{val:\"(-5,7)\",count:2}]}}");

    // with mincount>0
    client.testJQ(
        params(p, "json.facet", "{price:{type : range,field : num_i,mincount:3," +
            "ranges:[{range:\"(-5,7)\"},{range:\"(-5,7]\"}]}}"
        ),
        "facets=={count:6, price:{buckets:[{val:\"(-5,7]\",count:3}]}}");

    // with multiple ranges
    client.testJQ(
        params(p, "json.facet", "{price:{type : range,field : num_i," +
            "ranges:[{range:\"(-5,7)\"},{range:\"(-5,7]\"}]}}"
        ),
        "facets=={count:6, price:{buckets:[{val:\"(-5,7)\",count:2},{val:\"(-5,7]\",count:3}]}}");

    // with * as one of the values
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{range:\"(*,10]\"}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"(*,10]\",count:5}]}}");
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{range:\"[-5,*)\"}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"[-5,*)\",count:5}]}}");
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{range:\"[*,*]\"}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"[*,*]\",count:5}]}}");
  }

  @Test
  public void testRangeFacetWithRangesInNewFormatDistrib() throws Exception {
    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set( "shards", servers.getShards()).set("debugQuery", Boolean.toString(random().nextBoolean()) );
    doRangeFacetWithRangesInNewFormat(client);
  }

  @Test
  public void testRangeFacetWithRangesInNewFormat() throws Exception {
    Client client = Client.localClient();
    doRangeFacetWithRangesInNewFormat(client);
  }

  private void doRangeFacetWithRangesInNewFormat(Client client) throws Exception {
    client.queryDefaults().set("cache", cache);
    client.deleteByQuery("*:*", null);
    indexSimple(client);
    SolrParams p = params("q", "*:*", "rows", "0");

    //case without inclusive params
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{from:-5, to:7}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"[-5,7)\",count:4}]}}");

    //case without key param and to included
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{from:-5, to:7,inclusive_from:true ,inclusive_to:true}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"[-5,7]\",count:5}]}}");

    //case with all params
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{from:-5, to:7,inclusive_from:true ,inclusive_to:true}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"[-5,7]\",count:5}]}}");

    // from and to excluded
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{from:-5, to:7,inclusive_from:false ,inclusive_to:false}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"(-5,7)\",count:2}]}}");

    // from excluded and to included
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{from:-5, to:7,inclusive_from:false ,inclusive_to:true}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"(-5,7]\",count:3}]}}");

    // multiple ranges
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,include:[\"lower\"], outer:\"before\"," +
            "ranges:[{from:-5, to:7,inclusive_from:false ,inclusive_to:true},{from:-5, to:7,inclusive_from:false ,inclusive_to:false}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"(-5,7]\",count:3},{val:\"(-5,7)\",count:2}]}}");

    // with mincount>0
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,mincount:3" +
            "ranges:[{from:-5, to:7,inclusive_from:false ,inclusive_to:true},{from:-5, to:7,inclusive_from:false ,inclusive_to:false}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"(-5,7]\",count:3}]}}");

    // mix of old and new formats
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i," +
            "ranges:[{from:-5, to:7,inclusive_from:false ,inclusive_to:true},{range:\"(-5,7)\"}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"(-5,7]\",count:3},{val:\"(-5,7)\",count:2}]}}");

    // from==to
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{from:-5, to:-5,inclusive_from:false ,inclusive_to:true}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"(-5,-5]\",count:0}]}}");
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{from:-5, to:-5,inclusive_from:false ,inclusive_to:false}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"(-5,-5)\",count:0}]}}");
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{from:-5, to:-5,inclusive_from:true ,inclusive_to:false}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"[-5,-5)\",count:0}]}}");
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{from:-5, to:-5,inclusive_from:true ,inclusive_to:true}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"[-5,-5]\",count:2}]}}");

    // with * as one of the values
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{from:\"*\", to:10,inclusive_from:false ,inclusive_to:true}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"(*,10]\",count:5}]}}");
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{from:-5, to:\"*\",inclusive_from:true ,inclusive_to:false}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"[-5,*)\",count:5}]}}");
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{from:-5,inclusive_from:true ,inclusive_to:false}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"[-5,*)\",count:5}]}}");
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{from:\"*\", to:\"*\",inclusive_from:true ,inclusive_to:false}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"[*,*)\",count:5}]}}");
    client.testJQ(params(p, "json.facet"
        , "{price:{type : range,field : num_i,ranges:[{inclusive_from:true ,inclusive_to:false}]}}"),
        "facets=={count:6, price:{buckets:[{val:\"[*,*)\",count:5}]}}");
  }

}
