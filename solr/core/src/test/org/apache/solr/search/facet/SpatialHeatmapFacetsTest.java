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
import java.util.List;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test Heatmap Facets (both impls) */
public class SpatialHeatmapFacetsTest extends BaseDistributedSearchTestCase {
  private static final String FIELD = "srpt_quad";

  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    schemaString = "schema-spatial.xml";
    configString = "solrconfig-basic.xml";

    //Strictly not necessary (set already in Ant & Maven) but your IDE might not have this set
    System.setProperty("java.awt.headless", "true");
  }

  /** Tests SimpleFacets/Classic faceting implementation of heatmaps */
  @SuppressWarnings("unchecked")
  @Test
  public void testClassicFacets() throws Exception { // AKA SimpleFacets
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);

    SolrParams baseParams = params("q", "*:*", "rows", "0", "facet", "true", FacetParams.FACET_HEATMAP, FIELD);

    final String testBox = "[\"50 50\" TO \"180 90\"]";//top-right somewhere on edge (whatever)

    //----- First we test gridLevel derivation
    try {
      getHmObj(query(params(baseParams, FacetParams.FACET_HEATMAP_GEOM, testBox, FacetParams.FACET_HEATMAP_DIST_ERR, "0"))).get("gridLevel");
      fail();
    } catch (SolrException e) {
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    }
    try {
      getHmObj(query(params(baseParams, FacetParams.FACET_HEATMAP_GEOM, testBox, FacetParams.FACET_HEATMAP_DIST_ERR_PCT, "0"))).get("gridLevel");
      fail();
    } catch (SolrException e) {
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    }
    // Monkeying with these params changes the gridLevel in different directions. We don't test the exact
    // computation here; that's not _that_ relevant, and is Lucene spatial's job (not Solr) any way.
    assertEquals(7, getHmObj(query(params(baseParams, FacetParams.FACET_HEATMAP_GEOM, testBox))).get("gridLevel"));//default
    assertEquals(3, getHmObj(query(params(baseParams, FacetParams.FACET_HEATMAP_GEOM, testBox, FacetParams.FACET_HEATMAP_LEVEL, "3"))).get("gridLevel"));
    assertEquals(2, getHmObj(query(params(baseParams, FacetParams.FACET_HEATMAP_GEOM, testBox, FacetParams.FACET_HEATMAP_DIST_ERR, "100"))).get("gridLevel"));
    //TODO test impact of distance units
    assertEquals(9, getHmObj(query(params(baseParams, FacetParams.FACET_HEATMAP_GEOM, testBox, FacetParams.FACET_HEATMAP_DIST_ERR_PCT, "0.05"))).get("gridLevel"));
    assertEquals(6, getHmObj(query(params(baseParams, FacetParams.FACET_HEATMAP_DIST_ERR_PCT, "0.10"))).get("gridLevel"));

    //test key output label doing 2 heatmaps with different settings on the same field
    {
      final ModifiableSolrParams params = params(baseParams, FacetParams.FACET_HEATMAP_DIST_ERR_PCT, "0.10");
      String courseFormat = random().nextBoolean() ? "png" : "ints2D";
      params.add(FacetParams.FACET_HEATMAP, "{!key=course "
          + FacetParams.FACET_HEATMAP_LEVEL + "=2 "
          + FacetParams.FACET_HEATMAP_FORMAT + "=" + courseFormat
          + "}" + FIELD);
      final QueryResponse response = query(params);
      assertEquals(6, getHmObj(response).get("gridLevel"));//same test as above
      assertEquals(2, response.getResponse().findRecursive("facet_counts", "facet_heatmaps", "course", "gridLevel"));
      assertTrue(((NamedList<Object>) response.getResponse().findRecursive("facet_counts", "facet_heatmaps", "course"))
          .asMap(0).containsKey("counts_" + courseFormat));
    }

    // ------ Index data

    index("id", "0", FIELD, "ENVELOPE(100, 120, 80, 40)");// on right side
    index("id", "1", FIELD, "ENVELOPE(-120, -110, 80, 20)");// on left side (outside heatmap)
    index("id", "3", FIELD, "POINT(70 60)");//just left of BOX 0
    index("id", "4", FIELD, "POINT(91 89)");//just outside box 0 (above it) near pole,

    commit();

    // ----- Search
    // this test simply has some 0's, nulls, 1's and a 2 in there.
    @SuppressWarnings({"rawtypes"})
    NamedList hmObj = getHmObj(query(params(baseParams,
        FacetParams.FACET_HEATMAP_GEOM, "[\"50 20\" TO \"180 90\"]",
        FacetParams.FACET_HEATMAP_LEVEL, "4")));
    List<List<Integer>> counts = (List<List<Integer>>) hmObj.get("counts_ints2D");
    assertEquals(
        Arrays.asList(
            Arrays.asList(0, 0, 2, 1, 0, 0),
            Arrays.asList(0, 0, 1, 1, 0, 0),
            Arrays.asList(0, 1, 1, 1, 0, 0),
            Arrays.asList(0, 0, 1, 1, 0, 0),
            Arrays.asList(0, 0, 1, 1, 0, 0),
            null,
            null
        ),
        counts
    );

    // now this time we add a filter query and exclude it
    QueryResponse response = query(params(baseParams,
        "fq", "{!tag=excludeme}id:0", // filter to only be id:0
        FacetParams.FACET_HEATMAP, "{!ex=excludeme}" + FIELD, // exclude the filter
        FacetParams.FACET_HEATMAP_GEOM, "[\"50 20\" TO \"180 90\"]",
        FacetParams.FACET_HEATMAP_LEVEL, "4"));
    assertEquals(1, response.getResults().getNumFound());// because of our 'fq'
    hmObj = getHmObj(response);
    counts = (List<List<Integer>>) hmObj.get("counts_ints2D");
    assertEquals(
        Arrays.asList(  // same counts as before
            Arrays.asList(0, 0, 2, 1, 0, 0),
            Arrays.asList(0, 0, 1, 1, 0, 0),
            Arrays.asList(0, 1, 1, 1, 0, 0),
            Arrays.asList(0, 0, 1, 1, 0, 0),
            Arrays.asList(0, 0, 1, 1, 0, 0),
            null,
            null
        ),
        counts
    );

    // test using a circle input shape
    hmObj = getHmObj(query(params(baseParams,
        FacetParams.FACET_HEATMAP_GEOM, "BUFFER(POINT(110 40), 7)",
        FacetParams.FACET_HEATMAP_LEVEL, "7")));
    counts = (List<List<Integer>>) hmObj.get("counts_ints2D");
    assertEquals(
        Arrays.asList(
            Arrays.asList(0, 1, 1, 1, 1, 1, 1, 0),//curved; we have a 0
            Arrays.asList(0, 1, 1, 1, 1, 1, 1, 0),//curved; we have a 0
            Arrays.asList(0, 1, 1, 1, 1, 1, 1, 0),//curved; we have a 0
            Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1),
            Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1),
            Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1),
            null, null, null, null, null//no data here (below edge of rect 0)
        ),
        counts
    );

    // Search in no-where ville and get null counts
    assertNull(getHmObj(query(params(baseParams, FacetParams.FACET_HEATMAP_GEOM, "ENVELOPE(0, 10, -80, -90)"))).get("counts_ints2D"));

    Object v = getHmObj(query(params(baseParams, FacetParams.FACET_HEATMAP_FORMAT, "png"))).get("counts_png");
    assertTrue(v instanceof byte[]);
    //simply test we can read the image
    assertNotNull(FacetHeatmap.PngHelper.readImage((byte[]) v));
    //good enough for this test method
  }

  private ModifiableSolrParams params(SolrParams baseParams, String... moreParams) {
    final ModifiableSolrParams params = new ModifiableSolrParams(baseParams);
    params.add(params(moreParams));//actually replaces
    return params;
  }

  /** Tests JSON Facet module implementation of heatmaps. */
  @SuppressWarnings("unchecked")
  @Test
  public void testJsonFacets() throws Exception {
    /*
    THIS IS THE MOSTLY SAME CODE as above with tweaks to request it using the JSON Facet approach.
      Near-duplication is sad; not clear if one test doing both is better -- would be awkward
     */
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);

    SolrParams baseParams = params("q", "*:*", "rows", "0");

    final String testBox = "[\"50 50\" TO \"180 90\"]";//top-right somewhere on edge (whatever)

    // ------ Index data

    index("id", "0", FIELD, "ENVELOPE(100, 120, 80, 40)");// on right side
    index("id", "1", FIELD, "ENVELOPE(-120, -110, 80, 20)");// on left side (outside heatmap)
    index("id", "3", FIELD, "POINT(70 60)");//just left of BOX 0
    index("id", "4", FIELD, "POINT(91 89)");//just outside box 0 (above it) near pole,

    commit();

    //----- Test gridLevel derivation
    try {
      query(params(baseParams, "json.facet", "{f1:{type:heatmap, f:" + FIELD + ", geom:'" + testBox + "', distErr:0}}"));
      fail();
    } catch (SolrException e) {
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    }
    try {
      query(params(baseParams, "json.facet", "{f1:{type:heatmap, f:" + FIELD + ", geom:'" + testBox + "', distErrPct:0}}"));
      fail();
    } catch (SolrException e) {
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    }
    // Monkeying with these params changes the gridLevel in different directions. We don't test the exact
    // computation here; that's not _that_ relevant, and is Lucene spatial's job (not Solr) any way.
    assertEquals(7, getHmObj(query(params(baseParams,
        "json.facet", "{f1:{type:heatmap, f:" + FIELD + ", geom:'" + testBox + "'}}"))).get("gridLevel"));//default
    assertEquals(3, getHmObj(query(params(baseParams,
        "json.facet", "{f1:{type:heatmap, f:" + FIELD + ", geom:'" + testBox + "', gridLevel:3}}"))).get("gridLevel"));
    assertEquals(2, getHmObj(query(params(baseParams,
        "json.facet", "{f1:{type:heatmap, f:" + FIELD + ", geom:'" + testBox + "', distErr:100}}"))).get("gridLevel"));
    //TODO test impact of distance units
    assertEquals(9, getHmObj(query(params(baseParams,
        "json.facet", "{f1:{type:heatmap, f:" + FIELD + ", geom:'" + testBox + "', distErrPct:0.05}}"))).get("gridLevel"));
    assertEquals(6, getHmObj(query(params(baseParams,
        "json.facet", "{f1:{type:heatmap, f:" + FIELD + ", distErrPct:0.10}}"))).get("gridLevel"));

    // ----- Search
    // this test simply has some 0's, nulls, 1's and a 2 in there.
    @SuppressWarnings({"rawtypes"})
    NamedList hmObj = getHmObj(query(params(baseParams,
        "json.facet", "{f1:{type:heatmap, f:" + FIELD + " geom:'[\"50 20\" TO \"180 90\"]', gridLevel:4}}")));
    List<List<Integer>> counts = (List<List<Integer>>) hmObj.get("counts_ints2D");
    List<List<Integer>> expectedCounts1 = Arrays.asList(
        Arrays.asList(0, 0, 2, 1, 0, 0),
        Arrays.asList(0, 0, 1, 1, 0, 0),
        Arrays.asList(0, 1, 1, 1, 0, 0),
        Arrays.asList(0, 0, 1, 1, 0, 0),
        Arrays.asList(0, 0, 1, 1, 0, 0),
        null,
        null
    );
    assertEquals( expectedCounts1, counts);

    // now this time we add a filter query and exclude it
    QueryResponse response = query(params(baseParams,
        "fq", "{!tag=excludeme}id:0", // filter to only be id:0
        "json.facet", "{f1:{type:heatmap, excludeTags:['excludeme'], f:" + FIELD + ", geom:'[\"50 20\" TO \"180 90\"]', gridLevel:4}}"));

    assertEquals(1, response.getResults().getNumFound());// because of our 'fq'
    hmObj = getHmObj(response);
    counts = (List<List<Integer>>) hmObj.get("counts_ints2D");
    assertEquals( expectedCounts1, counts);

    {
      // impractical example but nonetheless encloses the points of both doc3 and doc4 (both of which are points)
      final String jsonHeatmap = "facet:{hm:{type:heatmap, f:" + FIELD + ", geom:'MULTIPOINT(70 60, 91 89)', distErrPct:0.2}}";
      response = query(params(baseParams,
          "json.facet", "{" +
              "q1:{type:query, q:'id:3', " + jsonHeatmap + " }, " +
              "q2:{type:query, q:'id:4', " + jsonHeatmap + " } " +
              "}"));
      {
        @SuppressWarnings({"rawtypes"})
        final NamedList q1Res = (NamedList) response.getResponse().findRecursive("facets", "q1");
        assertEquals("1", q1Res.get("count").toString());
        @SuppressWarnings({"rawtypes"})
        final NamedList q2Res = (NamedList) response.getResponse().findRecursive("facets", "q2");
        assertEquals("1", q2Res.get("count").toString());
        // essentially, these will differ only in the heatmap counts but otherwise will be the same
        assertNotNull(compare(q1Res, q2Res, flags, handle));
      }
    }

    // test using a circle input shape
    hmObj = getHmObj(query(params(baseParams,
        "json.facet", "{f1:{type:heatmap, f:" + FIELD + ", geom:'BUFFER(POINT(110 40), 7)', gridLevel:7}}")));
    counts = (List<List<Integer>>) hmObj.get("counts_ints2D");
    assertEquals(
        Arrays.asList(
            Arrays.asList(0, 1, 1, 1, 1, 1, 1, 0),//curved; we have a 0
            Arrays.asList(0, 1, 1, 1, 1, 1, 1, 0),//curved; we have a 0
            Arrays.asList(0, 1, 1, 1, 1, 1, 1, 0),//curved; we have a 0
            Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1),
            Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1),
            Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1),
            null, null, null, null, null//no data here (below edge of rect 0)
        ),
        counts
    );

    // Search in no-where ville and get null counts
    assertNull(getHmObj(query(params(baseParams,
        "json.facet", "{f1:{type:heatmap, f:" + FIELD + ", geom:'ENVELOPE(0, 10, -80, -90)'}}"))).get("counts_ints2D"));

    Object v = getHmObj(query(params(baseParams,
        "json.facet", "{f1:{type:heatmap, f:" + FIELD + ", format:png }}"))).get("counts_png");
    assertTrue(v instanceof byte[]);
    //simply test we can read the image
    assertNotNull(FacetHeatmap.PngHelper.readImage((byte[]) v));
    //good enough for this test method
  }

  @SuppressWarnings({"rawtypes"})
  private NamedList getHmObj(QueryResponse response) {
    // classic faceting
    final NamedList classicResp = (NamedList) response.getResponse().findRecursive("facet_counts", "facet_heatmaps", FIELD);
    if (classicResp != null) {
      return classicResp;
    }
    // JSON Facet
    return (NamedList) response.getResponse().findRecursive("facets", "f1");
  }

  @Test
  @Repeat(iterations = 3)
  public void testPng() {
    //We test via round-trip randomized data:

    // Make random data
    int columns = random().nextInt(100) + 1;
    int rows = random().nextInt(100) + 1;
    int[] counts = new int[columns * rows];
    for (int i = 0; i < counts.length; i++) {
      final int ri = random().nextInt(10);
      if (ri >= 0 && ri <= 3) {
        counts[i] = ri; // 0 thru 3 will be made common
      } else if (ri > 3) {
        counts[i] = Math.abs(random().nextInt());//lots of other possible values up to max
      }
    }
    // Round-trip
    final byte[] bytes = FacetHeatmap.asPngBytes(columns, rows, counts, null);
    int[] countsOut = random().nextBoolean() ? new int[columns * rows] : null;
    int base = 0;
    if (countsOut != null) {
      base = 9;
      Arrays.fill(countsOut, base);
    }
    countsOut = FacetHeatmap.addPngToIntArray(bytes, countsOut);
    // Test equal
    assertEquals(counts.length, countsOut.length);
    for (int i = 0; i < countsOut.length; i++) {
      assertEquals(counts[i], countsOut[i] - base);//back out the base input to prove we added
    }
  }

}
