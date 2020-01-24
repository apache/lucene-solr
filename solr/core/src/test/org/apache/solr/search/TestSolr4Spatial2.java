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
package org.apache.solr.search;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.SpatialUtils;
import org.apache.solr.util.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;

//Unlike TestSolr4Spatial, not parametrized / not generic.
public class TestSolr4Spatial2 extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-spatial.xml", "schema-spatial.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    RetrievalCombo.idCounter = 0;
  }

  @Test
  public void testQuadTreeRobustness() {
    assertU(adoc("id", "0", "oslocation", "244502.06 639062.07"));
    // old (pre 8.3.0) still works
    assertU(adoc("id", "0", "oslocationold", "244502.06 639062.07"));
  }

  @Test
  public void testBBox() throws Exception {
    String fieldName = random().nextBoolean() ? "bbox" : "bboxD_dynamic";
    assertU(adoc("id", "0"));//nothing
    assertU(adoc("id", "1", fieldName, "ENVELOPE(-10, 20, 15, 10)"));
    assertU(adoc("id", "2", fieldName, "ENVELOPE(22, 22, 10, 10)"));//pt
    assertU(commit());

    assertJQ(req("q", "{!field f="+fieldName+" filter=false score=overlapRatio " +
                "queryTargetProportion=0.25}" +
                "Intersects(ENVELOPE(10,25,12,10))",
            "fl", "*,score",
            "debug", "results"),//explain info
        "/response/docs/[0]/id=='2'",
        "/response/docs/[0]/score==0.75]",
        "/response/docs/[1]/id=='1'",
        "/response/docs/[1]/score==0.26666668]",
        "/response/docs/[2]/id=='0'",
        "/response/docs/[2]/score==0.0",

        "/response/docs/[1]/" + fieldName + "=='ENVELOPE(-10, 20, 15, 10)'"//stored value
        );

    //minSideLength with point query
    assertJQ(req("q", "{!field f="+fieldName+" filter=false score=overlapRatio " +
                "queryTargetProportion=0.5 minSideLength=1}" +
                "Intersects(ENVELOPE(0,0,12,12))",//pt
            "fl", "id,score",
            "debug", "results"),//explain info
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/score==0.50333333]"//just over 0.5
    );

    //area2D
    assertJQ(req("q", "{!field f=" + fieldName + " filter=false score=area2D}" +
                "Intersects(ENVELOPE(0,0,12,12))",//pt
            "fl", "id,score",
            "debug", "results"),//explain info
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/score==" + (30f * 5f) + "]"//150
    );
    //area (not 2D)
    assertJQ(req("q", "{!field f=" + fieldName + " filter=false score=area}" +
                "Intersects(ENVELOPE(0,0,12,12))",//pt
            "fl", "id,score",
            "debug", "results"),//explain info
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/score==" + 146.39793f + "]"//a bit less than 150
    );
  }

  @Test
  public void testBadScoreParam() throws Exception {
    String fieldName = "bbox";
    assertQEx("expect friendly error message",
        "area2D",
        req("{!field f=" + fieldName + " filter=false score=bogus}Intersects(ENVELOPE(0,0,12,12))"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testRptWithGeometryField() throws Exception {
    //note: fails with "srpt_geohash" because it's not as precise
    final boolean testCache = true;
    final boolean testHeatmap = true;
    final boolean testPolygon = false; // default spatialContext doesn't handle this
    testRptWithGeometryField("srptgeom", testCache, testHeatmap, testPolygon);
  }

  @Test
  public void testRptWithGeometryGeo3dField() throws Exception {
    final boolean testCache = true;
    final boolean testHeatmap = true;
    final boolean testPolygon = true;
    testRptWithGeometryField("srptgeom_geo3d", testCache, testHeatmap, testPolygon);
  }

  @Test
  public void testRptWithGeometryGeo3dS2Field() throws Exception {
    final boolean testCache = false; // the test data is designed to provoke the cache for non-S2
    final boolean testHeatmap = false; // incompatible
    final boolean testPolygon = true;
    testRptWithGeometryField("srptgeom_s2_geo3d", testCache, testHeatmap, testPolygon);
  }

  @Test @Repeat(iterations = 10)
  public void testLLPDecodeIsStableAndPrecise() throws Exception {
    // test that LatLonPointSpatialField decode of docValue will round-trip (re-index then re-decode) to the same value
    @SuppressWarnings({"resource", "IOResourceOpenedButNotSafelyClosed"})
    SolrClient client = new EmbeddedSolrServer(h.getCore());// do NOT close it; it will close Solr

    final String fld = "llp_1_dv_dvasst";
    String ptOrig = GeoTestUtil.nextLatitude() + "," + GeoTestUtil.nextLongitude();
    assertU(adoc("id", "0", fld, ptOrig));
    assertU(commit());
    // retrieve it (probably less precision)
    String ptDecoded1 = (String) client.query(params("q", "id:0")).getResults().get(0).get(fld);
    // now write it back
    assertU(adoc("id", "0", fld, ptDecoded1));
    assertU(commit());
    // retrieve it; assert that it's the same as written
    String ptDecoded2 = (String) client.query(params("q", "id:0")).getResults().get(0).get(fld);
    assertEquals("orig:" + ptOrig, ptDecoded1, ptDecoded2);

    // test that the representation is pretty accurate
    final Point ptOrigObj = SpatialUtils.parsePoint(ptOrig, SpatialContext.GEO);
    final Point ptDecodedObj = SpatialUtils.parsePoint(ptDecoded1, SpatialContext.GEO);
    double deltaCentimeters = SpatialContext.GEO.calcDistance(ptOrigObj, ptDecodedObj) * DistanceUtils.DEG_TO_KM * 1000.0 * 100.0;
    //See javadocs of LatLonDocValuesField for these constants
    final Point absErrorPt = SpatialContext.GEO.getShapeFactory().pointXY(8.381903171539307E-8, 4.190951585769653E-8);
    double deltaCentimetersMax
        = SpatialContext.GEO.calcDistance(absErrorPt, 0,0) * DistanceUtils.DEG_TO_KM * 1000.0 * 100.0;
    assertEquals(1.0420371840922256, deltaCentimetersMax, 0.0);// just so that we see it in black & white in the test

    //max found by trial & error.  If we used 8 decimal places then we could get down to 1.04cm accuracy but then we
    // lose the ability to round-trip -- 40 would become 39.99999997  (ugh).
    assertTrue("deltaCm too high: " + deltaCentimeters, deltaCentimeters < 1.41);
    // Pt(x=105.29894270124083,y=-0.4371673760042398) to  Pt(x=105.2989428,y=-0.4371673) is 1.38568
  }

  @Test
  public void testLatLonRetrieval() throws Exception {
    final String ptHighPrecision =   "40.2996543270,-74.0824956673";
    final String ptLossOfPrecision = "40.2996544,-74.0824957"; // rounded version of the one above, losing precision

    // "_1" is single, "_N" is multiValued
    // "_dv" is docValues (otherwise not),  "_dvasst" is useDocValuesAsStored (otherwise not)
    // "_st" is stored" (otherwise not)

    // a random point using the number of decimal places we support for round-tripping.
    String randPointStr =
        new BigDecimal(GeoTestUtil.nextLatitude()).setScale(7, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString() +
        "," + new BigDecimal(GeoTestUtil.nextLongitude()).setScale(7, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString();

    List<RetrievalCombo> combos = Arrays.asList(
        new RetrievalCombo("llp_1_dv_st", ptHighPrecision),
        new RetrievalCombo("llp_N_dv_st", Arrays.asList("-40,40", "-45,45")),
        new RetrievalCombo("llp_N_dv_st", Arrays.asList("-40,40")), // multiValued but 1 value

        new RetrievalCombo("llp_1_dv_dvasst", ptHighPrecision, ptLossOfPrecision),
        // this one comes back in a different order since it gets sorted low to high
        new RetrievalCombo("llp_N_dv_dvasst", Arrays.asList("-40,40", "-45,45"), Arrays.asList("-45,45", "-40,40")),
        new RetrievalCombo("llp_N_dv_dvasst", Arrays.asList(randPointStr)), // multiValued but 1 value
        // edge cases.  (note we sorted it as Lucene will internally)
        new RetrievalCombo("llp_N_dv_dvasst", Arrays.asList(
            "-90,180", "-90,-180",
            "0,0", "0,180", "0,-180",
            "90,0", "90,180", "90,-180")),

        new RetrievalCombo("llp_1_dv", ptHighPrecision, ptLossOfPrecision),
        new RetrievalCombo("llp_N_dv", Arrays.asList("-45,45", "-40,40"))

        );
    Collections.shuffle(combos, random());

    // add and commit
    for (RetrievalCombo combo : combos) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "" + combo.id);
      for (String indexValue : combo.indexValues) {
        doc.addField(combo.fieldName, indexValue);
      }
      assertU(adoc(doc));
      if (TestUtils.rarely()) { // induce segments to potentially change internal behavior
        assertU(commit());
      }
    }
    assertU(commit());

    // create an assertJQ assertion string, once for fl=*, another for when the field is listed
    List<String> assertJQsFlListed = new ArrayList<>();
    List<String> assertJQsFlStar = new ArrayList<>();
    for (RetrievalCombo combo : combos) {
      String expect = "response/docs/[" + combo.id + "]/" + combo.fieldName + "==" + combo.expectReturnJSON;
      assertJQsFlListed.add(expect);
      if (combo.fieldName.endsWith("_dv")) {
        expect =  "response/docs/[" + combo.id + "]=={'id':'" + combo.id + "'}"; // only the id, nothing else
      }
      assertJQsFlStar.add(expect);
    }
    // check
    assertJQ(req("q","*:*", "sort", "id asc",
        "fl","*"),
        assertJQsFlStar.toArray(new String[0]));
    assertJQ(req("q","*:*", "sort", "id asc",
        "fl", "id," + combos.stream().map(c -> c.fieldName).collect(Collectors.joining(","))),
        assertJQsFlListed.toArray(new String[0]));
  }

  private static class RetrievalCombo {
    static int idCounter = 0;
    final int id = idCounter++;
    final String fieldName;
    final List<String> indexValues;
    final String expectReturnJSON; //or null if not expected in response

    RetrievalCombo(String fieldName, List<String> indexValues) { this(fieldName, indexValues, indexValues);}
    RetrievalCombo(String fieldName, List<String> indexValues, List<String> returnValues) {
      this.fieldName = fieldName;
      this.indexValues = indexValues;
      this.expectReturnJSON = returnValues.stream().collect(Collectors.joining("', '", "['", "']"));
    }
    RetrievalCombo(String fieldName, String indexValue) { this(fieldName, indexValue, indexValue); }
    RetrievalCombo(String fieldName, String indexValue, String returnValue) {
      this.fieldName = fieldName;
      this.indexValues = Collections.singletonList(indexValue);
      this.expectReturnJSON = "'" + returnValue + "'";
    }
  }

  private void testRptWithGeometryField(String fieldName, boolean testCache, boolean testHeatmap, boolean testPolygon) throws Exception {
    assertU(adoc("id", "0", fieldName, "ENVELOPE(-10, 20, 15, 10)"));
    assertU(adoc("id", "1", fieldName, "BUFFER(POINT(-10 15), 5)"));//circle at top-left corner
    assertU(optimize("maxSegments", "1"));// one segment.
    assertU(commit());

    // Search to the edge but not quite touching the indexed envelope of id=0.  It requires geom validation to
    //  eliminate id=0.  id=1 is found and doesn't require validation.  cache=false means no query cache.
    final SolrQueryRequest sameReq = req(
        "q", "{!cache=false field f=" + fieldName + "}Intersects(ENVELOPE(-20, -10.0001, 30, 15.0001))",
        "sort", "id asc");
    assertJQ(sameReq, "/response/numFound==1", "/response/docs/[0]/id=='1'");

    if (testCache) {
      // The tricky thing is verifying the cache works correctly...

      MetricsMap cacheMetrics = (MetricsMap) ((SolrMetricManager.GaugeWrapper)h.getCore().getCoreMetricManager().getRegistry().getMetrics().get("CACHE.searcher.perSegSpatialFieldCache_" + fieldName)).getGauge();
      assertEquals("1", cacheMetrics.getValue().get("cumulative_inserts").toString());
      assertEquals("0", cacheMetrics.getValue().get("cumulative_hits").toString());

      // Repeat the query earlier
      assertJQ(sameReq, "/response/numFound==1", "/response/docs/[0]/id=='1'");
      assertEquals("1", cacheMetrics.getValue().get("cumulative_hits").toString());

      assertEquals("1 segment",
          1, getSearcher().getRawReader().leaves().size());
      // Get key of first leaf reader -- this one contains the match for sure.
      Object leafKey1 = getFirstLeafReaderKey();

      // add new segment
      assertU(adoc("id", "3"));

      assertU(commit()); // sometimes merges (to one seg), sometimes won't

      // can still find the same document
      assertJQ(sameReq, "/response/numFound==1", "/response/docs/[0]/id=='1'");

      // When there are new segments, we accumulate another hit. This tests the cache was not blown away on commit.
      // (i.e. the cache instance is new but it should've been regenerated from the old one).
      // Checking equality for the first reader's cache key indicates whether the cache should still be valid.
      Object leafKey2 = getFirstLeafReaderKey();
      // get the current instance of metrics - the old one may not represent the current cache instance
      cacheMetrics = (MetricsMap) ((SolrMetricManager.GaugeWrapper)h.getCore().getCoreMetricManager().getRegistry().getMetrics().get("CACHE.searcher.perSegSpatialFieldCache_" + fieldName)).getGauge();
      assertEquals(leafKey1.equals(leafKey2) ? "2" : "1", cacheMetrics.getValue().get("cumulative_hits").toString());
    }

    if (testHeatmap) {
      // Now try to see if heatmaps work:
      assertJQ(req("q", "*:*", "facet", "true", FacetParams.FACET_HEATMAP, fieldName, "json.nl", "map"),
          "/facet_counts/facet_heatmaps/" + fieldName + "/minX==-180.0");
    }

    if (testPolygon) {
      String polygonWKT = "POLYGON((-11 12, -11 11, 10.5 12, -11 12))"; //right-angle triangle.  Counter-clockwise order
      assertJQ(req(
          "q", "{!cache=false field f=" + fieldName + "}Intersects(" + polygonWKT + ")",
          "sort", "id asc"), "/response/numFound==2");

      assertU(adoc("id", "9",
          fieldName, "POLYGON((" + // rectangle. Counter-clockwise order.
              "-118.080201721669 54.5864541583249," +
              "-118.080078279314 54.5864541583249," +
              "-118.080078279314 54.5865258517606," +
              "-118.080201721669 54.5865258517606," +
              "-118.080201721669 54.5864541583249))" ));
      assertU(commit());
      // should NOT match
      assertJQ(req("q", fieldName+":[55.0260828,-115.5085624 TO 55.02646,-115.507337]"),
          "/response/numFound==0");
    }
  }

  protected SolrIndexSearcher getSearcher() {
    // neat trick; needn't deal with the hassle RefCounted
    return (SolrIndexSearcher) h.getCore().getInfoRegistry().get("searcher");
  }


  protected Object getFirstLeafReaderKey() {
    return getSearcher().getRawReader().leaves().get(0).reader().getCoreCacheHelper().getKey();
  }

  @Test// SOLR-8541
  public void testConstantScoreQueryWithFilterPartOnly() {
    final String[] doc1 = {"id", "1", "srptgeom", "56.9485,24.0980"};
    assertU(adoc(doc1));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!geofilt sfield=\"srptgeom\" pt=\"56.9484,24.0981\" d=100}");
    params.add("hl", "true");
    params.add("hl.fl", "srptgeom");
    assertQ(req(params), "*[count(//doc)=1]", "count(//lst[@name='highlighting']/*)=1");
  }

  @Test
  public void testErrorHandlingGeodist() throws Exception{
    assertU(adoc("id", "1", "llp", "32.7693246, -79.9289094"));
    assertQEx("wrong test exception message","sort param could not be parsed as a query, " +
            "and is not a field that exists in the index: geodist(llp,47.36667,8.55)",
        req(
            "q", "*:*",
            "sort", "geodist(llp,47.36667,8.55) asc"
        ), SolrException.ErrorCode.BAD_REQUEST);
  }
}
