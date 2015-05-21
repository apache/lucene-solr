package org.apache.solr.search;

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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

//Unlike TestSolr4Spatial, not parameterized / not generic.
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
    String fieldName = "srptgeom"; //note: fails with "srpt_geohash" because it's not as precise
    assertU(adoc("id", "0", fieldName, "ENVELOPE(-10, 20, 15, 10)"));
    assertU(adoc("id", "1", fieldName, "BUFFER(POINT(-10 15), 5)"));//circle at top-left corner
    assertU(optimize());// one segment.
    assertU(commit());

    // Search to the edge but not quite touching the indexed envelope of id=0.  It requires geom validation to
    //  eliminate id=0.  id=1 is found and doesn't require validation.  cache=false means no query cache.
    final SolrQueryRequest sameReq = req(
        "q", "{!cache=false field f=" + fieldName + "}Intersects(ENVELOPE(-20, -10.0001, 30, 15.0001))",
        "sort", "id asc");
    assertJQ(sameReq, "/response/numFound==1", "/response/docs/[0]/id=='1'");

    // The tricky thing is verifying the cache works correctly...

    SolrCache cache = (SolrCache) h.getCore().getInfoRegistry().get("perSegSpatialFieldCache_srptgeom");
    assertEquals("1", cache.getStatistics().get("cumulative_inserts").toString());
    assertEquals("0", cache.getStatistics().get("cumulative_hits").toString());

    // Repeat the query earlier
    assertJQ(sameReq, "/response/numFound==1", "/response/docs/[0]/id=='1'");
    assertEquals("1", cache.getStatistics().get("cumulative_hits").toString());

    assertEquals("1 segment",
        1, ((SolrIndexSearcher) h.getCore().getInfoRegistry().get("searcher")).getRawReader().leaves().size());
    // add new segment
    assertU(adoc("id", "3"));
    assertU(commit()); // sometimes merges (to one seg), sometimes won't
    boolean newSeg =
      (((SolrIndexSearcher)h.getCore().getInfoRegistry().get("searcher")).getRawReader().leaves().size() > 1);

    // can still find the same document
    assertJQ(sameReq, "/response/numFound==1", "/response/docs/[0]/id=='1'");

    // when there are new segments, we accumulate another hit. This tests the cache was not blown away on commit.
    assertEquals(newSeg ? "2" : "1", cache.getStatistics().get("cumulative_hits").toString());

    // Now try to see if heatmaps work:
    assertJQ(req("q", "*:*", "facet", "true", FacetParams.FACET_HEATMAP, fieldName, "json.nl", "map"),
        "/facet_counts/facet_heatmaps/" + fieldName + "/minX==-180.0");

  }

}
