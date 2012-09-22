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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test Solr 4's new spatial capabilities from the new Lucene spatial module. Don't thoroughly test it here because
 * Lucene spatial has its own tests.  Some of these tests were ported from Solr 3 spatial tests.
 */
public class TestSolr4Spatial extends SolrTestCaseJ4 {

  private String fieldName;

  public TestSolr4Spatial(String fieldName) {
    this.fieldName = fieldName;
  }

  @ParametersFactory
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][]{
        {"srpt_geohash"}, {"srpt_quad"}, {"stqpt_geohash"}, {"pointvector"}
    });
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml", "schema-spatial.xml");
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  private void setupDocs() {
    assertU(adoc("id", "1", fieldName, "32.7693246, -79.9289094"));
    assertU(adoc("id", "2", fieldName, "33.7693246, -80.9289094"));
    assertU(adoc("id", "3", fieldName, "-32.7693246, 50.9289094"));
    assertU(adoc("id", "4", fieldName, "-50.7693246, 60.9289094"));
    assertU(adoc("id", "5", fieldName, "0,0"));
    assertU(adoc("id", "6", fieldName, "0.1,0.1"));
    assertU(adoc("id", "7", fieldName, "-0.1,-0.1"));
    assertU(adoc("id", "8", fieldName, "0,179.9"));
    assertU(adoc("id", "9", fieldName, "0,-179.9"));
    assertU(adoc("id", "10", fieldName, "89.9,50"));
    assertU(adoc("id", "11", fieldName, "89.9,-130"));
    assertU(adoc("id", "12", fieldName, "-89.9,50"));
    assertU(adoc("id", "13", fieldName, "-89.9,-130"));
    assertU(commit());
  }

  @Test
  public void testIntersectFilter() throws Exception {
    setupDocs();
    //Try some edge cases
    checkHits(fieldName, "1,1", 175, 3, 5, 6, 7);
    checkHits(fieldName, "0,179.8", 200, 2, 8, 9);
    checkHits(fieldName, "89.8, 50", 200, 2, 10, 11);//this goes over the north pole
    checkHits(fieldName, "-89.8, 50", 200, 2, 12, 13);//this goes over the south pole
    //try some normal cases
    checkHits(fieldName, "33.0,-80.0", 300, 2);
    //large distance
    checkHits(fieldName, "1,1", 5000, 3, 5, 6, 7);
    //Because we are generating a box based on the west/east longitudes and the south/north latitudes, which then
    //translates to a range query, which is slightly more inclusive.  Thus, even though 0.0 is 15.725 kms away,
    //it will be included, b/c of the box calculation.
    checkHits(fieldName, false, "0.1,0.1", 15, 2, 5, 6);

    //try some more
    clearIndex();
    assertU(adoc("id", "14", fieldName, "0,5"));
    assertU(adoc("id", "15", fieldName, "0,15"));
    //3000KM from 0,0, see http://www.movable-type.co.uk/scripts/latlong.html
    assertU(adoc("id", "16", fieldName, "18.71111,19.79750"));
    assertU(adoc("id", "17", fieldName, "44.043900,-95.436643"));
    assertU(commit());

    checkHits(fieldName, "0,0", 1000, 1, 14);
    checkHits(fieldName, "0,0", 2000, 2, 14, 15);
    checkHits(fieldName, false, "0,0", 3000, 3, 14, 15, 16);
    checkHits(fieldName, "0,0", 3001, 3, 14, 15, 16);
    checkHits(fieldName, "0,0", 3000.1, 3, 14, 15, 16);

    //really fine grained distance and reflects some of the vagaries of how we are calculating the box
    checkHits(fieldName, "43.517030,-96.789603", 109, 0);

    //falls outside of the real distance, but inside the bounding box
    checkHits(fieldName, true,  "43.517030,-96.789603", 110, 0);
    checkHits(fieldName, false, "43.517030,-96.789603", 110, 1, 17);
  }

  @Test
  public void checkResultFormat() throws Exception {
    //Check input and output format is the same
    String IN = "89.9,-130";//lat,lon
    String OUT = IN;//IDENTICAL!

    assertU(adoc("id", "11", fieldName, IN));
    assertU(commit());

    assertQ(req(
        "fl", "id," + fieldName, "q", "*:*", "rows", "1000",
        "fq", "{!field needScore=false f="+fieldName+"}Intersects(Circle(89.9,-130 d=9))"),
        "//result/doc/*[@name='" + fieldName + "']//text()='" + OUT + "'");
  }

  @Test
  public void checkQueryEmptyIndex() {
    checkHits(fieldName, "0,0", 100, 0);//doesn't error
  }

  private void checkHits(String fieldName, String pt, double distKM, int count, int ... docIds) {
    checkHits(fieldName, true, pt, distKM, count, docIds);
  }

  private void checkHits(String fieldName, boolean exact, String ptStr, double distKM, int count, int ... docIds) {
    String [] tests = new String[docIds != null && docIds.length > 0 ? docIds.length + 1 : 1];
    //test for presence of required ids first
    int i = 0;
    if (docIds != null && docIds.length > 0) {
      for (int docId : docIds) {
        tests[i++] = "//result/doc/*[@name='id'][.='" + docId + "']";
      }
    }
    //check total length last; maybe response includes ids it shouldn't.  Nicer to check this last instead of first so
    // that there may be a more specific detailed id to investigate.
    tests[i++] = "*[count(//doc)=" + count + "]";

    //never actually need the score but lets test
    String score = new String[]{null, "none","distance","recipDistance"}[random().nextInt(4)];

    double distDEG = DistanceUtils.dist2Degrees(distKM, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    String circleStr = "Circle(" + ptStr.replaceAll(" ", "") + " d=" + distDEG + ")";
    String shapeStr;
    if (exact) {
      shapeStr = circleStr;
    } else {//bbox
      //the GEO is an assumption
      SpatialContext ctx = SpatialContext.GEO;
      shapeStr = ctx.toString( ctx.readShape(circleStr).getBoundingBox() );
    }

    //FYI default distErrPct=0.025 works with the tests in this file
    assertQ(req(
          "fl", "id", "q","*:*", "rows", "1000",
          "fq", "{!field f=" + fieldName + (score==null?"":" score="+score)
            + "}Intersects(" + shapeStr + ")"),
        tests);
  }

  @Test
  public void testRangeSyntax() {
    setupDocs();
    //match docId 1
    int docId = 1;
    int count = 1;
    boolean needScore = random().nextBoolean();//never actually need the score but lets test
    assertQ(req(
        "fl", "id", "q","*:*", "rows", "1000",
        "fq", "{! needScore="+needScore+" df="+fieldName+"}[32,-80 TO 33,-79]"),//lower-left to upper-right

        "//result/doc/*[@name='id'][.='" + docId + "']",
        "*[count(//doc)=" + count + "]");
  }

  @Test
  public void testSort() throws Exception {
    assertU(adoc("id", "100", fieldName, "1,2"));
    assertU(adoc("id", "101", fieldName, "4,-1"));
    assertU(adoc("id", "999", fieldName, "70,70"));//far away from these queries
    assertU(commit());

    //test absence of score=distance means it doesn't score
    assertJQ(req(
        "q", fieldName +":\"Intersects(Circle(3,4 d=9))\"",
        "fl","id,score")
        , 1e-9
        , "/response/docs/[0]/score==1.0"
        , "/response/docs/[1]/score==1.0"
    );

    //score by distance
    assertJQ(req(
        "q", "{! score=distance}"+fieldName +":\"Intersects(Circle(3,4 d=9))\"",
        "fl","id,score",
        "sort","score asc")//want ascending due to increasing distance
        , 1e-3
        , "/response/docs/[0]/id=='100'"
        , "/response/docs/[0]/score==2.827493"
        , "/response/docs/[1]/id=='101'"
        , "/response/docs/[1]/score==5.089807"
    );
    //score by recipDistance
    assertJQ(req(
        "q", "{! score=recipDistance}"+fieldName +":\"Intersects(Circle(3,4 d=9))\"",
        "fl","id,score",
        "sort","score desc")//want descending
        , 1e-3
        , "/response/docs/[0]/id=='100'"
        , "/response/docs/[0]/score==0.3099695"
        , "/response/docs/[1]/id=='101'"
        , "/response/docs/[1]/score==0.19970943"
    );

    //query again with the query point closer to #101, and check the new ordering
    assertJQ(req(
        "q", "{! score=distance}"+fieldName +":\"Intersects(Circle(4,0 d=9))\"",
        "fl","id,score",
        "sort","score asc")//want ascending due to increasing distance
        , 1e-4
        , "/response/docs/[0]/id=='101'"
        , "/response/docs/[1]/id=='100'"
    );

    //use sort=query(...)
    assertJQ(req(
        "q","-id:999",//exclude that doc
        "fl","id,score",
        "sort","query($sortQuery) asc", //want ascending due to increasing distance
        "sortQuery", "{! score=distance}"+fieldName +":\"Intersects(Circle(3,4 d=9))\"" )
        , 1e-4
        , "/response/docs/[0]/id=='100'"
        , "/response/docs/[1]/id=='101'"  );

    //check reversed direction with query point closer to #101
    assertJQ(req(
        "q","-id:999",//exclude that doc
        "fl","id,score",
        "sort","query($sortQuery) asc", //want ascending due to increasing distance
        "sortQuery", "{! score=distance}"+fieldName +":\"Intersects(Circle(4,0 d=9))\"" )
        , 1e-4
        , "/response/docs/[0]/id=='101'"
        , "/response/docs/[1]/id=='100'"  );
  }

  @Test
  public void testSortMultiVal() throws Exception {
    RandomizedTest.assumeFalse("Multivalue not supported for this field", fieldName.equals("pointvector"));

    assertU(adoc("id", "100", fieldName, "1,2"));//1 point
    assertU(adoc("id", "101", fieldName, "4,-1", fieldName, "3,5"));//2 points, 2nd is pretty close to query point
    assertU(commit());

    assertJQ(req(
        "q", "{! score=distance}"+fieldName +":\"Intersects(Circle(3,4 d=9))\"",
        "fl","id,score",
        "sort","score asc")//want ascending due to increasing distance
        , 1e-4
        , "/response/docs/[0]/id=='101'"
        , "/response/docs/[0]/score==0.99862987"//dist to 3,5
    );
  }

}
