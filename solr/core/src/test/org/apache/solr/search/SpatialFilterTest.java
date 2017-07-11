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
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 *
 *
 **/
public class SpatialFilterTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  private void setupDocs(String fieldName) {
    clearIndex();
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
  public void testPoints() throws Exception {
    String fieldName = "home";
    setupDocs(fieldName);
    //Try some edge cases
    checkHits(fieldName, "1,1", 100, 5, 3, 4, 5, 6, 7);
    checkHits(fieldName, "0,179.8", 200, 5, 3, 4, 8, 10, 12);
    checkHits(fieldName, "89.8, 50", 200, 9);
    //try some normal cases
    checkHits(fieldName, "33.0,-80.0", 300, 12);
    //large distance
    checkHits(fieldName, "33.0,-80.0", 5000, 13);
  }
  
  @Test
  public void testGeoHash() throws Exception {
    String fieldName = "home_gh";
    setupDocs(fieldName);
    //try some normal cases
    checkHits(fieldName, "33.0,-80.0", 300, 2, 1, 2);
    //large distance
    checkHits(fieldName, "33.0,-80.0", 5000, 2, 1, 2);
    //Try some edge cases
    checkHits(fieldName, "0,179.8", 200, 2);
    checkHits(fieldName, "1,1", 180, 3, 5, 6, 7);
    checkHits(fieldName, "89.8, 50", 200, 2);
    checkHits(fieldName, "-89.8, 50", 200, 2);//this goes over the south pole
  }

  @Test
  public void testLatLonType() throws Exception {
    String fieldName = "home_ll";
    setupDocs(fieldName);
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

    // falls outside of the real distance, but inside the bounding box   
    checkHits(fieldName, true, "43.517030,-96.789603", 110, 0);
    checkHits(fieldName, false, "43.517030,-96.789603", 110, 1, 17);
    
    
    // Tests SOLR-2829
    String fieldNameHome = "home_ll";
    String fieldNameWork = "work_ll";

    clearIndex();
    assertU(adoc("id", "1", fieldNameHome, "52.67,7.30", fieldNameWork,"48.60,11.61"));
    assertU(commit());

    checkHits(fieldNameHome, "52.67,7.30", 1, 1);
    checkHits(fieldNameWork, "48.60,11.61", 1, 1);
    checkHits(fieldNameWork, "52.67,7.30", 1, 0);
    checkHits(fieldNameHome, "48.60,11.61", 1, 0);

  }

  private void checkHits(String fieldName, String pt, double distance, int count, int ... docIds) {
    checkHits(fieldName, true, pt, distance, count, docIds);
  }

  private void checkHits(String fieldName, boolean exact, String pt, double distance, int count, int ... docIds) {
    String [] tests = new String[docIds != null && docIds.length > 0 ? docIds.length + 1 : 1];
    tests[0] = "*[count(//doc)=" + count + "]";
    if (docIds != null && docIds.length > 0) {
      int i = 1;
      for (int docId : docIds) {
        tests[i++] = "//result/doc/str[@name='id'][.='" + docId + "']";
      }
    }

    String method = exact ? "geofilt" : "bbox";
    int postFilterCount = DelegatingCollector.setLastDelegateCount;

    // throw in a random into the main query to prevent most cache hits
    assertQ(req("fl", "id", "q","*:* OR foo_i:" + random().nextInt(100), "rows", "1000", "fq", "{!"+method+" sfield=" +fieldName +"}",
              "pt", pt, "d", String.valueOf(distance)),
              tests);
    assertEquals(postFilterCount, DelegatingCollector.setLastDelegateCount);    // post filtering shouldn't be used
    
    // try uncached
    assertQ(req("fl", "id", "q","*:* OR foo_i:" + random().nextInt(100), "rows", "1000", "fq", "{!"+method+" sfield=" +fieldName + " cache=false" + "}",
        "pt", pt, "d", String.valueOf(distance)),
        tests);
    assertEquals(postFilterCount, DelegatingCollector.setLastDelegateCount);      // post filtering shouldn't be used

    // try post filtered for fields that support it
    if (fieldName.endsWith("ll")) {

    assertQ(req("fl", "id", "q","*:* OR foo_i:" + random().nextInt(100)+100, "rows", "1000", "fq", "{!"+method+" sfield=" +fieldName + " cache=false cost=150" + "}",
        "pt", pt, "d", String.valueOf(distance)),
        tests);
    assertEquals(postFilterCount + 1, DelegatingCollector.setLastDelegateCount);      // post filtering *should* have been used

    }
  }


}
 /*public void testSpatialQParser() throws Exception {
    ModifiableSolrParams local = new ModifiableSolrParams();
    local.add(CommonParams.FL, "home");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(SpatialParams.POINT, "5.0,5.0");
    params.add(SpatialParams.DISTANCE, "3");
    SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), "", "", 0, 10, new HashMap());
    SpatialFilterQParserPlugin parserPlugin;
    Query query;

    parserPlugin = new SpatialFilterQParserPlugin();
    QParser parser = parserPlugin.createParser("'foo'", local, params, req);
    query = parser.parse();
    assertNotNull("Query is null", query);
    assertTrue("query is not an instanceof "
            + BooleanQuery.class,
            query instanceof BooleanQuery);
    local = new ModifiableSolrParams();
    local.add(CommonParams.FL, "x");
    params = new ModifiableSolrParams();
    params.add(SpatialParams.POINT, "5.0");
    params.add(SpatialParams.DISTANCE, "3");
    req = new LocalSolrQueryRequest(h.getCore(), "", "", 0, 10, new HashMap());
    parser = parserPlugin.createParser("'foo'", local, params, req);
    query = parser.parse();
    assertNotNull("Query is null", query);
    assertTrue(query.getClass() + " is not an instanceof "
            + LegacyNumericRangeQuery.class,
            query instanceof LegacyNumericRangeQuery);
    req.close();
  }*/
