package org.apache.lucene.server;

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

import java.io.File;
import java.util.Locale;

import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/** Simple example of how to do distance drill-downs. */
public class TestSimpleSpatial extends ServerBaseTestCase {

  @BeforeClass
  public static void init() throws Exception {
    useDefaultIndex = false;
    curIndexName = "spatial";
    startServer();
    createAndStartIndex();
  }

  @AfterClass
  public static void fini() throws Exception {
    shutdownServer();
  }

  public void test() throws Exception {

    // Record latitude and longitude as double fields:
    send("registerFields",
         "{fields: {latitude: {type: double, storeDocValues: true}, longitude: {type: double, storeDocValues: true}}}");

    // Index 3 documents each with its own location:
    send("addDocument", "{fields: {latitude: 40.759011, longitude: -73.9844722}}");
    send("addDocument", "{fields: {latitude: 40.718266, longitude: -74.007819}}");
    send("addDocument", "{fields: {latitude: 40.7051157, longitude: -74.0088305}}");

    // Search, matching all documents and counting distance
    // facets from the home origin:
    double homeLatitude = 40.7143528;
    double homeLongitude = -74.0059731;
    send("search",
         "{query: MatchAllDocsQuery, " +
         "virtualFields: [{name: distance, expression: 'haversin(" + homeLatitude + "," + homeLongitude + ",latitude,longitude)'}], " +
         "facets: [{dim: distance, numericRanges: [" + 
         "{label: '< 1 km', min: 0.0, minInclusive: true, max: 1.0, maxInclusive: false}," +
         "{label: '< 2 km', min: 0.0, minInclusive: true, max: 2.0, maxInclusive: false}," +
         "{label: '< 5 km', min: 0.0, minInclusive: true, max: 5.0, maxInclusive: false}," +
         "{label: '< 10 km', min: 0.0, minInclusive: true, max: 10.0, maxInclusive: false}," +
         "]}]}");

    // Search, matching all documents and counting distance
    // facets from the home origin:
    assertEquals(3, getInt("totalHits"));
    assertEquals("top: 3, < 1 km: 1, < 2 km: 2, < 5 km: 2, < 10 km: 3",
                 TestFacets.formatFacetCounts(getObject("facets[0]")));

    // Now drill-down on '< 2 KM':
    send("search",
         "{query: MatchAllDocsQuery, " +
         "drillDowns: [{field: distance, numericRange: {label: '< 2 KM', min: 0.0, minInclusive: true, max: 2.0, maxInclusive: false}}], " +
         "virtualFields: [{name: distance, expression: 'haversin(" + homeLatitude + "," + homeLongitude + ",latitude,longitude)'}], " +
         "facets: [{dim: distance, numericRanges: [" + 
         "{label: '< 1 km', min: 0.0, minInclusive: true, max: 1.0, maxInclusive: false}," +
         "{label: '< 2 km', min: 0.0, minInclusive: true, max: 2.0, maxInclusive: false}," +
         "{label: '< 5 km', min: 0.0, minInclusive: true, max: 5.0, maxInclusive: false}," +
         "{label: '< 10 km', min: 0.0, minInclusive: true, max: 10.0, maxInclusive: false}," +
         "]}]}");

    assertEquals(2, getInt("totalHits"));

    // Drill-sideways counts are unchanged after a single drill-down:
    assertEquals("top: 3, < 1 km: 1, < 2 km: 2, < 5 km: 2, < 10 km: 3",
                 TestFacets.formatFacetCounts(getObject("facets[0]")));
  }
}

