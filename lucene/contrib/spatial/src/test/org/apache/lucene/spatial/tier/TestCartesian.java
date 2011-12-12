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
package org.apache.lucene.spatial.tier;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.spatial.DistanceUtils;
import org.apache.lucene.spatial.geohash.GeoHashUtils;
import org.apache.lucene.spatial.geometry.DistanceUnits;
import org.apache.lucene.spatial.geometry.FloatLatLng;
import org.apache.lucene.spatial.geometry.LatLng;
import org.apache.lucene.spatial.tier.projections.CartesianTierPlotter;
import org.apache.lucene.spatial.tier.projections.IProjector;
import org.apache.lucene.spatial.tier.projections.SinusoidalProjector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestCartesian extends LuceneTestCase {

  private Directory directory;
  private IndexSearcher searcher;
  // reston va
  private double lat = 38.969398; 
  private double lng= -77.386398;
  private String latField = "lat";
  private String lngField = "lng";
  private List<CartesianTierPlotter> ctps = new LinkedList<CartesianTierPlotter>();
  private String geoHashPrefix = "_geoHash_";
  
  private IProjector project = new SinusoidalProjector();
  


  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();

    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    
    setUpPlotter( 2, 15);
    
    addData(writer);
    
  }
  
  @Override
  public void tearDown() throws Exception {
    directory.close();
    super.tearDown();
  }
  
  
  private void setUpPlotter(int base, int top) {
    
    for (; base <= top; base ++){
      ctps.add(new CartesianTierPlotter(base,project,
          CartesianTierPlotter.DEFALT_FIELD_PREFIX));
    }
  }
  
  private void addPoint(IndexWriter writer, String name, double lat, double lng) throws IOException{
    
    Document doc = new Document();

    doc.add(newField("name", name, TextField.TYPE_STORED));
    
    // convert the lat / long to lucene fields
    doc.add(new NumericField(latField, Integer.MAX_VALUE, NumericField.TYPE_STORED).setDoubleValue(lat));
    doc.add(new NumericField(lngField, Integer.MAX_VALUE, NumericField.TYPE_STORED).setDoubleValue(lng));
    
    // add a default meta field to make searching all documents easy 
    doc.add(newField("metafile", "doc", TextField.TYPE_STORED));
    
    int ctpsize = ctps.size();
    for (int i =0; i < ctpsize; i++){
      CartesianTierPlotter ctp = ctps.get(i);
      doc.add(new NumericField(ctp.getTierFieldName(), Integer.MAX_VALUE, TextField.TYPE_STORED).setDoubleValue(ctp.getTierBoxId(lat,lng)));
      
      doc.add(newField(geoHashPrefix, GeoHashUtils.encode(lat,lng), StringField.TYPE_STORED));
    }
    writer.addDocument(doc);
    
  }
  
  
  
  private void addData(IndexWriter writer) throws IOException {
    addPoint(writer,"McCormick &amp; Schmick's Seafood Restaurant",38.9579000,-77.3572000);
    addPoint(writer,"Jimmy's Old Town Tavern",38.9690000,-77.3862000);
    addPoint(writer,"Ned Devine's",38.9510000,-77.4107000);
    addPoint(writer,"Old Brogue Irish Pub",38.9955000,-77.2884000);
    addPoint(writer,"Alf Laylah Wa Laylah",38.8956000,-77.4258000);
    addPoint(writer,"Sully's Restaurant &amp; Supper",38.9003000,-77.4467000);
    addPoint(writer,"TGIFriday",38.8725000,-77.3829000);
    addPoint(writer,"Potomac Swing Dance Club",38.9027000,-77.2639000);
    addPoint(writer,"White Tiger Restaurant",38.9027000,-77.2638000);
    addPoint(writer,"Jammin' Java",38.9039000,-77.2622000);
    addPoint(writer,"Potomac Swing Dance Club",38.9027000,-77.2639000);
    addPoint(writer,"WiseAcres Comedy Club",38.9248000,-77.2344000);
    addPoint(writer,"Glen Echo Spanish Ballroom",38.9691000,-77.1400000);
    addPoint(writer,"Whitlow's on Wilson",38.8889000,-77.0926000);
    addPoint(writer,"Iota Club and Cafe",38.8890000,-77.0923000);
    addPoint(writer,"Hilton Washington Embassy Row",38.9103000,-77.0451000);
    addPoint(writer,"HorseFeathers, Bar & Grill", 39.01220000000001, -77.3942);
    addPoint(writer,"Marshall Island Airfield",7.06, 171.2);
    addPoint(writer, "Wonga Wongue Reserve, Gabon", -0.546562,9.459229);
    addPoint(writer,"Midway Island",25.7, -171.7);
    addPoint(writer,"North Pole Way",55.0, 4.0);
   
    writer.commit();
    // TODO: fix CustomScoreQuery usage in testRange/testGeoHashRange so we don't need this.
    writer.forceMerge(1);
    writer.close();
  }


  public void testDistances() throws IOException, InvalidGeoException {
    LatLng p1 = new FloatLatLng( 7.06, 171.2 );
    LatLng p2 = new FloatLatLng( 21.6032207, -158.0 );
    double miles = p1.arcDistance( p2, DistanceUnits.MILES );
    if (VERBOSE) {
      System.out.println("testDistances");
      System.out.println("miles:" + miles);
    }
    assertEquals(2288.82495932794, miles, 0.001);
    LatLng p3 = new FloatLatLng( 41.6032207, -73.087749);
    LatLng p4 = new FloatLatLng( 55.0, 4.0 );
    miles = p3.arcDistance( p4, DistanceUnits.MILES );
    if (VERBOSE) System.out.println("miles:" + miles);
    assertEquals(3474.331719997617, miles, 0.001);
  }

  /*public void testCartesianPolyFilterBuilder() throws Exception {
    CartesianPolyFilterBuilder cpfb = new CartesianPolyFilterBuilder(CartesianTierPlotter.DEFALT_FIELD_PREFIX, 2, 15);
    //try out some shapes
    final double miles = 20.0;
        // Hawaii
        // 2300 miles to Marshall Island Airfield
    //Hawaii to Midway is 911 miles
    lat = 0;
    lng = -179.9;
    Shape shape;
    shape = cpfb.getBoxShape(lat, lng, miles);
    System.out.println("Tier: " + shape.getTierLevel());
    System.out.println("area: " + shape.getArea().size());
    lat = 30;
    lng = -100;
    shape = cpfb.getBoxShape(lat, lng, miles);
    System.out.println("Tier: " + shape.getTierLevel());
    System.out.println("area: " + shape.getArea().size());

    lat = 30;
    lng = 100;
    shape = cpfb.getBoxShape(lat, lng, miles);
    System.out.println("Tier: " + shape.getTierLevel());
    System.out.println("area: " + shape.getArea().size());
  }
*/


  public void testAntiM() throws IOException, InvalidGeoException {
    IndexReader reader = IndexReader.open(directory);
    searcher = new IndexSearcher(reader);

    final double miles = 2800.0;
        // Hawaii
        // 2300 miles to Marshall Island Airfield
    //Hawaii to Midway is 911 miles
    lat = 21.6032207;
    lng = -158.0;

    if (VERBOSE) System.out.println("testAntiM");
    // create a distance query
    final DistanceQueryBuilder dq = new DistanceQueryBuilder(lat, lng, miles,
        latField, lngField, CartesianTierPlotter.DEFALT_FIELD_PREFIX, true, 2, 15);

    if (VERBOSE) System.out.println(dq);
    //create a term query to search against all documents
    Query tq = new TermQuery(new Term("metafile", "doc"));
    // Create a distance sort
    // As the radius filter has performed the distance calculations
    // already, pass in the filter to reuse the results.
    //
    DistanceFieldComparatorSource dsort = new DistanceFieldComparatorSource(dq.distanceFilter);
    Sort sort = new Sort(new SortField("foo", dsort,false));

    // Perform the search, using the term query, the serial chain filter, and the
    // distance sort
    TopDocs hits = searcher.search(dq.getQuery(tq),null, 1000, sort);
    int results = hits.totalHits;
    ScoreDoc[] scoreDocs = hits.scoreDocs; 
    
    // Get a list of distances
    Map<Integer,Double> distances = dq.distanceFilter.getDistances();

    // distances calculated from filter first pass must be less than total
    // docs, from the above test of 20 items, 12 will come from the boundary box
    // filter, but only 5 are actually in the radius of the results.

    // Note Boundary Box filtering, is not accurate enough for most systems.


    if (VERBOSE) {
      System.out.println("Distance Filter filtered: " + distances.size());
      System.out.println("Results: " + results);
      System.out.println("=============================");
      System.out.println("Distances should be 2 "+ distances.size());
      System.out.println("Results should be 2 "+ results);
    }

    assertEquals(2, distances.size()); // fixed a store of only needed distances
    assertEquals(2, results);
    double lastDistance = 0;
    for(int i =0 ; i < results; i++){
      Document d = searcher.doc(scoreDocs[i].doc);

      String name = d.get("name");
      double rsLat = Double.parseDouble(d.get(latField));
      double rsLng = Double.parseDouble(d.get(lngField));
      Double geo_distance = distances.get(scoreDocs[i].doc);

      double distance = DistanceUtils.getDistanceMi(lat, lng, rsLat, rsLng);
      double llm = DistanceUtils.getLLMDistance(lat, lng, rsLat, rsLng);
      if (VERBOSE) System.out.println("Name: "+ name +", Distance "+ distance); //(res, ortho, harvesine):"+ distance +" |"+ geo_distance +"|"+ llm +" | score "+ hits.score(i));
      assertTrue(Math.abs((distance - llm)) < 1);
      assertTrue((distance < miles ));
      assertTrue(geo_distance >= lastDistance);
      lastDistance = geo_distance;
    }
    reader.close();
  }

  public void testPoleFlipping() throws IOException, InvalidGeoException {
    IndexReader reader = IndexReader.open(directory);
    searcher = new IndexSearcher(reader);

    final double miles = 3500.0;
    lat = 41.6032207;
    lng = -73.087749;

    if (VERBOSE) System.out.println("testPoleFlipping");

    // create a distance query
    final DistanceQueryBuilder dq = new DistanceQueryBuilder(lat, lng, miles,
        latField, lngField, CartesianTierPlotter.DEFALT_FIELD_PREFIX, true, 2, 15);

    if (VERBOSE) System.out.println(dq);
    //create a term query to search against all documents
    Query tq = new TermQuery(new Term("metafile", "doc"));
    // Create a distance sort
    // As the radius filter has performed the distance calculations
    // already, pass in the filter to reuse the results.
    //
    DistanceFieldComparatorSource dsort = new DistanceFieldComparatorSource(dq.distanceFilter);
    Sort sort = new Sort(new SortField("foo", dsort,false));

    // Perform the search, using the term query, the serial chain filter, and the
    // distance sort
    TopDocs hits = searcher.search(dq.getQuery(tq),null, 1000, sort);
    int results = hits.totalHits;
    ScoreDoc[] scoreDocs = hits.scoreDocs; 

    // Get a list of distances
    Map<Integer,Double> distances = dq.distanceFilter.getDistances();

    // distances calculated from filter first pass must be less than total
    // docs, from the above test of 20 items, 12 will come from the boundary box
    // filter, but only 5 are actually in the radius of the results.

    // Note Boundary Box filtering, is not accurate enough for most systems.


    if (VERBOSE) {
      System.out.println("Distance Filter filtered: " + distances.size());
      System.out.println("Results: " + results);
      System.out.println("=============================");
      System.out.println("Distances should be 18 "+ distances.size());
      System.out.println("Results should be 18 "+ results);
    }

    assertEquals(18, distances.size()); // fixed a store of only needed distances
    assertEquals(18, results);
    double lastDistance = 0;
    for(int i =0 ; i < results; i++){
      Document d = searcher.doc(scoreDocs[i].doc);
      String name = d.get("name");
      double rsLat = Double.parseDouble(d.get(latField));
      double rsLng = Double.parseDouble(d.get(lngField));
      Double geo_distance = distances.get(scoreDocs[i].doc);

      double distance = DistanceUtils.getDistanceMi(lat, lng, rsLat, rsLng);
      double llm = DistanceUtils.getLLMDistance(lat, lng, rsLat, rsLng);
      if (VERBOSE) System.out.println("Name: "+ name +", Distance "+ distance); //(res, ortho, harvesine):"+ distance +" |"+ geo_distance +"|"+ llm +" | score "+ hits.score(i));
      assertTrue(Math.abs((distance - llm)) < 1);
      if (VERBOSE) System.out.println("checking limit "+ distance + " < " + miles);
      assertTrue((distance < miles ));
      if (VERBOSE) System.out.println("checking sort "+ geo_distance + " >= " + lastDistance);
      assertTrue(geo_distance >= lastDistance);
      lastDistance = geo_distance;
    }
    reader.close();
  }
  
  public void testRange() throws IOException, InvalidGeoException {
    IndexReader reader = IndexReader.open(directory);
    searcher = new IndexSearcher(reader);

    final double[] milesToTest = new double[] {6.0, 0.5, 0.001, 0.0};
    final int[] expected = new int[] {7, 1, 0, 0};

    for(int x=0;x<expected.length;x++) {
    
      final double miles = milesToTest[x];
    
      // create a distance query
      final DistanceQueryBuilder dq = new DistanceQueryBuilder(lat, lng, miles, 
                                                               latField, lngField, CartesianTierPlotter.DEFALT_FIELD_PREFIX, true, 2, 15);
     
      if (VERBOSE) System.out.println(dq);
      //create a term query to search against all documents
      Query tq = new TermQuery(new Term("metafile", "doc"));
      // Create a distance sort
      // As the radius filter has performed the distance calculations
      // already, pass in the filter to reuse the results.
      // 
      DistanceFieldComparatorSource dsort = new DistanceFieldComparatorSource(dq.distanceFilter);
      Sort sort = new Sort(new SortField("foo", dsort,false));
    
      // Perform the search, using the term query, the serial chain filter, and the
      // distance sort
      TopDocs hits = searcher.search(dq.getQuery(tq),null, 1000, sort);
      int results = hits.totalHits;
      ScoreDoc[] scoreDocs = hits.scoreDocs; 
    
      // Get a list of distances 
      Map<Integer,Double> distances = dq.distanceFilter.getDistances();
    
      // distances calculated from filter first pass must be less than total
      // docs, from the above test of 20 items, 12 will come from the boundary box
      // filter, but only 5 are actually in the radius of the results.
    
      // Note Boundary Box filtering, is not accurate enough for most systems.
    
      if (VERBOSE) {
        System.out.println("Distance Filter filtered: " + distances.size());
        System.out.println("Results: " + results);
        System.out.println("=============================");
        System.out.println("Distances should be 7 "+ expected[x] + ":" + distances.size());
        System.out.println("Results should be 7 "+ expected[x] + ":" + results);
      }

      assertEquals(expected[x], distances.size()); // fixed a store of only needed distances
      assertEquals(expected[x], results);
      double lastDistance = 0;
      for(int i =0 ; i < results; i++){
        Document d = searcher.doc(scoreDocs[i].doc);
      
        String name = d.get("name");
        double rsLat = Double.parseDouble(d.get(latField));
        double rsLng = Double.parseDouble(d.get(lngField)); 
        Double geo_distance = distances.get(scoreDocs[i].doc);
      
        double distance = DistanceUtils.getDistanceMi(lat, lng, rsLat, rsLng);
        double llm = DistanceUtils.getLLMDistance(lat, lng, rsLat, rsLng);
        if (VERBOSE) System.out.println("Name: "+ name +", Distance "+ distance); //(res, ortho, harvesine):"+ distance +" |"+ geo_distance +"|"+ llm +" | score "+ hits.score(i));
        assertTrue(Math.abs((distance - llm)) < 1);
        assertTrue((distance < miles ));
        assertTrue(geo_distance > lastDistance);
        lastDistance = geo_distance;
      }
    }
    reader.close();
  }
  
  
  
  public void testGeoHashRange() throws IOException, InvalidGeoException {
    IndexReader reader = IndexReader.open(directory);
    searcher = new IndexSearcher(reader);
	    
    final double[] milesToTest = new double[] {6.0, 0.5, 0.001, 0.0};
    final int[] expected = new int[] {7, 1, 0, 0};

    for(int x=0;x<expected.length;x++) {
      final double miles = milesToTest[x];
	    
      // create a distance query
      final DistanceQueryBuilder dq = new DistanceQueryBuilder(lat, lng, miles, 
                                                               geoHashPrefix, CartesianTierPlotter.DEFALT_FIELD_PREFIX, true, 2, 15);
	     
      if (VERBOSE) System.out.println(dq);
      //create a term query to search against all documents
      Query tq = new TermQuery(new Term("metafile", "doc"));
      // Create a distance sort
      // As the radius filter has performed the distance calculations
      // already, pass in the filter to reuse the results.
      // 
      //DistanceFieldComparatorSource dsort = new DistanceFieldComparatorSource(dq.distanceFilter);
      //Sort sort = new Sort(new SortField("foo", dsort));
	    
      // Perform the search, using the term query, the serial chain filter, and the
      // distance sort
      TopDocs hits = searcher.search(tq,dq.getFilter(), 1000); //,sort);
      int results = hits.totalHits;
      ScoreDoc[] scoreDocs = hits.scoreDocs; 
	    
      // Get a list of distances 
      Map<Integer,Double> distances = dq.distanceFilter.getDistances();
	    
      // distances calculated from filter first pass must be less than total
      // docs, from the above test of 20 items, 12 will come from the boundary box
      // filter, but only 5 are actually in the radius of the results.
	    
      // Note Boundary Box filtering, is not accurate enough for most systems.
	    
	    if (VERBOSE) {
        System.out.println("Distance Filter filtered: " + distances.size());
        System.out.println("Results: " + results);
        System.out.println("=============================");
        System.out.println("Distances should be 14 "+ expected[x] + ":" + distances.size());
        System.out.println("Results should be 7 "+ expected[x] + ":" + results);
      }

      assertEquals(expected[x], distances.size());
      assertEquals(expected[x], results);
	    
      for(int i =0 ; i < results; i++){
        Document d = searcher.doc(scoreDocs[i].doc);
	      
        String name = d.get("name");
        double rsLat = Double.parseDouble(d.get(latField));
        double rsLng = Double.parseDouble(d.get(lngField)); 
        Double geo_distance = distances.get(scoreDocs[i].doc);
	      
        double distance = DistanceUtils.getDistanceMi(lat, lng, rsLat, rsLng);
        double llm = DistanceUtils.getLLMDistance(lat, lng, rsLat, rsLng);
        if (VERBOSE) System.out.println("Name: "+ name +", Distance (res, ortho, harvesine):"+ distance +" |"+ geo_distance +"|"+ llm +" | score "+ scoreDocs[i].score);
        assertTrue(Math.abs((distance - llm)) < 1);
        assertTrue((distance < miles ));
	      
      }
    }
    reader.close();
  }
}
