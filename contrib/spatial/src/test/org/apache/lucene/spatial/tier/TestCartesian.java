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

import junit.framework.TestCase;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.function.CustomScoreQuery;
import org.apache.lucene.search.function.FieldScoreQuery;
import org.apache.lucene.search.function.FieldScoreQuery.Type;
import org.apache.lucene.spatial.NumberUtils;
import org.apache.lucene.spatial.geohash.GeoHashUtils;
import org.apache.lucene.spatial.tier.projections.CartesianTierPlotter;
import org.apache.lucene.spatial.tier.projections.IProjector;
import org.apache.lucene.spatial.tier.projections.SinusoidalProjector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;


/**
 *
 */
public class TestCartesian extends TestCase{

  /**
   * @param args
   */
  
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
  


  protected void setUp() throws IOException {
    directory = new RAMDirectory();

    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true);
    
    setUpPlotter( 2, 15);
    
    addData(writer);
    
  }
  
  
  private void setUpPlotter(int base, int top) {
    
    for (; base <= top; base ++){
      ctps.add(new CartesianTierPlotter(base,project,
          CartesianTierPlotter.DEFALT_FIELD_PREFIX));
    }
  }
  
  private void addPoint(IndexWriter writer, String name, double lat, double lng) throws IOException{
    
    Document doc = new Document();
    
    doc.add(new Field("name", name,Field.Store.YES, Field.Index.TOKENIZED));
    
    // convert the lat / long to lucene fields
    doc.add(new Field(latField, NumberUtils.double2sortableStr(lat),Field.Store.YES, Field.Index.UN_TOKENIZED));
    doc.add(new Field(lngField, NumberUtils.double2sortableStr(lng),Field.Store.YES, Field.Index.UN_TOKENIZED));
    
    // add a default meta field to make searching all documents easy 
    doc.add(new Field("metafile", "doc",Field.Store.YES, Field.Index.TOKENIZED));
    
    int ctpsize = ctps.size();
    for (int i =0; i < ctpsize; i++){
      CartesianTierPlotter ctp = ctps.get(i);
      doc.add(new Field(ctp.getTierFieldName(), 
          NumberUtils.double2sortableStr(ctp.getTierBoxId(lat,lng)),
          Field.Store.YES, 
          Field.Index.NO_NORMS));
      
      doc.add(new Field(geoHashPrefix, GeoHashUtils.encode(lat,lng), 
    		  Field.Store.YES, 
    		  Field.Index.NO_NORMS));
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
   
    writer.commit();
    writer.close();
  }
  
  public void testRange() throws IOException, InvalidGeoException {
    searcher = new IndexSearcher(directory);
    
    final double miles = 6.0;
    
    // create a distance query
    final DistanceQueryBuilder dq = new DistanceQueryBuilder(lat, lng, miles, 
        latField, lngField, CartesianTierPlotter.DEFALT_FIELD_PREFIX, true);
     
    System.out.println(dq);
    //create a term query to search against all documents
    Query tq = new TermQuery(new Term("metafile", "doc"));
    
    FieldScoreQuery fsQuery = new FieldScoreQuery("geo_distance", Type.FLOAT);
    
    CustomScoreQuery customScore = new CustomScoreQuery(dq.getQuery(tq),fsQuery){
      
      @Override
      public float customScore(int doc, float subQueryScore, float valSrcScore){
        //System.out.println(doc);
        if (dq.distanceFilter.getDistance(doc) == null)
          return 0;
        
        double distance = dq.distanceFilter.getDistance(doc);
        // boost score shouldn't exceed 1
        if (distance < 1.0d)
          distance = 1.0d;
        //boost by distance is invertly proportional to
        // to distance from center point to location
        float score = new Float((miles - distance) / miles ).floatValue();
        return score * subQueryScore;
      }
    };
    // Create a distance sort
    // As the radius filter has performed the distance calculations
    // already, pass in the filter to reuse the results.
    // 
    DistanceFieldComparatorSource dsort = new DistanceFieldComparatorSource(dq.distanceFilter);
    Sort sort = new Sort(new SortField("foo", dsort,false));
    
    // Perform the search, using the term query, the serial chain filter, and the
    // distance sort
    Hits hits = searcher.search(customScore,null,sort);

    int results = hits.length();
    
    // Get a list of distances 
    Map<Integer,Double> distances = dq.distanceFilter.getDistances();
    
    // distances calculated from filter first pass must be less than total
    // docs, from the above test of 20 items, 12 will come from the boundary box
    // filter, but only 5 are actually in the radius of the results.
    
    // Note Boundary Box filtering, is not accurate enough for most systems.
    
    
    System.out.println("Distance Filter filtered: " + distances.size());
    System.out.println("Results: " + results);
    System.out.println("=============================");
    System.out.println("Distances should be 14 "+ distances.size());
    System.out.println("Results should be 7 "+ results);

    assertEquals(14, distances.size());
    assertEquals(7, results);
    double lastDistance = 0;
    for(int i =0 ; i < results; i++){
      Document d = hits.doc(i);
      
      String name = d.get("name");
      double rsLat = NumberUtils.SortableStr2double(d.get(latField));
      double rsLng = NumberUtils.SortableStr2double(d.get(lngField)); 
      Double geo_distance = distances.get(hits.id(i));
      
      double distance = DistanceUtils.getInstance().getDistanceMi(lat, lng, rsLat, rsLng);
      double llm = DistanceUtils.getInstance().getLLMDistance(lat, lng, rsLat, rsLng);
      System.out.println("Name: "+ name +", Distance "+ distance); //(res, ortho, harvesine):"+ distance +" |"+ geo_distance +"|"+ llm +" | score "+ hits.score(i));
      assertTrue(Math.abs((distance - llm)) < 1);
      assertTrue((distance < miles ));
      assertTrue(geo_distance > lastDistance);
      lastDistance = geo_distance;
    }
  }
  
  
  
  public void testGeoHashRange() throws IOException, InvalidGeoException {
	    searcher = new IndexSearcher(directory);
	    
	    final double miles = 6.0;
	    
	    // create a distance query
	    final DistanceQueryBuilder dq = new DistanceQueryBuilder(lat, lng, miles, 
	        geoHashPrefix, CartesianTierPlotter.DEFALT_FIELD_PREFIX, true);
	     
	    System.out.println(dq);
	    //create a term query to search against all documents
	    Query tq = new TermQuery(new Term("metafile", "doc"));
	    
	    FieldScoreQuery fsQuery = new FieldScoreQuery("geo_distance", Type.FLOAT);
	    CustomScoreQuery customScore = new CustomScoreQuery(tq,fsQuery){
	      
	      @Override
	      public float customScore(int doc, float subQueryScore, float valSrcScore){
	        //System.out.println(doc);
	        if (dq.distanceFilter.getDistance(doc) == null)
	          return 0;
	        
	        double distance = dq.distanceFilter.getDistance(doc);
	        // boost score shouldn't exceed 1
	        if (distance < 1.0d)
	          distance = 1.0d;
	        //boost by distance is invertly proportional to
	        // to distance from center point to location
	        float score = new Float((miles - distance) / miles ).floatValue();
	        return score * subQueryScore;
	      }
	    };
	    // Create a distance sort
	    // As the radius filter has performed the distance calculations
	    // already, pass in the filter to reuse the results.
	    // 
	    DistanceSortSource dsort = new DistanceSortSource(dq.distanceFilter);
	    Sort sort = new Sort(new SortField("foo", dsort));
	    
	    // Perform the search, using the term query, the serial chain filter, and the
	    // distance sort
	    Hits hits = searcher.search(customScore, dq.getFilter()); //,sort);

	    int results = hits.length();
	    
	    // Get a list of distances 
	    Map<Integer,Double> distances = dq.distanceFilter.getDistances();
	    
	    // distances calculated from filter first pass must be less than total
	    // docs, from the above test of 20 items, 12 will come from the boundary box
	    // filter, but only 5 are actually in the radius of the results.
	    
	    // Note Boundary Box filtering, is not accurate enough for most systems.
	    
	    
	    System.out.println("Distance Filter filtered: " + distances.size());
	    System.out.println("Results: " + results);
	    System.out.println("=============================");
	    System.out.println("Distances should be 14 "+ distances.size());
	    System.out.println("Results should be 7 "+ results);

	    assertEquals(14, distances.size());
	    assertEquals(7, results);
	    
	    for(int i =0 ; i < results; i++){
	      Document d = hits.doc(i);
	      
	      String name = d.get("name");
	      double rsLat = NumberUtils.SortableStr2double(d.get(latField));
	      double rsLng = NumberUtils.SortableStr2double(d.get(lngField)); 
	      Double geo_distance = distances.get(hits.id(i));
	      
	      double distance = DistanceUtils.getInstance().getDistanceMi(lat, lng, rsLat, rsLng);
	      double llm = DistanceUtils.getInstance().getLLMDistance(lat, lng, rsLat, rsLng);
	      System.out.println("Name: "+ name +", Distance (res, ortho, harvesine):"+ distance +" |"+ geo_distance +"|"+ llm +" | score "+ hits.score(i));
	      assertTrue(Math.abs((distance - llm)) < 1);
	      assertTrue((distance < miles ));
	      
	    }
	  }
  
}
