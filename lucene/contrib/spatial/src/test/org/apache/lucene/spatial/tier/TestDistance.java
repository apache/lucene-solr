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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.store.Directory;


public class TestDistance extends LuceneTestCase {
  
  private Directory directory;
  // reston va
  private double lat = 38.969398; 
  private double lng= -77.386398;
  private String latField = "lat";
  private String lngField = "lng";
  private IndexWriter writer;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    writer = new IndexWriter(directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    addData(writer);
    
  }

  @Override
  public void tearDown() throws Exception {
    writer.close();
    directory.close();
    super.tearDown();
  }
  
  private void addPoint(IndexWriter writer, String name, double lat, double lng) throws IOException{
    
    Document doc = new Document();

    doc.add(newField("name", name, TextField.TYPE_STORED));
    
    // convert the lat / long to lucene fields
    doc.add(new NumericField(latField, Integer.MAX_VALUE, NumericField.TYPE_STORED).setDoubleValue(lat));
    doc.add(new NumericField(lngField, Integer.MAX_VALUE, NumericField.TYPE_STORED).setDoubleValue(lng));
    
    // add a default meta field to make searching all documents easy 
    doc.add(newField("metafile", "doc", TextField.TYPE_STORED));
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
  }

  public void testLatLongFilterOnDeletedDocs() throws Exception {
    writer.deleteDocuments(new Term("name", "Potomac"));
    IndexReader r = IndexReader.open(writer, true);
    LatLongDistanceFilter f = new LatLongDistanceFilter(new QueryWrapperFilter(new MatchAllDocsQuery()),
                                                        lat, lng, 1.0, latField, lngField);

    AtomicReaderContext[] leaves = ReaderUtil.leaves(r.getTopReaderContext());
    for (int i = 0; i < leaves.length; i++) {
      f.getDocIdSet(leaves[i], leaves[i].reader.getLiveDocs());
    }
    r.close();
  }
 
  /* these tests do not test anything, as no assertions:
  public void testMiles() {
    double LLM = DistanceUtils.getInstance().getLLMDistance(lat, lng,39.012200001, -77.3942);
    System.out.println(LLM);
    System.out.println("-->"+DistanceUtils.getInstance().getDistanceMi(lat, lng, 39.0122, -77.3942));
  }
  
  public void testMiles2(){
    System.out.println("Test Miles 2");
    double LLM = DistanceUtils.getInstance().getLLMDistance(44.30073, -78.32131,43.687267, -79.39842);
    System.out.println(LLM);
    System.out.println("-->"+DistanceUtils.getInstance().getDistanceMi(44.30073, -78.32131, 43.687267, -79.39842));
    
  }
  */
  
//  public void testDistanceQueryCacheable() throws IOException {
//
//    // create two of the same distance queries
//    double miles = 6.0;
//    DistanceQuery dq1 = new DistanceQuery(lat, lng, miles, latField, lngField, true);
//    DistanceQuery dq2 = new DistanceQuery(lat, lng, miles, latField, lngField, true);
//
//    /* ensure that they hash to the same code, which will cause a cache hit in solr */
//    System.out.println("hash differences?");
//    assertEquals(dq1.getQuery().hashCode(), dq2.getQuery().hashCode());
//    
//    /* ensure that changing the radius makes a different hash code, creating a cache miss in solr */
//    DistanceQuery widerQuery = new DistanceQuery(lat, lng, miles + 5.0, latField, lngField, false);
//    assertTrue(dq1.getQuery().hashCode() != widerQuery.getQuery().hashCode());
//  }
}
