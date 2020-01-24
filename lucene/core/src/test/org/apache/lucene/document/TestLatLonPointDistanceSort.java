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
package org.apache.lucene.document;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.TestUtil;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/** Simple tests for {@link LatLonDocValuesField#newDistanceSort} */
public class TestLatLonPointDistanceSort extends LuceneTestCase {

  /** Add three points and sort by distance */
  public void testDistanceSort() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    
    // add some docs
    Document doc = new Document();
    doc.add(new LatLonDocValuesField("location", 40.759011, -73.9844722));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new LatLonDocValuesField("location", 40.718266, -74.007819));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new LatLonDocValuesField("location", 40.7051157, -74.0088305));
    iw.addDocument(doc);
    
    IndexReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();

    Sort sort = new Sort(LatLonDocValuesField.newDistanceSort("location", 40.7143528, -74.0059731));
    TopDocs td = searcher.search(new MatchAllDocsQuery(), 3, sort);
    
    FieldDoc d = (FieldDoc) td.scoreDocs[0];
    assertEquals(462.1028401330431, (Double)d.fields[0], 0.0D);
    
    d = (FieldDoc) td.scoreDocs[1];
    assertEquals(1054.9842850974826, (Double)d.fields[0], 0.0D);
    
    d = (FieldDoc) td.scoreDocs[2];
    assertEquals(5285.881528419706, (Double)d.fields[0], 0.0D);
    
    reader.close();
    dir.close();
  }
  
  /** Add two points (one doc missing) and sort by distance */
  public void testMissingLast() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    
    // missing
    Document doc = new Document();
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new LatLonDocValuesField("location", 40.718266, -74.007819));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new LatLonDocValuesField("location", 40.7051157, -74.0088305));
    iw.addDocument(doc);
    
    IndexReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();

    Sort sort = new Sort(LatLonDocValuesField.newDistanceSort("location", 40.7143528, -74.0059731));
    TopDocs td = searcher.search(new MatchAllDocsQuery(), 3, sort);
    
    FieldDoc d = (FieldDoc) td.scoreDocs[0];
    assertEquals(462.1028401330431D, (Double)d.fields[0], 0.0D);
    
    d = (FieldDoc) td.scoreDocs[1];
    assertEquals(1054.9842850974826, (Double)d.fields[0], 0.0D);
    
    d = (FieldDoc) td.scoreDocs[2];
    assertEquals(Double.POSITIVE_INFINITY, (Double)d.fields[0], 0.0D);
    
    reader.close();
    dir.close();
  }
  
  /** Run a few iterations with just 10 docs, hopefully easy to debug */
  public void testRandom() throws Exception {
    for (int iters = 0; iters < 100; iters++) {
      doRandomTest(10, 100);
    }
  }
  
  /** Runs with thousands of docs */
  @Nightly
  public void testRandomHuge() throws Exception {
    for (int iters = 0; iters < 10; iters++) {
      doRandomTest(2000, 100);
    }
  }
  
  // result class used for testing. holds an id+distance.
  // we sort these with Arrays.sort and compare with lucene's results
  static class Result implements Comparable<Result> {
    int id;
    double distance;
    
    Result(int id, double distance) {
      this.id = id;
      this.distance = distance;
    }

    @Override
    public int compareTo(Result o) {
      int cmp = Double.compare(distance, o.distance);
      if (cmp == 0) {
        return Integer.compare(id, o.id);
      }
      return cmp;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      long temp;
      temp = Double.doubleToLongBits(distance);
      result = prime * result + (int) (temp ^ (temp >>> 32));
      result = prime * result + id;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      Result other = (Result) obj;
      if (Double.doubleToLongBits(distance) != Double.doubleToLongBits(other.distance)) return false;
      if (id != other.id) return false;
      return true;
    }

    @Override
    public String toString() {
      return "Result [id=" + id + ", distance=" + distance + "]";
    }
  }
  
  private void doRandomTest(int numDocs, int numQueries) throws IOException {
    Directory dir = newDirectory();    
    IndexWriterConfig iwc = newIndexWriterConfig();
    // else seeds may not to reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new StoredField("id", i));
      doc.add(new NumericDocValuesField("id", i));
      if (random().nextInt(10) > 7) {
        double latRaw = GeoTestUtil.nextLatitude();
        double lonRaw = GeoTestUtil.nextLongitude();
        // pre-normalize up front, so we can just use quantized value for testing and do simple exact comparisons
        double lat = decodeLatitude(encodeLatitude(latRaw));
        double lon = decodeLongitude(encodeLongitude(lonRaw));

        doc.add(new LatLonDocValuesField("field", lat, lon));
        doc.add(new StoredField("lat", lat));
        doc.add(new StoredField("lon", lon));
      } // otherwise "missing"
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    for (int i = 0; i < numQueries; i++) {
      double lat = GeoTestUtil.nextLatitude();
      double lon = GeoTestUtil.nextLongitude();
      double missingValue = Double.POSITIVE_INFINITY;

      Result expected[] = new Result[reader.maxDoc()];
      
      for (int doc = 0; doc < reader.maxDoc(); doc++) {
        Document targetDoc = reader.document(doc);
        final double distance;
        if (targetDoc.getField("lat") == null) {
          distance = missingValue; // missing
        } else {
          double docLatitude = targetDoc.getField("lat").numericValue().doubleValue();
          double docLongitude = targetDoc.getField("lon").numericValue().doubleValue();
          distance = SloppyMath.haversinMeters(lat, lon, docLatitude, docLongitude);
        }
        int id = targetDoc.getField("id").numericValue().intValue();
        expected[doc] = new Result(id, distance);
      }
      
      Arrays.sort(expected);
      
      // randomize the topN a bit
      int topN = TestUtil.nextInt(random(), 1, reader.maxDoc());
      // sort by distance, then ID
      SortField distanceSort = LatLonDocValuesField.newDistanceSort("field", lat, lon);
      distanceSort.setMissingValue(missingValue);
      Sort sort = new Sort(distanceSort, 
                           new SortField("id", SortField.Type.INT));

      TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), topN, sort);
      for (int resultNumber = 0; resultNumber < topN; resultNumber++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[resultNumber];
        Result actual = new Result((Integer) fieldDoc.fields[1], (Double) fieldDoc.fields[0]);
        assertEquals(expected[resultNumber], actual);
      }

      // get page2 with searchAfter()
      if (topN < reader.maxDoc()) {
        int page2 = TestUtil.nextInt(random(), 1, reader.maxDoc() - topN);
        TopDocs topDocs2 = searcher.searchAfter(topDocs.scoreDocs[topN - 1], new MatchAllDocsQuery(), page2, sort);
        for (int resultNumber = 0; resultNumber < page2; resultNumber++) {
          FieldDoc fieldDoc = (FieldDoc) topDocs2.scoreDocs[resultNumber];
          Result actual = new Result((Integer) fieldDoc.fields[1], (Double) fieldDoc.fields[0]);
          assertEquals(expected[topN + resultNumber], actual);
        }
      }
    }
    reader.close();
    writer.close();
    dir.close();
  }
}
