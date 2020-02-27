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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.geo.ShapeTestUtil;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** Simple tests for {@link XYDocValuesField#newDistanceSort} */
public class TestXYPointDistanceSort extends LuceneTestCase {

  private double cartesianDistance(double x1, double y1, double x2, double y2) {
    final double diffX = x1 - x2;
    final double diffY = y1 - y2;
    return Math.sqrt(diffX * diffX + diffY * diffY);
  }

  /** Add three points and sort by distance */
  public void testDistanceSort() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    
    // add some docs
    Document doc = new Document();
    doc.add(new XYDocValuesField("location", 40.759011f, -73.9844722f));
    iw.addDocument(doc);
    double d1 = cartesianDistance(40.759011f, -73.9844722f, 40.7143528f, -74.0059731f);
    
    doc = new Document();
    doc.add(new XYDocValuesField("location", 40.718266f, -74.007819f));
    iw.addDocument(doc);
    double d2 = cartesianDistance(40.718266f, -74.007819f, 40.7143528f, -74.0059731f);
    
    doc = new Document();
    doc.add(new XYDocValuesField("location", 40.7051157f, -74.0088305f));
    iw.addDocument(doc);
    double d3 = cartesianDistance(40.7051157f, -74.0088305f, 40.7143528f, -74.0059731f);

    IndexReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();

    Sort sort = new Sort(XYDocValuesField.newDistanceSort("location", 40.7143528f, -74.0059731f));
    TopDocs td = searcher.search(new MatchAllDocsQuery(), 3, sort);
    
    FieldDoc d = (FieldDoc) td.scoreDocs[0];
    assertEquals(d2, (Double)d.fields[0], 0.0D);
    
    d = (FieldDoc) td.scoreDocs[1];
    assertEquals(d3, (Double)d.fields[0], 0.0D);
    
    d = (FieldDoc) td.scoreDocs[2];
    assertEquals(d1, (Double)d.fields[0], 0.0D);
    
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
    doc.add(new XYDocValuesField("location", 40.718266f, -74.007819f));
    iw.addDocument(doc);
    double d2 = cartesianDistance(40.718266f, -74.007819f, 40.7143528f, -74.0059731f);
    
    doc = new Document();
    doc.add(new XYDocValuesField("location", 40.7051157f, -74.0088305f));
    iw.addDocument(doc);
    double d3 = cartesianDistance(40.7051157f, -74.0088305f, 40.7143528f, -74.0059731f);


    IndexReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();

    Sort sort = new Sort(XYDocValuesField.newDistanceSort("location", 40.7143528f, -74.0059731f));
    TopDocs td = searcher.search(new MatchAllDocsQuery(), 3, sort);
    
    FieldDoc d = (FieldDoc) td.scoreDocs[0];
    assertEquals(d2, (Double)d.fields[0], 0.0D);
    
    d = (FieldDoc) td.scoreDocs[1];
    assertEquals(d3, (Double)d.fields[0], 0.0D);
    
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
        float x = ShapeTestUtil.nextFloat(random());
        float y = ShapeTestUtil.nextFloat(random());

        doc.add(new XYDocValuesField("field", x, y));
        doc.add(new StoredField("x", x));
        doc.add(new StoredField("y", y));
      } // otherwise "missing"
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    for (int i = 0; i < numQueries; i++) {
      float x = ShapeTestUtil.nextFloat(random());
      float y = ShapeTestUtil.nextFloat(random());
      double missingValue = Double.POSITIVE_INFINITY;

      Result expected[] = new Result[reader.maxDoc()];
      
      for (int doc = 0; doc < reader.maxDoc(); doc++) {
        Document targetDoc = reader.document(doc);
        final double distance;
        if (targetDoc.getField("x") == null) {
          distance = missingValue; // missing
        } else {
          double docX = targetDoc.getField("x").numericValue().floatValue();
          double docY = targetDoc.getField("y").numericValue().floatValue();
          distance = cartesianDistance(x, y, docX, docY);
        }
        int id = targetDoc.getField("id").numericValue().intValue();
        expected[doc] = new Result(id, distance);
      }
      
      Arrays.sort(expected);
      
      // randomize the topN a bit
      int topN = TestUtil.nextInt(random(), 1, reader.maxDoc());
      // sort by distance, then ID
      SortField distanceSort = XYDocValuesField.newDistanceSort("field", x, y);
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
