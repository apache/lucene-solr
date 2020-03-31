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

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SloppyMath;

public class TestLatLonPointDistanceFeatureQuery extends LuceneTestCase {

  public void testEqualsAndHashcode() {
    Query q1 = LatLonPoint.newDistanceFeatureQuery("foo", 3, 10, 10, 5);
    Query q2 = LatLonPoint.newDistanceFeatureQuery("foo", 3, 10, 10, 5);
    QueryUtils.checkEqual(q1, q2);

    Query q3 = LatLonPoint.newDistanceFeatureQuery("bar", 3, 10, 10,5);
    QueryUtils.checkUnequal(q1, q3);

    Query q4 = LatLonPoint.newDistanceFeatureQuery("foo", 4, 10, 10, 5);
    QueryUtils.checkUnequal(q1, q4);

    Query q5 = LatLonPoint.newDistanceFeatureQuery("foo", 3, 9, 10, 5);
    QueryUtils.checkUnequal(q1, q5);

    Query q6 = LatLonPoint.newDistanceFeatureQuery("foo", 3, 10, 9, 5);
    QueryUtils.checkUnequal(q1, q6);

    Query q7 = LatLonPoint.newDistanceFeatureQuery("foo", 3, 10, 10, 6);
    QueryUtils.checkUnequal(q1, q7);
  }

  public void testBasics() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig()
        .setMergePolicy(newLogMergePolicy(random().nextBoolean())));
    Document doc = new Document();
    LatLonPoint point = new LatLonPoint("foo", 0.0, 0.0);
    doc.add(point);
    LatLonDocValuesField docValue = new LatLonDocValuesField("foo",0.0, 0.0);
    doc.add(docValue);

    double pivotDistance = 5000;//5k

    point.setLocationValue(-7, -7);
    docValue.setLocationValue(-7, -7);
    w.addDocument(doc);

    point.setLocationValue(9, 9);
    docValue.setLocationValue(9, 9);
    w.addDocument(doc);


    point.setLocationValue(8, 8);
    docValue.setLocationValue(8, 8);
    w.addDocument(doc);

    point.setLocationValue(4, 4);
    docValue.setLocationValue(4, 4);
    w.addDocument(doc);

    point.setLocationValue(-1, -1);
    docValue.setLocationValue(-1, -1);
    w.addDocument(doc);

    DirectoryReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);
    
    Query q = LatLonPoint.newDistanceFeatureQuery("foo", 3, 10, 10, pivotDistance);
    TopScoreDocCollector collector = TopScoreDocCollector.create(2, null, 1);
    searcher.search(q, collector);
    TopDocs topHits = collector.topDocs();
    assertEquals(2, topHits.scoreDocs.length);

    double distance1 = SloppyMath.haversinMeters(GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(9)) , GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(9)), 10,10);
    double distance2 = SloppyMath.haversinMeters(GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(8)) , GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(8)), 10,10);

    CheckHits.checkEqual(q,
        new ScoreDoc[] {
            new ScoreDoc(1, (float) (3f * (pivotDistance / (pivotDistance + distance1)))),
            new ScoreDoc(2, (float) (3f * (pivotDistance / (pivotDistance + distance2))))
        },
        topHits.scoreDocs);

    distance1 = SloppyMath.haversinMeters(GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(9)) , GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(9)), 9,9);
    distance2 = SloppyMath.haversinMeters(GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(8)) , GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(8)), 9,9);

    q = LatLonPoint.newDistanceFeatureQuery("foo", 3, 9, 9,  pivotDistance);
    collector = TopScoreDocCollector.create(2, null, 1);
    searcher.search(q, collector);
    topHits = collector.topDocs();
    assertEquals(2, topHits.scoreDocs.length);
    CheckHits.checkExplanations(q, "", searcher);

    CheckHits.checkEqual(q,
        new ScoreDoc[] {
            new ScoreDoc(1, (float) (3f * (pivotDistance / (pivotDistance + distance1)))),
            new ScoreDoc(2, (float) (3f * (pivotDistance / (pivotDistance + distance2))))
        },
        topHits.scoreDocs);
    
    reader.close();
    w.close();
    dir.close();
  }

  public void testCrossesDateLine() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig()
        .setMergePolicy(newLogMergePolicy(random().nextBoolean())));
    Document doc = new Document();
    LatLonPoint point = new LatLonPoint("foo", 0.0, 0.0);
    doc.add(point);
    LatLonDocValuesField docValue = new LatLonDocValuesField("foo",0.0, 0.0);
    doc.add(docValue);

    double pivotDistance = 5000;//5k

    point.setLocationValue(0, -179);
    docValue.setLocationValue(0, -179);
    w.addDocument(doc);

    point.setLocationValue(0, 176);
    docValue.setLocationValue(0, 176);
    w.addDocument(doc);

    point.setLocationValue(0, -150);
    docValue.setLocationValue(0, -150);
    w.addDocument(doc);

    point.setLocationValue(0, -140);
    docValue.setLocationValue(0, -140);
    w.addDocument(doc);

    point.setLocationValue(0, 140);
    docValue.setLocationValue(01, 140);
    w.addDocument(doc);

    DirectoryReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);

    Query q = LatLonPoint.newDistanceFeatureQuery("foo", 3, 0, 179, pivotDistance);
    TopScoreDocCollector collector = TopScoreDocCollector.create(2, null, 1);
    searcher.search(q, collector);
    TopDocs topHits = collector.topDocs();
    assertEquals(2, topHits.scoreDocs.length);

    double distance1 = SloppyMath.haversinMeters(GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0)) , GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(-179)), 0,179);
    double distance2 = SloppyMath.haversinMeters(GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0)) , GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(176)), 0,179);

    CheckHits.checkEqual(q,
        new ScoreDoc[] {
            new ScoreDoc(0, (float) (3f * (pivotDistance / (pivotDistance + distance1)))),
            new ScoreDoc(1, (float) (3f * (pivotDistance / (pivotDistance + distance2))))
        },
        topHits.scoreDocs);

    reader.close();
    w.close();
    dir.close();
  }

  public void testMissingField() throws IOException {
    IndexReader reader = new MultiReader();
    IndexSearcher searcher = newSearcher(reader);
    
    Query q = LatLonPoint.newDistanceFeatureQuery("foo", 3, 10, 10, 5000);
    TopDocs topHits = searcher.search(q, 2);
    assertEquals(0, topHits.totalHits.value);
  }

  public void testMissingValue() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig()
        .setMergePolicy(newLogMergePolicy(random().nextBoolean())));
    Document doc = new Document();
    LatLonPoint point = new LatLonPoint("foo", 0, 0);
    doc.add(point);
    LatLonDocValuesField docValue = new LatLonDocValuesField("foo", 0, 0);
    doc.add(docValue);

    point.setLocationValue(3, 3);
    docValue.setLocationValue(3, 3);
    w.addDocument(doc);

    w.addDocument(new Document());

    point.setLocationValue(7, 7);
    docValue.setLocationValue(7, 7);
    w.addDocument(doc);

    DirectoryReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);
    
    Query q = LatLonPoint.newDistanceFeatureQuery("foo", 3, 10, 10, 5);
    TopScoreDocCollector collector = TopScoreDocCollector.create(3, null, 1);
    searcher.search(q, collector);
    TopDocs topHits = collector.topDocs();
    assertEquals(2, topHits.scoreDocs.length);

    double distance1 = SloppyMath.haversinMeters(GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(7)) , GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(7)), 10,10);
    double distance2 = SloppyMath.haversinMeters(GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(3)) , GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(3)), 10,10);

    CheckHits.checkEqual(q,
        new ScoreDoc[] {
            new ScoreDoc(2, (float) (3f * (5. / (5. + distance1)))),
            new ScoreDoc(0, (float) (3f * (5. / (5. + distance2))))
        },
        topHits.scoreDocs);

    CheckHits.checkExplanations(q, "", searcher);

    reader.close();
    w.close();
    dir.close();
  }

  public void testMultiValued() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig()
        .setMergePolicy(newLogMergePolicy(random().nextBoolean())));

    Document doc = new Document();
    for (double[] point  : new double[][] {{0, 0}, {30, 30}, {60, 60}}) {
      doc.add(new LatLonPoint("foo", point[0], point[1]));
      doc.add(new LatLonDocValuesField("foo", point[0], point[1]));
    }
    w.addDocument(doc);

    doc = new Document();
    for (double[] point  : new double[][] {{45, 0}, {-45, 0}, {-90, 0}, {90, 0}}) {
      doc.add(new LatLonPoint("foo", point[0], point[1]));
      doc.add(new LatLonDocValuesField("foo", point[0], point[1]));
    }
    w.addDocument(doc);

    doc = new Document();
    for (double[] point  : new double[][] {{0, 90}, {0, -90}, {0, 180}, {0, -180}}) {
      doc.add(new LatLonPoint("foo", point[0], point[1]));
      doc.add(new LatLonDocValuesField("foo", point[0], point[1]));
    }
    w.addDocument(doc);

    doc = new Document();
    for (double[] point  : new double[][] {{3, 2}}) {
      doc.add(new LatLonPoint("foo", point[0], point[1]));
      doc.add(new LatLonDocValuesField("foo", point[0], point[1]));
    }
    w.addDocument(doc);

    doc = new Document();
    for (double[] point  : new double[][] {{45, 45}, {-45, -45}}) {
      doc.add(new LatLonPoint("foo", point[0], point[1]));
      doc.add(new LatLonDocValuesField("foo", point[0], point[1]));
    }
    w.addDocument(doc);

    DirectoryReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);

    Query q = LatLonPoint.newDistanceFeatureQuery("foo", 3, 0, 0, 200);
    TopScoreDocCollector collector = TopScoreDocCollector.create(2, null, 1);
    searcher.search(q, collector);
    TopDocs topHits = collector.topDocs();
    assertEquals(2, topHits.scoreDocs.length);

    double distance1 = SloppyMath.haversinMeters(GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0)) , GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0)), 0,0);
    double distance2 = SloppyMath.haversinMeters(GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(3)) , GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(2)), 0,0);

    CheckHits.checkEqual(q,
        new ScoreDoc[] {
            new ScoreDoc(0, (float) (3f * (200 / (200 + distance1)))),
            new ScoreDoc(3, (float) (3f * (200 / (200 + distance2))))
        },
        topHits.scoreDocs);

    q = LatLonPoint.newDistanceFeatureQuery("foo", 3, -90, 0, 10000.);
    collector = TopScoreDocCollector.create(2, null, 1);
    searcher.search(q, collector);
    topHits = collector.topDocs();
    assertEquals(2, topHits.scoreDocs.length);
    CheckHits.checkExplanations(q, "", searcher);

    distance1 = SloppyMath.haversinMeters(GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(-90)) , GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0)), -90,0);
    distance2 = SloppyMath.haversinMeters(GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(-45)) , GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(-45)), -90,0);

    CheckHits.checkEqual(q,
        new ScoreDoc[] {
            new ScoreDoc(1, (float) (3f * (10000. / (10000. + distance1)))),
            new ScoreDoc(4, (float) (3f * (10000. / (10000. + distance2))))
        },
        topHits.scoreDocs);
    
    reader.close();
    w.close();
    dir.close();
  }

  public void testRandom() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig()
        .setMergePolicy(newLogMergePolicy(random().nextBoolean())));
    Document doc = new Document();
    LatLonPoint point = new LatLonPoint("foo", 0., 0.);
    doc.add(point);
    LatLonDocValuesField docValue = new LatLonDocValuesField("foo", 0., 0.);
    doc.add(docValue);

    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      double lat = random().nextDouble() * 180 - 90;
      double lon = random().nextDouble() * 360 - 180;
      point.setLocationValue(lat, lon);
      docValue.setLocationValue(lat, lon);
      w.addDocument(doc);
    }

    IndexReader reader = DirectoryReader.open(w);
    IndexSearcher searcher = newSearcher(reader);

    int numIters = atLeast(3);
    for (int iter = 0; iter < numIters; ++iter) {
      double lat = random().nextDouble() * 180 - 90;
      double lon = random().nextDouble() * 360 - 180;
      double  pivotDistance = random().nextDouble() * random().nextDouble() * Math.PI * GeoUtils.EARTH_MEAN_RADIUS_METERS;
      float boost = (1 + random().nextInt(10)) / 3f;
      Query q = LatLonPoint.newDistanceFeatureQuery("foo", boost, lat, lon, pivotDistance);

      CheckHits.checkTopScores(random(), q, searcher);
    }

    reader.close();
    w.close();
    dir.close();
  }

  public void testCompareSorting() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig()
        .setMergePolicy(newLogMergePolicy(random().nextBoolean())));

    Document doc = new Document();
    LatLonPoint point = new LatLonPoint("foo", 0., 0.);
    doc.add(point);
    LatLonDocValuesField docValue = new LatLonDocValuesField("foo", 0., 0.);
    doc.add(docValue);

    int numDocs = atLeast(10000);
    for (int i = 0; i < numDocs; ++i) {
      double lat = random().nextDouble() * 180 - 90;
      double lon = random().nextDouble() * 360 - 180;
      point.setLocationValue(lat, lon);
      docValue.setLocationValue(lat, lon);
      w.addDocument(doc);
    }

    DirectoryReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);

    double lat = random().nextDouble() * 180 - 90;
    double lon = random().nextDouble() * 360 - 180;
    double  pivotDistance = random().nextDouble() * random().nextDouble() * GeoUtils.EARTH_MEAN_RADIUS_METERS * Math.PI;
    float boost = (1 + random().nextInt(10)) / 3f;

    Query query1 = LatLonPoint.newDistanceFeatureQuery("foo", boost, lat, lon, pivotDistance);
    Sort sort1 = new Sort(SortField.FIELD_SCORE, LatLonDocValuesField.newDistanceSort("foo", lat, lon));

    Query query2 = new MatchAllDocsQuery();
    Sort sort2 = new Sort(LatLonDocValuesField.newDistanceSort("foo", lat, lon));

    TopDocs topDocs1 = searcher.search(query1, 10, sort1);
    TopDocs topDocs2 = searcher.search(query2, 10, sort2);
    for (int i =0; i< 10; i++) {
      assertTrue(topDocs1.scoreDocs[i].doc == topDocs2.scoreDocs[i].doc);
    }
    reader.close();
    w.close();
    dir.close();
  }
}
