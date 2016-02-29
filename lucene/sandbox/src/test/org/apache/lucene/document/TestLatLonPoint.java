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
import java.util.BitSet;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointFormat;
import org.apache.lucene.codecs.PointReader;
import org.apache.lucene.codecs.PointWriter;
import org.apache.lucene.codecs.lucene60.Lucene60PointReader;
import org.apache.lucene.codecs.lucene60.Lucene60PointWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.spatial.util.GeoDistanceUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.bkd.BKDWriter;

/** Simple tests for {@link LatLonPoint} */
public class TestLatLonPoint extends LuceneTestCase {

  /** Add a single address and search for it in a box */
  // NOTE: we don't currently supply an exact search, only ranges, because of the lossiness...
  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with an address
    Document document = new Document();
    document.add(new LatLonPoint("field", 18.313694, -65.227444));
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.count(LatLonPoint.newBoxQuery("field", 18, 19, -66, -65)));

    reader.close();
    writer.close();
    dir.close();
  }
    
  public void testToString() throws Exception {
    // looks crazy due to lossiness
    assertEquals("LatLonPoint <field:18.313693958334625,-65.22744392976165>",(new LatLonPoint("field", 18.313694, -65.227444)).toString());
    
    // looks crazy due to lossiness
    assertEquals("field:[17.99999997485429 TO 18.999999999068677},[-65.9999999217689 TO -64.99999998137355}", LatLonPoint.newBoxQuery("field", 18, 19, -66, -65).toString());
  }
  
  public void testRadiusRandom() throws Exception {
    for (int iters = 0; iters < 100; iters++) {
      doRandomTest(10, 100);
    }
  }
  
  @Nightly
  public void testRadiusRandomHuge() throws Exception {
    for (int iters = 0; iters < 10; iters++) {
      doRandomTest(2000, 100);
    }
  }
  
  private void doRandomTest(int numDocs, int numQueries) throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    int pointsInLeaf = 2 + random().nextInt(4);
    iwc.setCodec(new FilterCodec("Lucene60", TestUtil.getDefaultCodec()) {
      @Override
      public PointFormat pointFormat() {
        return new PointFormat() {
          @Override
          public PointWriter fieldsWriter(SegmentWriteState writeState) throws IOException {
            return new Lucene60PointWriter(writeState, pointsInLeaf, BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP);
          }

          @Override
          public PointReader fieldsReader(SegmentReadState readState) throws IOException {
            return new Lucene60PointReader(readState);
          }
        };
      }
    });
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    for (int i = 0; i < numDocs; i++) {
      double latRaw = -90 + 180.0 * random().nextDouble();
      double lonRaw = -180 + 360.0 * random().nextDouble();
      // pre-normalize up front, so we can just use quantized value for testing and do simple exact comparisons
      double lat = LatLonPoint.decodeLat(LatLonPoint.encodeLat(latRaw));
      double lon = LatLonPoint.decodeLon(LatLonPoint.encodeLon(lonRaw));
      Document doc = new Document();
      doc.add(new LatLonPoint("field", lat, lon));
      doc.add(new StoredField("lat", lat));
      doc.add(new StoredField("lon", lon));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);

    for (int i = 0; i < numQueries; i++) {
      double lat = -90 + 180.0 * random().nextDouble();
      double lon = -180 + 360.0 * random().nextDouble();
      double radius = 50000000 * random().nextDouble();

      BitSet expected = new BitSet();
      for (int doc = 0; doc < reader.maxDoc(); doc++) {
        double docLatitude = reader.document(doc).getField("lat").numericValue().doubleValue();
        double docLongitude = reader.document(doc).getField("lon").numericValue().doubleValue();
        double distance = GeoDistanceUtils.haversin(lat, lon, docLatitude, docLongitude);
        if (distance <= radius) {
          expected.set(doc);
        }
      }

      TopDocs topDocs = searcher.search(LatLonPoint.newDistanceQuery("field", lat, lon, radius), reader.maxDoc(), Sort.INDEXORDER);
      BitSet actual = new BitSet();
      for (ScoreDoc doc : topDocs.scoreDocs) {
        actual.set(doc.doc);
      }

      try {
        assertEquals(expected, actual);
      } catch (AssertionError e) {
        for (int doc = 0; doc < reader.maxDoc(); doc++) {
          double docLatitude = reader.document(doc).getField("lat").numericValue().doubleValue();
          double docLongitude = reader.document(doc).getField("lon").numericValue().doubleValue();
          double distance = GeoDistanceUtils.haversin(lat, lon, docLatitude, docLongitude);
          System.out.println("" + doc + ": (" + docLatitude + "," + docLongitude + "), distance=" + distance);
        }
        throw e;
      }
    }
    reader.close();
    writer.close();
    dir.close();
  }
}
