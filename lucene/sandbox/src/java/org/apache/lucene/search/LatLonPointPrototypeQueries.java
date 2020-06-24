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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.lucene86.Lucene86PointsFormat;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.bkd.BKDReader;

/**
 * Holder class for prototype sandboxed queries
 *
 * When the query graduates from sandbox, these static calls should be
 * placed in {@link LatLonPoint}
 *
 * @lucene.experimental
 */
public class LatLonPointPrototypeQueries {

    // no instance
    private LatLonPointPrototypeQueries() {
    }

  /**
   * Finds the {@code n} nearest indexed points to the provided point, according to Haversine distance.
   * <p>
   * This is functionally equivalent to running {@link MatchAllDocsQuery} with a {@link LatLonDocValuesField#newDistanceSort},
   * but is far more efficient since it takes advantage of properties the indexed BKD tree.  Currently this
   * only works with {@link Lucene86PointsFormat} (used by the default codec).  Multi-valued fields are
   * currently not de-duplicated, so if a document had multiple instances of the specified field that
   * make it into the top n, that document will appear more than once.
   * <p>
   * Documents are ordered by ascending distance from the location. The value returned in {@link FieldDoc} for
   * the hits contains a Double instance with the distance in meters.
   *
   * @param searcher IndexSearcher to find nearest points from.
   * @param field field name. must not be null.
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @param n the number of nearest neighbors to retrieve.
   * @return TopFieldDocs containing documents ordered by distance, where the field value for each {@link FieldDoc} is the distance in meters
   * @throws IllegalArgumentException if the underlying PointValues is not a {@code Lucene60PointsReader} (this is a current limitation), or
   *         if {@code field} or {@code searcher} is null, or if {@code latitude}, {@code longitude} or {@code n} are out-of-bounds
   * @throws IOException if an IOException occurs while finding the points.
   */
  // TODO: what about multi-valued documents? what happens?
  public static TopFieldDocs nearest(IndexSearcher searcher, String field, double latitude, double longitude, int n) throws IOException {
    GeoUtils.checkLatitude(latitude);
    GeoUtils.checkLongitude(longitude);
    if (n < 1) {
      throw new IllegalArgumentException("n must be at least 1; got " + n);
    }
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    if (searcher == null) {
      throw new IllegalArgumentException("searcher must not be null");
    }
    List<BKDReader> readers = new ArrayList<>();
    List<Integer> docBases = new ArrayList<>();
    List<Bits> liveDocs = new ArrayList<>();
    int totalHits = 0;
    for(LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
      PointValues points = leaf.reader().getPointValues(field);
      if (points != null) {
        if (points instanceof BKDReader == false) {
          throw new IllegalArgumentException("can only run on Lucene60PointsReader points implementation, but got " + points);
        }
        totalHits += points.getDocCount();
        BKDReader reader = (BKDReader) points;
        if (reader != null) {
          readers.add(reader);
          docBases.add(leaf.docBase);
          liveDocs.add(leaf.reader().getLiveDocs());
        }
      }
    }

    NearestNeighbor.NearestHit[] hits = NearestNeighbor.nearest(latitude, longitude, readers, liveDocs, docBases, n);

    // Convert to TopFieldDocs:
    ScoreDoc[] scoreDocs = new ScoreDoc[hits.length];
    for(int i=0;i<hits.length;i++) {
      NearestNeighbor.NearestHit hit = hits[i];
      double hitDistance = SloppyMath.haversinMeters(hit.distanceSortKey);
      scoreDocs[i] = new FieldDoc(hit.docID, 0.0f, new Object[] {Double.valueOf(hitDistance)});
    }
    return new TopFieldDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), scoreDocs, null);
  }
}
