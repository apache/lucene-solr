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
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.bkd.BKDReader;
import org.apache.lucene.util.bkd.BKDReader.IndexTree;
import org.apache.lucene.util.bkd.BKDReader.IntersectState;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;

/**
 * KNN search on top of 2D lat/lon indexed points.
 *
 * @lucene.experimental
 */
class NearestNeighbor {

  static class Cell implements Comparable<Cell> {
    final int readerIndex;
    final byte[] minPacked;
    final byte[] maxPacked;
    final IndexTree index;

    /**
     * The closest distance from a point in this cell to the query point, computed as a sort key through
     * {@link SloppyMath#haversinSortKey}. Note that this is an approximation to the closest distance,
     * and there could be a point in the cell that is closer.
     */
    final double distanceSortKey;

    public Cell(IndexTree index, int readerIndex, byte[] minPacked, byte[] maxPacked, double distanceSortKey) {
      this.index = index;
      this.readerIndex = readerIndex;
      this.minPacked = minPacked.clone();
      this.maxPacked = maxPacked.clone();
      this.distanceSortKey = distanceSortKey;
    }

    public int compareTo(Cell other) {
      return Double.compare(distanceSortKey, other.distanceSortKey);
    }

    @Override
    public String toString() {
      double minLat = decodeLatitude(minPacked, 0);
      double minLon = decodeLongitude(minPacked, Integer.BYTES);
      double maxLat = decodeLatitude(maxPacked, 0);
      double maxLon = decodeLongitude(maxPacked, Integer.BYTES);
      return "Cell(readerIndex=" + readerIndex + " nodeID=" + index.getNodeID() + " isLeaf=" + index.isLeafNode() + " lat=" + minLat + " TO " + maxLat + ", lon=" + minLon + " TO " + maxLon + "; distanceSortKey=" + distanceSortKey + ")";
    }
  }

  private static class NearestVisitor implements IntersectVisitor {

    public int curDocBase;
    public Bits curLiveDocs;
    final int topN;
    final PriorityQueue<NearestHit> hitQueue;
    final double pointLat;
    final double pointLon;
    private int setBottomCounter;

    private double minLon = Double.NEGATIVE_INFINITY;
    private double maxLon = Double.POSITIVE_INFINITY;
    private double minLat = Double.NEGATIVE_INFINITY;
    private double maxLat = Double.POSITIVE_INFINITY;

    // second set of longitude ranges to check (for cross-dateline case)
    private double minLon2 = Double.POSITIVE_INFINITY;

    public NearestVisitor(PriorityQueue<NearestHit> hitQueue, int topN, double pointLat, double pointLon) {
      this.hitQueue = hitQueue;
      this.topN = topN;
      this.pointLat = pointLat;
      this.pointLon = pointLon;
    }

    @Override
    public void visit(int docID) {
      throw new AssertionError();
    }

    private void maybeUpdateBBox() {
      if (setBottomCounter < 1024 || (setBottomCounter & 0x3F) == 0x3F) {
        NearestHit hit = hitQueue.peek();
        Rectangle box = Rectangle.fromPointDistance(pointLat, pointLon,
            SloppyMath.haversinMeters(hit.distanceSortKey));
        //System.out.println("    update bbox to " + box);
        minLat = box.minLat;
        maxLat = box.maxLat;
        if (box.crossesDateline()) {
          // box1
          minLon = Double.NEGATIVE_INFINITY;
          maxLon = box.maxLon;
          // box2
          minLon2 = box.minLon;
        } else {
          minLon = box.minLon;
          maxLon = box.maxLon;
          // disable box2
          minLon2 = Double.POSITIVE_INFINITY;
        }
      }
      setBottomCounter++;
    }

    @Override
    public void visit(int docID, byte[] packedValue) {
      //System.out.println("visit docID=" + docID + " liveDocs=" + curLiveDocs);

      if (curLiveDocs != null && curLiveDocs.get(docID) == false) {
        return;
      }

      double docLatitude = decodeLatitude(packedValue, 0);
      double docLongitude = decodeLongitude(packedValue, Integer.BYTES);

      // test bounding box
      if (docLatitude < minLat || docLatitude > maxLat) {
        return;
      }
      if ((docLongitude < minLon || docLongitude > maxLon) && (docLongitude < minLon2)) {
        return;
      }

      // Use the haversin sort key when comparing hits, as it is faster to compute than the true distance.
      double distanceSortKey = SloppyMath.haversinSortKey(pointLat, pointLon, docLatitude, docLongitude);

      //System.out.println("    visit docID=" + docID + " distanceSortKey=" + distanceSortKey + " docLat=" + docLatitude + " docLon=" + docLongitude);

      int fullDocID = curDocBase + docID;

      if (hitQueue.size() == topN) {
        // queue already full
        NearestHit hit = hitQueue.peek();
        //System.out.println("      bottom distanceSortKey=" + hit.distanceSortKey);
        // we don't collect docs in order here, so we must also test the tie-break case ourselves:
        if (distanceSortKey < hit.distanceSortKey || (distanceSortKey == hit.distanceSortKey && fullDocID < hit.docID)) {
          hitQueue.poll();
          hit.docID = fullDocID;
          hit.distanceSortKey = distanceSortKey;
          hitQueue.offer(hit);
          //System.out.println("      ** keep2, now bottom=" + hit);
          maybeUpdateBBox();
        }
        
      } else {
        NearestHit hit = new NearestHit();
        hit.docID = fullDocID;
        hit.distanceSortKey = distanceSortKey;
        hitQueue.offer(hit);
        //System.out.println("      ** keep1, now bottom=" + hit);
      }
    }

    @Override
    public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      double cellMinLat = decodeLatitude(minPackedValue, 0);
      double cellMinLon = decodeLongitude(minPackedValue, Integer.BYTES);
      double cellMaxLat = decodeLatitude(maxPackedValue, 0);
      double cellMaxLon = decodeLongitude(maxPackedValue, Integer.BYTES);

      if (cellMaxLat < minLat || maxLat < cellMinLat || ((cellMaxLon < minLon || maxLon < cellMinLon) && cellMaxLon < minLon2)) {
        // this cell is outside our search bbox; don't bother exploring any more
        return Relation.CELL_OUTSIDE_QUERY;
      }
      return Relation.CELL_CROSSES_QUERY;
    }
  }

  /** Holds one hit from {@link NearestNeighbor#nearest} */
  static class NearestHit {
    public int docID;

    /**
     * The distance from the hit to the query point, computed as a sort key through {@link SloppyMath#haversinSortKey}.
     */
    public double distanceSortKey;

    @Override
    public String toString() {
      return "NearestHit(docID=" + docID + " distanceSortKey=" + distanceSortKey + ")";
    }
  }

  // TODO: can we somehow share more with, or simply directly use, the LatLonPointDistanceComparator?  It's really doing the same thing as
  // our hitQueue...

  public static NearestHit[] nearest(double pointLat, double pointLon, List<BKDReader> readers, List<Bits> liveDocs, List<Integer> docBases, final int n) throws IOException {

    //System.out.println("NEAREST: readers=" + readers + " liveDocs=" + liveDocs + " pointLat=" + pointLat + " pointLon=" + pointLon);
    // Holds closest collected points seen so far:
    // TODO: if we used lucene's PQ we could just updateTop instead of poll/offer:
    final PriorityQueue<NearestHit> hitQueue = new PriorityQueue<>(n, new Comparator<NearestHit>() {
        @Override
        public int compare(NearestHit a, NearestHit b) {
          // sort by opposite distanceSortKey natural order
          int cmp = Double.compare(a.distanceSortKey, b.distanceSortKey);
          if (cmp != 0) {
            return -cmp;
          }

          // tie-break by higher docID:
          return b.docID - a.docID;
        }
      });

    // Holds all cells, sorted by closest to the point:
    PriorityQueue<Cell> cellQueue = new PriorityQueue<>();

    NearestVisitor visitor = new NearestVisitor(hitQueue, n, pointLat, pointLon);
    List<BKDReader.IntersectState> states = new ArrayList<>();

    // Add root cell for each reader into the queue:
    int bytesPerDim = -1;
    
    for(int i=0;i<readers.size();i++) {
      BKDReader reader = readers.get(i);
      if (bytesPerDim == -1) {
        bytesPerDim = reader.getBytesPerDimension();
      } else if (bytesPerDim != reader.getBytesPerDimension()) {
        throw new IllegalStateException("bytesPerDim changed from " + bytesPerDim + " to " + reader.getBytesPerDimension() + " across readers");
      }
      byte[] minPackedValue = reader.getMinPackedValue();
      byte[] maxPackedValue = reader.getMaxPackedValue();
      IntersectState state = reader.getIntersectState(visitor);
      states.add(state);

      cellQueue.offer(new Cell(state.index, i, reader.getMinPackedValue(), reader.getMaxPackedValue(),
                               approxBestDistance(minPackedValue, maxPackedValue, pointLat, pointLon)));
    }

    while (cellQueue.size() > 0) {
      Cell cell = cellQueue.poll();
      //System.out.println("  visit " + cell);

      // TODO: if we replace approxBestDistance with actualBestDistance, we can put an opto here to break once this "best" cell is fully outside of the hitQueue bottom's radius:
      BKDReader reader = readers.get(cell.readerIndex);

      if (cell.index.isLeafNode()) {
        //System.out.println("    leaf");
        // Leaf block: visit all points and possibly collect them:
        visitor.curDocBase = docBases.get(cell.readerIndex);
        visitor.curLiveDocs = liveDocs.get(cell.readerIndex);
        reader.visitLeafBlockValues(cell.index, states.get(cell.readerIndex));
        //System.out.println("    now " + hitQueue.size() + " hits");
      } else {
        //System.out.println("    non-leaf");
        // Non-leaf block: split into two cells and put them back into the queue:

        if (visitor.compare(cell.minPacked, cell.maxPacked) == Relation.CELL_OUTSIDE_QUERY) {
          continue;
        }
        
        BytesRef splitValue = BytesRef.deepCopyOf(cell.index.getSplitDimValue());
        int splitDim = cell.index.getSplitDim();
        
        // we must clone the index so that we we can recurse left and right "concurrently":
        IndexTree newIndex = cell.index.clone();
        byte[] splitPackedValue = cell.maxPacked.clone();
        System.arraycopy(splitValue.bytes, splitValue.offset, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);

        cell.index.pushLeft();
        cellQueue.offer(new Cell(cell.index, cell.readerIndex, cell.minPacked, splitPackedValue,
                                 approxBestDistance(cell.minPacked, splitPackedValue, pointLat, pointLon)));

        splitPackedValue = cell.minPacked.clone();
        System.arraycopy(splitValue.bytes, splitValue.offset, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);

        newIndex.pushRight();
        cellQueue.offer(new Cell(newIndex, cell.readerIndex, splitPackedValue, cell.maxPacked,
                                 approxBestDistance(splitPackedValue, cell.maxPacked, pointLat, pointLon)));
      }
    }

    NearestHit[] hits = new NearestHit[hitQueue.size()];
    int downTo = hitQueue.size()-1;
    while (hitQueue.size() != 0) {
      hits[downTo] = hitQueue.poll();
      downTo--;
    }

    return hits;
  }

  // NOTE: incoming args never cross the dateline, since they are a BKD cell
  private static double approxBestDistance(byte[] minPackedValue, byte[] maxPackedValue, double pointLat, double pointLon) {
    double minLat = decodeLatitude(minPackedValue, 0);
    double minLon = decodeLongitude(minPackedValue, Integer.BYTES);
    double maxLat = decodeLatitude(maxPackedValue, 0);
    double maxLon = decodeLongitude(maxPackedValue, Integer.BYTES);
    return approxBestDistance(minLat, maxLat, minLon, maxLon, pointLat, pointLon);
  }

  // NOTE: incoming args never cross the dateline, since they are a BKD cell
  private static double approxBestDistance(double minLat, double maxLat, double minLon, double maxLon, double pointLat, double pointLon) {
    
    // TODO: can we make this the trueBestDistance?  I.e., minimum distance between the point and ANY point on the box?  we can speed things
    // up if so, but not enrolling any BKD cell whose true best distance is > bottom of the current hit queue

    if (pointLat >= minLat && pointLat <= maxLat && pointLon >= minLon && pointLon <= maxLon) {
      // point is inside the cell!
      return 0.0;
    }

    double d1 = SloppyMath.haversinSortKey(pointLat, pointLon, minLat, minLon);
    double d2 = SloppyMath.haversinSortKey(pointLat, pointLon, minLat, maxLon);
    double d3 = SloppyMath.haversinSortKey(pointLat, pointLon, maxLat, maxLon);
    double d4 = SloppyMath.haversinSortKey(pointLat, pointLon, maxLat, minLon);
    return Math.min(Math.min(d1, d2), Math.min(d3, d4));
  }

}
