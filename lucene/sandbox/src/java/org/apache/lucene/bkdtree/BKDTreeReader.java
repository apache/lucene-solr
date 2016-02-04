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
package org.apache.lucene.bkdtree;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/** Handles intersection of a shape with a BKD tree previously written with {@link BKDTreeWriter}.
 *
 * @lucene.experimental
 *
 * @deprecated Use dimensional values in Lucene 6.0 instead */
@Deprecated
final class BKDTreeReader implements Accountable {
  final private int[] splitValues;
  final private int leafNodeOffset;
  final private long[] leafBlockFPs;
  final int maxDoc;
  final IndexInput in;

  enum Relation {CELL_INSIDE_SHAPE, SHAPE_CROSSES_CELL, SHAPE_OUTSIDE_CELL};

  interface LatLonFilter {
    // TODO: move DVs/encoding out on top: this method should just take a docID
    boolean accept(double lat, double lon);
    // TODO: move DVs/encoding out on top: this method should take ints and do its own decode
    Relation compare(double latMin, double latMax, double lonMin, double lonMax);
  }

  public BKDTreeReader(IndexInput in, int maxDoc) throws IOException {

    // Read index:
    int numLeaves = in.readVInt();
    leafNodeOffset = numLeaves;

    // Tree is fully balanced binary tree, so number of nodes = numLeaves-1, except our nodeIDs are 1-based (splitValues[0] is unused):
    splitValues = new int[numLeaves];
    for(int i=0;i<numLeaves;i++) {
      splitValues[i] = in.readInt();
    }
    leafBlockFPs = new long[numLeaves];
    for(int i=0;i<numLeaves;i++) {
      leafBlockFPs[i] = in.readVLong();
    }

    this.maxDoc = maxDoc;
    this.in = in;
  }

  private static final class QueryState {
    final IndexInput in;
    byte[] scratch = new byte[16];
    final ByteArrayDataInput scratchReader = new ByteArrayDataInput(scratch);
    final DocIdSetBuilder docs;
    final int latMinEnc;
    final int latMaxEnc;
    final int lonMinEnc;
    final int lonMaxEnc;
    final LatLonFilter latLonFilter;
    final SortedNumericDocValues sndv;

    public QueryState(IndexInput in, int maxDoc,
                      int latMinEnc, int latMaxEnc,
                      int lonMinEnc, int lonMaxEnc,
                      LatLonFilter latLonFilter,
                      SortedNumericDocValues sndv) {
      this.in = in;
      this.docs = new DocIdSetBuilder(maxDoc);
      this.latMinEnc = latMinEnc;
      this.latMaxEnc = latMaxEnc;
      this.lonMinEnc = lonMinEnc;
      this.lonMaxEnc = lonMaxEnc;
      this.latLonFilter = latLonFilter;
      this.sndv = sndv;
    }
  }

  // TODO: move DVs/encoding out on top: this method should take ints, and encode should be done up above
  public DocIdSet intersect(double latMin, double latMax, double lonMin, double lonMax, LatLonFilter filter, SortedNumericDocValues sndv) throws IOException {
    if (BKDTreeWriter.validLat(latMin) == false) {
      throw new IllegalArgumentException("invalid latMin: " + latMin);
    }
    if (BKDTreeWriter.validLat(latMax) == false) {
      throw new IllegalArgumentException("invalid latMax: " + latMax);
    }
    if (BKDTreeWriter.validLon(lonMin) == false) {
      throw new IllegalArgumentException("invalid lonMin: " + lonMin);
    }
    if (BKDTreeWriter.validLon(lonMax) == false) {
      throw new IllegalArgumentException("invalid lonMax: " + lonMax);
    }

    int latMinEnc = BKDTreeWriter.encodeLat(latMin);
    int latMaxEnc = BKDTreeWriter.encodeLat(latMax);
    int lonMinEnc = BKDTreeWriter.encodeLon(lonMin);
    int lonMaxEnc = BKDTreeWriter.encodeLon(lonMax);

    QueryState state = new QueryState(in.clone(), maxDoc,
                                      latMinEnc, latMaxEnc,
                                      lonMinEnc, lonMaxEnc,
                                      filter,
                                      sndv);

    int hitCount = intersect(state, 1,
                             BKDTreeWriter.encodeLat(-90.0),
                             BKDTreeWriter.encodeLat(Math.nextAfter(90.0, Double.POSITIVE_INFINITY)),
                             BKDTreeWriter.encodeLon(-180.0),
                             BKDTreeWriter.encodeLon(Math.nextAfter(180.0, Double.POSITIVE_INFINITY)));

    // NOTE: hitCount is an over-estimate in the multi-valued case:
    return state.docs.build(hitCount);
  }

  /** Fast path: this is called when the query rect fully encompasses all cells under this node. */
  private int addAll(QueryState state, int nodeID) throws IOException {

    //long latRange = (long) cellLatMaxEnc - (long) cellLatMinEnc;
    //long lonRange = (long) cellLonMaxEnc - (long) cellLonMinEnc;

    if (nodeID >= leafNodeOffset) {

      /*
      System.out.println("A: " + BKDTreeWriter.decodeLat(cellLatMinEnc)
                         + " " + BKDTreeWriter.decodeLat(cellLatMaxEnc)
                         + " " + BKDTreeWriter.decodeLon(cellLonMinEnc)
                         + " " + BKDTreeWriter.decodeLon(cellLonMaxEnc));
      */

      // Leaf node
      long fp = leafBlockFPs[nodeID-leafNodeOffset];
      //System.out.println("    leaf nodeID=" + nodeID + " vs leafNodeOffset=" + leafNodeOffset + " fp=" + fp);
      if (fp == 0) {
        // Dead end node (adversary case):
        return 0;
      }
      state.in.seek(fp);

      //System.out.println("    seek to leafFP=" + fp);
      // How many points are stored in this leaf cell:
      int count = state.in.readVInt();
      state.docs.grow(count);
      for(int i=0;i<count;i++) {
        int docID = state.in.readInt();
        state.docs.add(docID);
      }

      return count;
    } else {
      int splitValue = splitValues[nodeID];

      if (splitValue == Integer.MAX_VALUE) {
        // Dead end node (adversary case):
        return 0;
      }

      //System.out.println("  splitValue=" + splitValue);

      //System.out.println("  addAll: inner");
      int count = addAll(state, 2*nodeID);
      count += addAll(state, 2*nodeID+1);
      //System.out.println("  addAll: return count=" + count);
      return count;
    }
  }

  private int intersect(QueryState state,
                        int nodeID,
                        int cellLatMinEnc, int cellLatMaxEnc, int cellLonMinEnc, int cellLonMaxEnc)
    throws IOException {

    // 2.06 sec -> 1.52 sec for 225 OSM London queries:
    if (state.latLonFilter != null) {

      // Only call the filter when the current cell does not fully contain the bbox:
      if (cellLatMinEnc > state.latMinEnc || cellLatMaxEnc < state.latMaxEnc ||
          cellLonMinEnc > state.lonMinEnc || cellLonMaxEnc < state.lonMaxEnc) {

        Relation r = state.latLonFilter.compare(BKDTreeWriter.decodeLat(cellLatMinEnc),
                                                BKDTreeWriter.decodeLat(cellLatMaxEnc),
                                                BKDTreeWriter.decodeLon(cellLonMinEnc),
                                                BKDTreeWriter.decodeLon(cellLonMaxEnc));
        // System.out.println("BKD.intersect cellLat=" + BKDTreeWriter.decodeLat(cellLatMinEnc) + " TO " + BKDTreeWriter.decodeLat(cellLatMaxEnc) + ", cellLon=" + BKDTreeWriter.decodeLon(cellLonMinEnc) + " TO " + BKDTreeWriter.decodeLon(cellLonMaxEnc) + " compare=" + r);
        if (r == Relation.SHAPE_OUTSIDE_CELL) {
          // This cell is fully outside of the query shape: stop recursing
          return 0;
        } else if (r == Relation.CELL_INSIDE_SHAPE) {
          // This cell is fully inside of the query shape: recursively add all points in this cell without filtering
          return addAll(state, nodeID);
        } else {
          // The cell crosses the shape boundary, so we fall through and do full filtering
        }
      }
    // TODO: clean this up: the bbox case should also just be a filter, and we should assert filter != null at the start
    } else if (state.latMinEnc <= cellLatMinEnc && state.latMaxEnc >= cellLatMaxEnc && state.lonMinEnc <= cellLonMinEnc && state.lonMaxEnc >= cellLonMaxEnc) {
      // Bbox query: optimize the case when the query fully contains this cell: we can
      // recursively add all points without checking if they match the query:
      return addAll(state, nodeID);
    }

    long latRange = (long) cellLatMaxEnc - (long) cellLatMinEnc;
    long lonRange = (long) cellLonMaxEnc - (long) cellLonMinEnc;

    int dim;
    if (latRange >= lonRange) {
      dim = 0;
    } else {
      dim = 1;
    }

    //System.out.println("\nintersect node=" + nodeID + " vs " + leafNodeOffset);

    if (nodeID >= leafNodeOffset) {
      // Leaf node; scan and filter all points in this block:
      //System.out.println("    intersect leaf nodeID=" + nodeID + " vs leafNodeOffset=" + leafNodeOffset + " fp=" + leafBlockFPs[nodeID-leafNodeOffset]);
      int hitCount = 0;

      long fp = leafBlockFPs[nodeID-leafNodeOffset];
      if (fp == 0) {
        // Dead end node (adversary case):
        //System.out.println("    dead-end leaf");
        return 0;
      }

      /*
      System.out.println("I: " + BKDTreeWriter.decodeLat(cellLatMinEnc)
                         + " " + BKDTreeWriter.decodeLat(cellLatMaxEnc)
                         + " " + BKDTreeWriter.decodeLon(cellLonMinEnc)
                         + " " + BKDTreeWriter.decodeLon(cellLonMaxEnc));
      */

      state.in.seek(fp);

      // How many points are stored in this leaf cell:
      int count = state.in.readVInt();

      state.docs.grow(count);
      for(int i=0;i<count;i++) {
        int docID = state.in.readInt();
        state.sndv.setDocument(docID);
        // How many values this doc has:
        int docValueCount = state.sndv.count();
        for(int j=0;j<docValueCount;j++) {
          long enc = state.sndv.valueAt(j);

          int latEnc = (int) ((enc>>32) & 0xffffffffL);
          int lonEnc = (int) (enc & 0xffffffffL);

          if (latEnc >= state.latMinEnc &&
              latEnc < state.latMaxEnc &&
              lonEnc >= state.lonMinEnc &&
              lonEnc < state.lonMaxEnc &&
              (state.latLonFilter == null ||
               state.latLonFilter.accept(BKDTreeWriter.decodeLat(latEnc), BKDTreeWriter.decodeLon(lonEnc)))) {
            state.docs.add(docID);
            hitCount++;

            // Stop processing values for this doc:
            break;
          }
        }
      }

      return hitCount;

    } else {

      int splitValue = splitValues[nodeID];

      if (splitValue == Integer.MAX_VALUE) {
        // Dead end node (adversary case):
        //System.out.println("    dead-end sub-tree");
        return 0;
      }

      //System.out.println("  splitValue=" + splitValue);

      int count = 0;

      if (dim == 0) {

        //System.out.println("  split on lat=" + splitValue);

        // Inner node split on lat:

        // Left node:
        if (state.latMinEnc < splitValue) {
          //System.out.println("  recurse left");
          count += intersect(state,
                             2*nodeID,
                             cellLatMinEnc, splitValue, cellLonMinEnc, cellLonMaxEnc);
        }

        // Right node:
        if (state.latMaxEnc >= splitValue) {
          //System.out.println("  recurse right");
          count += intersect(state,
                             2*nodeID+1,
                             splitValue, cellLatMaxEnc, cellLonMinEnc, cellLonMaxEnc);
        }

      } else {
        // Inner node split on lon:
        assert dim == 1;

        // System.out.println("  split on lon=" + splitValue);

        // Left node:
        if (state.lonMinEnc < splitValue) {
          // System.out.println("  recurse left");
          count += intersect(state,
                             2*nodeID,
                             cellLatMinEnc, cellLatMaxEnc, cellLonMinEnc, splitValue);
        }

        // Right node:
        if (state.lonMaxEnc >= splitValue) {
          // System.out.println("  recurse right");
          count += intersect(state,
                             2*nodeID+1,
                             cellLatMinEnc, cellLatMaxEnc, splitValue, cellLonMaxEnc);
        }
      }

      return count;
    }
  }

  @Override
  public long ramBytesUsed() {
    return splitValues.length * RamUsageEstimator.NUM_BYTES_INT +
      leafBlockFPs.length * RamUsageEstimator.NUM_BYTES_LONG;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }
}
