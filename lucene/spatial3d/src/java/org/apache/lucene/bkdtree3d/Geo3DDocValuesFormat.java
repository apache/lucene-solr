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
package org.apache.lucene.bkdtree3d;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene54.Lucene54DocValuesFormat;
import org.apache.lucene.geo3d.PlanetModel;
import org.apache.lucene.geo3d.Vector;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * A {@link DocValuesFormat} to efficiently index geo-spatial 3D x,y,z points
 * from {@link Geo3DPointField} for fast shape intersection queries using
 * ({@link PointInGeo3DShapeQuery})
 *
 * <p>This wraps {@link Lucene54DocValuesFormat}, but saves its own BKD tree
 * structures to disk for fast query-time intersection. See <a
 * href="https://www.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf">this paper</a>
 * for details.
 *
 * <p>The BKD tree slices up 3D x,y,z space into smaller and
 * smaller 3D rectangles, until the smallest rectangles have approximately
 * between X/2 and X (X default is 1024) points in them, at which point
 * such leaf cells are written as a block to disk, while the index tree
 * structure records how space was sub-divided is loaded into HEAP
 * at search time.  At search time, the tree is recursed based on whether
 * each of left or right child overlap with the query shape, and once
 * a leaf block is reached, all documents in that leaf block are collected
 * if the cell is fully enclosed by the query shape, or filtered and then
 * collected, if not.
 *
 * <p>The index is also quite compact, because docs only appear once in
 * the tree (no "prefix terms").
 *
 * <p>In addition to the files written by {@link Lucene54DocValuesFormat}, this format writes:
 * <ol>
 *   <li><tt>.kd3d</tt>: BKD leaf data and index</li>
 *   <li><tt>.kd3m</tt>: BKD metadata</li>
 * </ol>
 *
 * <p>The disk format is experimental and free to change suddenly, and this code
 * likely has new and exciting bugs!
 *
 * @lucene.experimental
 *
 * @deprecated Use dimensional values in Lucene 6.0 instead */
@Deprecated
public class Geo3DDocValuesFormat extends DocValuesFormat {

  static final String DATA_CODEC_NAME = "Geo3DData";
  static final int DATA_VERSION_START = 0;
  static final int DATA_VERSION_CURRENT = DATA_VERSION_START;
  static final String DATA_EXTENSION = "g3dd";

  static final String META_CODEC_NAME = "Geo3DMeta";
  static final int META_VERSION_START = 0;
  static final int META_VERSION_CURRENT = META_VERSION_START;
  static final String META_EXTENSION = "g3dm";

  private final int maxPointsInLeafNode;
  private final int maxPointsSortInHeap;
  
  private final DocValuesFormat delegate = new Lucene54DocValuesFormat();

  private final PlanetModel planetModel;

  /** Default constructor */
  public Geo3DDocValuesFormat() {
    this(PlanetModel.WGS84, BKD3DTreeWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE, BKD3DTreeWriter.DEFAULT_MAX_POINTS_SORT_IN_HEAP);
  }

  /** Creates this with custom configuration.
   *
   * @param planetModel the {@link PlanetModel} to use; this is only used when writing
   * @param maxPointsInLeafNode Maximum number of points in each leaf cell.  Smaller values create a deeper tree with larger in-heap index and possibly
   *    faster searching.  The default is 1024.
   * @param maxPointsSortInHeap Maximum number of points where in-heap sort can be used.  When the number of points exceeds this, a (slower)
   *    offline sort is used.  The default is 128 * 1024.
   *
   * @lucene.experimental */
  public Geo3DDocValuesFormat(PlanetModel planetModel, int maxPointsInLeafNode, int maxPointsSortInHeap) {
    super("BKD3DTree");
    BKD3DTreeWriter.verifyParams(maxPointsInLeafNode, maxPointsSortInHeap);
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.maxPointsSortInHeap = maxPointsSortInHeap;
    this.planetModel = planetModel;
  }

  @Override
  public DocValuesConsumer fieldsConsumer(final SegmentWriteState state) throws IOException {
    return new Geo3DDocValuesConsumer(planetModel, delegate.fieldsConsumer(state), state, maxPointsInLeafNode, maxPointsSortInHeap);
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new Geo3DDocValuesProducer(delegate.fieldsProducer(state), state);
  }

  /** Clips the incoming value to the allowed min/max range before encoding, instead of throwing an exception. */
  static int encodeValueLenient(double planetMax, double x) {
    if (x > planetMax) {
      x = planetMax;
    } else if (x < -planetMax) {
      x = -planetMax;
    }
    return encodeValue(planetMax, x);
  }
    
  static int encodeValue(double planetMax, double x) {
    if (x > planetMax) {
      throw new IllegalArgumentException("value=" + x + " is out-of-bounds (greater than planetMax=" + planetMax + ")");
    }
    if (x < -planetMax) {
      throw new IllegalArgumentException("value=" + x + " is out-of-bounds (less than than -planetMax=" + -planetMax + ")");
    }
    long y = Math.round (x * (Integer.MAX_VALUE / planetMax));
    assert y >= Integer.MIN_VALUE;
    assert y <= Integer.MAX_VALUE;

    return (int) y;
  }

  /** Center decode */
  static double decodeValueCenter(double planetMax, int x) {
    return x * (planetMax / Integer.MAX_VALUE);
  }

  /** More negative decode, at bottom of cell */
  static double decodeValueMin(double planetMax, int x) {
    return (((double)x) - 0.5) * (planetMax / Integer.MAX_VALUE);
  }
  
  /** More positive decode, at top of cell  */
  static double decodeValueMax(double planetMax, int x) {
    return (((double)x) + 0.5) * (planetMax / Integer.MAX_VALUE);
  }
  

  static int readInt(byte[] bytes, int offset) {
    return ((bytes[offset] & 0xFF) << 24) | ((bytes[offset+1] & 0xFF) << 16)
         | ((bytes[offset+2] & 0xFF) <<  8) |  (bytes[offset+3] & 0xFF);
  }

  static void writeInt(int value, byte[] bytes, int offset) {
    bytes[offset] = (byte) ((value >> 24) & 0xff);
    bytes[offset+1] = (byte) ((value >> 16) & 0xff);
    bytes[offset+2] = (byte) ((value >> 8) & 0xff);
    bytes[offset+3] = (byte) (value & 0xff);
  }
}
