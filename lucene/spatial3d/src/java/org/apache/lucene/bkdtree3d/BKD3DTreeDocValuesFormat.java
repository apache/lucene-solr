package org.apache.lucene.bkdtree3d;

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

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene50.Lucene50DocValuesFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * A {@link DocValuesFormat} to efficiently index geo-spatial 3D x,y,z points
 * from {@link BKD3DPointField} for fast shape intersection queries using
 * ({@code BKD3DShapeQuery}) // nocommit use link here
 *
 * <p>This wraps {@link Lucene50DocValuesFormat}, but saves its own BKD tree
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
 * <p>In addition to the files written by {@link Lucene50DocValuesFormat}, this format writes:
 * <ol>
 *   <li><tt>.kd3d</tt>: BKD leaf data and index</li>
 *   <li><tt>.kd3m</tt>: BKD metadata</li>
 * </ol>
 *
 * <p>The disk format is experimental and free to change suddenly, and this code
 * likely has new and exciting bugs!
 *
 * @lucene.experimental */

// nocommit rename to Geo3DDVF

public class BKD3DTreeDocValuesFormat extends DocValuesFormat {

  static final String DATA_CODEC_NAME = "BKD3DData";
  static final int DATA_VERSION_START = 0;
  static final int DATA_VERSION_CURRENT = DATA_VERSION_START;
  static final String DATA_EXTENSION = "kd3d";

  static final String META_CODEC_NAME = "BKD3DMeta";
  static final int META_VERSION_START = 0;
  static final int META_VERSION_CURRENT = META_VERSION_START;
  static final String META_EXTENSION = "kd3m";

  private final int maxPointsInLeafNode;
  private final int maxPointsSortInHeap;
  
  private final DocValuesFormat delegate = new Lucene50DocValuesFormat();

  /** Default constructor */
  public BKD3DTreeDocValuesFormat() {
    this(BKD3DTreeWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE, BKD3DTreeWriter.DEFAULT_MAX_POINTS_SORT_IN_HEAP);
  }

  /** Creates this with custom configuration.
   *
   * @param maxPointsInLeafNode Maximum number of points in each leaf cell.  Smaller values create a deeper tree with larger in-heap index and possibly
   *    faster searching.  The default is 1024.
   * @param maxPointsSortInHeap Maximum number of points where in-heap sort can be used.  When the number of points exceeds this, a (slower)
   *    offline sort is used.  The default is 128 * 1024.
   *
   * @lucene.experimental */
  public BKD3DTreeDocValuesFormat(int maxPointsInLeafNode, int maxPointsSortInHeap) {
    super("BKD3DTree");
    BKD3DTreeWriter.verifyParams(maxPointsInLeafNode, maxPointsSortInHeap);
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.maxPointsSortInHeap = maxPointsSortInHeap;
  }

  @Override
  public DocValuesConsumer fieldsConsumer(final SegmentWriteState state) throws IOException {
    return new BKD3DTreeDocValuesConsumer(delegate.fieldsConsumer(state), state, maxPointsInLeafNode, maxPointsSortInHeap);
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new BKD3DTreeDocValuesProducer(delegate.fieldsProducer(state), state);
  }

  // nocommit is this ok?  PlanetModel.WGS84 seems to have max 1.0011188180710464 ?
  static final double MAX_ABS_VALUE = 1.002d;

  private static final double SCALE = Integer.MAX_VALUE / MAX_ABS_VALUE;

  static int encodeValue(double x) {
    if (x < -MAX_ABS_VALUE) {
      throw new IllegalArgumentException("value=" + x + " is out-of-bounds (less than MIN_VALUE=" + (-MAX_ABS_VALUE) + ")");
    }
    if (x > MAX_ABS_VALUE) {
      throw new IllegalArgumentException("value=" + x + " is out-of-bounds (greater than MAX_VALUE=" + MAX_ABS_VALUE + ")");
    }
    long y = (long) (x * SCALE);
    assert y >= Integer.MIN_VALUE;
    assert y <= Integer.MAX_VALUE;

    return (int) y;
  }

  static double decodeValue(int x) {
    return x / SCALE;
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
