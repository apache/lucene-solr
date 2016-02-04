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
package org.apache.lucene.rangetree;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene54.Lucene54DocValuesFormat;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField; // javadocs
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * A {@link DocValuesFormat} to efficiently index numeric values from
 * from {@link SortedNumericDocValuesField} or BytesRef values from {@link SortedSetDocValuesField}
 * for numeric range queries using ({@link NumericRangeTreeQuery}) and arbitrary binary
 * range queries using {@link SortedSetRangeTreeQuery}.
 *
 * <p>This wraps {@link Lucene54DocValuesFormat}, but saves its own numeric tree
 * structures to disk for fast query-time intersection. See <a
 * href="https://www.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf">this paper</a>
 * for details.
 *
 * <p>The numeric tree slices up 1D space into smaller and
 * smaller ranges, until the smallest ranges have approximately
 * between X/2 and X (X default is 1024) values in them, at which point
 * such leaf cells are written as a block to disk, while the index tree
 * structure records how space was sub-divided is loaded into HEAP
 * at search time.  At search time, the tree is recursed based on whether
 * each of left or right child overlap with the query range, and once
 * a leaf block is reached, all documents in that leaf block are collected
 * if the cell is fully enclosed by the query shape, or filtered and then
 * collected, if not.
 *
 * <p>The index is also quite compact, because docs only appear once in
 * the tree (no "prefix terms").
 *
 * <p>In addition to the files written by {@link Lucene54DocValuesFormat}, this format writes:
 * <ol>
 *   <li><tt>.ndd</tt>: numeric tree leaf data and index</li>
 *   <li><tt>.ndm</tt>: numeric tree metadata</li>
 * </ol>
 *
 * <p>The disk format is experimental and free to change suddenly, and this code likely has new and exciting bugs!
 *
 * @lucene.experimental */

public class RangeTreeDocValuesFormat extends DocValuesFormat {

  static final String DATA_CODEC_NAME = "RangeTreeData";
  static final int DATA_VERSION_START = 0;
  static final int DATA_VERSION_CURRENT = DATA_VERSION_START;
  static final String DATA_EXTENSION = "ndd";

  static final String META_CODEC_NAME = "RangeTreeMeta";
  static final int META_VERSION_START = 0;
  static final int META_VERSION_CURRENT = META_VERSION_START;
  static final String META_EXTENSION = "ndm";

  private final int maxPointsInLeafNode;
  private final int maxPointsSortInHeap;
  
  private final DocValuesFormat delegate = new Lucene54DocValuesFormat();

  /** Default constructor */
  public RangeTreeDocValuesFormat() {
    this(RangeTreeWriter.DEFAULT_MAX_VALUES_IN_LEAF_NODE, RangeTreeWriter.DEFAULT_MAX_VALUES_SORT_IN_HEAP);
  }

  /** Creates this with custom configuration.
   *
   * @param maxPointsInLeafNode Maximum number of points in each leaf cell.  Smaller values create a deeper tree with larger in-heap index and possibly
   *    faster searching.  The default is 1024.
   * @param maxPointsSortInHeap Maximum number of points where in-heap sort can be used.  When the number of points exceeds this, a (slower)
   *    offline sort is used.  The default is 128 * 1024.
   *
   * @lucene.experimental */
  public RangeTreeDocValuesFormat(int maxPointsInLeafNode, int maxPointsSortInHeap) {
    super("RangeTree");
    RangeTreeWriter.verifyParams(maxPointsInLeafNode, maxPointsSortInHeap);
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.maxPointsSortInHeap = maxPointsSortInHeap;
  }

  @Override
  public DocValuesConsumer fieldsConsumer(final SegmentWriteState state) throws IOException {
    return new RangeTreeDocValuesConsumer(delegate.fieldsConsumer(state), state, maxPointsInLeafNode, maxPointsSortInHeap);
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new RangeTreeDocValuesProducer(delegate.fieldsProducer(state), state);
  }
}
