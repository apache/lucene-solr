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
package org.apache.lucene.codecs.lucene80;


import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.SmallFloat;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Lucene 8.0 DocValues format.
 * <p>
 * Documents that have a value for the field are encoded in a way that it is always possible to
 * know the ordinal of the current document in the set of documents that have a value. For instance,
 * say the set of documents that have a value for the field is <tt>{1, 5, 6, 11}</tt>. When the
 * iterator is on <tt>6</tt>, it knows that this is the 3rd item of the set. This way, values can
 * be stored densely and accessed based on their index at search time. If all documents in a segment
 * have a value for the field, the index is the same as the doc ID, so this case is encoded implicitly
 * and is very fast at query time. On the other hand if some documents are missing a value for the
 * field then the set of documents that have a value is encoded into blocks. All doc IDs that share
 * the same upper 16 bits are encoded into the same block with the following strategies:
 * <ul>
 *     <li>SPARSE: This strategy is used when a block contains at most 4095 documents. The lower 16
 *         bits of doc IDs are stored as {@link DataOutput#writeShort(short) shorts} while the upper
 *         16 bits are given by the block ID.
 *     <li>DENSE: This strategy is used when a block contains between 4096 and 65535 documents. The
 *         lower bits of doc IDs are stored in a bit set. Advancing &lt; 512 documents is performed using
 *         {@link Long#numberOfTrailingZeros(long) ntz} operations while the index is computed by
 *         accumulating the {@link Long#bitCount(long) bit counts} of the visited longs.
 *         Advancing &gt;= 512 documents is performed by skipping to the start of the needed 512 document
 *         sub-block and iterating to the specific document within that block. The index for the
 *         sub-block that is skipped to is retrieved from a rank-table positioned beforethe bit set.
 *         The rank-table holds the origo index numbers for all 512 documents sub-blocks, represented
 *         as an unsigned short for each 128 blocks.
 *     <li>ALL: This strategy is used when a block contains exactly 65536 documents, meaning that
 *         the block is full. In that case doc IDs do not need to be stored explicitly. This is
 *         typically faster than both SPARSE and DENSE which is a reason why it is preferable to have
 *         all documents that have a value for a field using contiguous doc IDs, for instance by
 *         using {@link IndexWriterConfig#setIndexSort(org.apache.lucene.search.Sort) index sorting}.
 * </ul>
 * <p>
 * Skipping blocks to arrive at a wanted document is either done on an iterative basis or by using the
 * jump-table stored at the end of the chain of blocks. The jump-table holds the offset as well as the
 * index for all blocks, packed in a single long per block.
 * </p>
 * <p>
 * Then the five per-document value types (Numeric,Binary,Sorted,SortedSet,SortedNumeric) are
 * encoded using the following strategies:
 * <p>
 * {@link DocValuesType#NUMERIC NUMERIC}:
 * <ul>
 *    <li>Delta-compressed: per-document integers written as deltas from the minimum value,
 *        compressed with bitpacking. For more information, see {@link DirectWriter}.
 *    <li>Table-compressed: when the number of unique values is very small (&lt; 256), and
 *        when there are unused "gaps" in the range of values used (such as {@link SmallFloat}),
 *        a lookup table is written instead. Each per-document entry is instead the ordinal
 *        to this table, and those ordinals are compressed with bitpacking ({@link DirectWriter}).
 *    <li>GCD-compressed: when all numbers share a common divisor, such as dates, the greatest
 *        common denominator (GCD) is computed, and quotients are stored using Delta-compressed Numerics.
 *    <li>Monotonic-compressed: when all numbers are monotonically increasing offsets, they are written
 *        as blocks of bitpacked integers, encoding the deviation from the expected delta.
 *    <li>Const-compressed: when there is only one possible value, no per-document data is needed and
 *        this value is encoded alone.
 * </ul>
 * <p>
 * Depending on calculated gains, the numbers might be split into blocks of 16384 values. In that case,
 * a jump-table with block offsets is appended to the blocks for O(1) access to the needed block.
 * </p>
 * <p>
 * {@link DocValuesType#BINARY BINARY}:
 * <ul>
 *    <li>Fixed-width Binary: one large concatenated byte[] is written, along with the fixed length.
 *        Each document's value can be addressed directly with multiplication ({@code docID * length}).
 *    <li>Variable-width Binary: one large concatenated byte[] is written, along with end addresses
 *        for each document. The addresses are written as Monotonic-compressed numerics.
 *    <li>Prefix-compressed Binary: values are written in chunks of 16, with the first value written
 *        completely and other values sharing prefixes. chunk addresses are written as Monotonic-compressed
 *        numerics. A reverse lookup index is written from a portion of every 1024th term.
 * </ul>
 * <p>
 * {@link DocValuesType#SORTED SORTED}:
 * <ul>
 *    <li>Sorted: a mapping of ordinals to deduplicated terms is written as Prefix-compressed Binary,
 *        along with the per-document ordinals written using one of the numeric strategies above.
 * </ul>
 * <p>
 * {@link DocValuesType#SORTED_SET SORTED_SET}:
 * <ul>
 *    <li>Single: if all documents have 0 or 1 value, then data are written like SORTED.
 *    <li>SortedSet: a mapping of ordinals to deduplicated terms is written as Binary,
 *        an ordinal list and per-document index into this list are written using the numeric strategies
 *        above.
 * </ul>
 * <p>
 * {@link DocValuesType#SORTED_NUMERIC SORTED_NUMERIC}:
 * <ul>
 *    <li>Single: if all documents have 0 or 1 value, then data are written like NUMERIC.
 *    <li>SortedNumeric: a value list and per-document index into this list are written using the numeric
 *        strategies above.
 * </ul>
 * <p>
 * Files:
 * <ol>
 *   <li><tt>.dvd</tt>: DocValues data</li>
 *   <li><tt>.dvm</tt>: DocValues metadata</li>
 * </ol>
 * @lucene.experimental
 */
public final class Lucene80DocValuesFormat extends DocValuesFormat {

  /** Configuration option for doc values. */
  public static enum Mode {
    /** Trade compression ratio for retrieval speed. */
    BEST_SPEED,
    /** Trade retrieval speed for compression ratio. */
    BEST_COMPRESSION
  }

  /** Attribute key for compression mode. */
  public static final String MODE_KEY = Lucene80DocValuesFormat.class.getSimpleName() + ".mode";

  private final Mode mode;

  /** Default constructor. */
  public Lucene80DocValuesFormat() {
    this(Mode.BEST_SPEED);
  }

  /** Constructor */
  public Lucene80DocValuesFormat(Mode mode) {
    super("Lucene80");
    this.mode = Objects.requireNonNull(mode);
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new Lucene80DocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION, mode);
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new Lucene80DocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
  }

  static final String DATA_CODEC = "Lucene80DocValuesData";
  static final String DATA_EXTENSION = "dvd";
  static final String META_CODEC = "Lucene80DocValuesMetadata";
  static final String META_EXTENSION = "dvm";
  static final int VERSION_START = 0;
  static final int VERSION_BIN_COMPRESSED = 1;  
  static final int VERSION_CONFIGURABLE_COMPRESSION = 2;
  static final int VERSION_CURRENT = VERSION_CONFIGURABLE_COMPRESSION;

  // indicates docvalues type
  static final byte NUMERIC = 0;
  static final byte BINARY = 1;
  static final byte SORTED = 2;
  static final byte SORTED_SET = 3;
  static final byte SORTED_NUMERIC = 4;

  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  static final int NUMERIC_BLOCK_SHIFT = 14;
  static final int NUMERIC_BLOCK_SIZE = 1 << NUMERIC_BLOCK_SHIFT;

  static final int BINARY_BLOCK_SHIFT = 5;
  static final int BINARY_DOCS_PER_COMPRESSED_BLOCK = 1 << BINARY_BLOCK_SHIFT;
  
  static final int TERMS_DICT_BLOCK_SHIFT = 4;
  static final int TERMS_DICT_BLOCK_SIZE = 1 << TERMS_DICT_BLOCK_SHIFT;
  static final int TERMS_DICT_BLOCK_MASK = TERMS_DICT_BLOCK_SIZE - 1;

  static final int TERMS_DICT_BLOCK_COMPRESSION_THRESHOLD = 32;
  static final int TERMS_DICT_BLOCK_LZ4_SHIFT = 6;
  static final int TERMS_DICT_BLOCK_LZ4_SIZE = 1 << TERMS_DICT_BLOCK_LZ4_SHIFT;
  static final int TERMS_DICT_BLOCK_LZ4_MASK = TERMS_DICT_BLOCK_LZ4_SIZE - 1;
  static final int TERMS_DICT_COMPRESSOR_LZ4_CODE = 1;
  // Writing a special code so we know this is a LZ4-compressed block.
  static final int TERMS_DICT_BLOCK_LZ4_CODE =
      TERMS_DICT_BLOCK_LZ4_SHIFT << 16 | TERMS_DICT_COMPRESSOR_LZ4_CODE;

  static final int TERMS_DICT_REVERSE_INDEX_SHIFT = 10;
  static final int TERMS_DICT_REVERSE_INDEX_SIZE = 1 << TERMS_DICT_REVERSE_INDEX_SHIFT;
  static final int TERMS_DICT_REVERSE_INDEX_MASK = TERMS_DICT_REVERSE_INDEX_SIZE - 1;
}
