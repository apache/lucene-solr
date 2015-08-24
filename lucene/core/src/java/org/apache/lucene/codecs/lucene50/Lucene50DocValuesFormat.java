package org.apache.lucene.codecs.lucene50;

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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.SmallFloat;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;

/**
 * Lucene 5.0 DocValues format.
 * <p>
 * Encodes the five per-document value types (Numeric,Binary,Sorted,SortedSet,SortedNumeric) with these strategies:
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
 *    <li>Const-compressed: when there is only one possible non-missing value, only the missing
 *        bitset is encoded.
 * </ul>
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
 *    <li>Sorted: a mapping of ordinals to deduplicated terms is written as Binary, 
 *        along with the per-document ordinals written using one of the numeric strategies above.
 * </ul>
 * <p>
 * {@link DocValuesType#SORTED_SET SORTED_SET}:
 * <ul>
 *    <li>Single: if all documents have 0 or 1 value, then data are written like SORTED.
 *    <li>SortedSet table: when there are few unique sets of values (&lt; 256) then each set is assigned
 *        an id, a lookup table is written and the mapping from document to set id is written using the
 *        numeric strategies above.
 *    <li>SortedSet: a mapping of ordinals to deduplicated terms is written as Binary, 
 *        an ordinal list and per-document index into this list are written using the numeric strategies 
 *        above.
 * </ul>
 * <p>
 * {@link DocValuesType#SORTED_NUMERIC SORTED_NUMERIC}:
 * <ul>
 *    <li>Single: if all documents have 0 or 1 value, then data are written like NUMERIC.
 *    <li>SortedSet table: when there are few unique sets of values (&lt; 256) then each set is assigned
 *        an id, a lookup table is written and the mapping from document to set id is written using the
 *        numeric strategies above.
 *    <li>SortedNumeric: a value list and per-document index into this list are written using the numeric
 *        strategies above.
 * </ul>
 * <p>
 * Files:
 * <ol>
 *   <li><tt>.dvd</tt>: DocValues data</li>
 *   <li><tt>.dvm</tt>: DocValues metadata</li>
 * </ol>
 * <ol>
 *   <li><a name="dvm"></a>
 *   <p>The DocValues metadata or .dvm file.</p>
 *   <p>For DocValues field, this stores metadata, such as the offset into the 
 *      DocValues data (.dvd)</p>
 *   <p>DocValues metadata (.dvm) --&gt; Header,&lt;Entry&gt;<sup>NumFields</sup>,Footer</p>
 *   <ul>
 *     <li>Entry --&gt; NumericEntry | BinaryEntry | SortedEntry | SortedSetEntry | SortedNumericEntry</li>
 *     <li>NumericEntry --&gt; GCDNumericEntry | TableNumericEntry | DeltaNumericEntry</li>
 *     <li>GCDNumericEntry --&gt; NumericHeader,MinValue,GCD,BitsPerValue</li>
 *     <li>TableNumericEntry --&gt; NumericHeader,TableSize,{@link DataOutput#writeLong Int64}<sup>TableSize</sup>,BitsPerValue</li>
 *     <li>DeltaNumericEntry --&gt; NumericHeader,MinValue,BitsPerValue</li>
 *     <li>MonotonicNumericEntry --&gt; NumericHeader,PackedVersion,BlockSize</li>
 *     <li>NumericHeader --&gt; FieldNumber,EntryType,NumericType,MissingOffset,DataOffset,Count,EndOffset</li>
 *     <li>BinaryEntry --&gt; FixedBinaryEntry | VariableBinaryEntry | PrefixBinaryEntry</li>
 *     <li>FixedBinaryEntry --&gt; BinaryHeader</li>
 *     <li>VariableBinaryEntry --&gt; BinaryHeader,AddressOffset,PackedVersion,BlockSize</li>
 *     <li>PrefixBinaryEntry --&gt; BinaryHeader,AddressInterval,AddressOffset,PackedVersion,BlockSize</li>
 *     <li>BinaryHeader --&gt; FieldNumber,EntryType,BinaryType,MissingOffset,MinLength,MaxLength,DataOffset</li>
 *     <li>SortedEntry --&gt; FieldNumber,EntryType,BinaryEntry,NumericEntry</li>
 *     <li>SortedSetEntry --&gt; SingleSortedSetEntry | AddressesSortedSetEntry | TableSortedSetEntry</li>
 *     <li>SingleSortedSetEntry --&gt; SetHeader,SortedEntry</li>
 *     <li>AddressesSortedSetEntry --&gt; SetHeader,BinaryEntry,NumericEntry,NumericEntry</li>
 *     <li>TableSortedSetEntry --&gt; SetHeader,TotalTableLength,{@link DataOutput#writeLong Int64}<sup>TotalTableLength</sup>,TableSize,{@link DataOutput#writeInt Int32}<sup>TableSize</sup>,BinaryEntry,NumericEntry</li>
 *     <li>SetHeader --&gt; FieldNumber,EntryType,SetType</li>
 *     <li>SortedNumericEntry --&gt; SingleSortedNumericEntry | AddressesSortedNumericEntry | TableSortedNumericEntry</li>
 *     <li>SingleNumericEntry --&gt; SetHeader,NumericEntry</li>
 *     <li>AddressesSortedNumericEntry --&gt; SetHeader,NumericEntry,NumericEntry</li>
 *     <li>TableSortedNumericEntry --&gt; SetHeader,TotalTableLength,{@link DataOutput#writeLong Int64}<sup>TotalTableLength</sup>,TableSize,{@link DataOutput#writeInt Int32}<sup>TableSize</sup>,NumericEntry</li>
 *     <li>FieldNumber,PackedVersion,MinLength,MaxLength,BlockSize,ValueCount --&gt; {@link DataOutput#writeVInt VInt}</li>
 *     <li>EntryType,CompressionType --&gt; {@link DataOutput#writeByte Byte}</li>
 *     <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 *     <li>MinValue,GCD,MissingOffset,AddressOffset,DataOffset,EndOffset --&gt; {@link DataOutput#writeLong Int64}</li>
 *     <li>TableSize,BitsPerValue,TotalTableLength --&gt; {@link DataOutput#writeVInt vInt}</li>
 *     <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 *   </ul>
 *   <p>Sorted fields have two entries: a BinaryEntry with the value metadata,
 *      and an ordinary NumericEntry for the document-to-ord metadata.</p>
 *   <p>FieldNumber of -1 indicates the end of metadata.</p>
 *   <p>EntryType is a 0 (NumericEntry) or 1 (BinaryEntry)</p>
 *   <p>DataOffset is the pointer to the start of the data in the DocValues data (.dvd)</p>
 *   <p>EndOffset is the pointer to the end of the data in the DocValues data (.dvd)</p>
 *   <p>NumericType indicates how Numeric values will be compressed:
 *      <ul>
 *         <li>0 --&gt; delta-compressed. For each block of 16k integers, every integer is delta-encoded
 *             from the minimum value within the block. 
 *         <li>1 --&gt; gcd-compressed. When all integers share a common divisor, only quotients are stored
 *             using blocks of delta-encoded ints.
 *         <li>2 --&gt; table-compressed. When the number of unique numeric values is small and it would save space,
 *             a lookup table of unique values is written, followed by the ordinal for each document.
 *         <li>3 --&gt; monotonic-compressed. Used to implement addressing for BINARY, SORTED_SET, SORTED_NUMERIC.
 *         <li>4 --&gt; const-compressed. Used when all non-missing values are the same.
 *      </ul>
 *   <p>BinaryType indicates how Binary values will be stored:
 *      <ul>
 *         <li>0 --&gt; fixed-width. All values have the same length, addressing by multiplication. 
 *         <li>1 --&gt; variable-width. An address for each value is stored.
 *         <li>2 --&gt; prefix-compressed. An address to the start of every interval'th value is stored.
 *      </ul>
 *   <p>SetType indicates how SortedSet and SortedNumeric values will be stored:
 *       <ul>
 *         <li>0 --&gt; with addresses. There are two numeric entries: a first one from document to start
 *             offset, and a second one from offset to ord/value.
 *         <li>1 --&gt; single-valued. Used when all documents have at most one value and is encoded like
 *             a regular Sorted/Numeric entry.
 *         <li>2 --&gt; table-encoded. A lookup table of unique sets of values is written, followed by a
 *             numeric entry that maps each document to an ordinal in this table.
 *       </ul>
 *   <p>MinLength and MaxLength represent the min and max byte[] value lengths for Binary values.
 *      If they are equal, then all values are of a fixed size, and can be addressed as DataOffset + (docID * length).
 *      Otherwise, the binary values are of variable size, and packed integer metadata (PackedVersion,BlockSize)
 *      is written for the addresses.
 *   <p>MissingOffset points to a byte[] containing a bitset of all documents that had a value for the field.
 *      If it's -1, then there are no missing values. If it's -2, all values are missing.
 *   <li><a name="dvd"></a>
 *   <p>The DocValues data or .dvd file.</p>
 *   <p>For DocValues field, this stores the actual per-document data (the heavy-lifting)</p>
 *   <p>DocValues data (.dvd) --&gt; Header,&lt;NumericData | BinaryData | SortedData&gt;<sup>NumFields</sup>,Footer</p>
 *   <ul>
 *     <li>NumericData --&gt; DeltaCompressedNumerics | TableCompressedNumerics | GCDCompressedNumerics</li>
 *     <li>BinaryData --&gt;  {@link DataOutput#writeByte Byte}<sup>DataLength</sup>,Addresses</li>
 *     <li>SortedData --&gt; {@link FST FST&lt;Int64&gt;}</li>
 *     <li>DeltaCompressedNumerics,TableCompressedNumerics,GCDCompressedNumerics --&gt; {@link DirectWriter PackedInts}</li>
 *     <li>Addresses --&gt; {@link MonotonicBlockPackedWriter MonotonicBlockPackedInts(blockSize=16k)}</li>
 *     <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 *   </ul>
 * </ol>
 * @lucene.experimental
 */
public final class Lucene50DocValuesFormat extends DocValuesFormat {

  /** Sole Constructor */
  public Lucene50DocValuesFormat() {
    super("Lucene50");
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new Lucene50DocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new Lucene50DocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
  }
  
  static final String DATA_CODEC = "Lucene50DocValuesData";
  static final String DATA_EXTENSION = "dvd";
  static final String META_CODEC = "Lucene50DocValuesMetadata";
  static final String META_EXTENSION = "dvm";
  static final int VERSION_START = 0;
  static final int VERSION_SORTEDSET_TABLE = 1;
  static final int VERSION_CURRENT = VERSION_SORTEDSET_TABLE;
  
  // indicates docvalues type
  static final byte NUMERIC = 0;
  static final byte BINARY = 1;
  static final byte SORTED = 2;
  static final byte SORTED_SET = 3;
  static final byte SORTED_NUMERIC = 4;
  
  // address terms in blocks of 16 terms
  static final int INTERVAL_SHIFT = 4;
  static final int INTERVAL_COUNT = 1 << INTERVAL_SHIFT;
  static final int INTERVAL_MASK = INTERVAL_COUNT - 1;
  
  // build reverse index from every 1024th term
  static final int REVERSE_INTERVAL_SHIFT = 10;
  static final int REVERSE_INTERVAL_COUNT = 1 << REVERSE_INTERVAL_SHIFT;
  static final int REVERSE_INTERVAL_MASK = REVERSE_INTERVAL_COUNT - 1;
  
  // for conversion from reverse index to block
  static final int BLOCK_INTERVAL_SHIFT = REVERSE_INTERVAL_SHIFT - INTERVAL_SHIFT;
  static final int BLOCK_INTERVAL_COUNT = 1 << BLOCK_INTERVAL_SHIFT;
  static final int BLOCK_INTERVAL_MASK = BLOCK_INTERVAL_COUNT - 1;

  /** Compressed using packed blocks of ints. */
  static final int DELTA_COMPRESSED = 0;
  /** Compressed by computing the GCD. */
  static final int GCD_COMPRESSED = 1;
  /** Compressed by giving IDs to unique values. */
  static final int TABLE_COMPRESSED = 2;
  /** Compressed with monotonically increasing values */
  static final int MONOTONIC_COMPRESSED = 3;
  /** Compressed with constant value (uses only missing bitset) */
  static final int CONST_COMPRESSED = 4;
  
  /** Uncompressed binary, written directly (fixed length). */
  static final int BINARY_FIXED_UNCOMPRESSED = 0;
  /** Uncompressed binary, written directly (variable length). */
  static final int BINARY_VARIABLE_UNCOMPRESSED = 1;
  /** Compressed binary with shared prefixes */
  static final int BINARY_PREFIX_COMPRESSED = 2;

  /** Standard storage for sorted set values with 1 level of indirection:
   *  {@code docId -> address -> ord}. */
  static final int SORTED_WITH_ADDRESSES = 0;
  /** Single-valued sorted set values, encoded as sorted values, so no level
   *  of indirection: {@code docId -> ord}. */
  static final int SORTED_SINGLE_VALUED = 1;
  /** Compressed giving IDs to unique sets of values:
   * {@code docId -> setId -> ords} */
  static final int SORTED_SET_TABLE = 2;
  
  /** placeholder for missing offset that means there are no missing values */
  static final int ALL_LIVE = -1;
  /** placeholder for missing offset that means all values are missing */
  static final int ALL_MISSING = -2;
  
  // addressing uses 16k blocks
  static final int MONOTONIC_BLOCK_SIZE = 16384;
}
