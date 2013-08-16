package org.apache.lucene.codecs.lucene45;

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
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.SmallFloat;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.packed.BlockPackedWriter;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Lucene 4.5 DocValues format.
 * <p>
 * Encodes the four per-document value types (Numeric,Binary,Sorted,SortedSet) with these strategies:
 * <p>
 * {@link DocValuesType#NUMERIC NUMERIC}:
 * <ul>
 *    <li>Delta-compressed: per-document integers written in blocks of 16k. For each block
 *        the minimum value in that block is encoded, and each entry is a delta from that 
 *        minimum value. Each block of deltas is compressed with bitpacking. For more 
 *        information, see {@link BlockPackedWriter}.
 *    <li>Table-compressed: when the number of unique values is very small (&lt; 256), and
 *        when there are unused "gaps" in the range of values used (such as {@link SmallFloat}), 
 *        a lookup table is written instead. Each per-document entry is instead the ordinal 
 *        to this table, and those ordinals are compressed with bitpacking ({@link PackedInts}). 
 *    <li>GCD-compressed: when all numbers share a common divisor, such as dates, the greatest
 *        common denominator (GCD) is computed, and quotients are stored using Delta-compressed Numerics.
 * </ul>
 * <p>
 * {@link DocValuesType#BINARY BINARY}:
 * <ul>
 *    <li>Fixed-width Binary: one large concatenated byte[] is written, along with the fixed length.
 *        Each document's value can be addressed directly with multiplication ({@code docID * length}). 
 *    <li>Variable-width Binary: one large concatenated byte[] is written, along with end addresses 
 *        for each document. The addresses are written in blocks of 16k, with the current absolute
 *        start for the block, and the average (expected) delta per entry. For each document the 
 *        deviation from the delta (actual - expected) is written.
 *    <li>Prefix-compressed Binary: nocommit
 * </ul>
 * <p>
 * {@link DocValuesType#SORTED SORTED}:
 * <ul>
 *    <li>Sorted: an FST mapping deduplicated terms to ordinals is written, along with the per-document
 *        ordinals written using one of the numeric strategies above.
 * </ul>
 * <p>
 * {@link DocValuesType#SORTED_SET SORTED_SET}:
 * <ul>
 *    <li>SortedSet: an FST mapping deduplicated terms to ordinals is written, along with the per-document
 *        ordinal list written using one of the binary strategies above.  
 * </ul>
 * <p>
 * Files:
 * <ol>
 *   <li><tt>.dvd</tt>: DocValues data</li>
 *   <li><tt>.dvm</tt>: DocValues metadata</li>
 * </ol>
 * <ol>
 *   <li><a name="dvm" id="dvm"></a>
 *   <p>The DocValues metadata or .dvm file.</p>
 *   <p>For DocValues field, this stores metadata, such as the offset into the 
 *      DocValues data (.dvd)</p>
 *   <p>DocValues metadata (.dvm) --&gt; Header,&lt;FieldNumber,EntryType,Entry&gt;<sup>NumFields</sup></p>
 *   <ul>
 *     <li>Entry --&gt; NumericEntry | BinaryEntry | SortedEntry</li>
 *     <li>NumericEntry --&gt; DataOffset,NumericCompressionType,PackedVersion</li>
 *     <li>BinaryEntry --&gt; DataOffset,DataLength,MinLength,MaxLength,PackedVersion?,BlockSize?</li>
 *     <li>SortedEntry --&gt; DataOffset,ValueCount</li>
 *     <li>FieldNumber,PackedVersion,MinLength,MaxLength,BlockSize,ValueCount --&gt; {@link DataOutput#writeVInt VInt}</li>
 *     <li>DataOffset,DataLength --&gt; {@link DataOutput#writeLong Int64}</li>
 *     <li>EntryType,CompressionType --&gt; {@link DataOutput#writeByte Byte}</li>
 *     <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   </ul>
 *   <p>Sorted fields have two entries: a SortedEntry with the FST metadata,
 *      and an ordinary NumericEntry for the document-to-ord metadata.</p>
 *   <p>SortedSet fields have two entries: a SortedEntry with the FST metadata,
 *      and an ordinary BinaryEntry for the document-to-ord-list metadata.</p>
 *   <p>FieldNumber of -1 indicates the end of metadata.</p>
 *   <p>EntryType is a 0 (NumericEntry), 1 (BinaryEntry, or 2 (SortedEntry)</p>
 *   <p>DataOffset is the pointer to the start of the data in the DocValues data (.dvd)</p>
 *   <p>NumericCompressionType indicates how Numeric values will be compressed:
 *      <ul>
 *         <li>0 --&gt; delta-compressed. For each block of 16k integers, every integer is delta-encoded
 *             from the minimum value within the block. 
 *         <li>1 --&gt, gcd-compressed. When all integers share a common divisor, only quotients are stored
 *             using blocks of delta-encoded ints.
 *         <li>2 --&gt; table-compressed. When the number of unique numeric values is small and it would save space,
 *             a lookup table of unique values is written, followed by the ordinal for each document.
 *      </ul>
 *   <p>MinLength and MaxLength represent the min and max byte[] value lengths for Binary values.
 *      If they are equal, then all values are of a fixed size, and can be addressed as DataOffset + (docID * length).
 *      Otherwise, the binary values are of variable size, and packed integer metadata (PackedVersion,BlockSize)
 *      is written for the addresses.
 *   <li><a name="dvd" id="dvd"></a>
 *   <p>The DocValues data or .dvd file.</p>
 *   <p>For DocValues field, this stores the actual per-document data (the heavy-lifting)</p>
 *   <p>DocValues data (.dvd) --&gt; Header,&lt;NumericData | BinaryData | SortedData&gt;<sup>NumFields</sup></p>
 *   <ul>
 *     <li>NumericData --&gt; DeltaCompressedNumerics | TableCompressedNumerics | GCDCompressedNumerics</li>
 *     <li>BinaryData --&gt;  {@link DataOutput#writeByte Byte}<sup>DataLength</sup>,Addresses</li>
 *     <li>SortedData --&gt; {@link FST FST&lt;Int64&gt;}</li>
 *     <li>DeltaCompressedNumerics --&gt; {@link BlockPackedWriter BlockPackedInts(blockSize=16k)}</li>
 *     <li>TableCompressedNumerics --&gt; TableSize,{@link DataOutput#writeLong Int64}<sup>TableSize</sup>,{@link PackedInts PackedInts}</li>
 *     <li>GCDCompressedNumerics --&gt; MinValue,GCD,{@link BlockPackedWriter BlockPackedInts(blockSize=16k)}</li>
 *     <li>Addresses --&gt; {@link MonotonicBlockPackedWriter MonotonicBlockPackedInts(blockSize=16k)}</li>
 *     <li>TableSize --&gt; {@link DataOutput#writeVInt vInt}</li>
 *     <li>MinValue --&gt; {@link DataOutput#writeLong Int64}</li>
 *     <li>GCD --&gt; {@link DataOutput#writeLong Int64}</li>
 *   </ul>
 *   <p>SortedSet entries store the list of ordinals in their BinaryData as a
 *      sequences of increasing {@link DataOutput#writeVLong vLong}s, delta-encoded.</p>
 * </ol>
 * @lucene.experimental
 */
// nocommit: docs are incomplete
public final class Lucene45DocValuesFormat extends DocValuesFormat {

  public Lucene45DocValuesFormat() {
    super("Lucene45");
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new Lucene45DocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new Lucene45DocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
  }
  
  public static final String DATA_CODEC = "Lucene45DocValuesData";
  public static final String DATA_EXTENSION = "dvd";
  public static final String META_CODEC = "Lucene45ValuesMetadata";
  public static final String META_EXTENSION = "dvm";
  public static final int VERSION_START = 0;
  public static final int VERSION_CURRENT = VERSION_START;
  public static final byte NUMERIC = 0;
  public static final byte BINARY = 1;
  public static final byte SORTED = 2;
  public static final byte SORTED_SET = 3;
}
