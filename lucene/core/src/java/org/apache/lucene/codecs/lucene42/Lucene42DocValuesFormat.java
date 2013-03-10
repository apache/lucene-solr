package org.apache.lucene.codecs.lucene42;

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
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.BlockPackedWriter;

/**
 * Lucene 4.2 DocValues format.
 * <p>
 * Encodes the four per-document value types (Numeric,Binary,Sorted,SortedSet) with seven basic strategies.
 * <p>
 * <ul>
 *    <li>Delta-compressed Numerics: per-document integers written in blocks of 4096. For each block
 *        the minimum value is encoded, and each entry is a delta from that minimum value.
 *    <li>Table-compressed Numerics: when the number of unique values is very small, a lookup table
 *        is written instead. Each per-document entry is instead the ordinal to this table.
 *    <li>Uncompressed Numerics: when all values would fit into a single byte, and the 
 *        <code>acceptableOverheadRatio</code> would pack values into 8 bits per value anyway, they
 *        are written as absolute values (with no indirection or packing) for performance.
 *    <li>Fixed-width Binary: one large concatenated byte[] is written, along with the fixed length.
 *        Each document's value can be addressed by maxDoc*length. 
 *    <li>Variable-width Binary: one large concatenated byte[] is written, along with end addresses 
 *        for each document. The addresses are written in blocks of 4096, with the current absolute
 *        start for the block, and the average (expected) delta per entry. For each document the 
 *        deviation from the delta (actual - expected) is written.
 *    <li>Sorted: an FST mapping deduplicated terms to ordinals is written, along with the per-document
 *        ordinals written using one of the numeric strategies above.
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
 *     <li>NumericEntry --&gt; DataOffset,CompressionType,PackedVersion</li>
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
 *   <p>CompressionType indicates how Numeric values will be compressed:
 *      <ul>
 *         <li>0 --&gt; delta-compressed. For each block of 4096 integers, every integer is delta-encoded
 *             from the minimum value within the block. 
 *         <li>1 --&gt; table-compressed. When the number of unique numeric values is small and it would save space,
 *             a lookup table of unique values is written, followed by the ordinal for each document.
 *         <li>2 --&gt; uncompressed. When the <code>acceptableOverheadRatio</code> parameter would upgrade the number
 *             of bits required to 8, and all values fit in a byte, these are written as absolute binary values
 *             for performance.
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
 *     <li>NumericData --&gt; DeltaCompressedNumerics | TableCompressedNumerics | UncompressedNumerics</li>
 *     <li>BinaryData --&gt;  {@link DataOutput#writeByte Byte}<sup>DataLength</sup>,Addresses</li>
 *     <li>SortedData --&gt; {@link FST FST&lt;Int64&gt;}</li>
 *     <li>DeltaCompressedNumerics --&gt; {@link BlockPackedWriter BlockPackedInts(blockSize=4096)}</li>
 *     <li>TableCompressedNumerics --&gt; TableSize,{@link DataOutput#writeLong Int64}<sup>TableSize</sup>,{@link PackedInts PackedInts}</li>
 *     <li>UncompressedNumerics --&gt; {@link DataOutput#writeByte Byte}<sup>maxdoc</sup></li>
 *     <li>Addresses --&gt; {@link MonotonicBlockPackedWriter MonotonicBlockPackedInts(blockSize=4096)}</li>
 *   </ul>
 *   <p>SortedSet entries store the list of ordinals in their BinaryData as a
 *      sequences of increasing {@link DataOutput#writeVLong vLong}s, delta-encoded.</p>       
 * </ol>
 */
public final class Lucene42DocValuesFormat extends DocValuesFormat {

  /** Sole constructor */
  public Lucene42DocValuesFormat() {
    super("Lucene42");
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    // note: we choose DEFAULT here (its reasonably fast, and for small bpv has tiny waste)
    return new Lucene42DocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION, PackedInts.DEFAULT);
  }
  
  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new Lucene42DocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION);
  }
  
  private static final String DATA_CODEC = "Lucene42DocValuesData";
  private static final String DATA_EXTENSION = "dvd";
  private static final String METADATA_CODEC = "Lucene42DocValuesMetadata";
  private static final String METADATA_EXTENSION = "dvm";
}
