package org.apache.lucene.codecs.lucene40;

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
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.codecs.PerDocProducer;
import org.apache.lucene.index.DocValues; // javadocs
import org.apache.lucene.index.DocValues.Type; // javadocs
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.CompoundFileDirectory; // javadocs
import org.apache.lucene.store.DataOutput; // javadocs
import org.apache.lucene.util.packed.PackedInts; // javadocs

/**
 * Lucene 4.0 DocValues format.
 * <p>
 * Files:
 * <ul>
 *   <li><tt>.dv.cfs</tt>: {@link CompoundFileDirectory compound container}</li>
 *   <li><tt>.dv.cfe</tt>: {@link CompoundFileDirectory compound entries}</li>
 * </ul>
 * Entries within the compound file:
 * <ul>
 *   <li><tt>&lt;segment&gt;_&lt;fieldNumber&gt;.dat</tt>: data values</li>
 *   <li><tt>&lt;segment&gt;_&lt;fieldNumber&gt;.idx</tt>: index into the .dat for DEREF types</li>
 * </ul>
 * <p>
 * There are several many types of {@link DocValues} with different encodings.
 * From the perspective of filenames, all types store their values in <tt>.dat</tt>
 * entries within the compound file. In the case of dereferenced/sorted types, the <tt>.dat</tt>
 * actually contains only the unique values, and an additional <tt>.idx</tt> file contains
 * pointers to these unique values.
 * </p>
 * Formats:
 * <ul>
 *    <li>{@link Type#VAR_INTS VAR_INTS} .dat --&gt; Header, PackedType, MinValue, 
 *        DefaultValue, PackedStream</li>
 *    <li>{@link Type#FIXED_INTS_8 FIXED_INTS_8} .dat --&gt; Header, ValueSize, 
 *        {@link DataOutput#writeByte Byte}<sup>maxdoc</sup></li>
 *    <li>{@link Type#FIXED_INTS_16 FIXED_INTS_16} .dat --&gt; Header, ValueSize,
 *        {@link DataOutput#writeShort Short}<sup>maxdoc</sup></li>
 *    <li>{@link Type#FIXED_INTS_32 FIXED_INTS_32} .dat --&gt; Header, ValueSize,
 *        {@link DataOutput#writeInt Int32}<sup>maxdoc</sup></li>
 *    <li>{@link Type#FIXED_INTS_64 FIXED_INTS_64} .dat --&gt; Header, ValueSize,
 *        {@link DataOutput#writeLong Int64}<sup>maxdoc</sup></li>
 *    <li>{@link Type#FLOAT_32 FLOAT_32} .dat --&gt; Header, ValueSize,
 *        Float32<sup>maxdoc</sup></li>
 *    <li>{@link Type#FLOAT_64 FLOAT_64} .dat --&gt; Header, ValueSize,
 *        Float64<sup>maxdoc</sup></li>
 *    <li>{@link Type#BYTES_FIXED_STRAIGHT BYTES_FIXED_STRAIGHT} .dat --&gt; Header, ValueSize,
 *        ({@link DataOutput#writeByte Byte} * ValueSize)<sup>maxdoc</sup></li>
 *    <li>{@link Type#BYTES_VAR_STRAIGHT BYTES_VAR_STRAIGHT} .idx --&gt; Header, MaxAddress,
 *        Addresses</li>
 *    <li>{@link Type#BYTES_VAR_STRAIGHT BYTES_VAR_STRAIGHT} .dat --&gt; Header, TotalBytes,
 *        Addresses, ({@link DataOutput#writeByte Byte} *
 *        <i>variable ValueSize</i>)<sup>maxdoc</sup></li>
 *    <li>{@link Type#BYTES_FIXED_DEREF BYTES_FIXED_DEREF} .idx --&gt; Header, NumValues,
 *        Addresses</li>
 *    <li>{@link Type#BYTES_FIXED_DEREF BYTES_FIXED_DEREF} .dat --&gt; Header, ValueSize,
 *        ({@link DataOutput#writeByte Byte} * ValueSize)<sup>NumValues</sup></li>
 *    <li>{@link Type#BYTES_VAR_DEREF BYTES_VAR_DEREF} .idx --&gt; Header, TotalVarBytes,
 *        Addresses</li>
 *    <li>{@link Type#BYTES_VAR_DEREF BYTES_VAR_DEREF} .dat --&gt; Header,
 *        (LengthPrefix + {@link DataOutput#writeByte Byte} * <i>variable ValueSize</i>)<sup>NumValues</sup></li>
 *    <li>{@link Type#BYTES_FIXED_SORTED BYTES_FIXED_SORTED} .idx --&gt; Header, NumValues,
 *        Ordinals</li>
 *    <li>{@link Type#BYTES_FIXED_SORTED BYTES_FIXED_SORTED} .dat --&gt; Header, ValueSize,
 *        ({@link DataOutput#writeByte Byte} * ValueSize)<sup>NumValues</sup></li>
 *    <li>{@link Type#BYTES_VAR_SORTED BYTES_VAR_SORTED} .idx --&gt; Header, TotalVarBytes,
 *        Addresses, Ordinals</li>
 *    <li>{@link Type#BYTES_VAR_SORTED BYTES_VAR_SORTED} .dat --&gt; Header,
 *        ({@link DataOutput#writeByte Byte} * <i>variable ValueSize</i>)<sup>NumValues</sup></li>
 * </ul>
 * Data Types:
 * <ul>
 *    <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *    <li>PackedType --&gt; {@link DataOutput#writeByte Byte}</li>
 *    <li>MaxAddress, MinValue, DefaultValue --&gt; {@link DataOutput#writeLong Int64}</li>
 *    <li>PackedStream, Addresses, Ordinals --&gt; {@link PackedInts}</li>
 *    <li>ValueSize, NumValues --&gt; {@link DataOutput#writeInt Int32}</li>
 *    <li>Float32 --&gt; 32-bit float encoded with {@link Float#floatToRawIntBits(float)}
 *                       then written as {@link DataOutput#writeInt Int32}</li>
 *    <li>Float64 --&gt; 64-bit float encoded with {@link Double#doubleToRawLongBits(double)}
 *                       then written as {@link DataOutput#writeLong Int64}</li>
 *    <li>TotalBytes --&gt; {@link DataOutput#writeVLong VLong}</li>
 *    <li>TotalVarBytes --&gt; {@link DataOutput#writeLong Int64}</li>
 *    <li>LengthPrefix --&gt; Length of the data value as {@link DataOutput#writeVInt VInt} (maximum
 *                       of 2 bytes)</li>
 * </ul>
 * Notes:
 * <ul>
 *    <li>PackedType is a 0 when compressed, 1 when the stream is written as 64-bit integers.</li>
 *    <li>Addresses stores pointers to the actual byte location (indexed by docid). In the VAR_STRAIGHT
 *        case, each entry can have a different length, so to determine the length, docid+1 is 
 *        retrieved. A sentinel address is written at the end for the VAR_STRAIGHT case, so the Addresses 
 *        stream contains maxdoc+1 indices. For the deduplicated VAR_DEREF case, each length
 *        is encoded as a prefix to the data itself as a {@link DataOutput#writeVInt VInt} 
 *        (maximum of 2 bytes).</li>
 *    <li>Ordinals stores the term ID in sorted order (indexed by docid). In the FIXED_SORTED case,
 *        the address into the .dat can be computed from the ordinal as 
 *        <code>Header+ValueSize+(ordinal*ValueSize)</code> because the byte length is fixed.
 *        In the VAR_SORTED case, there is double indirection (docid -> ordinal -> address), but
 *        an additional sentinel ordinal+address is always written (so there are NumValues+1 ordinals). To
 *        determine the length, ord+1's address is looked up as well.</li>
 *    <li>{@link Type#BYTES_VAR_STRAIGHT BYTES_VAR_STRAIGHT} in contrast to other straight 
 *        variants uses a <tt>.idx</tt> file to improve lookup perfromance. In contrast to 
 *        {@link Type#BYTES_VAR_DEREF BYTES_VAR_DEREF} it doesn't apply deduplication of the document values.
 *    </li>
 * </ul>
 */
public class Lucene40DocValuesFormat extends DocValuesFormat {

  /** Sole constructor. */
  public Lucene40DocValuesFormat() {
  }

  @Override
  public PerDocConsumer docsConsumer(PerDocWriteState state) throws IOException {
    return new Lucene40DocValuesConsumer(state, Lucene40DocValuesConsumer.DOC_VALUES_SEGMENT_SUFFIX);
  }

  @Override
  public PerDocProducer docsProducer(SegmentReadState state) throws IOException {
    return new Lucene40DocValuesProducer(state, Lucene40DocValuesConsumer.DOC_VALUES_SEGMENT_SUFFIX);
  }
}
