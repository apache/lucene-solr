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
package org.apache.lucene.backward_codecs.lucene50;

import org.apache.lucene.backward_codecs.lucene50.compressing.Lucene50CompressingTermVectorsFormat;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.FieldsIndexWriter;
import org.apache.lucene.codecs.lucene87.Lucene87StoredFieldsFormat;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.BlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Lucene 5.0 {@link TermVectorsFormat term vectors format}.
 *
 * <p>Very similarly to {@link Lucene87StoredFieldsFormat}, this format is based on compressed
 * chunks of data, with document-level granularity so that a document can never span across distinct
 * chunks. Moreover, data is made as compact as possible:
 *
 * <ul>
 *   <li>textual data is compressed using the very light, <a
 *       href="http://code.google.com/p/lz4/">LZ4</a> compression algorithm,
 *   <li>binary data is written using fixed-size blocks of {@link PackedInts packed ints}.
 * </ul>
 *
 * <p>Term vectors are stored using two files
 *
 * <ul>
 *   <li>a data file where terms, frequencies, positions, offsets and payloads are stored,
 *   <li>an index file, loaded into memory, used to locate specific documents in the data file.
 * </ul>
 *
 * Looking up term vectors for any document requires at most 1 disk seek.
 *
 * <p><b>File formats</b>
 *
 * <ol>
 *   <li><a id="vector_data"></a>
 *       <p>A vector data file (extension <code>.tvd</code>). This file stores terms, frequencies,
 *       positions, offsets and payloads for every document. Upon writing a new segment, it
 *       accumulates data into memory until the buffer used to store terms and payloads grows beyond
 *       4KB. Then it flushes all metadata, terms and positions to disk using <a
 *       href="http://code.google.com/p/lz4/">LZ4</a> compression for terms and payloads and {@link
 *       BlockPackedWriter blocks of packed ints} for positions.
 *       <p>Here is a more detailed description of the field data file format:
 *       <ul>
 *         <li>VectorData (.tvd) --&gt; &lt;Header&gt;, PackedIntsVersion, ChunkSize,
 *             &lt;Chunk&gt;<sup>ChunkCount</sup>, ChunkCount, DirtyChunkCount, Footer
 *         <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *         <li>PackedIntsVersion --&gt; {@link PackedInts#VERSION_CURRENT} as a {@link
 *             DataOutput#writeVInt VInt}
 *         <li>ChunkSize is the number of bytes of terms to accumulate before flushing, as a {@link
 *             DataOutput#writeVInt VInt}
 *         <li>ChunkCount is not known in advance and is the number of chunks necessary to store all
 *             document of the segment
 *         <li>Chunk --&gt; DocBase, ChunkDocs, &lt; NumFields &gt;, &lt; FieldNums &gt;, &lt;
 *             FieldNumOffs &gt;, &lt; Flags &gt;, &lt; NumTerms &gt;, &lt; TermLengths &gt;, &lt;
 *             TermFreqs &gt;, &lt; Positions &gt;, &lt; StartOffsets &gt;, &lt; Lengths &gt;, &lt;
 *             PayloadLengths &gt;, &lt; TermAndPayloads &gt;
 *         <li>DocBase is the ID of the first doc of the chunk as a {@link DataOutput#writeVInt
 *             VInt}
 *         <li>ChunkDocs is the number of documents in the chunk
 *         <li>NumFields --&gt; DocNumFields<sup>ChunkDocs</sup>
 *         <li>DocNumFields is the number of fields for each doc, written as a {@link
 *             DataOutput#writeVInt VInt} if ChunkDocs==1 and as a {@link PackedInts} array
 *             otherwise
 *         <li>FieldNums --&gt; FieldNumDelta<sup>TotalDistincFields</sup>, a delta-encoded list of
 *             the sorted unique field numbers present in the chunk
 *         <li>FieldNumOffs --&gt; FieldNumOff<sup>TotalFields</sup>, as a {@link PackedInts} array
 *         <li>FieldNumOff is the offset of the field number in FieldNums
 *         <li>TotalFields is the total number of fields (sum of the values of NumFields)
 *         <li>Flags --&gt; Bit &lt; FieldFlags &gt;
 *         <li>Bit is a single bit which when true means that fields have the same options for every
 *             document in the chunk
 *         <li>FieldFlags --&gt; if Bit==1: Flag<sup>TotalDistinctFields</sup> else
 *             Flag<sup>TotalFields</sup>
 *         <li>Flag: a 3-bits int where:
 *             <ul>
 *               <li>the first bit means that the field has positions
 *               <li>the second bit means that the field has offsets
 *               <li>the third bit means that the field has payloads
 *             </ul>
 *         <li>NumTerms --&gt; FieldNumTerms<sup>TotalFields</sup>
 *         <li>FieldNumTerms: the number of terms for each field, using {@link BlockPackedWriter
 *             blocks of 64 packed ints}
 *         <li>TermLengths --&gt; PrefixLength<sup>TotalTerms</sup>
 *             SuffixLength<sup>TotalTerms</sup>
 *         <li>TotalTerms: total number of terms (sum of NumTerms)
 *         <li>PrefixLength: 0 for the first term of a field, the common prefix with the previous
 *             term otherwise using {@link BlockPackedWriter blocks of 64 packed ints}
 *         <li>SuffixLength: length of the term minus PrefixLength for every term using {@link
 *             BlockPackedWriter blocks of 64 packed ints}
 *         <li>TermFreqs --&gt; TermFreqMinus1<sup>TotalTerms</sup>
 *         <li>TermFreqMinus1: (frequency - 1) for each term using {@link BlockPackedWriter blocks
 *             of 64 packed ints}
 *         <li>Positions --&gt; PositionDelta<sup>TotalPositions</sup>
 *         <li>TotalPositions is the sum of frequencies of terms of all fields that have positions
 *         <li>PositionDelta: the absolute position for the first position of a term, and the
 *             difference with the previous positions for following positions using {@link
 *             BlockPackedWriter blocks of 64 packed ints}
 *         <li>StartOffsets --&gt; (AvgCharsPerTerm<sup>TotalDistinctFields</sup>)
 *             StartOffsetDelta<sup>TotalOffsets</sup>
 *         <li>TotalOffsets is the sum of frequencies of terms of all fields that have offsets
 *         <li>AvgCharsPerTerm: average number of chars per term, encoded as a float on 4 bytes.
 *             They are not present if no field has both positions and offsets enabled.
 *         <li>StartOffsetDelta: (startOffset - previousStartOffset - AvgCharsPerTerm *
 *             PositionDelta). previousStartOffset is 0 for the first offset and AvgCharsPerTerm is
 *             0 if the field has no positions using {@link BlockPackedWriter blocks of 64 packed
 *             ints}
 *         <li>Lengths --&gt; LengthMinusTermLength<sup>TotalOffsets</sup>
 *         <li>LengthMinusTermLength: (endOffset - startOffset - termLength) using {@link
 *             BlockPackedWriter blocks of 64 packed ints}
 *         <li>PayloadLengths --&gt; PayloadLength<sup>TotalPayloads</sup>
 *         <li>TotalPayloads is the sum of frequencies of terms of all fields that have payloads
 *         <li>PayloadLength is the payload length encoded using {@link BlockPackedWriter blocks of
 *             64 packed ints}
 *         <li>TermAndPayloads --&gt; LZ4-compressed representation of &lt; FieldTermsAndPayLoads
 *             &gt;<sup>TotalFields</sup>
 *         <li>FieldTermsAndPayLoads --&gt; Terms (Payloads)
 *         <li>Terms: term bytes
 *         <li>Payloads: payload bytes (if the field has payloads)
 *         <li>ChunkCount --&gt; the number of chunks in this file
 *         <li>DirtyChunkCount --&gt; the number of prematurely flushed chunks in this file
 *         <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 *       </ul>
 *   <li><a id="vector_index"></a>
 *       <p>An index file (extension <code>.tvx</code>).
 *       <ul>
 *         <li>VectorIndex (.tvx) --&gt; &lt;Header&gt;, &lt;ChunkIndex&gt;, Footer
 *         <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *         <li>ChunkIndex: See {@link FieldsIndexWriter}
 *         <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 *       </ul>
 * </ol>
 *
 * @lucene.experimental
 */
public final class Lucene50TermVectorsFormat extends Lucene50CompressingTermVectorsFormat {

  /** Sole constructor. */
  public Lucene50TermVectorsFormat() {
    super("Lucene50TermVectorsData", "", CompressionMode.FAST, 1 << 12, 10);
  }
}
