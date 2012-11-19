package org.apache.lucene.codecs.lucene41;

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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsFormat;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Lucene 4.1 stored fields format.
 *
 * <p><b>Principle</b></p>
 * <p>This {@link StoredFieldsFormat} compresses blocks of 16KB of documents in
 * order to improve the compression ratio compared to document-level
 * compression. It uses the <a href="http://code.google.com/p/lz4/">LZ4</a>
 * compression algorithm, which is fast to compress and very fast to decompress
 * data. Although the compression method that is used focuses more on speed
 * than on compression ratio, it should provide interesting compression ratios
 * for redundant inputs (such as log files, HTML or plain text).</p>
 * <p><b>File formats</b></p>
 * <p>Stored fields are represented by two files:</p>
 * <ol>
 * <li><a name="field_data" id="field_data"></a>
 * <p>A fields data file (extension <tt>.fdt</tt>). This file stores a compact
 * representation of documents in compressed blocks of 16KB or more. When
 * writing a segment, documents are appended to an in-memory <tt>byte[]</tt>
 * buffer. When its size reaches 16KB or more, some metadata about the documents
 * is flushed to disk, immediately followed by a compressed representation of
 * the buffer using the
 * <a href="http://code.google.com/p/lz4/">LZ4</a>
 * <a href="http://fastcompression.blogspot.fr/2011/05/lz4-explained.html">compression format</a>.</p>
 * <p>Here is a more detailed description of the field data file format:</p>
 * <ul>
 * <li>FieldData (.fdt) --&gt; &lt;Header&gt;, PackedIntsVersion, CompressionFormat, &lt;Chunk&gt;<sup>ChunkCount</sup></li>
 * <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 * <li>PackedIntsVersion --&gt; {@link PackedInts#VERSION_CURRENT} as a {@link DataOutput#writeVInt VInt}</li>
 * <li>ChunkCount is not known in advance and is the number of chunks necessary to store all document of the segment</li>
 * <li>Chunk --&gt; DocBase, ChunkDocs, DocFieldCounts, DocLengths, &lt;CompressedDocs&gt;</li>
 * <li>DocBase --&gt; the ID of the first document of the chunk as a {@link DataOutput#writeVInt VInt}</li>
 * <li>ChunkDocs --&gt; the number of documents in the chunk as a {@link DataOutput#writeVInt VInt}</li>
 * <li>DocFieldCounts --&gt; the number of stored fields of every document in the chunk, encoded as followed:<ul>
 *   <li>if chunkDocs=1, the unique value is encoded as a {@link DataOutput#writeVInt VInt}</li>
 *   <li>else read a {@link DataOutput#writeVInt VInt} (let's call it <tt>bitsRequired</tt>)<ul>
 *     <li>if <tt>bitsRequired</tt> is <tt>0</tt> then all values are equal, and the common value is the following {@link DataOutput#writeVInt VInt}</li>
 *     <li>else <tt>bitsRequired</tt> is the number of bits required to store any value, and values are stored in a {@link PackedInts packed} array where every value is stored on exactly <tt>bitsRequired</tt> bits</li>
 *   </ul></li>
 * </ul></li>
 * <li>DocLengths --&gt; the lengths of all documents in the chunk, encoded with the same method as DocFieldCounts</li>
 * <li>CompressedDocs --&gt; a compressed representation of &lt;Docs&gt; using the LZ4 compression format</li>
 * <li>Docs --&gt; &lt;Doc&gt;<sup>ChunkDocs</sup></li>
 * <li>Doc --&gt; &lt;FieldNumAndType, Value&gt;<sup>DocFieldCount</sup></li>
 * <li>FieldNumAndType --&gt; a {@link DataOutput#writeVLong VLong}, whose 3 last bits are Type and other bits are FieldNum</li>
 * <li>Type --&gt;<ul>
 *   <li>0: Value is String</li>
 *   <li>1: Value is BinaryValue</li>
 *   <li>2: Value is Int</li>
 *   <li>3: Value is Float</li>
 *   <li>4: Value is Long</li>
 *   <li>5: Value is Double</li>
 *   <li>6, 7: unused</li>
 * </ul></li>
 * <li>FieldNum --&gt; an ID of the field</li>
 * <li>Value --&gt; {@link DataOutput#writeString(String) String} | BinaryValue | Int | Float | Long | Double depending on Type</li>
 * <li>BinaryValue --&gt; ValueLength &lt;Byte&gt;<sup>ValueLength</sup></li>
 * </ul>
 * <p>Notes</p>
 * <ul>
 * <li>If documents are larger than 16KB then chunks will likely contain only
 * one document. However, documents can never spread across several chunks (all
 * fields of a single document are in the same chunk).</li>
 * <li>Given that the original lengths are written in the metadata of the chunk,
 * the decompressor can leverage this information to stop decoding as soon as
 * enough data has been decompressed.</li>
 * <li>In case documents are incompressible, CompressedDocs will be less than
 * 0.5% larger than Docs.</li>
 * </ul>
 * </li>
 * <li><a name="field_index" id="field_index"></a>
 * <p>A fields index file (extension <tt>.fdx</tt>). The data stored in this
 * file is read to load an in-memory data-structure that can be used to locate
 * the start offset of a block containing any document in the fields data file.</p>
 * <p>In order to have a compact in-memory representation, for every block of
 * 1024 chunks, this stored fields index computes the average number of bytes per
 * chunk and for every chunk, only stores the difference between<ul>
 * <li>${chunk number} * ${average length of a chunk}</li>
 * <li>and the actual start offset of the chunk</li></ul></p>
 * <p>Data is written as follows:</p>
 * <ul>
 * <li>FieldsIndex (.fdx) --&gt; &lt;Header&gt;, FieldsIndex, PackedIntsVersion, &lt;Block&gt;<sup>BlockCount</sup>, BlocksEndMarker</li>
 * <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 * <li>PackedIntsVersion --&gt; {@link PackedInts#VERSION_CURRENT} as a {@link DataOutput#writeVInt VInt}</li>
 * <li>BlocksEndMarker --&gt; <tt>0</tt> as a {@link DataOutput#writeVInt VInt}, this marks the end of blocks since blocks are not allowed to start with <tt>0</tt></li>
 * <li>Block --&gt; BlockChunks, &lt;DocBases&gt;, &lt;StartPointers&gt;</li>
 * <li>BlockChunks --&gt; a {@link DataOutput#writeVInt VInt} which is the number of chunks encoded in the block</li>
 * <li>DocBases --&gt; DocBase, AvgChunkDocs, BitsPerDocBaseDelta, DocBaseDeltas</li>
 * <li>DocBase --&gt; first document ID of the block of chunks, as a {@link DataOutput#writeVInt VInt}</li>
 * <li>AvgChunkDocs --&gt; average number of documents in a single chunk, as a {@link DataOutput#writeVInt VInt}</li>
 * <li>BitsPerDocBaseDelta --&gt; number of bits required to represent a delta from the average using <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">ZigZag encoding</a></li>
 * <li>DocBaseDeltas --&gt; {@link PackedInts packed} array of BlockChunks elements of BitsPerDocBaseDelta bits each, representing the deltas from the average doc base using <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">ZigZag encoding</a>.</li>
 * <li>StartPointers --&gt; StartPointerBase, AvgChunkSize, BitsPerStartPointerDelta, StartPointerDeltas</li>
 * <li>StartPointerBase --&gt; the first start pointer of the block, as a {@link DataOutput#writeVLong VLong}</li>
 * <li>AvgChunkSize --&gt; the average size of a chunk of compressed documents, as a {@link DataOutput#writeVLong VLong}</li>
 * <li>BitsPerStartPointerDelta --&gt; number of bits required to represent a delta from the average using <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">ZigZag encoding</a></li>
 * <li>StartPointerDeltas --&gt; {@link PackedInts packed} array of BlockChunks elements of BitsPerStartPointerDelta bits each, representing the deltas from the average start pointer using <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">ZigZag encoding</a></li>
 * </ul>
 * <p>Notes</p>
 * <ul>
 * <li>For any block, the doc base of the n-th chunk can be restored with
 * <code>DocBase + AvgChunkDocs * n + DocBaseDeltas[n]</code>.</li>
 * <li>For any block, the start pointer of the n-th chunk can be restored with
 * <code>StartPointerBase + AvgChunkSize * n + StartPointerDeltas[n]</code>.</li>
 * <li>Once data is loaded into memory, you can lookup the start pointer of any
 * document by performing two binary searches: a first one based on the values
 * of DocBase in order to find the right block, and then inside the block based
 * on DocBaseDeltas (by reconstructing the doc bases for every chunk).</li>
 * </ul>
 * </li>
 * </ol>
 * <p><b>Known limitations</b></p>
 * <p>This {@link StoredFieldsFormat} does not support individual documents
 * larger than (<tt>2<sup>31</sup> - 2<sup>14</sup></tt>) bytes. In case this
 * is a problem, you should use another format, such as
 * {@link Lucene40StoredFieldsFormat}.</p>
 * @lucene.experimental
 */
public final class Lucene41StoredFieldsFormat extends CompressingStoredFieldsFormat {

  /** Sole constructor. */
  public Lucene41StoredFieldsFormat() {
    super("Lucene41StoredFields", CompressionMode.FAST, 1 << 14);
  }

}
