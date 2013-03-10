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
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsIndexWriter;
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
 * <li>FieldData (.fdt) --&gt; &lt;Header&gt;, PackedIntsVersion, &lt;Chunk&gt;<sup>ChunkCount</sup></li>
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
 * <p>A fields index file (extension <tt>.fdx</tt>).</p>
 * <ul>
 * <li>FieldsIndex (.fdx) --&gt; &lt;Header&gt;, &lt;ChunkIndex&gt;</li>
 * <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 * <li>ChunkIndex: See {@link CompressingStoredFieldsIndexWriter}</li>
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
