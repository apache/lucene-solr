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
package org.apache.lucene.codecs.lucene87;

import java.io.IOException;
import java.util.Objects;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Lucene 8.7 stored fields format.
 *
 * <p><b>Principle</b>
 * <p>This {@link StoredFieldsFormat} compresses blocks of documents in
 * order to improve the compression ratio compared to document-level
 * compression. It uses the <a href="http://code.google.com/p/lz4/">LZ4</a>
 * compression algorithm by default in 16KB blocks, which is fast to compress 
 * and very fast to decompress data. Although the default compression method 
 * that is used ({@link Mode#BEST_SPEED BEST_SPEED}) focuses more on speed than on 
 * compression ratio, it should provide interesting compression ratios
 * for redundant inputs (such as log files, HTML or plain text). For higher
 * compression, you can choose ({@link Mode#BEST_COMPRESSION BEST_COMPRESSION}),
 * which uses the <a href="http://en.wikipedia.org/wiki/DEFLATE">DEFLATE</a>
 * algorithm with 48kB blocks and shared dictionaries for a better ratio at the
 * expense of slower performance. These two options can be configured like this:
 * <pre class="prettyprint">
 *   // the default: for high performance
 *   indexWriterConfig.setCodec(new Lucene87Codec(Mode.BEST_SPEED));
 *   // instead for higher performance (but slower):
 *   // indexWriterConfig.setCodec(new Lucene87Codec(Mode.BEST_COMPRESSION));
 * </pre>
 * <p><b>File formats</b>
 * <p>Stored fields are represented by three files:
 * <ol>
 * <li><a id="field_data"></a>
 * <p>A fields data file (extension <code>.fdt</code>). This file stores a compact
 * representation of documents in compressed blocks of 16KB or more. When
 * writing a segment, documents are appended to an in-memory <code>byte[]</code>
 * buffer. When its size reaches 16KB or more, some metadata about the documents
 * is flushed to disk, immediately followed by a compressed representation of
 * the buffer using the
 * <a href="https://github.com/lz4/lz4">LZ4</a>
 * <a href="http://fastcompression.blogspot.fr/2011/05/lz4-explained.html">compression format</a>.</p>
 * <p>Notes
 * <ul>
 * <li>When at least one document in a chunk is large enough so that the chunk
 * is larger than 32KB, the chunk will actually be compressed in several LZ4
 * blocks of 16KB. This allows {@link StoredFieldVisitor}s which are only
 * interested in the first fields of a document to not have to decompress 10MB
 * of data if the document is 10MB, but only 16KB.</li>
 * <li>Given that the original lengths are written in the metadata of the chunk,
 * the decompressor can leverage this information to stop decoding as soon as
 * enough data has been decompressed.</li>
 * <li>In case documents are incompressible, the overhead of the compression format
 * is less than 0.5%.</li>
 * </ul>
 * </li>
 * <li><a id="field_index"></a>
 * <p>A fields index file (extension <code>.fdx</code>). This file stores two
 * {@link DirectMonotonicWriter monotonic arrays}, one for the first doc IDs of
 * each block of compressed documents, and another one for the corresponding
 * offsets on disk. At search time, the array containing doc IDs is
 * binary-searched in order to find the block that contains the expected doc ID,
 * and the associated offset on disk is retrieved from the second array.</p>
 * <li><a id="field_meta"></a>
 * <p>A fields meta file (extension <code>.fdm</code>). This file stores metadata
 * about the monotonic arrays stored in the index file.</p>
 * </li>
 * </ol>
 * <p><b>Known limitations</b>
 * <p>This {@link StoredFieldsFormat} does not support individual documents
 * larger than (<code>2<sup>31</sup> - 2<sup>14</sup></code>) bytes.
 * @lucene.experimental
 */
public class Lucene87StoredFieldsFormat extends StoredFieldsFormat {
  
  /** Configuration option for stored fields. */
  public static enum Mode {
    /** Trade compression ratio for retrieval speed. */
    BEST_SPEED,
    /** Trade retrieval speed for compression ratio. */
    BEST_COMPRESSION
  }
  
  /** Attribute key for compression mode. */
  public static final String MODE_KEY = Lucene87StoredFieldsFormat.class.getSimpleName() + ".mode";
  
  final Mode mode;
  
  /** Stored fields format with default options */
  public Lucene87StoredFieldsFormat() {
    this(Mode.BEST_SPEED);
  }

  /** Stored fields format with specified mode */
  public Lucene87StoredFieldsFormat(Mode mode) {
    this.mode = Objects.requireNonNull(mode);
  }

  @Override
  public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
    String value = si.getAttribute(MODE_KEY);
    if (value == null) {
      throw new IllegalStateException("missing value for " + MODE_KEY + " for segment: " + si.name);
    }
    Mode mode = Mode.valueOf(value);
    return impl(mode).fieldsReader(directory, si, fn, context);
  }

  @Override
  public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
    String previous = si.putAttribute(MODE_KEY, mode.name());
    if (previous != null && previous.equals(mode.name()) == false) {
      throw new IllegalStateException("found existing value for " + MODE_KEY + " for segment: " + si.name +
                                      "old=" + previous + ", new=" + mode.name());
    }
    return impl(mode).fieldsWriter(directory, si, context);
  }
  
  StoredFieldsFormat impl(Mode mode) {
    switch (mode) {
      case BEST_SPEED:
        return new CompressingStoredFieldsFormat("Lucene87StoredFieldsFastData", CompressionMode.FAST, 16*1024, 128, 10);
      case BEST_COMPRESSION:
        return new CompressingStoredFieldsFormat("Lucene87StoredFieldsHighData", BEST_COMPRESSION_MODE, BEST_COMPRESSION_BLOCK_LENGTH, 512, 10);
      default: throw new AssertionError();
    }
  }

  // 8kB seems to be a good trade-off between higher compression rates by not
  // having to fully bootstrap a dictionary, and indexing rate by not spending
  // too much CPU initializing data-structures to find strings in this preset
  // dictionary.
  private static final int BEST_COMPRESSION_DICT_LENGTH = 8 * 1024;
  // 48kB seems like a nice trade-off because it's small enough to keep
  // retrieval fast, yet sub blocks can find strings in a window of 26kB of
  // data on average (the window grows from 8kB to 32kB in the first 24kB, and
  // then DEFLATE can use 32kB for the last 24kB) which is close enough to the
  // maximum window length of DEFLATE of 32kB.
  private static final int BEST_COMPRESSION_SUB_BLOCK_LENGTH = 48 * 1024;
  // We shoot for 10 sub blocks per block, which should hopefully amortize the
  // space overhead of having the first 8kB compressed without any preset dict,
  // and then remove 8kB in order to avoid creating a tiny 11th sub block if
  // documents are small.
  private static final int BEST_COMPRESSION_BLOCK_LENGTH = BEST_COMPRESSION_DICT_LENGTH + 10 * BEST_COMPRESSION_SUB_BLOCK_LENGTH - 8 * 1024;

  /** Compression mode for {@link Mode#BEST_COMPRESSION} */
  public static final DeflateWithPresetDict BEST_COMPRESSION_MODE = new DeflateWithPresetDict(BEST_COMPRESSION_DICT_LENGTH, BEST_COMPRESSION_SUB_BLOCK_LENGTH);

  /**
   * A compression mode that trades speed for compression ratio. Although
   * compression and decompression might be slow, this compression mode should
   * provide a good compression ratio. This mode might be interesting if/when
   * your index size is much bigger than your OS cache.
   */
  public static class DeflateWithPresetDict extends CompressionMode {

    private final int dictLength, subBlockLength;

    /** Sole constructor. */
    public DeflateWithPresetDict(int dictLength, int subBlockLength) {
      this.dictLength = dictLength;
      this.subBlockLength = subBlockLength;
    }

    @Override
    public Compressor newCompressor() {
      // notes:
      // 3 is the highest level that doesn't have lazy match evaluation
      // 6 is the default, higher than that is just a waste of cpu
      return new DeflateWithPresetDictCompressor(6, dictLength, subBlockLength);
    }

    @Override
    public Decompressor newDecompressor() {
      return new DeflateWithPresetDictDecompressor();
    }

    @Override
    public String toString() {
      return "BEST_COMPRESSION";
    }

  };

  private static final class DeflateWithPresetDictDecompressor extends Decompressor {

    byte[] compressed;

    DeflateWithPresetDictDecompressor() {
      compressed = new byte[0];
    }

    private void doDecompress(DataInput in, Inflater decompressor, BytesRef bytes) throws IOException {
      final int compressedLength = in.readVInt();
      if (compressedLength == 0) {
        return;
      }
      // pad with extra "dummy byte": see javadocs for using Inflater(true)
      // we do it for compliance, but it's unnecessary for years in zlib.
      final int paddedLength = compressedLength + 1;
      compressed = ArrayUtil.grow(compressed, paddedLength);
      in.readBytes(compressed, 0, compressedLength);
      compressed[compressedLength] = 0; // explicitly set dummy byte to 0

      // extra "dummy byte"
      decompressor.setInput(compressed, 0, paddedLength);
      try {
        bytes.length += decompressor.inflate(bytes.bytes, bytes.length, bytes.bytes.length - bytes.length);
      } catch (DataFormatException e) {
        throw new IOException(e);
      }
      if (decompressor.finished() == false) {
        throw new CorruptIndexException("Invalid decoder state: needsInput=" + decompressor.needsInput()
        + ", needsDict=" + decompressor.needsDictionary(), in);
      }
    }

    @Override
    public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
      assert offset + length <= originalLength;
      if (length == 0) {
        bytes.length = 0;
        return;
      }
      final int dictLength = in.readVInt();
      final int blockLength = in.readVInt();
      bytes.bytes = ArrayUtil.grow(bytes.bytes, dictLength);
      bytes.offset = bytes.length = 0;

      final Inflater decompressor = new Inflater(true);
      try {
        // Read the dictionary
        doDecompress(in, decompressor, bytes);
        if (dictLength != bytes.length) {
          throw new CorruptIndexException("Unexpected dict length", in);
        }

        int offsetInBlock = dictLength;
        int offsetInBytesRef = offset;

        // Skip unneeded blocks
        while (offsetInBlock + blockLength < offset) {
          final int compressedLength = in.readVInt();
          in.skipBytes(compressedLength);
          offsetInBlock += blockLength;
          offsetInBytesRef -= blockLength;
        }

        // Read blocks that intersect with the interval we need
        while (offsetInBlock < offset + length) {
          bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + blockLength);
          decompressor.reset();
          decompressor.setDictionary(bytes.bytes, 0, dictLength);
          doDecompress(in, decompressor, bytes);
          offsetInBlock += blockLength;
        }

        bytes.offset = offsetInBytesRef;
        bytes.length = length;
        assert bytes.isValid();
      } finally {
        decompressor.end();
      }
    }

    @Override
    public Decompressor clone() {
      return new DeflateWithPresetDictDecompressor();
    }

  }

  private static class DeflateWithPresetDictCompressor extends Compressor {

    final int dictLength;
    final int blockLength;
    final Deflater compressor;
    byte[] compressed;
    boolean closed;

    DeflateWithPresetDictCompressor(int level, int dictLength, int blockLength) {
      compressor = new Deflater(level, true);
      compressed = new byte[64];
      this.dictLength = dictLength;
      this.blockLength = blockLength;
    }

    private void doCompress(byte[] bytes, int off, int len, DataOutput out) throws IOException {
      if (len == 0) {
        out.writeVInt(0);
        return;
      }
      compressor.setInput(bytes, off, len);
      compressor.finish();
      if (compressor.needsInput()) {
        throw new IllegalStateException();
      }

      int totalCount = 0;
      for (;;) {
        final int count = compressor.deflate(compressed, totalCount, compressed.length - totalCount);
        totalCount += count;
        assert totalCount <= compressed.length;
        if (compressor.finished()) {
          break;
        } else {
          compressed = ArrayUtil.grow(compressed);
        }
      }

      out.writeVInt(totalCount);
      out.writeBytes(compressed, totalCount);
    }

    @Override
    public void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {
      final int dictLength = Math.min(this.dictLength, len);
      out.writeVInt(dictLength);
      out.writeVInt(blockLength);
      final int end = off + len;

      // Compress the dictionary first
      compressor.reset();
      doCompress(bytes, off, dictLength, out);

      // And then sub blocks
      for (int start = off + dictLength; start < end; start += blockLength) {
        compressor.reset();
        compressor.setDictionary(bytes, off, dictLength);
        doCompress(bytes, start, Math.min(blockLength, off + len - start), out);
      }
    }

    @Override
    public void close() throws IOException {
      if (closed == false) {
        compressor.end();
        closed = true;
      }
    }
  }

}
