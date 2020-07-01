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
package org.apache.lucene.codecs.compressing;


import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.compress.LZ4;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDecompressCtx;

/**
 * A compression mode. Tells how much effort should be spent on compression and
 * decompression of stored fields.
 * @lucene.experimental
 */
public abstract class CompressionMode {



  /**
   * A compression mode that trades compression ratio for speed. Although the
   * compression ratio might remain high, compression and decompression are
   * very fast. Use this mode with indices that have a high update rate but
   * should be able to load documents from disk quickly.
   */
  public static final CompressionMode FAST = new CompressionMode() {

    @Override
    public Compressor newCompressor() {
      return new LZ4FastCompressor();
    }

    @Override
    public Decompressor newDecompressor() {
      return LZ4_DECOMPRESSOR;
    }

    @Override
    public String toString() {
      return "FAST";
    }

  };

  /**
   * A compression mode that trades speed for compression ratio. Although
   * compression and decompression might be slow, this compression mode should
   * provide a good compression ratio. This mode might be interesting if/when
   * your index size is much bigger than your OS cache.
   */
  public static final CompressionMode HIGH_COMPRESSION = new CompressionMode() {

    @Override
    public Compressor newCompressor() {
      // notes:
      // 3 is the highest level that doesn't have lazy match evaluation
      // 6 is the default, higher than that is just a waste of cpu
      return new DeflateCompressor(6);
    }

    @Override
    public Decompressor newDecompressor() {
      return new DeflateDecompressor();
    }

    @Override
    public String toString() {
      return "HIGH_COMPRESSION";
    }

  };

  /**
   * This compression mode is similar to {@link #FAST} but it spends more time
   * compressing in order to improve the compression ratio. This compression
   * mode is best used with indices that have a low update rate but should be
   * able to load documents from disk quickly.
   */
  public static final CompressionMode FAST_DECOMPRESSION = new CompressionMode() {

    @Override
    public Compressor newCompressor() {
      return new LZ4HighCompressor();
    }

    @Override
    public Decompressor newDecompressor() {
      return LZ4_DECOMPRESSOR;
    }

    @Override
    public String toString() {
      return "FAST_DECOMPRESSION";
    }

  };

  public static final CompressionMode NO_COMPRESSION = new CompressionMode() {

    @Override
    public Compressor newCompressor() {
      return new noCompressor();
    }

    @Override
    public Decompressor newDecompressor() {
      return new noDecompressor();
    }

    @Override
    public String toString() {
      return "NO_DECOMPRESSION";
    }

  };

  public static final CompressionMode ZSTD_COMPRESSION = new CompressionMode() {

    @Override
    public Compressor newCompressor() {
      // notes:
      // 3 is the highest level that doesn't have lazy match evaluation
      // 6 is the default, higher than that is just a waste of cpu
      return new zstdCompressor(3);
    }

    @Override
    public Decompressor newDecompressor() {
      return new zstdDecompressor();
    }

    @Override
    public String toString() {
      return "ZSTD_COMPRESSION";
    }

  };

  /** Sole constructor. */
  protected CompressionMode() {}

  /**
   * Create a new {@link Compressor} instance.
   */
  public abstract Compressor newCompressor();

  /**
   * Create a new {@link Decompressor} instance.
   */
  public abstract Decompressor newDecompressor();

  private static final Decompressor LZ4_DECOMPRESSOR = new Decompressor() {

    @Override
    public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
      assert offset + length <= originalLength;
      // add 7 padding bytes, this is not necessary but can help decompression run faster
      if (bytes.bytes.length < originalLength + 7) {
        bytes.bytes = new byte[ArrayUtil.oversize(originalLength + 7, 1)];
      }
      final int decompressedLength = LZ4.decompress(in, offset + length, bytes.bytes);
      if (decompressedLength > originalLength) {
        throw new CorruptIndexException("Corrupted: lengths mismatch: " + decompressedLength + " > " + originalLength, in);
      }
      bytes.offset = offset;
      bytes.length = length;
    }

    @Override
    public Decompressor clone() {
      return this;
    }

  };

  private static final class noDecompressor extends Decompressor {

    byte[] compressed;

    noDecompressor() {
      compressed = new byte[100000];
    }
    @Override
    public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
      assert offset + length <= originalLength;
      if (length == 0) {
        bytes.length = 0;
        return;
      }
      final int noCompressedLength = in.readVInt();
      // pad with extra "dummy byte": see javadocs for using Inflater(true)
      // we do it for compliance, but it's unnecessary for years in zlib.
      final int paddedLength = noCompressedLength + 1;
      compressed = ArrayUtil.grow(compressed, paddedLength);
      in.readBytes(compressed, 0, noCompressedLength);
      compressed[noCompressedLength] = 0; // explicitly set dummy byte to 0

      bytes.bytes = compressed;
      bytes.offset = offset;
      bytes.length = length;
    }

    @Override
    public Decompressor clone() {
      return this;
    }

  };

  private static final class LZ4FastCompressor extends Compressor {

    private final LZ4.FastCompressionHashTable ht;

    LZ4FastCompressor() {
      ht = new LZ4.FastCompressionHashTable();
    }

    @Override
    public void compress(byte[] bytes, int off, int len, DataOutput out)
            throws IOException {
      LZ4.compress(bytes, off, len, out, ht);
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }

  private static final class noCompressor extends Compressor {


    @Override
    public void compress(byte[] bytes, int off, int len, DataOutput out)
            throws IOException {
      out.writeVInt(bytes.length);
      out.writeBytes(bytes,bytes.length);
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }

  private static final class LZ4HighCompressor extends Compressor {

    private final LZ4.HighCompressionHashTable ht;

    LZ4HighCompressor() {
      ht = new LZ4.HighCompressionHashTable();
    }

    @Override
    public void compress(byte[] bytes, int off, int len, DataOutput out)
            throws IOException {
      LZ4.compress(bytes, off, len, out, ht);
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }

  private static final class DeflateDecompressor extends Decompressor {

    byte[] compressed;

    DeflateDecompressor() {
      compressed = new byte[0];
    }

    @Override
    public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
      assert offset + length <= originalLength;
      if (length == 0) {
        bytes.length = 0;
        return;
      }
      final int compressedLength = in.readVInt();
      // pad with extra "dummy byte": see javadocs for using Inflater(true)
      // we do it for compliance, but it's unnecessary for years in zlib.
      final int paddedLength = compressedLength + 1;
      compressed = ArrayUtil.grow(compressed, paddedLength);
      in.readBytes(compressed, 0, compressedLength);
      compressed[compressedLength] = 0; // explicitly set dummy byte to 0

      final Inflater decompressor = new Inflater(true);
      try {
        // extra "dummy byte"
        decompressor.setInput(compressed, 0, paddedLength);

        bytes.offset = bytes.length = 0;
        bytes.bytes = ArrayUtil.grow(bytes.bytes, originalLength);
        try {
          bytes.length = decompressor.inflate(bytes.bytes, bytes.length, originalLength);
        } catch (DataFormatException e) {
          throw new IOException(e);
        }
        if (!decompressor.finished()) {
          throw new CorruptIndexException("Invalid decoder state: needsInput=" + decompressor.needsInput()
                  + ", needsDict=" + decompressor.needsDictionary(), in);
        }
      } finally {
        decompressor.end();
      }
      if (bytes.length != originalLength) {
        throw new CorruptIndexException("Lengths mismatch: " + bytes.length + " != " + originalLength, in);
      }
      bytes.offset = offset;
      bytes.length = length;
    }

    @Override
    public Decompressor clone() {
      return new DeflateDecompressor();
    }

  }
  private static final class zstdDecompressor extends Decompressor {

    byte[] compressed;

    zstdDecompressor() {
      compressed = new byte[0];
    }

    @Override
    public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
      assert offset + length <= originalLength;
      if (length == 0) {
        bytes.length = 0;
        return;
      }
      final int compressedLength = in.readVInt();
      // pad with extra "dummy byte": see javadocs for using Inflater(true)
      // we do it for compliance, but it's unnecessary for years in zlib.
      final int paddedLength = compressedLength + 1;
      compressed = ArrayUtil.grow(compressed, paddedLength);
      in.readBytes(compressed, 0, compressedLength);
      compressed[compressedLength] = 0; // explicitly set dummy byte to 0

      final ZstdDecompressCtx decompressor = new ZstdDecompressCtx();
      try {

        bytes.offset = bytes.length = 0;
        bytes.bytes = ArrayUtil.grow(bytes.bytes, originalLength);
        try {
          bytes.length = decompressor.decompress(bytes.bytes,compressed);
          decompressor.finish();
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      finally {
        decompressor.end();
      }

      bytes.offset = offset;
      bytes.length = length;
    }

    @Override
    public Decompressor clone() {
      return new zstdDecompressor();
    }

  }

  private static class DeflateCompressor extends Compressor {

    final Deflater compressor;
    byte[] compressed;
    boolean closed;

    DeflateCompressor(int level) {
      compressor = new Deflater(level, true);
      compressed = new byte[64];
    }

    @Override
    public void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {
      compressor.reset();
      compressor.setInput(bytes, off, len);
      compressor.finish();

      if (compressor.needsInput()) {
        // no output
        assert len == 0 : len;
        out.writeVInt(0);
        return;
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
    public void close() throws IOException {
      if (closed == false) {
        compressor.end();
        closed = true;
      }
    }

  }
  private static class zstdCompressor extends Compressor {

    Zstd compressor = new Zstd();
    byte[] compressed;
    boolean closed;
    int level;

    zstdCompressor(int level) {
      compressed = new byte[0];
      this.level = level;
    }

    @Override
    public void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {


      if (len==0) {
        // no output
        assert len == 0 : len;
        out.writeVInt(0);
        return;
      }

      compressed = ArrayUtil.grow(compressed,len);

      final int count = (int)compressor.compress(compressed,bytes,level);


      out.writeVInt(count);
      out.writeBytes(compressed, count);
    }

    @Override
    public void close() throws IOException {
      if (closed == false) {
        closed = true;
      }
    }

  }

}
