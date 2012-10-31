package org.apache.lucene.codecs.compressing;

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
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/**
 * A compression mode. Tells how much effort should be spent on compression and
 * decompression of stored fields.
 * @lucene.experimental
 */
public enum CompressionMode {

  /**
   * A compression mode that trades compression ratio for speed. Although the
   * compression ratio might remain high, compression and decompression are
   * very fast. Use this mode with indices that have a high update rate but
   * should be able to load documents from disk quickly.
   */
  FAST(0) {

    @Override
    Compressor newCompressor() {
      return LZ4_FAST_COMPRESSOR;
    }

    @Override
    Decompressor newDecompressor() {
      return LZ4_DECOMPRESSOR;
    }

  },

  /**
   * A compression mode that trades speed for compression ratio. Although
   * compression and decompression might be slow, this compression mode should
   * provide a good compression ratio. This mode might be interesting if/when
   * your index size is much bigger than your OS cache.
   */
  HIGH_COMPRESSION(1) {

    @Override
    Compressor newCompressor() {
      return new DeflateCompressor(Deflater.BEST_COMPRESSION);
    }

    @Override
    Decompressor newDecompressor() {
      return new DeflateDecompressor();
    }

  },

  /**
   * This compression mode is similar to {@link #FAST} but it spends more time
   * compressing in order to improve the compression ratio. This compression
   * mode is best used with indices that have a low update rate but should be
   * able to load documents from disk quickly.
   */
  FAST_DECOMPRESSION(2) {

    @Override
    Compressor newCompressor() {
      return LZ4_HIGH_COMPRESSOR;
    }

    @Override
    Decompressor newDecompressor() {
      return LZ4_DECOMPRESSOR;
    }

  };

  public static CompressionMode byId(int id) {
    for (CompressionMode mode : CompressionMode.values()) {
      if (mode.getId() == id) {
        return mode;
      }
    }
    throw new IllegalArgumentException("Unknown id: " + id);
  }

  private final int id;

  private CompressionMode(int id) {
    this.id = id;
  }

  /**
   * Returns an ID for this compression mode. Should be unique across
   * {@link CompressionMode}s as it is used for serialization and
   * unserialization.
   */
  public final int getId() {
    return id;
  }

  /**
   * Create a new {@link Compressor} instance.
   */
  abstract Compressor newCompressor();

  /**
   * Create a new {@link Decompressor} instance.
   */
  abstract Decompressor newDecompressor();


  private static final Decompressor LZ4_DECOMPRESSOR = new Decompressor() {

    @Override
    public void decompress(DataInput in, BytesRef bytes) throws IOException {
      final int decompressedLen = in.readVInt();
      if (bytes.bytes.length < decompressedLen + 8) {
        bytes.bytes = ArrayUtil.grow(bytes.bytes, decompressedLen + 8);
      }
      LZ4.decompress(in, decompressedLen, bytes);
      if (bytes.length != decompressedLen) {
        throw new IOException("Corrupted");
      }
    }

    @Override
    public void decompress(DataInput in, int offset, int length, BytesRef bytes) throws IOException {
      final int decompressedLen = in.readVInt();
      if (offset > decompressedLen) {
        bytes.length = 0;
        return;
      }
      if (bytes.bytes.length < decompressedLen) {
        bytes.bytes = ArrayUtil.grow(bytes.bytes, decompressedLen);
      }
      LZ4.decompress(in, offset + length, bytes);
      bytes.offset = offset;
      if (offset + length >= decompressedLen) {
        if (bytes.length != decompressedLen) {
          throw new IOException("Corrupted");
        }
        bytes.length = decompressedLen - offset;
      } else {
        bytes.length = length;
      }
    }

    public void copyCompressedData(DataInput in, DataOutput out) throws IOException {
      final int decompressedLen = in.readVInt();
      out.writeVInt(decompressedLen);
      if (decompressedLen == 0) {
        out.writeByte((byte) 0); // the token
        return;
      }
      int n = 0;
      while (n < decompressedLen) {
        // literals
        final byte token = in.readByte();
        out.writeByte(token);
        int literalLen = (token & 0xFF) >>> 4;
        if (literalLen == 0x0F) {
          byte len;
          while ((len = in.readByte()) == (byte) 0xFF) {
            literalLen += 0xFF;
            out.writeByte(len);
          }
          literalLen += len & 0xFF;
          out.writeByte(len);
        }
        out.copyBytes(in, literalLen);
        n += literalLen;
        if (n >= decompressedLen) {
          break;
        }

        // matchs
        out.copyBytes(in, 2); // match dec
        int matchLen = token & 0x0F;
        if (matchLen == 0x0F) {
          byte len;
          while ((len = in.readByte()) == (byte) 0xFF) {
            matchLen += 0xFF;
            out.writeByte(len);
          }
          matchLen += len & 0xFF;
          out.writeByte(len);
        }
        matchLen += LZ4.MIN_MATCH;
        n += matchLen;
      }

      if (n != decompressedLen) {
        throw new IOException("Currupted compressed stream: expected " + decompressedLen + " bytes, but got at least" + n);
      }
    }

    @Override
    public Decompressor clone() {
      return this;
    }

  };

  private static final Compressor LZ4_FAST_COMPRESSOR = new Compressor() {

    @Override
    public void compress(byte[] bytes, int off, int len, DataOutput out)
        throws IOException {
      out.writeVInt(len);
      LZ4.compress(bytes, off, len, out);
    }

  };

  private static final Compressor LZ4_HIGH_COMPRESSOR = new Compressor() {

    @Override
    public void compress(byte[] bytes, int off, int len, DataOutput out)
        throws IOException {
      out.writeVInt(len);
      LZ4.compressHC(bytes, off, len, out);
    }

  };

  private static final class DeflateDecompressor extends Decompressor {

    final Inflater decompressor;
    byte[] compressed;

    DeflateDecompressor() {
      decompressor = new Inflater();
      compressed = new byte[0];
    }

    @Override
    public void decompress(DataInput in, BytesRef bytes) throws IOException {
      bytes.offset = bytes.length = 0;

      final int compressedLength = in.readVInt();
      if (compressedLength > compressed.length) {
        compressed = ArrayUtil.grow(compressed, compressedLength);
      }
      in.readBytes(compressed, 0, compressedLength);

      decompressor.reset();
      decompressor.setInput(compressed, 0, compressedLength);
      if (decompressor.needsInput()) {
        return;
      }

      while (true) {
        final int count;
        try {
          final int remaining = bytes.bytes.length - bytes.length;
          count = decompressor.inflate(bytes.bytes, bytes.length, remaining);
        } catch (DataFormatException e) {
          throw new IOException(e);
        }
        bytes.length += count;
        if (decompressor.finished()) {
          break;
        } else {
          bytes.bytes = ArrayUtil.grow(bytes.bytes);
        }
      }
    }

    @Override
    public void copyCompressedData(DataInput in, DataOutput out) throws IOException {
      final int compressedLength = in.readVInt();
      out.writeVInt(compressedLength);
      out.copyBytes(in, compressedLength);
    }

    @Override
    public Decompressor clone() {
      return new DeflateDecompressor();
    }

  }

  private static class DeflateCompressor extends Compressor {

    final Deflater compressor;
    byte[] compressed;

    DeflateCompressor(int level) {
      compressor = new Deflater(level);
      compressed = new byte[64];
    }

    @Override
    public void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {
      compressor.reset();
      compressor.setInput(bytes, off, len);
      compressor.finish();

      if (compressor.needsInput()) {
        // no output
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

  }

}
