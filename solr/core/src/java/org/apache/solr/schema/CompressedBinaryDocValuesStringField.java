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
package org.apache.solr.schema;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;

/**
 * A String field that supports per-value compression of values stored in binary docValues.
 * Since {@link BinaryDocValuesStringField} supports field retrieval only (not faceting
 * or sorting), the performance overhead of decompression should be acceptable.
 *
 * The purpose of {@link BinaryDocValuesStringField} is to support large string values
 * (smaller values are well supported by {@link StrField}). This class is motivated by the
 * common case in which larger string values tend to benefit more from compression.
 *
 * Configuration options:
 *
 * {@value #DICTIONARY_FILE_ARGNAME}: For cases where values contain predictable patterns,
 * the field type may be configured with an optional {@value #DICTIONARY_FILE_ARGNAME} to
 * prime compression with references to common binary patterns.
 *
 * {@value #TRY_COMPRESS_THRESHOLD_ARGNAME}: Input byte count threshold below which input
 * is stored without compression. Defaults to {@value #DEFAULT_TRY_COMPRESS_THRESHOLD} bytes.
 *
 * {@value #COMPRESSION_LEVEL_ARGNAME}: values 1-9 select Deflate compression algorithm.
 * Higher values prioritize compression, lower values prioritize speed. Default value is
 * {@value #DEFAULT_COMPRESSION_LEVEL}.
 *
 * {@value #COMPRESSION_THRESHOLD_ARGNAME}: double compression threshold ratio. If the ratio
 * of compressedBytes to uncompressedBytes exceeds the configured value, the input will be
 * stored uncompressed. Defaults to {@value #DEFAULT_COMPRESSION_THRESHOLD}.
 */
public class CompressedBinaryDocValuesStringField extends BinaryDocValuesStringField {

  private static final String DICTIONARY_FILE_ARGNAME = "dictionaryFile";
  private static final String COMPRESSION_LEVEL_ARGNAME = "compressionLevel";
  private static final String TRY_COMPRESS_THRESHOLD_ARGNAME = "tryCompressThreshold";
  private static final String COMPRESSION_THRESHOLD_ARGNAME = "compressionThreshold";
  private static final int DEFAULT_COMPRESSION_LEVEL = Deflater.BEST_COMPRESSION;
  private static final int DEFAULT_TRY_COMPRESS_THRESHOLD = 32;
  private static final double DEFAULT_COMPRESSION_THRESHOLD = 0.5;

  private byte[] dictionary;
  private int compressionLevel;
  private int tryCompressThreshold;
  private double compressionThreshold;
  private ThreadLocal<Deflater> deflater;
  private ThreadLocal<Inflater> inflater;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    String dictionaryFile = args.remove(DICTIONARY_FILE_ARGNAME);
    if (dictionaryFile != null) {
      int outLength = 4096;
      byte[] build = new byte[outLength];
      int size = 0;
      InputStream in = null;
      try {
        in = schema.getResourceLoader().openResource(dictionaryFile);
        int read;
        while ((read = in.read(build, size, outLength - size)) != -1) {
          size += read;
          if (outLength == size) {
            // out of space; resize
            outLength = outLength << 1;
            build = ArrayUtil.growExact(build, outLength);
          }
        }
        dictionary = ArrayUtil.copyOfSubArray(build, 0, size);
      } catch (IOException ex) {
        throw new AssertionError("error reading dictionaryFile: "+dictionaryFile, ex);
      } finally {
        try {
          if (in != null) {
            in.close();
          }
        } catch (IOException ex) {
          throw new AssertionError("error closing dictionaryFile: "+dictionaryFile, ex);
        }
      }
    }
    String tmp = args.remove(COMPRESSION_LEVEL_ARGNAME);
    this.compressionLevel = tmp == null ? DEFAULT_COMPRESSION_LEVEL : Integer.parseInt(tmp);
    tmp = args.remove(TRY_COMPRESS_THRESHOLD_ARGNAME);
    this.tryCompressThreshold = tmp == null ? DEFAULT_TRY_COMPRESS_THRESHOLD : Integer.parseInt(tmp);
    tmp = args.remove(COMPRESSION_THRESHOLD_ARGNAME);
    this.compressionThreshold = tmp == null ? DEFAULT_COMPRESSION_THRESHOLD : Double.parseDouble(tmp);
    initFlaters();
    super.init(schema, args);
  }

  private void initFlaters() {
    deflater = new ThreadLocal<Deflater>() {
      @Override
      protected Deflater initialValue() {
        Deflater ret = new Deflater(compressionLevel, true);
        return ret;
      }

      @Override
      public Deflater get() {
        Deflater ret = super.get();
        ret.reset();
        if (dictionary != null) {
          ret.setDictionary(dictionary);
        }
        return ret;
      }
    };
    inflater = new ThreadLocal<Inflater>() {
      @Override
      protected Inflater initialValue() {
        Inflater ret = new Inflater(true);
        return ret;
      }
      @Override
      public Inflater get() {
        Inflater ret = super.get();
        ret.reset();
        if (dictionary != null) {
          ret.setDictionary(dictionary);
        }
        return ret;
      }
    };
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return inflate(term).utf8ToString();
  }

  @Override
  public CharsRef indexedToReadable(BytesRef input, CharsRefBuilder output) {
    output.copyUTF8Bytes(inflate(input));
    return output.get();
  }

  private BytesRef inflate(BytesRef input) {
    final int expectedSize = readVInt(input);
    if (expectedSize == 0) {
      // not compressed
      return input;
    }
    byte[] res = new byte[expectedSize];
    final Inflater i = this.inflater.get();
    i.setInput(input.bytes, input.offset, input.length);
    final int actualSize;
    try {
      actualSize = i.inflate(res);
    } catch (DataFormatException ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, ex);
    }
    assert actualSize == expectedSize : "expected size "+expectedSize+"; got "+actualSize;
    return new BytesRef(res, 0, actualSize);
  }

  @Override
  protected BytesRef bytesToIndexedBytesRef(byte[] buf, int offset, final int originalSize) {
    final int outSizeThreshold;
    if (originalSize < tryCompressThreshold || (outSizeThreshold = (int)(compressionThreshold * originalSize)) < MIN_COMPRESSED_SIZE) {
      return uncompressed(buf, offset, originalSize);
    }
    final Deflater d = this.deflater.get();
    d.setInput(buf, offset, originalSize);
    d.finish();
    byte[] out = new byte[outSizeThreshold];
    final int startCompressed = writeVInt(originalSize, out);
    int compressedSize = d.deflate(out, startCompressed, outSizeThreshold - startCompressed, Deflater.FULL_FLUSH);
    if (d.finished()) {
      return new BytesRef(out, 0, compressedSize + startCompressed);
    } else {
      return uncompressed(buf, offset, originalSize);
    }
  }

  private BytesRef uncompressed(byte[] buf, int offset, final int originalSize) {
    final int sizeWithHeader = originalSize + 1;
    byte[] out = new byte[sizeWithHeader]; // first byte will be 0 (also vint 0)
    System.arraycopy(buf, offset, out, 1, originalSize);
    return new BytesRef(out, 0, sizeWithHeader);
  }

  /**
   * This is the absolute minimum possible output size required for {@link Deflater#finished()}
   * to return true. This minimal case would require:
   * 1. the input size to be small enough to encode as 1 byte (header),
   * 2. perfect compression (full, complete, exact match on supplied dictionary), which
   * would require 5 bytes of available space in the output buffer for {@link Deflater#finished()}
   * to return true.
   */
  private static final int MIN_COMPRESSED_SIZE = 6;

  /**
   * Special method for variable length int (copied from lucene). Usually used for writing the length of a
   * collection/array/map In most of the cases the length can be represented in one byte (length &lt; 127) so it saves 3
   * bytes/object
   */
  public static int writeVInt(int i, byte[] bs) {
    int iOut = 0;
    while ((i & ~0x7F) != 0) {
      bs[iOut++] = ((byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    bs[iOut++] = ((byte) i);
    return iOut;
  }

  public static int readVInt(BytesRef input) {
    byte[] bs = input.bytes;
    int offset = input.offset;
    byte b = bs[offset++];
    int i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = bs[offset++];
      i |= (b & 0x7F) << shift;
    }
    input.length -= offset - input.offset;
    input.offset = offset;
    return i;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    CharsRefBuilder cref = new CharsRefBuilder();
    indexedToReadable(f.binaryValue(), cref);
    writer.writeStr(name, cref.toString(), true);
  }
}
