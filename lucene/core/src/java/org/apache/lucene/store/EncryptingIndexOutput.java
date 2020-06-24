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

package org.apache.lucene.store;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.util.StringHelper;

import static org.apache.lucene.store.EncryptingUtil.*;

public class EncryptingIndexOutput extends IndexOutput {

  /**
   * Must be a multiple of {@link EncryptingUtil#AES_BLOCK_SIZE}.
   */
  private static final int BUFFER_CAPACITY = 64 * AES_BLOCK_SIZE; // 1024
  static {
    assert BUFFER_CAPACITY % AES_BLOCK_SIZE == 0;
  }

  private static final String HEADER_CODEC = "Crypto";
  private static final int HEADER_VERSION = 0;
  private static final byte[] HEADER_FAKE_ID = new byte[StringHelper.ID_LENGTH];
  static final int HEADER_LENGTH = 32; // Must be a multiple of AES_BLOCK_SIZE.
  static {
    assert HEADER_LENGTH % AES_BLOCK_SIZE == 0;
  }

  private final IndexOutput indexOutput;
  private final boolean footerMatters;
  private final CipherPool cipherPool;
  private Cipher cipher;
  private final ByteBuffer inBuffer;
  private final ByteBuffer outBuffer;
  private final byte[] outArray;
  private final byte[] oneByteBuf;
  private final Checksum clearChecksum;
  private long filePointer;

  public EncryptingIndexOutput(IndexOutput indexOutput, byte[] key, CipherPool cipherPool) throws IOException {
    this(indexOutput, key, cipherPool, null);
  }

  /**
   * @param segmentId may be null, in this case it is replaced by a fake id.
   */
  public EncryptingIndexOutput(IndexOutput indexOutput, byte[] key, CipherPool cipherPool, byte[] segmentId) throws IOException {
    super("Encrypting " + indexOutput.toString(), indexOutput.getName());
    this.indexOutput = indexOutput;

    // Always write a header. This is required for the file to be handle without decryption (e.g for compound files).
    CodecUtil.writeIndexHeader(indexOutput, HEADER_CODEC, HEADER_VERSION, segmentId == null ? HEADER_FAKE_ID : segmentId, "");
    assert indexOutput.getFilePointer() == HEADER_LENGTH : "Header length changed: " + indexOutput.getFilePointer();
    // Only write the real footer when it matters because it computes a checksum.
    footerMatters = segmentId != null;

    this.cipherPool = cipherPool;
    cipher = cipherPool.get(CipherPool.AES_CTR_TRANSFORMATION);
    assert cipher.getBlockSize() == AES_BLOCK_SIZE : "Invalid AES block size: " + cipher.getBlockSize();

    byte[] iv = generateRandomIv();
    try {
      cipher.init(Cipher.ENCRYPT_MODE, createAesKey(key), new IvParameterSpec(iv));
    } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
      throw new IOException(e);
    }
    // IV is written at the beginning of the index output. It's public.
    // Even if the delegate indexOutput is positioned after the initial IV, this index output file pointer is 0 initially.
    indexOutput.writeBytes(iv, 0, iv.length);

    inBuffer = ByteBuffer.allocate(getBufferCapacity());
    outBuffer = ByteBuffer.allocate(getBufferCapacity() + AES_BLOCK_SIZE);
    assert inBuffer.hasArray() && outBuffer.hasArray();
    assert outBuffer.arrayOffset() == 0;
    outArray = outBuffer.array();
    oneByteBuf = new byte[1];

    // Compute the checksum to skip the initial IV, because an external checksum checker will not see it.
    clearChecksum = new BufferedChecksum(new CRC32());
  }

  /**
   * Generates an AES/CTR random IV of length {@link EncryptingUtil#IV_LENGTH}.
   */
  protected byte[] generateRandomIv() {
    return generateRandomAesCtrIv();
  }

  /**
   * Gets the buffer capacity. It must be a multiple of {@link EncryptingUtil#AES_BLOCK_SIZE}.
   */
  protected int getBufferCapacity() {
    return BUFFER_CAPACITY;
  }

  @Override
  public void close() throws IOException {
    if (cipher != null) {
      try {
        if (inBuffer.position() != 0) {
          encryptBufferAndWrite();
        }
        writeFooter();
        indexOutput.close();
      } finally {
        cipherPool.release(cipher);
        cipher = null;
      }
    }
  }

  private void writeFooter() throws IOException {
    if (footerMatters) {
      CodecUtil.writeFooter(indexOutput);
    } else {
      // Write a fake footer to avoid computing a checksum.
      for (int i = 0, length = CodecUtil.footerLength(); i < length; i++) {
        indexOutput.writeByte((byte) 0);
      }
    }
  }

  @Override
  public long getFilePointer() {
    // With AES, the plain data and encrypted data have the same file pointers.
    // The algorithm is AES/CTR/NoPadding. This means the encrypted data length is the same as the plain data length.
    // We return here the file pointer excluding the header and initial IV length at the beginning of the file.
    // It does not include the footer because it is written when this index output is closed.
    return filePointer;
  }

  @Override
  public long getChecksum() {
    // The checksum is computed on the clear data (excluding the initial IV).
    return clearChecksum.getValue();
  }

  @Override
  public void writeByte(byte b) throws IOException {
    oneByteBuf[0] = b;
    writeBytes(oneByteBuf, 0, oneByteBuf.length);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    if (offset < 0 || length < 0 || offset + length > b.length) {
      throw new IllegalArgumentException("Invalid write buffer parameters (offset=" + offset + ", length=" + length + ", arrayLength=" + b.length + ")");
    }
    clearChecksum.update(b, offset, length);
    filePointer += length;
    while (length > 0) {
      int remaining = inBuffer.remaining();
      if (length < remaining) {
        inBuffer.put(b, offset, length);
        break;
      } else {
        inBuffer.put(b, offset, remaining);
        offset += remaining;
        length -= remaining;
        encryptBufferAndWrite();
      }
    }
  }

  private void encryptBufferAndWrite() throws IOException {
    assert inBuffer.position() != 0;
    inBuffer.flip();
    outBuffer.clear();
    int inputSize = inBuffer.remaining();
    int numEncryptedBytes;
    try {
      numEncryptedBytes = cipher.update(inBuffer, outBuffer);
    } catch (ShortBufferException e) {
      throw new IOException(e);
    }
    if (numEncryptedBytes < inputSize) {
      throw new UnsupportedOperationException("The Cipher implementation does not maintain an encryption context; this is not supported");
    }
    inBuffer.clear();
    outBuffer.flip();
    indexOutput.writeBytes(outArray, 0, outBuffer.limit());
  }
}
