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
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;

import org.apache.lucene.codecs.CodecUtil;

import static org.apache.lucene.store.EncryptingIndexOutput.HEADER_LENGTH;
import static org.apache.lucene.store.EncryptingUtil.*;

public class EncryptingIndexInput extends IndexInput {

  /**
   * Must be a multiple of {@link EncryptingUtil#AES_BLOCK_SIZE}.
   */
  private static final int BUFFER_SIZE = 64 * AES_BLOCK_SIZE; // 1024 B

  private static final long AES_BLOCK_SIZE_MOD_MASK = AES_BLOCK_SIZE - 1;
  static final int HEADER_IV_LENGTH = HEADER_LENGTH + IV_LENGTH;

  private static final byte[] EMPTY_BYTES = new byte[0];

  // Some fields are not final for the clone() method.
  private boolean isClone;
  private final long sliceOffset;
  private final long sliceEnd;
  private IndexInput indexInput;
  private final Key key;
  private final CipherPool cipherPool;
  private Cipher cipher;
  private final byte[] initialIv;
  private byte[] iv;
  private ReusableIvParameterSpec ivParameterSpec;
  private ByteBuffer inBuffer;
  private ByteBuffer outBuffer;
  private byte[] inArray;
  private byte[] oneByteBuf;
  private int padding;

  public EncryptingIndexInput(IndexInput indexInput, byte[] key, CipherPool cipherPool) throws IOException {
    this("Decrypting " + indexInput.toString(),
        HEADER_IV_LENGTH, indexInput.length() - HEADER_IV_LENGTH - CodecUtil.footerLength(), false,
        indexInput, createAesKey(key), cipherPool, readInitialIv(indexInput));
  }

  private EncryptingIndexInput(String resourceDescription, long sliceOffset, long sliceLength, boolean isClone,
                               IndexInput indexInput, Key key, CipherPool cipherPool, byte[] initialIv) throws IOException {
    super(resourceDescription);
    assert sliceOffset >= 0 && sliceLength >= 0;
    this.sliceOffset = sliceOffset;
    this.sliceEnd = sliceOffset + sliceLength;
    this.isClone = isClone;
    this.indexInput = indexInput;
    this.key = key;
    this.cipherPool = cipherPool;
    this.initialIv = initialIv;

    cipher = cipherPool.get(CipherPool.AES_CTR_TRANSFORMATION);
    assert cipher.getBlockSize() == AES_BLOCK_SIZE : "Invalid AES block size: " + cipher.getBlockSize();
    iv = initialIv.clone();
    ivParameterSpec = new ReusableIvParameterSpec(iv);
    try {
      cipher.init(Cipher.DECRYPT_MODE, this.key, ivParameterSpec);
    } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
      throw new IOException(e);
    }

    inBuffer = ByteBuffer.allocate(getBufferSize());
    outBuffer = ByteBuffer.allocate(getBufferSize() + AES_BLOCK_SIZE);
    outBuffer.limit(0);
    assert inBuffer.hasArray() && outBuffer.hasArray();
    assert inBuffer.arrayOffset() == 0;
    inArray = inBuffer.array();
    oneByteBuf = new byte[1];
  }

  /**
   * Reads the initial IV at the beginning of the index input.
   */
  private static byte[] readInitialIv(IndexInput indexInput) throws IOException {
    indexInput.seek(HEADER_LENGTH);
    byte[] initialIv = new byte[IV_LENGTH];
    indexInput.readBytes(initialIv, 0, initialIv.length, false);
    return initialIv;
  }

  /**
   * Gets the buffer size. It must be a multiple of {@link EncryptingUtil#AES_BLOCK_SIZE}.
   */
  protected int getBufferSize() {
    return BUFFER_SIZE;
  }

  @Override
  public void close() throws IOException {
    if (cipher != null) {
      try {
        if (!isClone) {
          indexInput.close();
        }
      } finally {
        cipherPool.release(cipher);
        cipher = null;
      }
    }
  }

  @Override
  public long getFilePointer() {
    return getPosition() - sliceOffset;
  }

  /**
   * Gets the current internal position in the delegate {@link IndexInput}. It includes the initial IV length.
   */
  private long getPosition() {
    return indexInput.getFilePointer() - outBuffer.remaining();
  }

  @Override
  public void seek(long position) throws IOException {
    if (position < 0) {
      throw new IllegalArgumentException("Invalid position=" + position);
    }
    if (position > length()) {
      throw new EOFException("Seek beyond EOF (position=" + position + ", length=" + length() + ") in " + this);
    }
    long targetPosition = position + sliceOffset;
    long delegatePosition = indexInput.getFilePointer();
    long currentPosition = delegatePosition - outBuffer.remaining();
    if (targetPosition >= currentPosition && targetPosition <= delegatePosition) {
      outBuffer.position(outBuffer.position() + (int) (targetPosition - currentPosition));
      assert targetPosition == delegatePosition - outBuffer.remaining();
    } else {
      indexInput.seek(targetPosition);
      setPosition(targetPosition);
    }
  }

  private void setPosition(long position) throws IOException {
    inBuffer.clear();
    outBuffer.clear();
    outBuffer.limit(0);
    resetCipher(position);
    padding = (int) (position & AES_BLOCK_SIZE_MOD_MASK);
    inBuffer.position(padding);
  }

  private void resetCipher(long position) throws IOException {
    // Compute the counter by ignoring the header and initial IV.
    long counter = (position - HEADER_IV_LENGTH) / AES_BLOCK_SIZE;
    buildAesCtrIv(initialIv, counter, iv);
    try {
      cipher.init(Cipher.DECRYPT_MODE, key, ivParameterSpec);
    } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long length() {
    return sliceEnd - sliceOffset;
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    if (offset < 0 || length < 0 || offset + length > length()) {
      throw new IllegalArgumentException("Slice \"" + sliceDescription + "\" out of bounds (offset=" + offset + ", sliceLength=" + length + ", fileLength=" + length() + ") of " + this);
    }
    EncryptingIndexInput slice = new EncryptingIndexInput(getFullSliceDescription(sliceDescription), sliceOffset + offset, length, true, indexInput.clone(), key, cipherPool, initialIv);
    slice.seek(0);
    return slice;
  }

  @Override
  public byte readByte() throws IOException {
    readBytes(oneByteBuf, 0, 1);
    return oneByteBuf[0];
  }

  @Override
  public void readBytes(byte[] b, int offset, int length) throws IOException {
    if (offset < 0 || length < 0 || offset + length > b.length) {
      throw new IllegalArgumentException("Invalid read buffer parameters (offset=" + offset + ", length=" + length + ", arrayLength=" + b.length + ")");
    }
    if (getPosition() + length > sliceEnd) {
      throw new EOFException("Read beyond EOF (position=" + (getPosition() - sliceOffset) + ", arrayLength=" + length + ", fileLength=" + length() + ") in " + this);
    }
    while (length > 0) {
      // Transfer decrypted bytes from outBuffer.
      int outRemaining = outBuffer.remaining();
      if (outRemaining > 0) {
        if (length <= outRemaining) {
          outBuffer.get(b, offset, length);
          return;
        }
        outBuffer.get(b, offset, outRemaining);
        assert outBuffer.remaining() == 0;
        offset += outRemaining;
        length -= outRemaining;
      }

      readToFillBuffer(length);
      decryptBuffer();
    }
  }

  private void readToFillBuffer(int length) throws IOException {
    assert length > 0;
    int inRemaining = inBuffer.remaining();
    if (inRemaining > 0) {
      int position = inBuffer.position();
      int numBytesToRead = Math.min(inRemaining, length);
      indexInput.readBytes(inArray, position, numBytesToRead);
      inBuffer.position(position + numBytesToRead);
    }
  }

  private void decryptBuffer() throws IOException {
    assert inBuffer.position() > padding : "position=" + inBuffer.position() + ", padding=" + padding;
    inBuffer.flip();
    outBuffer.clear();
    int inputSize = inBuffer.remaining();
    int numDecryptedBytes;
    try {
      numDecryptedBytes = cipher.update(inBuffer, outBuffer);
    } catch (ShortBufferException e) {
      throw new IOException(e);
    }
    if (numDecryptedBytes < inputSize) {
      throw new UnsupportedOperationException("The Cipher implementation does not maintain an encryption context; this is not supported");
    }
    inBuffer.clear();
    outBuffer.flip();
    if (padding > 0) {
      outBuffer.position(padding);
      padding = 0;
    }
  }

  @Override
  public EncryptingIndexInput clone() {
    EncryptingIndexInput clone = (EncryptingIndexInput) super.clone();
    clone.isClone = true;
    clone.indexInput = indexInput.clone();
    assert clone.indexInput.getFilePointer() == indexInput.getFilePointer();
    // key, cipherPool and initialIv are the same references.
    clone.cipher = cipherPool.get(CipherPool.AES_CTR_TRANSFORMATION);
    clone.iv = initialIv.clone();
    clone.ivParameterSpec = new ReusableIvParameterSpec(clone.iv);
    clone.inBuffer = ByteBuffer.allocate(getBufferSize());
    clone.outBuffer = ByteBuffer.allocate(getBufferSize() + AES_BLOCK_SIZE);
    clone.inArray = clone.inBuffer.array();
    clone.oneByteBuf = new byte[1];
    try {
      clone.setPosition(getPosition());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return clone;
  }

  /**
   * Avoids cloning the IV in the constructor each time we need an {@link IvParameterSpec}.
   */
  private static class ReusableIvParameterSpec extends IvParameterSpec {

    final byte[] iv;

    ReusableIvParameterSpec(byte[] iv) {
      super(EMPTY_BYTES);
      this.iv = iv;
    }

    public byte[] getIV() {
      return iv.clone();
    }
  }
}