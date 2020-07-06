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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.util.crypto.AesCtrEncrypter;
import org.apache.lucene.util.crypto.AesCtrEncrypterFactory;
import org.apache.lucene.util.crypto.CipherAesCtrEncrypter;
import org.apache.lucene.util.crypto.EncryptionUtil;

import static org.apache.lucene.store.EncryptingIndexOutput.HEADER_LENGTH;
import static org.apache.lucene.util.crypto.EncryptionUtil.*;

/**
 * {@link IndexInput} that reads from a delegate {@link IndexInput} and decrypts data on the fly.
 * <p>It decrypts with the AES algorithm in CTR (counter) mode with no padding. It is appropriate for random access.
 * It can decrypt data previously encrypted with an {@link EncryptingIndexOutput}.</p>
 * <p>It first reads the file {@link CodecUtil#readIndexHeader(IndexInput) header}. Then it reads the CTR Initialization
 * Vector (IV). This random IV is not encrypted. Finally it can decrypt the rest of the file, which probably contains a
 * header and footer itself, with random access. The final footer at the end of the file is ignored.</p>
 *
 * @see EncryptingIndexOutput
 * @see AesCtrEncrypter
 *
 * @lucene.experimental
 */
public class EncryptingIndexInput extends IndexInput {

  /**
   * Must be a multiple of {@link EncryptionUtil#AES_BLOCK_SIZE}.
   * Benchmarks showed 6 x {@link EncryptionUtil#AES_BLOCK_SIZE} is a good buffer size.
   */
  private static final int BUFFER_CAPACITY = 6 * AES_BLOCK_SIZE; // 96 B

  private static final long AES_BLOCK_SIZE_MOD_MASK = AES_BLOCK_SIZE - 1;
  static final int HEADER_IV_LENGTH = HEADER_LENGTH + IV_LENGTH;

  // Most fields are not final for the clone() method.
  private boolean isClone;
  private final long sliceOffset;
  private final long sliceEnd;
  private IndexInput indexInput;
  private AesCtrEncrypter encrypter;
  private ByteBuffer inBuffer;
  private ByteBuffer outBuffer;
  private byte[] inArray;
  private byte[] oneByteBuf;
  private int padding;
  private boolean closed;

  /**
   * @param indexInput The delegate {@link IndexInput} to read encrypted data from.
   * @param key        The encryption key. It is cloned internally, its content is not modified, and no reference to it is kept.
   */
  public EncryptingIndexInput(IndexInput indexInput, byte[] key) throws IOException {
    this(indexInput, key, CipherAesCtrEncrypter.FACTORY);
  }

  /**
   * @param indexInput The delegate {@link IndexInput} to read encrypted data from.
   * @param key        The encryption key. It is cloned internally, its content is not modified, and no reference to it is kept.
   * @param factory    The factory to use to create one instance of {@link AesCtrEncrypter}. This instance may be cloned.
   */
  public EncryptingIndexInput(IndexInput indexInput, byte[] key, AesCtrEncrypterFactory factory) throws IOException {
    this("Decrypting " + indexInput.toString(), HEADER_IV_LENGTH, getEncryptedDataLength(indexInput),
        false, indexInput, createEncrypter(indexInput, key, factory));
  }

  private EncryptingIndexInput(String resourceDescription, long sliceOffset, long sliceLength, boolean isClone,
                               IndexInput indexInput, AesCtrEncrypter encrypter) {
    super(resourceDescription);
    assert sliceOffset >= 0 && sliceLength >= 0;
    this.sliceOffset = sliceOffset;
    this.sliceEnd = sliceOffset + sliceLength;
    this.isClone = isClone;
    this.indexInput = indexInput;
    this.encrypter = encrypter;
    encrypter.init(0);
    inBuffer = ByteBuffer.allocate(getBufferCapacity());
    outBuffer = ByteBuffer.allocate(getBufferCapacity() + AES_BLOCK_SIZE);
    outBuffer.limit(0);
    assert inBuffer.hasArray() && outBuffer.hasArray();
    assert inBuffer.arrayOffset() == 0;
    inArray = inBuffer.array();
    oneByteBuf = new byte[1];
  }

  /**
   * Gets the length of the encrypted data in the delegate {@link IndexInput}.
   * It ignores the index header and the IV at the beginning of the file, as well as the index footer at the end.
   */
  private static long getEncryptedDataLength(IndexInput indexInput) {
    return indexInput.length() - HEADER_IV_LENGTH - CodecUtil.footerLength();
  }

  /**
   * Creates the {@link AesCtrEncrypter} based on the secret key and the IV at the beginning of the index input (just after the header).
   */
  private static AesCtrEncrypter createEncrypter(IndexInput indexInput, byte[] key, AesCtrEncrypterFactory factory) throws IOException {
    indexInput.seek(HEADER_LENGTH);
    byte[] iv = new byte[IV_LENGTH];
    indexInput.readBytes(iv, 0, iv.length, false);
    return factory.create(key, iv);
  }

  /**
   * Gets the buffer capacity. It must be a multiple of {@link EncryptionUtil#AES_BLOCK_SIZE}.
   */
  protected int getBufferCapacity() {
    return BUFFER_CAPACITY;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      if (!isClone) {
        indexInput.close();
      }
    }
  }

  @Override
  public long getFilePointer() {
    return getPosition() - sliceOffset;
  }

  /**
   * Gets the current internal position in the delegate {@link IndexInput}. It includes the header and IV length.
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
      // The target position is within the buffered output. Just move the output buffer position.
      outBuffer.position(outBuffer.position() + (int) (targetPosition - currentPosition));
      assert targetPosition == delegatePosition - outBuffer.remaining();
    } else {
      indexInput.seek(targetPosition);
      setPosition(targetPosition);
    }
  }

  private void setPosition(long position) {
    inBuffer.clear();
    outBuffer.clear();
    outBuffer.limit(0);
    // Compute the counter by ignoring the header and IV.
    long counter = (position - HEADER_IV_LENGTH) / AES_BLOCK_SIZE;
    encrypter.init(counter);
    padding = (int) (position & AES_BLOCK_SIZE_MOD_MASK);
    inBuffer.position(padding);
  }

  /**
   * Returns the number of encrypted/decrypted bytes in the file.
   * <p>It is the logical length of the file, not the physical length. It excludes the top header and IV added artificially
   * to manage the encryption. It includes only and all the encrypted bytes (probably a header, content, and a footer).</p>
   * <p>With AES/CTR/NoPadding encryption, the length of the encrypted data is identical to the length of the decrypted data.</p>
   */
  @Override
  public long length() {
    return sliceEnd - sliceOffset;
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    if (offset < 0 || length < 0 || offset + length > length()) {
      throw new IllegalArgumentException("Slice \"" + sliceDescription + "\" out of bounds (offset=" + offset
          + ", sliceLength=" + length + ", fileLength=" + length() + ") of " + this);
    }
    EncryptingIndexInput slice = new EncryptingIndexInput(getFullSliceDescription(sliceDescription),
        sliceOffset + offset, length, true, indexInput.clone(), encrypter.clone());
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
      throw new IllegalArgumentException("Invalid read buffer parameters (offset=" + offset + ", length=" + length
          + ", arrayLength=" + b.length + ")");
    }
    if (getPosition() + length > sliceEnd) {
      throw new EOFException("Read beyond EOF (position=" + (getPosition() - sliceOffset) + ", arrayLength=" + length
          + ", fileLength=" + length() + ") in " + this);
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

  private void decryptBuffer() {
    assert inBuffer.position() > padding : "position=" + inBuffer.position() + ", padding=" + padding;
    inBuffer.flip();
    outBuffer.clear();
    encrypter.process(inBuffer, outBuffer);
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
    clone.encrypter = encrypter.clone();
    clone.inBuffer = ByteBuffer.allocate(getBufferCapacity());
    clone.outBuffer = ByteBuffer.allocate(getBufferCapacity() + AES_BLOCK_SIZE);
    clone.inArray = clone.inBuffer.array();
    clone.oneByteBuf = new byte[1];
    // The clone must be initialized.
    clone.setPosition(getPosition());
    return clone;
  }
}