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

package org.apache.lucene.util.crypto;

import java.nio.ByteBuffer;

/**
 * Stateful Encrypter specialized for AES algorithm in CTR (counter) mode with no padding.
 * <p>In CTR mode, encryption and decryption are actually the same operation, so this API does not require to specify
 * whether it is used to encrypt or decrypt.</p>
 * <p>An {@link AesCtrEncrypter} must be first {@link #init(long) initialized} before it can be used to
 * {@link #process(ByteBuffer, ByteBuffer) encrypt/decrypt}.</p>
 * <p>Not thread safe.</p>
 *
 * @lucene.experimental
 */
public interface AesCtrEncrypter extends Cloneable {

  /**
   * Initializes this encrypter at the provided CTR block counter (counter of blocks of size {@link EncryptionUtil#AES_BLOCK_SIZE}).
   * <p>For example, the data byte at index i is inside the block at counter = i / {@link EncryptionUtil#AES_BLOCK_SIZE}.
   * CTR mode computes an IV for this block based on the initial IV (at counter 0) and the provided counter. This allows
   * efficient random access to encrypted data. Only the target block needs to be decrypted.</p>
   * <p>This method must be called first. Then the next call to {@link #process(ByteBuffer, ByteBuffer)} will start at the
   * beginning of the block: the first byte of data at input buffer {@link ByteBuffer#position()} must be the first byte
   * of the block.</p>
   */
  void init(long counter);

  /**
   * Encrypts/decrypts the provided input buffer data and stores the encrypted/decrypted data in an output buffer.
   * In CTR mode, encryption and decryption are actually the same operation.
   * <p>Both buffers must be backed by array ({@link ByteBuffer#hasArray()} returns true), and must not share the same
   * array.</p>
   * <p>Do not call this method when this {@link AesCtrEncrypter} is not {@link #init(long) initialized}.</p>
   * <p>This method takes care of incrementing the CTR counter while encrypting/decrypting the data. It can be called
   * repeatedly without calling {@link #init(long)} again. {@link #init(long)} is called only to jump to a given block.</p>
   *
   * @param inBuffer  Input data from {@link ByteBuffer#position()} (inclusive) to {@link ByteBuffer#limit()} (exclusive).
   * @param outBuffer Output data to be stored at {@link ByteBuffer#position()}. outBuffer {@link ByteBuffer#remaining()}
   *                  must be greater than or equal to inBuffer {@link ByteBuffer#remaining()}.
   */
  void process(ByteBuffer inBuffer, ByteBuffer outBuffer);

  /**
   * Clones this {@link AesCtrEncrypter} for efficiency as it clones the internal encryption key and IV.
   * The returned clone must be initialized by calling {@link #init(long)} first.
   */
  AesCtrEncrypter clone();
}
