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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.util.IOBiFunction;
import org.apache.lucene.util.IOUtils;

/**
 * Abstract {@link FilterDirectory} to encrypt/decrypt files.
 * <p>For each file, if {@link #getKey(String)} returns a non-null key, then the file is encrypted/decrypted on the fly
 * in the created {@link IndexOutput} or {@link IndexInput}.</p>
 *
 * @see EncryptingIndexOutput
 * @see EncryptingIndexInput
 *
 * @lucene.experimental
 */
public abstract class EncryptingDirectory extends FilterDirectory {

  protected EncryptingDirectory(Directory directory) {
    super(directory);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return createEncryptingIndexIOSafely(in.createOutput(name, context), name, this::createEncryptingIndexOutput);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    IndexOutput tmpOutput = in.createTempOutput(prefix, suffix, context);
    return createEncryptingIndexIOSafely(tmpOutput, tmpOutput.getName(), this::createEncryptingIndexOutput);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    return createEncryptingIndexIOSafely(in.openInput(name, context), name, this::createEncryptingIndexInput);
  }

  private <T extends Closeable> T createEncryptingIndexIOSafely(T delegate, String name, IOBiFunction<T, byte[], T> encryptingIOFactory) throws IOException {
    boolean success = false;
    try {
      byte[] key = getKey(name);
      T indexIO = key == null ? delegate : encryptingIOFactory.apply(delegate, key);
      success = true;
      return indexIO;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(delegate);
      }
    }
  }

  /**
   * Gets the encryption key for the provided file name in this directory.
   *
   * @return The key, its content is not modified; or null if none, in this case the data is not encrypted/decrypted.
   */
  protected abstract byte[] getKey(String fileName);

  /**
   * Creates an {@link IndexOutput} that encrypts data on the fly using the provided key.
   *
   * @param indexOutput The delegate {@link IndexOutput} to write the encrypted data to.
   * @param key         It has to be cloned. Its content must not be modified.
   */
  protected abstract IndexOutput createEncryptingIndexOutput(IndexOutput indexOutput, byte[] key) throws IOException;

  /**
   * Creates an {@link IndexInput} that decrypts data on the fly using the provided key.
   *
   * @param indexInput The delegate {@link IndexInput} to read the encrypted data from.
   * @param key        It has to be cloned. Its content must not be modified.
   */
  protected abstract IndexInput createEncryptingIndexInput(IndexInput indexInput, byte[] key) throws IOException;
}