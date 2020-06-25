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

import java.io.IOException;

public class FileEncryptingDirectory extends EncryptingDirectory {

  private final KeySupplier keySupplier;

  public FileEncryptingDirectory(Directory directory, KeySupplier keySupplier) {
    super(directory);
    this.keySupplier = keySupplier;
  }

  @Override
  protected byte[] getKey(String fileName) {
    return keySupplier.getKey(fileName);
  }

  @Override
  protected IndexOutput createEncryptingIndexOutput(IndexOutput indexOutput, byte[] key) throws IOException {
    return new EncryptingIndexOutput(indexOutput, key);
  }

  @Override
  protected IndexInput createEncryptingIndexInput(IndexInput indexInput, byte[] key) throws IOException {
    return new EncryptingIndexInput(indexInput, key);
  }

  public interface KeySupplier {
    /**
     * Gets the encryption key for the provided file name.
     * @return The key; or null if none, in this case the data is not encrypted. It must be either 128, 192 or 256 bits long.
     */
    byte[] getKey(String fileName);
  }
}
