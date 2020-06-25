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

public abstract class EncryptingDirectory extends FilterDirectory {

  protected EncryptingDirectory(Directory directory) {
    super(directory);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context)
      throws IOException {
    IndexOutput indexOutput = in.createOutput(name, context);
    byte[] key = getKey(name);
    return key == null ? indexOutput : createEncryptingIndexOutput(indexOutput, key);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    IndexOutput indexOutput = in.createTempOutput(prefix, suffix, context);
    byte[] key = getKey(indexOutput.getName());
    return key == null ? indexOutput : createEncryptingIndexOutput(indexOutput, key);
  }

  @Override
  public IndexInput openInput(String name, IOContext context)
      throws IOException {
    IndexInput indexInput = in.openInput(name, context);
    byte[] key = getKey(name);
    return key == null ? indexInput : createEncryptingIndexInput(indexInput, key);
  }

  /**
   * Gets the encryption key for the provided file name.
   * @return The key, this array content is not modified; or null if none, in this case the data is not encrypted.
   */
  protected abstract byte[] getKey(String fileName);

  protected abstract IndexOutput createEncryptingIndexOutput(IndexOutput indexOutput, byte[] key) throws IOException;

  protected abstract IndexInput createEncryptingIndexInput(IndexInput indexInput, byte[] key) throws IOException;
}