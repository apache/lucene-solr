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

import org.apache.lucene.index.SegmentInfo;

public class EncryptingDirectory extends FilterDirectory {

  private final KeySupplier keySupplier;
  private final SegmentKeySupplier segmentKeySupplier;
  private final CipherPool cipherPool;
  private final SegmentInfo segmentInfo;

  public EncryptingDirectory(Directory directory, KeySupplier keySupplier, CipherPool cipherPool) {
    super(directory);
    this.keySupplier = keySupplier;
    segmentKeySupplier = null;
    this.cipherPool = cipherPool;
    this.segmentInfo = null;
  }

  public EncryptingDirectory(Directory directory, SegmentKeySupplier keySupplier, CipherPool cipherPool, SegmentInfo segmentInfo) {
    super(directory);
    this.keySupplier = null;
    segmentKeySupplier = keySupplier;
    this.cipherPool = cipherPool;
    this.segmentInfo = segmentInfo;
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context)
      throws IOException {
    IndexOutput indexOutput = in.createOutput(name, context);
    byte[] key = getKey(name);
    return key == null ? indexOutput : new EncryptingIndexOutput(indexOutput, key, cipherPool, getSegmentId());
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    IndexOutput indexOutput = in.createTempOutput(prefix, suffix, context);
    byte[] key = getKey(indexOutput.getName());
    return key == null ? indexOutput : new EncryptingIndexOutput(indexOutput, key, cipherPool, getSegmentId());
  }

  @Override
  public IndexInput openInput(String name, IOContext context)
      throws IOException {
    IndexInput indexInput = in.openInput(name, context);
    byte[] key = getKey(name);
    return key == null ? indexInput : new EncryptingIndexInput(indexInput, key, cipherPool);
  }

  private byte[] getKey(String fileName) {
    return segmentInfo == null ? keySupplier.getKey(fileName) : segmentKeySupplier.getKey(segmentInfo, fileName);
  }

  private byte[] getSegmentId() {
    return segmentInfo == null ? null : segmentInfo.getId();
  }

  public interface KeySupplier {

    /**
     * Gets the encryption key for the provided file name.
     * @return The key; or null if none, in this case the data is not encrypted. It must be either 128, 192 or 256 bits long.
     */
    byte[] getKey(String fileName);
  }

  public interface SegmentKeySupplier {

    /**
     * Gets the encryption key for the provided file name of a specific segment.
     * @return The key; or null if none, in this case the data is not encrypted. It must be either 128, 192 or 256 bits long.
     */
    byte[] getKey(SegmentInfo segmentInfo, String fileName);
  }
}