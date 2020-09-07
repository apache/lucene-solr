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

/**
 * {@link EncryptingDirectory} specific to a given {@link SegmentInfo}. It determines the encryption keys based on the
 * {@link SegmentInfo} and file name.
 *
 * @lucene.experimental
 */
public class SegmentEncryptingDirectory extends EncryptingDirectory {

  private final SegmentKeySupplier segmentKeySupplier;
  protected final SegmentInfo segmentInfo;

  /**
   * @param directory          The delegate {@link Directory} to get files from.
   * @param segmentKeySupplier The encryption key supplier.
   * @param segmentInfo        The specific {@link SegmentInfo} this directory will use to get encryption keys for.
   */
  public SegmentEncryptingDirectory(Directory directory, SegmentKeySupplier segmentKeySupplier, SegmentInfo segmentInfo) {
    super(directory);
    this.segmentKeySupplier = segmentKeySupplier;
    this.segmentInfo = segmentInfo;
  }

  @Override
  protected byte[] getKey(String fileName) {
    return segmentKeySupplier.getKey(segmentInfo, fileName);
  }

  @Override
  protected IndexOutput createEncryptingIndexOutput(IndexOutput indexOutput, byte[] key) throws IOException {
    return new EncryptingIndexOutput(indexOutput, key, segmentInfo.getId());
  }

  @Override
  protected IndexInput createEncryptingIndexInput(IndexInput indexInput, byte[] key) throws IOException {
    return new EncryptingIndexInput(indexInput, key);
  }

  /**
   * Provides encryption keys depending on {@link SegmentInfo} and file name.
   */
  public interface SegmentKeySupplier {
    /**
     * Gets the encryption key for the provided file name of a specific segment.
     *
     * @return The key, its content is not modified; or null if none, in this case the data is not encrypted.
     * It must be either 16, 24 or 32 bytes long.
     */
    byte[] getKey(SegmentInfo segmentInfo, String fileName);
  }
}
