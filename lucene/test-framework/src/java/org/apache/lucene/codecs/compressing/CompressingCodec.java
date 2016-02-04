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
package org.apache.lucene.codecs.compressing;

import java.util.Random;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.compressing.dummy.DummyCompressingCodec;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomInts;

/**
 * A codec that uses {@link CompressingStoredFieldsFormat} for its stored
 * fields and delegates to the default codec for everything else.
 */
public abstract class CompressingCodec extends FilterCodec {

  /**
   * Create a random instance.
   */
  public static CompressingCodec randomInstance(Random random, int chunkSize, int maxDocsPerChunk, boolean withSegmentSuffix, int blockSize) {
    switch (random.nextInt(4)) {
    case 0:
      return new FastCompressingCodec(chunkSize, maxDocsPerChunk, withSegmentSuffix, blockSize);
    case 1:
      return new FastDecompressionCompressingCodec(chunkSize, maxDocsPerChunk, withSegmentSuffix, blockSize);
    case 2:
      return new HighCompressionCompressingCodec(chunkSize, maxDocsPerChunk, withSegmentSuffix, blockSize);
    case 3:
      return new DummyCompressingCodec(chunkSize, maxDocsPerChunk, withSegmentSuffix, blockSize);
    default:
      throw new AssertionError();
    }
  }

  /**
   * Creates a random {@link CompressingCodec} that is using an empty segment 
   * suffix
   */
  public static CompressingCodec randomInstance(Random random) {
    final int chunkSize = random.nextBoolean() ? RandomInts.randomIntBetween(random, 1, 10) : RandomInts.randomIntBetween(random, 1, 1 << 15);
    final int chunkDocs = random.nextBoolean() ? RandomInts.randomIntBetween(random, 1, 10) : RandomInts.randomIntBetween(random, 64, 1024);
    final int blockSize = random.nextBoolean() ? RandomInts.randomIntBetween(random, 1, 10) : RandomInts.randomIntBetween(random, 1, 1024);
    return randomInstance(random, chunkSize, chunkDocs, false, blockSize);
  }
  
  /**
   * Creates a random {@link CompressingCodec} that is using a segment suffix
   */
  public static CompressingCodec randomInstance(Random random, boolean withSegmentSuffix) {
    return randomInstance(random, 
                          RandomInts.randomIntBetween(random, 1, 1 << 15), 
                          RandomInts.randomIntBetween(random, 64, 1024), 
                          withSegmentSuffix,
                          RandomInts.randomIntBetween(random, 1, 1024));
  }

  private final CompressingStoredFieldsFormat storedFieldsFormat;
  private final CompressingTermVectorsFormat termVectorsFormat;

  /**
   * Creates a compressing codec with a given segment suffix
   */
  public CompressingCodec(String name, String segmentSuffix, CompressionMode compressionMode, int chunkSize, int maxDocsPerChunk, int blockSize) {
    super(name, TestUtil.getDefaultCodec());
    this.storedFieldsFormat = new CompressingStoredFieldsFormat(name, segmentSuffix, compressionMode, chunkSize, maxDocsPerChunk, blockSize);
    this.termVectorsFormat = new CompressingTermVectorsFormat(name, segmentSuffix, compressionMode, chunkSize, blockSize);
  }
  
  /**
   * Creates a compressing codec with an empty segment suffix
   */
  public CompressingCodec(String name, CompressionMode compressionMode, int chunkSize, int maxDocsPerChunk, int blockSize) {
    this(name, "", compressionMode, chunkSize, maxDocsPerChunk, blockSize);
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return storedFieldsFormat;
  }

  @Override
  public TermVectorsFormat termVectorsFormat() {
    return termVectorsFormat;
  }

  @Override
  public String toString() {
    return getName() + "(storedFieldsFormat=" + storedFieldsFormat + ", termVectorsFormat=" + termVectorsFormat + ")";
  }
}
