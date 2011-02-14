package org.apache.lucene.index.codecs.fixed;

/**
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

import java.io.IOException;

import org.apache.lucene.index.codecs.intblock.FixedIntBlockIndexOutput;
import org.apache.lucene.index.codecs.sep.IntIndexOutput;

/**
 * Interleaves docs and freqs into a single file, by buffer freqs
 * until the docs block is flushed, and then flushing the freqs block.
 */
final class InterleavedIntBlockIndexOutput extends IntIndexOutput {
  private final FixedIntBlockIndexOutput a;
  private final FixedIntBlockIndexOutput b;
  private final int buffer[];
  private int bufferIdx;
  private boolean isB;
  
  /**
   * @param a Fixed int block codec for docs
   * @param b Fixed int block codec for freqs
   * <p>
   * NOTE: the two codecs must use the same blocksize.
   */
  public InterleavedIntBlockIndexOutput(FixedIntBlockIndexOutput a, FixedIntBlockIndexOutput b) {
    if (a.blockSize != b.blockSize) {
      throw new IllegalArgumentException("interleaved blocks must have the same block size");
    }
    this.a = a;
    this.b = b;
    this.buffer = new int[a.blockSize];
  }
  
  @Override
  public void write(int v) throws IOException {
    if (isB) {
      buffer[bufferIdx++] = v;
      if (bufferIdx == buffer.length) {
        // we have written a full block of documents,
        // so flush any pending freqs.
        flushFreqs();
        // we don't need to force a flush on the block output,
        // as we know we are at blocksize here and it just flushed.
      }
    } else {
      a.write(v);
    }
    isB = !isB;
  }

  private void flushFreqs() throws IOException {
    for (int i = 0; i < bufferIdx; i++) {
      b.write(buffer[i]);
    }
    bufferIdx = 0;
  }

  @Override
  public Index index() throws IOException {
    return a.index();
  }

  /**
   * Force a flush of any pending docs/freqs.
   * This is necessary when we cross field boundaries that have different
   * omitTF settings.
   */
  public void flush() throws IOException {
    a.flush();
    flushFreqs();
    b.flush();
  }
  
  @Override
  public void close() throws IOException {
    try {
      flush();
    } finally {
      b.close();
    }
  }
}
