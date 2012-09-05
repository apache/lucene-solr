package org.apache.lucene.codecs.intblock;

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

/** Naive int block API that writes vInts.  This is
 *  expected to give poor performance; it's really only for
 *  testing the pluggability.  One should typically use pfor instead. */

import java.io.IOException;

import org.apache.lucene.codecs.sep.IntIndexOutput;
import org.apache.lucene.store.IndexOutput;

/** Abstract base class that writes fixed-size blocks of ints
 *  to an IndexOutput.  While this is a simple approach, a
 *  more performant approach would directly create an impl
 *  of IntIndexOutput inside Directory.  Wrapping a generic
 *  IndexInput will likely cost performance.
 *
 * @lucene.experimental
 */
public abstract class FixedIntBlockIndexOutput extends IntIndexOutput {

  protected final IndexOutput out;
  private final int blockSize;
  protected final int[] buffer;
  private int upto;

  protected FixedIntBlockIndexOutput(IndexOutput out, int fixedBlockSize) throws IOException {
    blockSize = fixedBlockSize;
    this.out = out;
    out.writeVInt(blockSize);
    buffer = new int[blockSize];
  }

  protected abstract void flushBlock() throws IOException;

  @Override
  public IntIndexOutput.Index index() throws IOException {
    return new Index();
  }

  private class Index extends IntIndexOutput.Index {
    long fp;
    int upto;
    long lastFP;
    int lastUpto;

    @Override
    public void mark() throws IOException {
      fp = out.getFilePointer();
      upto = FixedIntBlockIndexOutput.this.upto;
    }

    @Override
    public void copyFrom(IntIndexOutput.Index other, boolean copyLast) throws IOException {
      Index idx = (Index) other;
      fp = idx.fp;
      upto = idx.upto;
      if (copyLast) {
        lastFP = fp;
        lastUpto = upto;
      }
    }

    @Override
    public void write(IndexOutput indexOut, boolean absolute) throws IOException {
      if (absolute) {
        indexOut.writeVInt(upto);
        indexOut.writeVLong(fp);
      } else if (fp == lastFP) {
        // same block
        assert upto >= lastUpto;
        int uptoDelta = upto - lastUpto;
        indexOut.writeVInt(uptoDelta << 1 | 1);
      } else {      
        // new block
        indexOut.writeVInt(upto << 1);
        indexOut.writeVLong(fp - lastFP);
      }
      lastUpto = upto;
      lastFP = fp;
    }

    @Override
    public String toString() {
      return "fp=" + fp + " upto=" + upto;
    }
  }

  @Override
  public void write(int v) throws IOException {
    buffer[upto++] = v;
    if (upto == blockSize) {
      flushBlock();
      upto = 0;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (upto > 0) {
        // NOTE: entries in the block after current upto are
        // invalid
        flushBlock();
      }
    } finally {
      out.close();
    }
  }
}
