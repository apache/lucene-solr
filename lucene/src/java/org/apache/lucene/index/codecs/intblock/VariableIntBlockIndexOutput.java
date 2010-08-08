package org.apache.lucene.index.codecs.intblock;

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

/** Naive int block API that writes vInts.  This is
 *  expected to give poor performance; it's really only for
 *  testing the pluggability.  One should typically use pfor instead. */

import java.io.IOException;

import org.apache.lucene.index.codecs.sep.IntIndexOutput;
import org.apache.lucene.store.IndexOutput;

// TODO: much of this can be shared code w/ the fixed case

/** Abstract base class that writes variable-size blocks of ints
 *  to an IndexOutput.  While this is a simple approach, a
 *  more performant approach would directly create an impl
 *  of IntIndexOutput inside Directory.  Wrapping a generic
 *  IndexInput will likely cost performance.
 *
 * @lucene.experimental
 */
public abstract class VariableIntBlockIndexOutput extends IntIndexOutput {

  protected final IndexOutput out;

  private int upto;

  private static final int MAX_BLOCK_SIZE = 1 << 8;

  /** NOTE: maxBlockSize plus the max non-causal lookahead
   *  of your codec must be less than 256.  EG Simple9
   *  requires lookahead=1 because on seeing the Nth value
   *  it knows it must now encode the N-1 values before it. */
  protected VariableIntBlockIndexOutput(IndexOutput out, int maxBlockSize) throws IOException {
    if (maxBlockSize > MAX_BLOCK_SIZE) {
      throw new IllegalArgumentException("maxBlockSize must be <= " + MAX_BLOCK_SIZE + "; got " + maxBlockSize);
    }
    this.out = out;
    out.writeInt(maxBlockSize);
  }

  /** Called one value at a time.  Return the number of
   *  buffered input values that have been written to out. */
  protected abstract int add(int value) throws IOException;

  @Override
  public Index index() throws IOException {
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
      upto = VariableIntBlockIndexOutput.this.upto;
    }

    @Override
    public void set(IntIndexOutput.Index other) throws IOException {
      Index idx = (Index) other;
      lastFP = fp = idx.fp;
      lastUpto = upto = idx.upto;
    }

    @Override
    public void write(IndexOutput indexOut, boolean absolute) throws IOException {
      assert upto >= 0;
      if (absolute) {
        indexOut.writeVLong(fp);
        indexOut.writeByte((byte) upto);
      } else if (fp == lastFP) {
        // same block
        indexOut.writeVLong(0);
        assert upto >= lastUpto;
        indexOut.writeByte((byte) upto);
      } else {      
        // new block
        indexOut.writeVLong(fp - lastFP);
        indexOut.writeByte((byte) upto);
      }
      lastUpto = upto;
      lastFP = fp;
    }
  }

  @Override
  public void write(int v) throws IOException {
    upto -= add(v)-1;
    assert upto >= 0;
  }

  @Override
  public void close() throws IOException {
    try {
      // stuff 0s in until the "real" data is flushed:
      int stuffed = 0;
      while(upto > stuffed) {
        upto -= add(0)-1;
        assert upto >= 0;
        stuffed += 1;
      }
    } finally {
      out.close();
    }
  }
}
