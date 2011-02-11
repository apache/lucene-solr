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

import java.io.IOException;

import org.apache.lucene.index.codecs.sep.IntIndexOutput;
import org.apache.lucene.store.IndexOutput;

//TODO: much of this can be shared code w/ the variable case
//TODO: not specific to simple64, (e.g. can be used by simple9/simple16 at least)

/** Abstract base class that writes variable-size blocks of ints
 *  to an IndexOutput that have a fixed physical size in bytes.  
 *  While this is a simple approach, a
 *  more performant approach would directly create an impl
 *  of IntIndexOutput inside Directory.  Wrapping a generic
 *  IndexInput will likely cost performance.
 *
 * @lucene.experimental
 */
public abstract class VariableIntFixedPhyBlockIndexOutput extends IntIndexOutput {

  protected final IndexOutput out;

  private int upto;

  // TODO: use vint so we can use unused simple selectors for larger blocks of 1s?
  private static final int MAX_BLOCK_SIZE = 1 << 8;
  private final int phyBlockSize;
  private static final int HEADER = 8; /* two ints */
  
  /** NOTE: maxBlockSize plus the max non-causal lookahead
   *  of your codec must be less than 256.  EG Simple9
   *  requires lookahead=1 because on seeing the Nth value
   *  it knows it must now encode the N-1 values before it. */
  protected VariableIntFixedPhyBlockIndexOutput(IndexOutput out, int maxBlockSize, int phyBlockSize) throws IOException {
    if (maxBlockSize > MAX_BLOCK_SIZE) {
      throw new IllegalArgumentException("maxBlockSize must be <= " + MAX_BLOCK_SIZE + "; got " + maxBlockSize);
    }
    this.out = out;
    this.phyBlockSize = phyBlockSize;
    out.writeInt(maxBlockSize);
    out.writeInt(phyBlockSize);
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
      upto = VariableIntFixedPhyBlockIndexOutput.this.upto;
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
      assert (fp - HEADER) % phyBlockSize == 0;
      if (absolute) {
        indexOut.writeVLong((fp - HEADER) / phyBlockSize);
        indexOut.writeByte((byte) upto);
      } else if (fp == lastFP) {
        // same block
        assert upto >= lastUpto;
        int uptoDelta = upto - lastUpto;
        indexOut.writeVLong(uptoDelta << 1 | 1);
      } else {      
        // new block
        indexOut.writeVLong(((fp - lastFP) / phyBlockSize) << 1);
        indexOut.writeByte((byte) upto);
      }
      lastUpto = upto;
      lastFP = fp;
    }

    @Override
    public String toString() {
      return "VarIntFixedPhyBlock.Output fp=" + fp + " upto=" + upto;
    }
  }

  private boolean abort;

  @Override
  public void write(int v) throws IOException {
    boolean success = false;
    try {
      upto -= add(v)-1;
      assert upto >= 0;
      success = true;
    } finally {
      abort |= !success;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      // stuff 0s in until the "real" data is flushed:
      if (!abort) {
        int stuffed = 0;
        while(upto > stuffed) {
          upto -= add(0)-1;
          assert upto >= 0;
          stuffed += 1;
        }
      }
    } finally {
      out.close();
    }
  }
}
