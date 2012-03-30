package org.apache.lucene.codecs.intblock;

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

import org.apache.lucene.codecs.sep.IntIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IntsRef;

// TODO: much of this can be shared code w/ the fixed case

/** Abstract base class that reads variable-size blocks of ints
 *  from an IndexInput.  While this is a simple approach, a
 *  more performant approach would directly create an impl
 *  of IntIndexInput inside Directory.  Wrapping a generic
 *  IndexInput will likely cost performance.
 *
 * @lucene.experimental
 */
public abstract class VariableIntBlockIndexInput extends IntIndexInput {

  protected final IndexInput in;
  protected final int maxBlockSize;

  protected VariableIntBlockIndexInput(final IndexInput in) throws IOException {
    this.in = in;
    maxBlockSize = in.readInt();
  }

  @Override
  public Reader reader() throws IOException {
    final int[] buffer = new int[maxBlockSize];
    final IndexInput clone = (IndexInput) in.clone();
    // TODO: can this be simplified?
    return new Reader(clone, buffer, this.getBlockReader(clone, buffer));
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public Index index() {
    return new Index();
  }

  protected abstract BlockReader getBlockReader(IndexInput in, int[] buffer) throws IOException;

  public interface BlockReader {
    public int readBlock() throws IOException;
    public void seek(long pos) throws IOException;
  }

  public static class Reader extends IntIndexInput.Reader {
    private final IndexInput in;

    public final int[] pending;
    int upto;

    private boolean seekPending;
    private long pendingFP;
    private int pendingUpto;
    private long lastBlockFP;
    private int blockSize;
    private final BlockReader blockReader;
    private final IntsRef bulkResult = new IntsRef();

    public Reader(final IndexInput in, final int[] pending, final BlockReader blockReader)
      throws IOException {
      this.in = in;
      this.pending = pending;
      bulkResult.ints = pending;
      this.blockReader = blockReader;
    }

    void seek(final long fp, final int upto) throws IOException {
      // TODO: should we do this in real-time, not lazy?
      pendingFP = fp;
      pendingUpto = upto;
      assert pendingUpto >= 0: "pendingUpto=" + pendingUpto;
      seekPending = true;
    }

    private final void maybeSeek() throws IOException {
      if (seekPending) {
        if (pendingFP != lastBlockFP) {
          // need new block
          in.seek(pendingFP);
          blockReader.seek(pendingFP);
          lastBlockFP = pendingFP;
          blockSize = blockReader.readBlock();
        }
        upto = pendingUpto;

        // TODO: if we were more clever when writing the
        // index, such that a seek point wouldn't be written
        // until the int encoder "committed", we could avoid
        // this (likely minor) inefficiency:

        // This is necessary for int encoders that are
        // non-causal, ie must see future int values to
        // encode the current ones.
        while(upto >= blockSize) {
          upto -= blockSize;
          lastBlockFP = in.getFilePointer();
          blockSize = blockReader.readBlock();
        }
        seekPending = false;
      }
    }

    @Override
    public int next() throws IOException {
      this.maybeSeek();
      if (upto == blockSize) {
        lastBlockFP = in.getFilePointer();
        blockSize = blockReader.readBlock();
        upto = 0;
      }

      return pending[upto++];
    }

    @Override
    public IntsRef read(final int count) throws IOException {
      this.maybeSeek();
      if (upto == blockSize) {
        lastBlockFP = in.getFilePointer();
        blockSize = blockReader.readBlock();
        upto = 0;
      }
      bulkResult.offset = upto;
      if (upto + count < blockSize) {
        bulkResult.length = count;
        upto += count;
      } else {
        bulkResult.length = blockSize - upto;
        upto = blockSize;
      }

      return bulkResult;
    }
  }

  private class Index extends IntIndexInput.Index {
    private long fp;
    private int upto;

    @Override
    public void read(final DataInput indexIn, final boolean absolute) throws IOException {
      if (absolute) {
        upto = indexIn.readVInt();
        fp = indexIn.readVLong();
      } else {
        final int uptoDelta = indexIn.readVInt();
        if ((uptoDelta & 1) == 1) {
          // same block
          upto += uptoDelta >>> 1;
        } else {
          // new block
          upto = uptoDelta >>> 1;
          fp += indexIn.readVLong();
        }
      }
      // TODO: we can't do this assert because non-causal
      // int encoders can have upto over the buffer size
      //assert upto < maxBlockSize: "upto=" + upto + " max=" + maxBlockSize;
    }

    @Override
    public String toString() {
      return "VarIntBlock.Index fp=" + fp + " upto=" + upto + " maxBlock=" + maxBlockSize;
    }

    @Override
    public void seek(final IntIndexInput.Reader other) throws IOException {
      ((Reader) other).seek(fp, upto);
    }

    @Override
    public void set(final IntIndexInput.Index other) {
      final Index idx = (Index) other;
      fp = idx.fp;
      upto = idx.upto;
    }

    @Override
    public Index clone() {
      Index other = new Index();
      other.fp = fp;
      other.upto = upto;
      return other;
    }
  }
}
