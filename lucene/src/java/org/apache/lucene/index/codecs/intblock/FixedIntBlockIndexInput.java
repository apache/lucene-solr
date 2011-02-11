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

import org.apache.lucene.index.codecs.sep.IntIndexInput;
import org.apache.lucene.index.BulkPostingsEnum;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;

/** Abstract base class that reads fixed-size blocks of ints
 *  from an IndexInput.  While this is a simple approach, a
 *  more performant approach would directly create an impl
 *  of IntIndexInput inside Directory.  Wrapping a generic
 *  IndexInput will likely cost performance.
 *
 * @lucene.experimental
 */
public abstract class FixedIntBlockIndexInput extends IntIndexInput {

  private final IndexInput in;
  protected final int blockSize;
  
  public FixedIntBlockIndexInput(final IndexInput in) throws IOException {
    this.in = in;
    //blockSize = in.readVInt();
    blockSize = in.readInt();
    //System.out.println("BLOCK size " + blockSize);
  }

  @Override
  public Reader reader() throws IOException {
    final int[] buffer = new int[blockSize];
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
    public void readBlock() throws IOException;
    // nocommit -- need seek here so mmapdir "knows"
  }

  private static class Reader extends BulkPostingsEnum.BlockReader {
    private final IndexInput in;

    protected final int[] pending;
    private int offset;

    private long lastBlockFP;
    private final BlockReader blockReader;
    private final int blockSize;

    public Reader(final IndexInput in, final int[] pending, final BlockReader blockReader)
      throws IOException {
      this.in = in;
      this.pending = pending;
      this.blockSize = pending.length;
      this.blockReader = blockReader;
    }

    void seek(final long fp, final int upto) throws IOException {
      offset = upto;
      if (fp != lastBlockFP) {
        // Seek to new block; this may in fact be the next
        // block ie when caller is doing sequential scan (eg
        // PrefixQuery)
        //System.out.println("  seek block fp=" + fp + " vs last=" + lastBlockFP + " upto=" + upto);
        in.seek(fp);
        fill();
      } else {
        // Seek within current block
        //System.out.println("  seek in-block fp=" + fp + " upto=" + offset);
      }
    }

    @Override
    public int[] getBuffer() {
      return pending;
    }

    @Override
    public int end() {
      return blockSize;
    }

    @Override
    public int offset() {
      return offset;
    }

    @Override
    public int fill() throws IOException {
      //System.out.println("fii.fill seekPending=" + seekPending + " set lastFP=" + pendingFP + " this=" + this);
      // nocommit -- not great that we do this on each
      // fill -- but we need it to detect seek w/in block
      // case:
      // nocommit: can't we += blockNumBytes instead?
      lastBlockFP = in.getFilePointer();
      blockReader.readBlock();
      return blockSize;
    }
  }

  private class Index extends IntIndexInput.Index {
    private long fp;
    private int upto;

    // This is used when reading skip data:
    @Override
    public void read(final DataInput indexIn, final boolean absolute) throws IOException {
      // nocommit -- somehow we should share the "upto" for
      // doc & freq since they will always be "in sync"
      if (absolute) {
        fp = indexIn.readVLong();
        upto = indexIn.readVInt();
      } else {
        // nocommit -- can't this be more efficient?  read a
        // single byte and check a bit?  block size is 128...
        final long delta = indexIn.readVLong();
        if (delta == 0) {
          // same block
          upto += indexIn.readVInt();
        } else {
          // new block
          fp += delta;
          upto = indexIn.readVInt();
        }
      }
      assert upto < blockSize;
    }

    @Override
    public void seek(final BulkPostingsEnum.BlockReader other) throws IOException {
      ((Reader) other).seek(fp, upto);
    }

    @Override
    public void set(final IntIndexInput.Index other) {
      final Index idx = (Index) other;
      fp = idx.fp;
      upto = idx.upto;
    }

    @Override
    public Object clone() {
      Index other = new Index();
      other.fp = fp;
      other.upto = upto;
      return other;
    }

    @Override
    public String toString() {
      return "FixedBlockIndex(fp=" + fp + " offset=" + upto + ")";
    }
  }
}
