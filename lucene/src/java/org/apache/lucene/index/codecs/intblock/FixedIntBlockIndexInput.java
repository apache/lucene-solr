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
  /** @lucene.internal */
  public final int blockSize;
  
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
  
  /** Return a reader piggybacking on a previous reader.
   * They share the same underlying indexinput (e.g. interleaved docs/freqs)
   */
  public Reader reader(Reader parent) throws IOException {
    final int[] buffer = new int[blockSize];
    return new Reader(parent.in, buffer, this.getBlockReader(parent.in, buffer));
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
    /** Tead a block of integers */
    public void readBlock() throws IOException;
    /** 
     * Skip over a block of integers. 
     * Decoding of the integers is not actually needed.
     * A trivial implementation can always be to call readBlock(),
     * but its preferred to avoid decoding and minimize i/o.
     */
    public void skipBlock() throws IOException;
    // nocommit -- need seek here so mmapdir "knows"
  }

  /** @lucene.internal */
  public static class Reader extends BulkPostingsEnum.BlockReader {
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
      //System.out.println("parent fill: " + in.getFilePointer() + " last=" + lastBlockFP + " fp=" + fp);
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
      //System.out.println("fill complete: " + in.getFilePointer());
    }
    
    /**
     * Position both this reader and its child reader to an index.
     * If the child's data is not actually needed (e.g. reading only docs but skipping over freqs),
     * then the parameter <code>fill</code> is true.
     */
    public void seek(IntIndexInput.Index idx, Reader child, boolean fill) throws IOException {
      //nocommit: could this be more ugly?
      Index index = (Index) idx;
      final long fp = index.fp;

      // synchronize both the parent and child to the index offset, as they are in parallel.
      child.offset = offset = index.upto;
      
      // nocommit: if the child previously skipBlock'ed, we fill both.. can we do better? 
      if ((index.fp != lastBlockFP) || (fill && child.lastBlockFP == -1)) {
        in.seek(fp);
        fill();
        if (fill) {
          child.fill();
        } else {
          child.skipBlock(); // the child blocks are not actually needed.
        }
      } else {
        // seek within block
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
    
    public void skipBlock() throws IOException {
      lastBlockFP = -1; /* nocommit: clear lastblockFP */
      blockReader.skipBlock();
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
