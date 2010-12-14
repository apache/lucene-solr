package org.apache.lucene.index;

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

import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;

public final class MultiBulkPostingsEnum extends BulkPostingsEnum {
  private EnumWithSlice[] subs;
  int numSubs;

  private final DocDeltasReader docDeltasReader = new DocDeltasReader();
  private final FreqsReader freqsReader = new FreqsReader();
  private final PositionsReader positionsReader = new PositionsReader();

  MultiBulkPostingsEnum reset(final EnumWithSlice[] subs, final int numSubs, boolean doFreqs, boolean doPositions) throws IOException {
    this.numSubs = numSubs;
    this.subs = new EnumWithSlice[subs.length];
    for(int i=0;i<subs.length;i++) {
      this.subs[i] = new EnumWithSlice();
      this.subs[i].postingsEnum = subs[i].postingsEnum;
      this.subs[i].slice = subs[i].slice;
      this.subs[i].docFreq = subs[i].docFreq;
    }
    //System.out.println("MULTI bulk enum init numSub=" + numSubs);
    docDeltasReader.init();
    if (doFreqs) {
      freqsReader.init();
    }
    if (doPositions) {
      positionsReader.init();
    }
    return this;
  }

  private abstract class MultiBlockReader extends BlockReader {
    // nocommit -- size must be max of all subs
    int[] buffer;
    int upto;
    int currentLeft;
    int currentEnd;
    BlockReader current;
    
    @Override
    public int[] getBuffer() {
      return buffer;
    }

    @Override
    public int offset() {
      return 0;
    }

    @Override
    public void setOffset(int offset) {
      assert offset == 0;
    }

    @Override
    public int end() {
      return currentEnd;
    }

    protected abstract int getBufferSize() throws IOException;

    public void init() throws IOException {
      final int bufferSize = getBufferSize();
      if (buffer == null || buffer.length < bufferSize) {
        buffer = new int[ArrayUtil.oversize(bufferSize, RamUsageEstimator.NUM_BYTES_INT)];
      }
      upto = 0;
      //System.out.println("mbr.init this=" + this);
      // nocommit -- must get max buffer size and maybe grow our buffer
      current = getBlockReader(upto);
      currentLeft = subs[upto].docFreq;
      int limit = current.end();
      int offset = current.offset();
      if (offset >= limit) {
        //System.out.println("prefill limit=" + limit + " offs=" + offset + " current=" + current);
        limit = current.fill();
        //System.out.println("  new limit=" + limit);
      } else {
        //System.out.println("  no prefill offset=" + offset + " limit=" + limit);
      }
      if (limit > offset) {
        doCopy(offset, limit);
      }
    }

    private int doCopy(int offset, int limit) {
      int chunk = limit - offset;
      assert chunk > 0;
      if (chunk > currentLeft) {
        chunk = currentLeft;
      }
      //System.out.println("  doCopy chunk=" + chunk + " offset=" + offset + " limit=" + limit + " this=" + this);
      System.arraycopy(current.getBuffer(),
                       offset,
                       buffer,
                       0,
                       chunk);
      currentLeft -= chunk;
      //System.out.println("  currentLeft=" + currentLeft);
      if (currentLeft == 0) {
        current = null;
        //System.out.println("    set current null");
      }
      currentEnd = chunk;
      onFill();
      return currentEnd;
    }

    @Override
    public int fill() throws IOException {
      //System.out.println("fill this=" + this);
      while(true) {
        if (current == null) {
          if (upto == numSubs-1) {
            currentEnd = 0;
            return 0;
          } else {
            upto++;
            current = getBlockReader(upto);
            currentLeft = subs[upto].docFreq;
            //System.out.println("  fill current=" + current + " upto=" + upto + " this=" + this);
            int limit = current.end();
            int offset = current.offset();
            if (offset >= limit) {
              //System.out.println("prefill2");
              limit = current.fill();
            }
            if (limit > offset) {
              return doCopy(offset, limit);
            }
          }
        }
        int limit = current.fill();
        //int offset = current.offset();
        return doCopy(0, limit);
      }
    }

    protected abstract BlockReader getBlockReader(int upto) throws IOException;
    protected void onFill() {};
  }

  private class DocDeltasReader extends MultiBlockReader {
    int lastDocID;
    int lastSeg;

    @Override
    protected int getBufferSize() throws IOException {
      int maxBufferSize = 0;
      for(int sub=0;sub<numSubs;sub++) {
        maxBufferSize = Math.max(maxBufferSize, subs[sub].postingsEnum.getDocDeltasReader().getBuffer().length);
      }
      return maxBufferSize;
    }

    @Override
    public void init() throws IOException {
      lastDocID = 0;
      lastSeg = -1;
      //System.out.println("docDeltasInit");
      super.init();
      //System.out.println("docDeltasInit done");
    }

    @Override
    protected BlockReader getBlockReader(int upto) throws IOException {
      return subs[upto].postingsEnum.getDocDeltasReader();
    }

    @Override
    protected void onFill() {
      if (upto != lastSeg) {
        assert lastDocID < subs[upto].slice.start || lastDocID == 0;
        buffer[0] += subs[upto].slice.start - lastDocID;
        //System.out.println("  add delta to [0] " + (subs[upto].slice.start - lastDocID) + " nextStart=" + subs[upto].slice.start + " vs lastDocID=" + lastDocID + " now buffer[0]=" + buffer[0]);
        lastSeg = upto;
      }
      for(int deltaUpto=0;deltaUpto<currentEnd;deltaUpto++) {
        lastDocID += buffer[deltaUpto];
      }
      //System.out.println("  now lastDocID=" + lastDocID);
    }
  }

  private class FreqsReader extends MultiBlockReader {
    @Override
    protected int getBufferSize() throws IOException {
      int maxBufferSize = 0;
      for(int sub=0;sub<numSubs;sub++) {
        maxBufferSize = Math.max(maxBufferSize, subs[sub].postingsEnum.getFreqsReader().getBuffer().length);
      }
      return maxBufferSize;
    }

    @Override
    protected BlockReader getBlockReader(int upto) throws IOException {
      return subs[upto].postingsEnum.getFreqsReader();
    }
  }

  private class PositionsReader extends MultiBlockReader {
    @Override
    protected int getBufferSize() throws IOException {
      int maxBufferSize = 0;
      for(int sub=0;sub<numSubs;sub++) {
        maxBufferSize = Math.max(maxBufferSize, subs[sub].postingsEnum.getPositionDeltasReader().getBuffer().length);
      }
      return maxBufferSize;
    }
    @Override
    protected BlockReader getBlockReader(int upto) throws IOException {
      return subs[upto].postingsEnum.getPositionDeltasReader();
    }
  }

  @Override
  public BlockReader getDocDeltasReader() {
    return docDeltasReader;
  }

  @Override
  public BlockReader getFreqsReader() {
    return freqsReader;
  }

  @Override
  public BlockReader getPositionDeltasReader() {
    return positionsReader;
  }

  public int getNumSubs() {
    return numSubs;
  }

  public EnumWithSlice[] getSubs() {
    return subs;
  }

  @Override
  public JumpResult jump(int target, int curCount) throws IOException {
    // nocommit
    return null;
  }

  // TODO: implement bulk read more efficiently than super
  public final static class EnumWithSlice {
    public BulkPostingsEnum postingsEnum;
    public ReaderUtil.Slice slice;
    public int docFreq;
  }
}

