package org.apache.lucene.codecs.blockpacked;

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

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.codecs.MultiLevelSkipListWriter;

// nocommit do we need more frequent skips at level > 0?
// 128*128 is immense?  may need to decouple
// baseSkipInterval & theRestSkipInterval?

final class BlockPackedSkipWriter extends MultiLevelSkipListWriter {
  private boolean DEBUG = BlockPackedPostingsReader.DEBUG;
  
  private int[] lastSkipDoc;
  private long[] lastSkipDocPointer;
  private long[] lastSkipPosPointer;
  private long[] lastSkipPayPointer;
  private int[] lastEndOffset;
  private int[] lastPayloadByteUpto;

  private final IndexOutput docOut;
  private final IndexOutput posOut;
  private final IndexOutput payOut;

  private int curDoc;
  private long curDocPointer;
  private long curPosPointer;
  private long curPayPointer;
  private int curPosBufferUpto;
  private int curEndOffset;
  private int curPayloadByteUpto;
  private boolean fieldHasPositions;
  private boolean fieldHasOffsets;
  private boolean fieldHasPayloads;

  public BlockPackedSkipWriter(int skipInterval, int maxSkipLevels, int docCount, IndexOutput docOut, IndexOutput posOut, IndexOutput payOut) {
    super(skipInterval, maxSkipLevels, docCount);
    this.docOut = docOut;
    this.posOut = posOut;
    this.payOut = payOut;
    
    lastSkipDoc = new int[maxSkipLevels];
    lastSkipDocPointer = new long[maxSkipLevels];
    if (posOut != null) {
      lastSkipPosPointer = new long[maxSkipLevels];
      if (payOut != null) {
        lastSkipPayPointer = new long[maxSkipLevels];
      }
      lastEndOffset = new int[maxSkipLevels];
      lastPayloadByteUpto = new int[maxSkipLevels];
    }
  }

  public void setField(boolean fieldHasPositions, boolean fieldHasOffsets, boolean fieldHasPayloads) {
    this.fieldHasPositions = fieldHasPositions;
    this.fieldHasOffsets = fieldHasOffsets;
    this.fieldHasPayloads = fieldHasPayloads;
  }

  @Override
  public void resetSkip() {
    super.resetSkip();
    Arrays.fill(lastSkipDoc, 0);
    Arrays.fill(lastSkipDocPointer, docOut.getFilePointer());
    if (fieldHasPositions) {
      Arrays.fill(lastSkipPosPointer, posOut.getFilePointer());
      if (fieldHasOffsets) {
        Arrays.fill(lastEndOffset, 0);
      }
      if (fieldHasPayloads) {
        Arrays.fill(lastPayloadByteUpto, 0);
      }
      if (fieldHasOffsets || fieldHasPayloads) {
        Arrays.fill(lastSkipPayPointer, payOut.getFilePointer());
      }
    }
  }

  /**
   * Sets the values for the current skip data. 
   */
  public void bufferSkip(int doc, int numDocs, long posFP, long payFP, int posBufferUpto, int endOffset, int payloadByteUpto) throws IOException {
    this.curDoc = doc;
    this.curDocPointer = docOut.getFilePointer();
    this.curPosPointer = posFP;
    this.curPayPointer = payFP;
    this.curPosBufferUpto = posBufferUpto;
    this.curPayloadByteUpto = payloadByteUpto;
    this.curEndOffset = endOffset;
    bufferSkip(numDocs);
  }
  
  @Override
  protected void writeSkipData(int level, IndexOutput skipBuffer) throws IOException {
    int delta = curDoc - lastSkipDoc[level];
    if (DEBUG) {
      System.out.println("writeSkipData level=" + level + " lastDoc=" + curDoc + " delta=" + delta + " curDocPointer=" + curDocPointer);
    }
    skipBuffer.writeVInt(delta);
    lastSkipDoc[level] = curDoc;

    skipBuffer.writeVInt((int) (curDocPointer - lastSkipDocPointer[level]));
    lastSkipDocPointer[level] = curDocPointer;

    if (fieldHasPositions) {
      if (DEBUG) {
        System.out.println("  curPosPointer=" + curPosPointer + " curPosBufferUpto=" + curPosBufferUpto);
      }
      skipBuffer.writeVInt((int) (curPosPointer - lastSkipPosPointer[level]));
      lastSkipPosPointer[level] = curPosPointer;
      skipBuffer.writeVInt(curPosBufferUpto);

      if (fieldHasPayloads) {
        skipBuffer.writeVInt(curPayloadByteUpto);
      }

      if (fieldHasOffsets) {
        skipBuffer.writeVInt(curEndOffset - lastEndOffset[level]);
        lastEndOffset[level] = curEndOffset;
      }

      if (fieldHasOffsets || fieldHasPayloads) {
        skipBuffer.writeVInt((int) (curPayPointer - lastSkipPayPointer[level]));
        lastSkipPayPointer[level] = curPayPointer;
      }
    }
  }
}
