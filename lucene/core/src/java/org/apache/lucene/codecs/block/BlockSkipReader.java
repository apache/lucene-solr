package org.apache.lucene.codecs.block;

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

import org.apache.lucene.codecs.MultiLevelSkipListReader;
import org.apache.lucene.store.IndexInput;

/**
 * Implements the skip list reader for the 4.0 posting list format
 * that stores positions and payloads.
 * 
 * @see Lucene40PostingsFormat
 * @lucene.experimental
 */
final class BlockSkipReader extends MultiLevelSkipListReader {
  private boolean DEBUG = BlockPostingsReader.DEBUG;

  private long docPointer[];
  private long posPointer[];
  private long payPointer[];
  private int posBufferUpto[];
  private int endOffset[];
  private int payloadByteUpto[];

  private long lastPosPointer;
  private long lastPayPointer;
  private int lastEndOffset;
  private int lastPayloadByteUpto;
  private long lastDocPointer;
  private int lastPosBufferUpto;

  public BlockSkipReader(IndexInput skipStream, int maxSkipLevels, int skipInterval, boolean hasPos, boolean hasOffsets, boolean hasPayloads) {
    super(skipStream, maxSkipLevels, skipInterval);
    docPointer = new long[maxSkipLevels];
    if (hasPos) {
      posPointer = new long[maxSkipLevels];
      posBufferUpto = new int[maxSkipLevels];
      if (hasPayloads) {
        payloadByteUpto = new int[maxSkipLevels];
      } else {
        payloadByteUpto = null;
      }
      if (hasOffsets) {
        endOffset = new int[maxSkipLevels];
      } else {
        endOffset = null;
      }
      if (hasOffsets || hasPayloads) {
        payPointer = new long[maxSkipLevels];
      } else {
        payPointer = null;
      }
    } else {
      posPointer = null;
    }
  }

  public void init(long skipPointer, long docBasePointer, long posBasePointer, long payBasePointer, int df) {
    super.init(skipPointer, df);
    lastDocPointer = docBasePointer;
    lastPosPointer = posBasePointer;
    lastPayPointer = payBasePointer;

    Arrays.fill(docPointer, docBasePointer);
    if (posPointer != null) {
      Arrays.fill(posPointer, posBasePointer);
      if (payPointer != null) {
        Arrays.fill(payPointer, payBasePointer);
      }
    } else {
      assert posBasePointer == 0;
    }
  }

  /** Returns the doc pointer of the doc to which the last call of 
   * {@link MultiLevelSkipListReader#skipTo(int)} has skipped.  */
  public long getDocPointer() {
    return lastDocPointer;
  }

  public long getPosPointer() {
    return lastPosPointer;
  }

  public int getPosBufferUpto() {
    return lastPosBufferUpto;
  }

  public long getPayPointer() {
    return lastPayPointer;
  }

  public int getEndOffset() {
    return lastEndOffset;
  }

  public int getPayloadByteUpto() {
    return lastPayloadByteUpto;
  }

  @Override
  protected void seekChild(int level) throws IOException {
    super.seekChild(level);
    if (DEBUG) {
      System.out.println("seekChild level=" + level);
    }
    docPointer[level] = lastDocPointer;
    if (posPointer != null) {
      posPointer[level] = lastPosPointer;
      posBufferUpto[level] = lastPosBufferUpto;
      if (endOffset != null) {
        endOffset[level] = lastEndOffset;
      }
      if (payloadByteUpto != null) {
        payloadByteUpto[level] = lastPayloadByteUpto;
      }
      if (payPointer != null) {
        payPointer[level] = lastPayPointer;
      }
    }
  }
  
  @Override
  protected void setLastSkipData(int level) {
    super.setLastSkipData(level);
    lastDocPointer = docPointer[level];
    if (DEBUG) {
      System.out.println("setLastSkipData level=" + level);
      System.out.println("  lastDocPointer=" + lastDocPointer);
    }
    if (posPointer != null) {
      lastPosPointer = posPointer[level];
      lastPosBufferUpto = posBufferUpto[level];
      if (DEBUG) {
        System.out.println("  lastPosPointer=" + lastPosPointer + " lastPosBUfferUpto=" + lastPosBufferUpto);
      }
      if (payPointer != null) {
        lastPayPointer = payPointer[level];
      }
      if (endOffset != null) {
        lastEndOffset = endOffset[level];
      }
      if (payloadByteUpto != null) {
        lastPayloadByteUpto = payloadByteUpto[level];
      }
    }
  }

  @Override
  protected int readSkipData(int level, IndexInput skipStream) throws IOException {
    if (DEBUG) {
      System.out.println("readSkipData level=" + level);
    }
    int delta = skipStream.readVInt();
    if (DEBUG) {
      System.out.println("  delta=" + delta);
    }
    docPointer[level] += skipStream.readVInt();
    if (DEBUG) {
      System.out.println("  docFP=" + docPointer[level]);
    }

    if (posPointer != null) {
      posPointer[level] += skipStream.readVInt();
      if (DEBUG) {
        System.out.println("  posFP=" + posPointer[level]);
      }
      posBufferUpto[level] = skipStream.readVInt();
      if (DEBUG) {
        System.out.println("  posBufferUpto=" + posBufferUpto[level]);
      }

      if (payloadByteUpto != null) {
        payloadByteUpto[level] = skipStream.readVInt();
      }

      if (endOffset != null) {
        endOffset[level] += skipStream.readVInt();
      }

      if (payPointer != null) {
        payPointer[level] += skipStream.readVInt();
      }
    }
    return delta;
  }
}
