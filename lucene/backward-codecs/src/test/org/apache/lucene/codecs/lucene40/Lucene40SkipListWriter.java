package org.apache.lucene.codecs.lucene40;

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


/**
 * Implements the skip list writer for the 4.0 posting list format
 * that stores positions and payloads.
 * 
 * @see Lucene40PostingsFormat
 * @deprecated Only for reading old 4.0 segments
 */
@Deprecated
public class Lucene40SkipListWriter extends MultiLevelSkipListWriter {
  private int[] lastSkipDoc;
  private int[] lastSkipPayloadLength;
  private int[] lastSkipOffsetLength;
  private long[] lastSkipFreqPointer;
  private long[] lastSkipProxPointer;
  
  private IndexOutput freqOutput;
  private IndexOutput proxOutput;

  private int curDoc;
  private boolean curStorePayloads;
  private boolean curStoreOffsets;
  private int curPayloadLength;
  private int curOffsetLength;
  private long curFreqPointer;
  private long curProxPointer;

  /** Sole constructor. */
  public Lucene40SkipListWriter(int skipInterval, int numberOfSkipLevels, int docCount, IndexOutput freqOutput, IndexOutput proxOutput) {
    super(skipInterval, numberOfSkipLevels, docCount);
    this.freqOutput = freqOutput;
    this.proxOutput = proxOutput;
    
    lastSkipDoc = new int[numberOfSkipLevels];
    lastSkipPayloadLength = new int[numberOfSkipLevels];
    lastSkipOffsetLength = new int[numberOfSkipLevels];
    lastSkipFreqPointer = new long[numberOfSkipLevels];
    lastSkipProxPointer = new long[numberOfSkipLevels];
  }

  /**
   * Sets the values for the current skip data. 
   */
  public void setSkipData(int doc, boolean storePayloads, int payloadLength, boolean storeOffsets, int offsetLength) {
    assert storePayloads || payloadLength == -1;
    assert storeOffsets  || offsetLength == -1;
    this.curDoc = doc;
    this.curStorePayloads = storePayloads;
    this.curPayloadLength = payloadLength;
    this.curStoreOffsets = storeOffsets;
    this.curOffsetLength = offsetLength;
    this.curFreqPointer = freqOutput.getFilePointer();
    if (proxOutput != null)
      this.curProxPointer = proxOutput.getFilePointer();
  }

  @Override
  public void resetSkip() {
    super.resetSkip();
    Arrays.fill(lastSkipDoc, 0);
    Arrays.fill(lastSkipPayloadLength, -1);  // we don't have to write the first length in the skip list
    Arrays.fill(lastSkipOffsetLength, -1);  // we don't have to write the first length in the skip list
    Arrays.fill(lastSkipFreqPointer, freqOutput.getFilePointer());
    if (proxOutput != null)
      Arrays.fill(lastSkipProxPointer, proxOutput.getFilePointer());
  }
  
  @Override
  protected void writeSkipData(int level, IndexOutput skipBuffer) throws IOException {
    // To efficiently store payloads/offsets in the posting lists we do not store the length of
    // every payload/offset. Instead we omit the length if the previous lengths were the same
    //
    // However, in order to support skipping, the length at every skip point must be known.
    // So we use the same length encoding that we use for the posting lists for the skip data as well:
    // Case 1: current field does not store payloads/offsets
    //           SkipDatum                 --> DocSkip, FreqSkip, ProxSkip
    //           DocSkip,FreqSkip,ProxSkip --> VInt
    //           DocSkip records the document number before every SkipInterval th  document in TermFreqs. 
    //           Document numbers are represented as differences from the previous value in the sequence.
    // Case 2: current field stores payloads/offsets
    //           SkipDatum                 --> DocSkip, PayloadLength?,OffsetLength?,FreqSkip,ProxSkip
    //           DocSkip,FreqSkip,ProxSkip --> VInt
    //           PayloadLength,OffsetLength--> VInt    
    //         In this case DocSkip/2 is the difference between
    //         the current and the previous value. If DocSkip
    //         is odd, then a PayloadLength encoded as VInt follows,
    //         if DocSkip is even, then it is assumed that the
    //         current payload/offset lengths equals the lengths at the previous
    //         skip point
    int delta = curDoc - lastSkipDoc[level];
    
    if (curStorePayloads || curStoreOffsets) {
      assert curStorePayloads || curPayloadLength == lastSkipPayloadLength[level];
      assert curStoreOffsets  || curOffsetLength == lastSkipOffsetLength[level];

      if (curPayloadLength == lastSkipPayloadLength[level] && curOffsetLength == lastSkipOffsetLength[level]) {
        // the current payload/offset lengths equals the lengths at the previous skip point,
        // so we don't store the lengths again
        skipBuffer.writeVInt(delta << 1);
      } else {
        // the payload and/or offset length is different from the previous one. We shift the DocSkip, 
        // set the lowest bit and store the current payload and/or offset lengths as VInts.
        skipBuffer.writeVInt(delta << 1 | 1);

        if (curStorePayloads) {
          skipBuffer.writeVInt(curPayloadLength);
          lastSkipPayloadLength[level] = curPayloadLength;
        }
        if (curStoreOffsets) {
          skipBuffer.writeVInt(curOffsetLength);
          lastSkipOffsetLength[level] = curOffsetLength;
        }
      }
    } else {
      // current field does not store payloads or offsets
      skipBuffer.writeVInt(delta);
    }

    skipBuffer.writeVInt((int) (curFreqPointer - lastSkipFreqPointer[level]));
    skipBuffer.writeVInt((int) (curProxPointer - lastSkipProxPointer[level]));

    lastSkipDoc[level] = curDoc;
    
    lastSkipFreqPointer[level] = curFreqPointer;
    lastSkipProxPointer[level] = curProxPointer;
  }

}
