package org.apache.lucene.codecs.lucene3x;

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
 * @deprecated (4.0) This is only used to read indexes created
 * before 4.0.
 */
@Deprecated
final class Lucene3xSkipListReader extends MultiLevelSkipListReader {
  private boolean currentFieldStoresPayloads;
  private long freqPointer[];
  private long proxPointer[];
  private int payloadLength[];
  
  private long lastFreqPointer;
  private long lastProxPointer;
  private int lastPayloadLength;
                           
  public Lucene3xSkipListReader(IndexInput skipStream, int maxSkipLevels, int skipInterval) {
    super(skipStream, maxSkipLevels, skipInterval);
    freqPointer = new long[maxSkipLevels];
    proxPointer = new long[maxSkipLevels];
    payloadLength = new int[maxSkipLevels];
  }

  public void init(long skipPointer, long freqBasePointer, long proxBasePointer, int df, boolean storesPayloads) {
    super.init(skipPointer, df);
    this.currentFieldStoresPayloads = storesPayloads;
    lastFreqPointer = freqBasePointer;
    lastProxPointer = proxBasePointer;

    Arrays.fill(freqPointer, freqBasePointer);
    Arrays.fill(proxPointer, proxBasePointer);
    Arrays.fill(payloadLength, 0);
  }

  /** Returns the freq pointer of the doc to which the last call of 
   * {@link MultiLevelSkipListReader#skipTo(int)} has skipped.  */
  public long getFreqPointer() {
    return lastFreqPointer;
  }

  /** Returns the prox pointer of the doc to which the last call of 
   * {@link MultiLevelSkipListReader#skipTo(int)} has skipped.  */
  public long getProxPointer() {
    return lastProxPointer;
  }
  
  /** Returns the payload length of the payload stored just before 
   * the doc to which the last call of {@link MultiLevelSkipListReader#skipTo(int)} 
   * has skipped.  */
  public int getPayloadLength() {
    return lastPayloadLength;
  }
  
  @Override
  protected void seekChild(int level) throws IOException {
    super.seekChild(level);
    freqPointer[level] = lastFreqPointer;
    proxPointer[level] = lastProxPointer;
    payloadLength[level] = lastPayloadLength;
  }
  
  @Override
  protected void setLastSkipData(int level) {
    super.setLastSkipData(level);
    lastFreqPointer = freqPointer[level];
    lastProxPointer = proxPointer[level];
    lastPayloadLength = payloadLength[level];
  }

  @Override
  protected int readSkipData(int level, IndexInput skipStream) throws IOException {
    int delta;
    if (currentFieldStoresPayloads) {
      // the current field stores payloads.
      // if the doc delta is odd then we have
      // to read the current payload length
      // because it differs from the length of the
      // previous payload
      delta = skipStream.readVInt();
      if ((delta & 1) != 0) {
        payloadLength[level] = skipStream.readVInt();
      }
      delta >>>= 1;
    } else {
      delta = skipStream.readVInt();
    }

    freqPointer[level] += skipStream.readVInt();
    proxPointer[level] += skipStream.readVInt();
    
    return delta;
  }
}
