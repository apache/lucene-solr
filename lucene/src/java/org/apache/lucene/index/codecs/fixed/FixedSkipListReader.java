package org.apache.lucene.index.codecs.fixed;

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
import java.util.Arrays;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.index.codecs.MultiLevelSkipListReader;
import org.apache.lucene.index.codecs.sep.IntIndexInput;

/**
 * Implements the skip list reader for the default posting list format
 * that stores positions and payloads.
 *
 * @lucene.experimental
 */

// TODO: rewrite this as recursive classes?
class FixedSkipListReader extends MultiLevelSkipListReader {
  private boolean currentFieldStoresPayloads;
  private IntIndexInput.Index docIndex[];
  private IntIndexInput.Index posIndex[];
  private long payloadPointer[];
  private int payloadLength[];

  private final IntIndexInput.Index lastDocIndex;
  // TODO: -- make private again
  final IntIndexInput.Index lastPosIndex;
  
  private long lastPayloadPointer;
  private int lastPayloadLength;
                           
  FixedSkipListReader(IndexInput skipStream,
                    IntIndexInput docIn,
                    IntIndexInput posIn,
                    int maxSkipLevels,
                    int skipInterval)
    throws IOException {
    super(skipStream, maxSkipLevels, skipInterval);
    docIndex = new IntIndexInput.Index[maxSkipLevels];
    if (posIn != null) {
      posIndex = new IntIndexInput.Index[maxNumberOfSkipLevels];
    }
    for(int i=0;i<maxSkipLevels;i++) {
      docIndex[i] = docIn.index();
      if (posIn != null) {
        posIndex[i] = posIn.index();
      }
    }
    payloadPointer = new long[maxSkipLevels];
    payloadLength = new int[maxSkipLevels];

    lastDocIndex = docIn.index();
    if (posIn != null) {
      lastPosIndex = posIn.index();
    } else {
      lastPosIndex = null;
    }
  }
  
  boolean omitTF;

  void setOmitTF(boolean v) {
    omitTF = v;
  }

  void init(long skipPointer,
            IntIndexInput.Index docBaseIndex,
            IntIndexInput.Index posBaseIndex,
            long payloadBasePointer,
            int df,
            boolean storesPayloads) {

    super.init(skipPointer, df);
    this.currentFieldStoresPayloads = storesPayloads;

    lastPayloadPointer = payloadBasePointer;

    for(int i=0;i<maxNumberOfSkipLevels;i++) {
      docIndex[i].set(docBaseIndex);
      if (posBaseIndex != null) {
        posIndex[i].set(posBaseIndex);
      }
    }
    Arrays.fill(payloadPointer, payloadBasePointer);
    Arrays.fill(payloadLength, 0);
  }

  long getPayloadPointer() {
    return lastPayloadPointer;
  }
  
  /** Returns the payload length of the payload stored just before 
   * the doc to which the last call of {@link MultiLevelSkipListReader#skipTo(int)} 
   * has skipped.  */
  int getPayloadLength() {
    return lastPayloadLength;
  }
  
  @Override
  protected void seekChild(int level) throws IOException {
    super.seekChild(level);
    payloadPointer[level] = lastPayloadPointer;
    payloadLength[level] = lastPayloadLength;
  }
  
  @Override
  protected void setLastSkipData(int level) {
    super.setLastSkipData(level);

    lastPayloadPointer = payloadPointer[level];
    lastPayloadLength = payloadLength[level];
    lastDocIndex.set(docIndex[level]);
    if (lastPosIndex != null) {
      lastPosIndex.set(posIndex[level]);
    }

    if (level > 0) {
      docIndex[level-1].set(docIndex[level]);
      if (posIndex != null) {
        posIndex[level-1].set(posIndex[level]);
      }
    }
  }

  IntIndexInput.Index getPosIndex() {
    return lastPosIndex;
  }

  IntIndexInput.Index getDocIndex() {
    return lastDocIndex;
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
    docIndex[level].read(skipStream, false);
    if (!omitTF) {
      posIndex[level].read(skipStream, false);
      payloadPointer[level] += skipStream.readVInt();
    }
    
    return delta;
  }
}
