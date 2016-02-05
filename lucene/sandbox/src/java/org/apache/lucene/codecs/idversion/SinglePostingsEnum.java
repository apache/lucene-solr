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
package org.apache.lucene.codecs.idversion;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.BytesRef;

class SinglePostingsEnum extends PostingsEnum {
  private int doc;
  private int pos;
  private int singleDocID;
  private long version;
  private final BytesRef payload;

  public SinglePostingsEnum() {
    payload = new BytesRef(8);
    payload.length = 8;
  }

  /** For reuse */
  public void reset(int singleDocID, long version) {
    doc = -1;
    this.singleDocID = singleDocID;
    this.version = version;
  }

  @Override
  public int nextDoc() {
    if (doc == -1) {
      doc = singleDocID;
    } else {
      doc = NO_MORE_DOCS;
    }
    pos = -1;
    
    return doc;
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int advance(int target) {
    if (doc == -1 && target <= singleDocID) {
      doc = singleDocID;
      pos = -1;
    } else {
      doc = NO_MORE_DOCS;
    }
    return doc;
  }

  @Override
  public long cost() {
    return 1;
  }

  @Override
  public int freq() {
    return 1;
  }

  @Override
  public int nextPosition() {
    assert pos == -1;
    pos = 0;
    IDVersionPostingsFormat.longToBytes(version, payload);
    return pos;
  }

  @Override
  public BytesRef getPayload() {
    return payload;
  }

  @Override
  public int startOffset() {
    return -1;
  }

  @Override
  public int endOffset() {
    return -1;
  }
}
