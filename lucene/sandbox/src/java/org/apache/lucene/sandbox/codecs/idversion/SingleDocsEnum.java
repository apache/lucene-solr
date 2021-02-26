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
package org.apache.lucene.sandbox.codecs.idversion;

import java.io.IOException;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.BytesRef;

class SingleDocsEnum extends PostingsEnum {

  private int doc;
  private int singleDocID;

  /** For reuse */
  public void reset(int singleDocID) {
    doc = -1;
    this.singleDocID = singleDocID;
  }

  @Override
  public int nextDoc() {
    if (doc == -1) {
      doc = singleDocID;
    } else {
      doc = NO_MORE_DOCS;
    }

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
  public int nextPosition() throws IOException {
    return -1;
  }

  @Override
  public int startOffset() throws IOException {
    return -1;
  }

  @Override
  public int endOffset() throws IOException {
    return -1;
  }

  @Override
  public BytesRef getPayload() throws IOException {
    throw new UnsupportedOperationException();
  }
}
