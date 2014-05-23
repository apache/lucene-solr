package org.apache.lucene.codecs.idversion;

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

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.util.Bits;

class SingleDocsEnum extends DocsEnum {

  private int doc;
  private int singleDocID;
  private Bits liveDocs;

  /** For reuse */
  public void reset(int singleDocID, Bits liveDocs) {
    doc = -1;
    this.liveDocs = liveDocs;
    this.singleDocID = singleDocID;
  }

  @Override
  public int nextDoc() {
    if (doc == -1 && (liveDocs == null || liveDocs.get(singleDocID))) {
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
    if (doc == -1 && target <= singleDocID && (liveDocs == null || liveDocs.get(singleDocID))) {
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
}
