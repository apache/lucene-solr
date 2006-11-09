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

import java.io.IOException;

final class SegmentMergeInfo {
  Term term;
  int base;
  TermEnum termEnum;
  IndexReader reader;
  private TermPositions postings;  // use getPositions()
  private int[] docMap;  // use getDocMap()

  SegmentMergeInfo(int b, TermEnum te, IndexReader r)
    throws IOException {
    base = b;
    reader = r;
    termEnum = te;
    term = te.term();
  }

  // maps around deleted docs
  int[] getDocMap() {
    if (docMap == null) {
    // build array which maps document numbers around deletions 
    if (reader.hasDeletions()) {
      int maxDoc = reader.maxDoc();
      docMap = new int[maxDoc];
      int j = 0;
      for (int i = 0; i < maxDoc; i++) {
        if (reader.isDeleted(i))
          docMap[i] = -1;
        else
          docMap[i] = j++;
      }
    }
  }
    return docMap;
  }

  TermPositions getPositions() throws IOException {
    if (postings == null) {
      postings = reader.termPositions();
    }
    return postings;
  }

  final boolean next() throws IOException {
    if (termEnum.next()) {
      term = termEnum.term();
      return true;
    } else {
      term = null;
      return false;
    }
  }

  final void close() throws IOException {
    termEnum.close();
    if (postings != null) {
    postings.close();
  }
}
}

