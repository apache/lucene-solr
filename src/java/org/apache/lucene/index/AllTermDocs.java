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

package org.apache.lucene.index;

import org.apache.lucene.util.BitVector;
import java.io.IOException;

class AllTermDocs implements TermDocs {
  protected BitVector deletedDocs;
  protected int maxDoc;
  protected int doc = -1;

  protected AllTermDocs(SegmentReader parent) {
    synchronized (parent) {
      this.deletedDocs = parent.deletedDocs;
    }
    this.maxDoc = parent.maxDoc();
  }

  public void seek(Term term) throws IOException {
    if (term==null) {
      doc = -1;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public void seek(TermEnum termEnum) throws IOException {
    throw new UnsupportedOperationException();
  }

  public int doc() {
    return doc;
  }

  public int freq() {
    return 1;
  }

  public boolean next() throws IOException {
    return skipTo(doc+1);
  }

  public int read(int[] docs, int[] freqs) throws IOException {
    final int length = docs.length;
    int i = 0;
    while (i < length && doc < maxDoc) {
      if (deletedDocs == null || !deletedDocs.get(doc)) {
        docs[i] = doc;
        freqs[i] = 1;
        ++i;
      }
      doc++;
    }
    return i;
  }

  public boolean skipTo(int target) throws IOException {
    doc = target;
    while (doc < maxDoc) {
      if (deletedDocs == null || !deletedDocs.get(doc)) {
        return true;
      }
      doc++;
    }
    return false;
  }

  public void close() throws IOException {
  }
}
