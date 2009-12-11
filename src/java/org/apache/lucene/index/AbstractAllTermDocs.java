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

import java.io.IOException;

/** Base class for enumerating all but deleted docs.
 * 
 * <p>NOTE: this class is meant only to be used internally
 * by Lucene; it's only public so it can be shared across
 * packages.  This means the API is freely subject to
 * change, and, the class could be removed entirely, in any
 * Lucene release.  Use directly at your own risk! */
public abstract class AbstractAllTermDocs implements TermDocs {

  protected int maxDoc;
  protected int doc = -1;

  protected AbstractAllTermDocs(int maxDoc) {
    this.maxDoc = maxDoc;
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
      if (!isDeleted(doc)) {
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
      if (!isDeleted(doc)) {
        return true;
      }
      doc++;
    }
    return false;
  }

  public void close() throws IOException {
  }

  public abstract boolean isDeleted(int doc);
}