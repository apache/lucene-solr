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

import org.apache.lucene.util.Bits;
import java.io.IOException;

class AllDocsEnum extends DocsEnum {
  protected final Bits skipDocs;
  protected final int maxDoc;
  protected final IndexReader reader;
  protected int doc = -1;

  protected AllDocsEnum(IndexReader reader, Bits skipDocs) {
    this.skipDocs = skipDocs;
    this.maxDoc = reader.maxDoc();
    this.reader = reader;
  }

  @Override
  public int freq() {
    return 1;
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(doc+1);
  }

  @Override
  public int read() throws IOException {
    final int[] docs = bulkResult.docs.ints;
    final int[] freqs = bulkResult.freqs.ints;
    int i = 0;
    while (i < docs.length && doc < maxDoc) {
      if (skipDocs == null || !skipDocs.get(doc)) {
        docs[i] = doc;
        freqs[i] = 1;
        ++i;
      }
      doc++;
    }
    return i;
  }

  @Override
  public int advance(int target) throws IOException {
    doc = target;
    while (doc < maxDoc) {
      if (skipDocs == null || !skipDocs.get(doc)) {
        return doc;
      }
      doc++;
    }
    doc = NO_MORE_DOCS;
    return doc;
  }
}
