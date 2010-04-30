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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.IntsRef;

/** Iterates through the documents, term freq and positions.
 *  NOTE: you must first call {@link #nextDoc} before using
 *  any of the per-doc methods (this does not apply to the
 *  bulk read {@link #read} method).
 *
 *  @lucene.experimental */
public abstract class DocsEnum extends DocIdSetIterator {

  private AttributeSource atts = null;

  /** Returns term frequency in the current document.  Do
   *  not call this before {@link #nextDoc} is first called,
   *  nor after {@link #nextDoc} returns NO_MORE_DOCS. */
  public abstract int freq();
  
  /** Returns the related attributes. */
  public AttributeSource attributes() {
    if (atts == null) atts = new AttributeSource();
    return atts;
  }

  // TODO: maybe add bulk read only docIDs (for eventual
  // match-only scoring)

  public static class BulkReadResult {
    public final IntsRef docs = new IntsRef();
    public final IntsRef freqs = new IntsRef();
  }

  protected BulkReadResult bulkResult;

  protected final void initBulkResult() {
    if (bulkResult == null) {
      bulkResult = new BulkReadResult();
      bulkResult.docs.ints = new int[64];
      bulkResult.freqs.ints = new int[64];
    }
  }

  /** Call this once, up front, and hold a reference to the
   *  returned bulk result.  When you call {@link #read}, it
   *  fills the docs and freqs of this pre-shared bulk
   *  result. */
  public BulkReadResult getBulkResult() {
    initBulkResult();
    return bulkResult;
  }
  
  /** Bulk read (docs and freqs).  After this is called,
   *  {@link #docID()} and {@link #freq} are undefined.
   *  This returns the count read, or 0 if the end is
   *  reached.  The resulting docs and freqs are placed into
   *  the pre-shard {@link BulkReadResult} instance returned
   *  by {@link #getBulkResult}.  Note that the {@link
   *  IntsRef} for docs and freqs will not have their length
   *  set.
   * 
   *  <p>NOTE: the default impl simply delegates to {@link
   *  #nextDoc}, but subclasses may do this more
   *  efficiently. */
  public int read() throws IOException {
    int count = 0;
    final int[] docs = bulkResult.docs.ints;
    final int[] freqs = bulkResult.freqs.ints;
    while(count < docs.length) {
      final int doc = nextDoc();
      if (doc != NO_MORE_DOCS) {
        docs[count] = doc;
        freqs[count] = freq();
        count++;
      } else {
        break;
      }
    }
    return count;
  }
}
