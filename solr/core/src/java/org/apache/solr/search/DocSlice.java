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
package org.apache.solr.search;

import java.util.Collection;
import java.util.Collections;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * <code>DocSlice</code> implements DocList as an array of docids and optional scores.
 *
 *
 * @since solr 0.9
 */
public class DocSlice extends DocSetBase implements DocList {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DocSlice.class) + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

  final int offset;    // starting position of the docs (zero based)
  final int len;       // number of positions used in arrays
  final int[] docs;    // a slice of documents (docs 0-100 of the query)

  final float[] scores;  // optional score list
  final long matches;
  final float maxScore;
  final long ramBytesUsed; // cached value

  /**
   * Primary constructor for a DocSlice instance.
   *
   * @param offset  starting offset for this range of docs
   * @param len     length of results
   * @param docs    array of docids starting at position 0
   * @param scores  array of scores that corresponds to docs, may be null
   * @param matches total number of matches for the query
   */
  public DocSlice(int offset, int len, int[] docs, float[] scores, long matches, float maxScore) {
    this.offset=offset;
    this.len=len;
    this.docs=docs;
    this.scores=scores;
    this.matches=matches;
    this.maxScore=maxScore;
    this.ramBytesUsed = BASE_RAM_BYTES_USED + ((long)docs.length << 2) + (scores == null ? 0 : ((long)scores.length<<2)+RamUsageEstimator.NUM_BYTES_ARRAY_HEADER);
  }

  @Override
  public DocList subset(int offset, int len) {
    if (this.offset == offset && this.len==len) return this;

    // if we didn't store enough (and there was more to store)
    // then we can't take a subset.
    int requestedEnd = offset + len;
    if (requestedEnd > docs.length && this.matches > docs.length) return null;
    int realEndDoc = Math.min(requestedEnd, docs.length);
    int realLen = Math.max(realEndDoc-offset,0);
    if (this.offset == offset && this.len == realLen) return this;
    return new DocSlice(offset, realLen, docs, scores, matches, maxScore);
  }

  @Override
  public boolean hasScores() {
    return scores!=null;
  }

  @Override
  public float maxScore() {
    return maxScore;
  }


  @Override
  public int offset()  { return offset; }
  @Override
  public int size()    { return len; }
  @Override
  public long matches() { return matches; }


  @Override
  public boolean exists(int doc) {
    int end = offset+len;
    for (int i=offset; i<end; i++) {
      if (docs[i]==doc) return true;
    }
    return false;
  }

  // Hmmm, maybe I could have reused the scorer interface here...
  // except that it carries Similarity baggage...
  @Override
  public DocIterator iterator() {
    return new DocIterator() {
      int pos=offset;
      final int end=offset+len;
      @Override
      public boolean hasNext() {
        return pos < end;
      }

      @Override
      public Integer next() {
        return nextDoc();
      }

      /**
       * The remove  operation is not supported by this Iterator.
       */
      @Override
      public void remove() {
        throw new UnsupportedOperationException("The remove  operation is not supported by this Iterator.");
      }

      @Override
      public int nextDoc() {
        return docs[pos++];
      }

      @Override
      public float score() {
        return scores[pos-1];
      }
    };
  }


  @Override
  public DocSet intersection(DocSet other) {
    if (other instanceof SortedIntDocSet || other instanceof HashDocSet) {
      return other.intersection(this);
    }
    HashDocSet h = new HashDocSet(docs,offset,len);
    return h.intersection(other);
  }

  @Override
  public int intersectionSize(DocSet other) {
    if (other instanceof SortedIntDocSet || other instanceof HashDocSet) {
      return other.intersectionSize(this);
    }
    HashDocSet h = new HashDocSet(docs,offset,len);
    return h.intersectionSize(other);  
  }

  @Override
  public boolean intersects(DocSet other) {
    if (other instanceof SortedIntDocSet || other instanceof HashDocSet) {
      return other.intersects(this);
    }
    HashDocSet h = new HashDocSet(docs,offset,len);
    return h.intersects(other);
  }

  @Override
  public DocSlice clone() {
    return (DocSlice) super.clone();
  }

  /** WARNING: this can over-estimate real memory use since backing arrays are shared with other DocSlice instances */
  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }
}
