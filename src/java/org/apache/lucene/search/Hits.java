package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.Vector;
import java.util.Iterator;

import org.apache.lucene.document.Document;

/** A ranked list of documents, used to hold search results. */
public final class Hits {
  private Weight weight;
  private Searcher searcher;
  private Filter filter = null;
  private Sort sort = null;

  private int length;				  // the total number of hits
  private Vector hitDocs = new Vector();	  // cache of hits retrieved

  private HitDoc first;         // head of LRU cache
  private HitDoc last;          // tail of LRU cache
  private int numDocs = 0;      // number cached
  private int maxDocs = 200;    // max to cache

  Hits(Searcher s, Query q, Filter f) throws IOException {
    weight = q.weight(s);
    searcher = s;
    filter = f;
    getMoreDocs(50); // retrieve 100 initially
  }

  Hits(Searcher s, Query q, Filter f, Sort o) throws IOException {
    weight = q.weight(s);
    searcher = s;
    filter = f;
    sort = o;
    getMoreDocs(50); // retrieve 100 initially
  }

  /**
   * Tries to add new documents to hitDocs.
   * Ensures that the hit numbered <code>min</code> has been retrieved.
   */
  private final void getMoreDocs(int min) throws IOException {
    if (hitDocs.size() > min) {
      min = hitDocs.size();
    }

    int n = min * 2;	// double # retrieved
    TopDocs topDocs = (sort == null) ? searcher.search(weight, filter, n) : searcher.search(weight, filter, n, sort);
    length = topDocs.totalHits;
    ScoreDoc[] scoreDocs = topDocs.scoreDocs;

    float scoreNorm = 1.0f;
    
    if (length > 0 && topDocs.getMaxScore() > 1.0f) {
      scoreNorm = 1.0f / topDocs.getMaxScore();
    }

    int end = scoreDocs.length < length ? scoreDocs.length : length;
    for (int i = hitDocs.size(); i < end; i++) {
      hitDocs.addElement(new HitDoc(scoreDocs[i].score * scoreNorm,
                                    scoreDocs[i].doc));
    }
  }

  /** Returns the total number of hits available in this set. */
  public final int length() {
    return length;
  }

  /** Returns the stored fields of the n<sup>th</sup> document in this set.
   <p>Documents are cached, so that repeated requests for the same element may
   return the same Document object. */
  public final Document doc(int n) throws IOException {
    HitDoc hitDoc = hitDoc(n);

    // Update LRU cache of documents
    remove(hitDoc);               // remove from list, if there
    addToFront(hitDoc);           // add to front of list
    if (numDocs > maxDocs) {      // if cache is full
      HitDoc oldLast = last;
      remove(last);             // flush last
      oldLast.doc = null;       // let doc get gc'd
    }

    if (hitDoc.doc == null) {
      hitDoc.doc = searcher.doc(hitDoc.id);  // cache miss: read document
    }

    return hitDoc.doc;
  }

  /** Returns the score for the nth document in this set. */
  public final float score(int n) throws IOException {
    return hitDoc(n).score;
  }

  /** Returns the id for the nth document in this set. */
  public final int id(int n) throws IOException {
    return hitDoc(n).id;
  }

  /**
   * Returns a {@link HitIterator} to navigate the Hits.  Each item returned
   * from {@link Iterator#next()} is a {@link Hit}.
   * <p>
   * <b>Caution:</b> Iterate only over the hits needed.  Iterating over all
   * hits is generally not desirable and may be the source of
   * performance issues.
   * </p>
   */
  public Iterator iterator() {
    return new HitIterator(this);
  }

  private final HitDoc hitDoc(int n) throws IOException {
    if (n >= length) {
      throw new IndexOutOfBoundsException("Not a valid hit number: " + n);
    }

    if (n >= hitDocs.size()) {
      getMoreDocs(n);
    }

    return (HitDoc) hitDocs.elementAt(n);
  }

  private final void addToFront(HitDoc hitDoc) {  // insert at front of cache
    if (first == null) {
      last = hitDoc;
    } else {
      first.prev = hitDoc;
    }

    hitDoc.next = first;
    first = hitDoc;
    hitDoc.prev = null;

    numDocs++;
  }

  private final void remove(HitDoc hitDoc) {	  // remove from cache
    if (hitDoc.doc == null) {     // it's not in the list
      return;					  // abort
    }

    if (hitDoc.next == null) {
      last = hitDoc.prev;
    } else {
      hitDoc.next.prev = hitDoc.prev;
    }

    if (hitDoc.prev == null) {
      first = hitDoc.next;
    } else {
      hitDoc.prev.next = hitDoc.next;
    }

    numDocs--;
  }
}

final class HitDoc {
  float score;
  int id;
  Document doc = null;

  HitDoc next;  // in doubly-linked cache
  HitDoc prev;  // in doubly-linked cache

  HitDoc(float s, int i) {
    score = s;
    id = i;
  }
}
