package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.IOException;
import java.util.Vector;
import java.util.BitSet;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;

/** A ranked list of documents, used to hold search results. */
public final class Hits {
  private Query query;
  private Searcher searcher;
  private Filter filter = null;

  private int length;				  // the total number of hits
  private Vector hitDocs = new Vector();	  // cache of hits retrieved

  private HitDoc first;				  // head of LRU cache
  private HitDoc last;				  // tail of LRU cache
  private int numDocs = 0;			  // number cached
  private int maxDocs = 200;			  // max to cache

  Hits(Searcher s, Query q, Filter f) throws IOException {
    query = q;
    searcher = s;
    filter = f;
    getMoreDocs(50);				  // retrieve 100 initially
  }

  // Tries to add new documents to hitDocs.
  // Ensures that the hit numbered <code>min</code> has been retrieved.
  private final void getMoreDocs(int min) throws IOException {
    if (hitDocs.size() > min)
      min = hitDocs.size();

    int n = min * 2;				  // double # retrieved
    TopDocs topDocs = searcher.search(query, filter, n);
    length = topDocs.totalHits;
    ScoreDoc[] scoreDocs = topDocs.scoreDocs;

    float scoreNorm = 1.0f;
    if (length > 0 && scoreDocs[0].score > 1.0f)
      scoreNorm = 1.0f / scoreDocs[0].score;

    int end = scoreDocs.length < length ? scoreDocs.length : length;
    for (int i = hitDocs.size(); i < end; i++)
      hitDocs.addElement(new HitDoc(scoreDocs[i].score*scoreNorm,
				    scoreDocs[i].doc));
  }

  /** Returns the total number of hits available in this set. */
  public final int length() {
    return length;
  }

  /** Returns the nth document in this set.
    <p>Documents are cached, so that repeated requests for the same element may
    return the same Document object. */ 
  public final Document doc(int n) throws IOException {
    HitDoc hitDoc = hitDoc(n);

    // Update LRU cache of documents
    remove(hitDoc);				  // remove from list, if there
    addToFront(hitDoc);				  // add to front of list
    if (numDocs > maxDocs) {			  // if cache is full
      HitDoc oldLast = last;
      remove(last);				  // flush last
      oldLast.doc = null;			  // let doc get gc'd
    }

    if (hitDoc.doc == null)
      hitDoc.doc = searcher.doc(hitDoc.id);	  // cache miss: read document
      
    return hitDoc.doc;
  }

  /** Returns the score for the nth document in this set. */ 
  public final float score(int n) throws IOException {
    return hitDoc(n).score;
  }

  private final HitDoc hitDoc(int n) throws IOException {
    if (n >= length)
      throw new IndexOutOfBoundsException("Not a valid hit number: " + n);
    if (n >= hitDocs.size())
      getMoreDocs(n);

    return (HitDoc)hitDocs.elementAt(n);
  }

  private final void addToFront(HitDoc hitDoc) {  // insert at front of cache
    if (first == null)
      last = hitDoc;
    else
      first.prev = hitDoc;
    
    hitDoc.next = first;
    first = hitDoc;
    hitDoc.prev = null;

    numDocs++;
  }

  private final void remove(HitDoc hitDoc) {	  // remove from cache
    if (hitDoc.doc == null)			  // it's not in the list
      return;					  // abort

    if (hitDoc.next == null)
      last = hitDoc.prev;
    else
      hitDoc.next.prev = hitDoc.prev;
    
    if (hitDoc.prev == null)
      first = hitDoc.next;
    else
      hitDoc.prev.next = hitDoc.next;

    numDocs--;
  }
}

final class HitDoc {
  float score;
  int id;
  Document doc = null;

  HitDoc next;					  // in doubly-linked cache
  HitDoc prev;					  // in doubly-linked cache

  HitDoc(float s, int i) {
    score = s;
    id = i;
  }
}
