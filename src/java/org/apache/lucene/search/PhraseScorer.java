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
import org.apache.lucene.util.*;
import org.apache.lucene.index.*;

abstract class PhraseScorer extends Scorer {
  protected byte[] norms;
  protected float weight;

  protected PhraseQueue pq;
  protected PhrasePositions first, last;

  PhraseScorer(TermPositions[] tps, byte[] n, float w) throws IOException {
    norms = n;
    weight = w;

    // use PQ to build a sorted list of PhrasePositions
    pq = new PhraseQueue(tps.length);
    for (int i = 0; i < tps.length; i++)
      pq.put(new PhrasePositions(tps[i], i));
    pqToList();
  }

  final void score(HitCollector results, int end) throws IOException {
    while (last.doc < end) {			  // find doc w/ all the terms
      while (first.doc < last.doc) {		  // scan forward in first
	do {
	  first.next();
	} while (first.doc < last.doc);
	firstToLast();
	if (last.doc >= end)
	  return;
      }

      // found doc with all terms
      float freq = phraseFreq();		  // check for phrase

      if (freq > 0.0) {
	float score = Similarity.tf(freq)*weight; // compute score
	score *= Similarity.norm(norms[first.doc]); // normalize
	results.collect(first.doc, score);	  // add to results
      }
      last.next();				  // resume scanning
    }
  }

  abstract protected float phraseFreq() throws IOException;

  protected final void pqToList() {
    last = first = null;
    while (pq.top() != null) {
      PhrasePositions pp = (PhrasePositions)pq.pop();
      if (last != null) {			  // add next to end of list
	last.next = pp;
      } else
	first = pp;
      last = pp;
      pp.next = null;
    }
  }

  protected final void firstToLast() {
    last.next = first;			  // move first to end of list
    last = first;
    first = first.next;
    last.next = null;
  }
}
