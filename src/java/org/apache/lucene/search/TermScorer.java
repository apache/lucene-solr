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

import org.apache.lucene.index.TermDocs;

final class TermScorer extends Scorer {
  private Weight weight;
  private TermDocs termDocs;
  private byte[] norms;
  private float weightValue;
  private int doc;

  private final int[] docs = new int[32];	  // buffered doc numbers
  private final int[] freqs = new int[32];	  // buffered term freqs
  private int pointer;
  private int pointerMax;

  private static final int SCORE_CACHE_SIZE = 32;
  private float[] scoreCache = new float[SCORE_CACHE_SIZE];

  TermScorer(Weight weight, TermDocs td, Similarity similarity,
             byte[] norms) throws IOException {
    super(similarity);
    this.weight = weight;
    this.termDocs = td;
    this.norms = norms;
    this.weightValue = weight.getValue();

    for (int i = 0; i < SCORE_CACHE_SIZE; i++)
      scoreCache[i] = getSimilarity().tf(i) * weightValue;

    pointerMax = termDocs.read(docs, freqs);	  // fill buffers

    if (pointerMax != 0)
      doc = docs[0];
    else {
      termDocs.close();				  // close stream
      doc = Integer.MAX_VALUE;			  // set to sentinel value
    }
  }

  public final void score(HitCollector c, final int end) throws IOException {
    int d = doc;				  // cache doc in local
    Similarity similarity = getSimilarity();      // cache sim in local
    while (d < end) {				  // for docs in window
      final int f = freqs[pointer];
      float score =				  // compute tf(f)*weight
	f < SCORE_CACHE_SIZE			  // check cache
	 ? scoreCache[f]			  // cache hit
	 : similarity.tf(f)*weightValue;          // cache miss

      score *= Similarity.decodeNorm(norms[d]);	  // normalize for field

      c.collect(d, score);			  // collect score

      if (++pointer == pointerMax) {
	pointerMax = termDocs.read(docs, freqs);  // refill buffers
	if (pointerMax != 0) {
	  pointer = 0;
	} else {
	  termDocs.close();			  // close stream
	  doc = Integer.MAX_VALUE;		  // set to sentinel value
	  return;
	}
      } 
      d = docs[pointer];
    }
    doc = d;					  // flush cache
  }

  public Explanation explain(int doc) throws IOException {
    TermQuery query = (TermQuery)weight.getQuery();
    Explanation tfExplanation = new Explanation();
    int tf = 0;
    while (pointer < pointerMax) {
      if (docs[pointer] == doc)
        tf = freqs[pointer];
      pointer++;
    }
    if (tf == 0) {
      while (termDocs.next()) {
        if (termDocs.doc() == doc) {
          tf = termDocs.freq();
        }
      }
    }
    termDocs.close();
    tfExplanation.setValue(getSimilarity().tf(tf));
    tfExplanation.setDescription("tf(termFreq("+query.getTerm()+")="+tf+")");
    
    return tfExplanation;
  }
}
