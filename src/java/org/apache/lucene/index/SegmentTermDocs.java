package org.apache.lucene.index;

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
import org.apache.lucene.util.BitVector;
import org.apache.lucene.store.InputStream;

class SegmentTermDocs implements TermDocs {
  protected SegmentReader parent;
  private InputStream freqStream;
  private int freqCount;
  private BitVector deletedDocs;
  int doc = 0;
  int freq;

  SegmentTermDocs(SegmentReader p) throws IOException {
    parent = p;
    freqStream = parent.getFreqStream();
    deletedDocs = parent.deletedDocs;
  }

  SegmentTermDocs(SegmentReader p, TermInfo ti) throws IOException {
    this(p);
    seek(ti);
  }
  
  void seek(TermInfo ti) throws IOException {
    freqCount = ti.docFreq;
    doc = 0;
    freqStream.seek(ti.freqPointer);
  }
  
  public void close() throws IOException {
    freqStream.close();
  }

  public final int doc() { return doc; }
  public final int freq() { return freq; }

  protected void skippingDoc() throws IOException {
  }

  public boolean next() throws IOException {
    while (true) {
      if (freqCount == 0)
	return false;

      int docCode = freqStream.readVInt();
      doc += docCode >>> 1;			  // shift off low bit
      if ((docCode & 1) != 0)			  // if low bit is set
	freq = 1;				  // freq is one
      else
	freq = freqStream.readVInt();		  // else read freq
 
      freqCount--;
    
      if (deletedDocs == null || !deletedDocs.get(doc))
	break;
      skippingDoc();
    }
    return true;
  }

  /** Optimized implementation. */
  public int read(final int[] docs, final int[] freqs)
      throws IOException {
    final int end = docs.length;
    int i = 0;
    while (i < end && freqCount > 0) {

      // manually inlined call to next() for speed
      final int docCode = freqStream.readVInt();
      doc += docCode >>> 1;			  // shift off low bit
      if ((docCode & 1) != 0)			  // if low bit is set
	freq = 1;				  // freq is one
      else
	freq = freqStream.readVInt();		  // else read freq
      freqCount--;
   
      if (deletedDocs == null || !deletedDocs.get(doc)) {
	docs[i] = doc;
	freqs[i] = freq;
	++i;
      }
     }
    return i;
  }

  /** As yet unoptimized implementation. */
  public boolean skipTo(int target) throws IOException {
    do {
      if (!next())
	return false;
    } while (target > doc);
    return true;
  }
}
