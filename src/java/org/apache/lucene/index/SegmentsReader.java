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
import java.util.Hashtable;

import org.apache.lucene.store.Directory;
import org.apache.lucene.document.Document;

final class SegmentsReader extends IndexReader {
  protected SegmentReader[] readers;
  protected int[] starts;			  // 1st docno for each segment
  private Hashtable normsCache = new Hashtable();
  private int maxDoc = 0;
  private int numDocs = -1;

  SegmentsReader(Directory directory, SegmentReader[] r) throws IOException {
    super(directory);
    readers = r;
    starts = new int[readers.length + 1];	  // build starts array
    for (int i = 0; i < readers.length; i++) {
      starts[i] = maxDoc;
      maxDoc += readers[i].maxDoc();		  // compute maxDocs
    }
    starts[readers.length] = maxDoc;
  }

  public synchronized final int numDocs() {
    if (numDocs == -1) {			  // check cache
      int n = 0;				  // cache miss--recompute
      for (int i = 0; i < readers.length; i++)
	n += readers[i].numDocs();		  // sum from readers
      numDocs = n;
    }
    return numDocs;
  }

  public final int maxDoc() {
    return maxDoc;
  }

  public final Document document(int n) throws IOException {
    int i = readerIndex(n);			  // find segment num
    return readers[i].document(n - starts[i]);	  // dispatch to segment reader
  }

  public final boolean isDeleted(int n) {
    int i = readerIndex(n);			  // find segment num
    return readers[i].isDeleted(n - starts[i]);	  // dispatch to segment reader
  }

  synchronized final void doDelete(int n) throws IOException {
    numDocs = -1;				  // invalidate cache
    int i = readerIndex(n);			  // find segment num
    readers[i].doDelete(n - starts[i]);		  // dispatch to segment reader
  }

  private final int readerIndex(int n) {	  // find reader for doc n:
    int lo = 0;					  // search starts array
    int hi = readers.length - 1;		  // for first element less
						  // than n, return its index
    while (hi >= lo) {
      int mid = (lo + hi) >> 1;
      int midValue = starts[mid];
      if (n < midValue)
	hi = mid - 1;
      else if (n > midValue)
	lo = mid + 1;
      else
	return mid;
    }
    return hi;
  }

  public final synchronized byte[] norms(String field) throws IOException {
    byte[] bytes = (byte[])normsCache.get(field);
    if (bytes != null)
      return bytes;				  // cache hit

    bytes = new byte[maxDoc()];
    for (int i = 0; i < readers.length; i++)
      readers[i].norms(field, bytes, starts[i]);
    normsCache.put(field, bytes);		  // update cache
    return bytes;
  }

  public final TermEnum terms() throws IOException {
    return new SegmentsTermEnum(readers, starts, null);
  }

  public final TermEnum terms(Term term) throws IOException {
    return new SegmentsTermEnum(readers, starts, term);
  }

  public final int docFreq(Term t) throws IOException {
    int total = 0;				  // sum freqs in segments
    for (int i = 0; i < readers.length; i++)
      total += readers[i].docFreq(t);
    return total;
  }

  public final TermDocs termDocs() throws IOException {
    return new SegmentsTermDocs(readers, starts);
  }

  public final TermPositions termPositions() throws IOException {
    return new SegmentsTermPositions(readers, starts);
  }

  final synchronized void doClose() throws IOException {
    for (int i = 0; i < readers.length; i++)
      readers[i].close();
  }
}

class SegmentsTermEnum extends TermEnum {
  private SegmentMergeQueue queue;

  private Term term;
  private int docFreq;

  SegmentsTermEnum(SegmentReader[] readers, int[] starts, Term t)
       throws IOException {
    queue = new SegmentMergeQueue(readers.length);
    for (int i = 0; i < readers.length; i++) {
      SegmentReader reader = readers[i];
      SegmentTermEnum termEnum;

      if (t != null) {
	termEnum = (SegmentTermEnum)reader.terms(t);
      } else
	termEnum = (SegmentTermEnum)reader.terms();
      
      SegmentMergeInfo smi = new SegmentMergeInfo(starts[i], termEnum, reader);
      if (t == null ? smi.next() : termEnum.term() != null)
	queue.put(smi);				  // initialize queue
      else
	smi.close();
    }

    if (t != null && queue.size() > 0) {
      SegmentMergeInfo top = (SegmentMergeInfo)queue.top();
      term = top.termEnum.term();
      docFreq = top.termEnum.docFreq();
    }
  }

  public final boolean next() throws IOException {
    SegmentMergeInfo top = (SegmentMergeInfo)queue.top();
    if (top == null) {
      term = null;
      return false;
    }
      
    term = top.term;
    docFreq = 0;
    
    while (top != null && term.compareTo(top.term) == 0) {
      queue.pop();
      docFreq += top.termEnum.docFreq();	  // increment freq
      if (top.next())
	queue.put(top);				  // restore queue
      else
	top.close();				  // done with a segment
      top = (SegmentMergeInfo)queue.top();
    }
    return true;
  }

  public final Term term() {
    return term;
  }

  public final int docFreq() {
    return docFreq;
  }

  public final void close() throws IOException {
    queue.close();
  }
}

class SegmentsTermDocs implements TermDocs {
  protected SegmentReader[] readers;
  protected int[] starts;
  protected Term term;

  protected int base = 0;
  protected int pointer = 0;

  private SegmentTermDocs[] segTermDocs;
  protected SegmentTermDocs current;              // == segTermDocs[pointer]
  
  SegmentsTermDocs(SegmentReader[] r, int[] s) {
    readers = r;
    starts = s;

    segTermDocs = new SegmentTermDocs[r.length];
  }

  public final int doc() {
    return base + current.doc;
  }
  public final int freq() {
    return current.freq;
  }

  public final void seek(Term term) {
    this.term = term;
    this.base = 0;
    this.pointer = 0;
    this.current = null;
  }

  public final boolean next() throws IOException {
    if (current != null && current.next()) {
      return true;
    } else if (pointer < readers.length) {
      base = starts[pointer];
      current = termDocs(pointer++);
      return next();
    } else
      return false;
  }

  /** Optimized implementation. */
  public final int read(final int[] docs, final int[] freqs)
      throws IOException {
    while (true) {
      while (current == null) {
	if (pointer < readers.length) {		  // try next segment
	  base = starts[pointer];
	  current = termDocs(pointer++);
	} else {
	  return 0;
	}
      }
      int end = current.read(docs, freqs);
      if (end == 0) {				  // none left in segment
	current = null;
      } else {					  // got some
	final int b = base;			  // adjust doc numbers
	for (int i = 0; i < end; i++)
	  docs[i] += b;
	return end;
      }
    }
  }

  /** As yet unoptimized implementation. */
  public boolean skipTo(int target) throws IOException {
    do {
      if (!next())
	return false;
    } while (target > doc());
    return true;
  }

  private SegmentTermDocs termDocs(int i) throws IOException {
    if (term == null)
      return null;
    SegmentTermDocs result = segTermDocs[i];
    if (result == null)
      result = segTermDocs[i] = termDocs(readers[i]);
    result.seek(term);
    return result;
  }

  protected SegmentTermDocs termDocs(SegmentReader reader)
    throws IOException {
    return (SegmentTermDocs)reader.termDocs();
  }

  public final void close() throws IOException {
    for (int i = 0; i < segTermDocs.length; i++) {
      if (segTermDocs[i] != null)
        segTermDocs[i].close();
    }
  }
}

class SegmentsTermPositions extends SegmentsTermDocs implements TermPositions {
  SegmentsTermPositions(SegmentReader[] r, int[] s) {
    super(r,s);
  }

  protected final SegmentTermDocs termDocs(SegmentReader reader)
       throws IOException {
    return (SegmentTermDocs)reader.termPositions();
  }

  public final int nextPosition() throws IOException {
    return ((SegmentTermPositions)current).nextPosition();
  }
}
