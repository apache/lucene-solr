package org.apache.lucene.index;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001, 2002, 2003 The Apache Software Foundation.
 * All rights reserved.
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
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;

/** An IndexReader which reads multiple indexes, appending their content.
 *
 * @version $Id$
 */
public class MultiReader extends IndexReader {
  private IndexReader[] readers;
  private int[] starts;                           // 1st docno for each segment
  private Hashtable normsCache = new Hashtable();
  private int maxDoc = 0;
  private int numDocs = -1;
  private boolean hasDeletions = false;
  
  /** Construct reading the named set of readers. */
  public MultiReader(IndexReader[] readers) throws IOException {
    this(readers.length == 0 ? null : readers[0].directory(), readers);
  }

  /** Construct reading the named set of readers. */
  public MultiReader(Directory directory, IndexReader[] readers)
    throws IOException {
    super(directory);
    this.readers = readers;
    starts = new int[readers.length + 1];	  // build starts array
    for (int i = 0; i < readers.length; i++) {
      starts[i] = maxDoc;
      maxDoc += readers[i].maxDoc();		  // compute maxDocs

      if (readers[i].hasDeletions())
        hasDeletions = true;
    }
    starts[readers.length] = maxDoc;
  }

  public synchronized int numDocs() {
    if (numDocs == -1) {			  // check cache
      int n = 0;				  // cache miss--recompute
      for (int i = 0; i < readers.length; i++)
	n += readers[i].numDocs();		  // sum from readers
      numDocs = n;
    }
    return numDocs;
  }

  public int maxDoc() {
    return maxDoc;
  }

  public Document document(int n) throws IOException {
    int i = readerIndex(n);			  // find segment num
    return readers[i].document(n - starts[i]);	  // dispatch to segment reader
  }

  public boolean isDeleted(int n) {
    int i = readerIndex(n);			  // find segment num
    return readers[i].isDeleted(n - starts[i]);	  // dispatch to segment reader
  }

  public boolean hasDeletions() { return hasDeletions; }

  protected synchronized void doDelete(int n) throws IOException {
    numDocs = -1;				  // invalidate cache
    int i = readerIndex(n);			  // find segment num
    readers[i].doDelete(n - starts[i]);		  // dispatch to segment reader
    hasDeletions = true;
  }

  public void undeleteAll() throws IOException {
    for (int i = 0; i < readers.length; i++)
      readers[i].undeleteAll();
    hasDeletions = false;
  }

  private int readerIndex(int n) {	  // find reader for doc n:
    int lo = 0;					  // search starts array
    int hi = readers.length - 1;                  // for first element less

    while (hi >= lo) {
      int mid = (lo + hi) >> 1;
      int midValue = starts[mid];
      if (n < midValue)
	hi = mid - 1;
      else if (n > midValue)
	lo = mid + 1;
      else {                                      // found a match
        while (mid+1 < readers.length && starts[mid+1] == midValue) {
          mid++;                                  // scan to last match
        }
	return mid;
      }
    }
    return hi;
  }

  public synchronized byte[] norms(String field) throws IOException {
    byte[] bytes = (byte[])normsCache.get(field);
    if (bytes != null)
      return bytes;				  // cache hit

    bytes = new byte[maxDoc()];
    for (int i = 0; i < readers.length; i++)
      readers[i].norms(field, bytes, starts[i]);
    normsCache.put(field, bytes);		  // update cache
    return bytes;
  }

  public synchronized void norms(String field, byte[] result, int offset)
    throws IOException {
    byte[] bytes = (byte[])normsCache.get(field);
    if (bytes != null)                            // cache hit
      System.arraycopy(bytes, 0, result, offset, maxDoc());

    for (int i = 0; i < readers.length; i++)      // read from segments
      readers[i].norms(field, result, offset + starts[i]);
  }

  public synchronized void setNorm(int n, String field, byte value)
    throws IOException {
    normsCache.remove(field);                     // clear cache
    int i = readerIndex(n);			  // find segment num
    readers[i].setNorm(n-starts[i], field, value); // dispatch
  }

  public TermEnum terms() throws IOException {
    return new MultiTermEnum(readers, starts, null);
  }

  public TermEnum terms(Term term) throws IOException {
    return new MultiTermEnum(readers, starts, term);
  }

  public int docFreq(Term t) throws IOException {
    int total = 0;				  // sum freqs in segments
    for (int i = 0; i < readers.length; i++)
      total += readers[i].docFreq(t);
    return total;
  }

  public TermDocs termDocs() throws IOException {
    return new MultiTermDocs(readers, starts);
  }

  public TermPositions termPositions() throws IOException {
    return new MultiTermPositions(readers, starts);
  }

  protected synchronized void doClose() throws IOException {
    for (int i = 0; i < readers.length; i++)
      readers[i].close();
  }

  /**
   * @see IndexReader#getFieldNames()
   */
  public Collection getFieldNames() throws IOException {
    // maintain a unique set of field names
    Set fieldSet = new HashSet();
    for (int i = 0; i < readers.length; i++) {
      IndexReader reader = readers[i];
      Collection names = reader.getFieldNames();
      // iterate through the field names and add them to the set
      for (Iterator iterator = names.iterator(); iterator.hasNext();) {
        String s = (String) iterator.next();
        fieldSet.add(s);
      }
    }
    return fieldSet;
  }

  /**
   * @see IndexReader#getFieldNames(boolean)
   */
  public Collection getFieldNames(boolean indexed) throws IOException {
    // maintain a unique set of field names
    Set fieldSet = new HashSet();
    for (int i = 0; i < readers.length; i++) {
      IndexReader reader = readers[i];
      Collection names = reader.getFieldNames(indexed);
      fieldSet.addAll(names);
    }
    return fieldSet;
  }
}

class MultiTermEnum extends TermEnum {
  private SegmentMergeQueue queue;

  private Term term;
  private int docFreq;

  public MultiTermEnum(IndexReader[] readers, int[] starts, Term t)
    throws IOException {
    queue = new SegmentMergeQueue(readers.length);
    for (int i = 0; i < readers.length; i++) {
      IndexReader reader = readers[i];
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
      next();
    }
  }

  public boolean next() throws IOException {
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

  public Term term() {
    return term;
  }

  public int docFreq() {
    return docFreq;
  }

  public void close() throws IOException {
    queue.close();
  }
}

class MultiTermDocs implements TermDocs {
  protected IndexReader[] readers;
  protected int[] starts;
  protected Term term;

  protected int base = 0;
  protected int pointer = 0;

  private SegmentTermDocs[] segTermDocs;
  protected SegmentTermDocs current;              // == segTermDocs[pointer]

  public MultiTermDocs(IndexReader[] r, int[] s) {
    readers = r;
    starts = s;

    segTermDocs = new SegmentTermDocs[r.length];
  }

  public int doc() {
    return base + current.doc;
  }
  public int freq() {
    return current.freq;
  }

  public void seek(Term term) {
    this.term = term;
    this.base = 0;
    this.pointer = 0;
    this.current = null;
  }

  public void seek(TermEnum termEnum) throws IOException {
    seek(termEnum.term());
  }

  public boolean next() throws IOException {
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
  public int read(final int[] docs, final int[] freqs)
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

  protected SegmentTermDocs termDocs(IndexReader reader)
    throws IOException {
    return (SegmentTermDocs)reader.termDocs();
  }

  public void close() throws IOException {
    for (int i = 0; i < segTermDocs.length; i++) {
      if (segTermDocs[i] != null)
        segTermDocs[i].close();
    }
  }
}

class MultiTermPositions extends MultiTermDocs implements TermPositions {
  public MultiTermPositions(IndexReader[] r, int[] s) {
    super(r,s);
  }

  protected SegmentTermDocs termDocs(IndexReader reader)
       throws IOException {
    return (SegmentTermDocs)reader.termPositions();
  }

  public int nextPosition() throws IOException {
    return ((SegmentTermPositions)current).nextPosition();
  }

}
