package org.apache.lucene.index;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2003 The Apache Software Foundation. All rights reserved.
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

import org.apache.lucene.document.Document;

/**  A <code>FilterIndexReader</code> contains another IndexReader, which it
 * uses as its basic source of data, possibly transforming the data along the
 * way or providing additional functionality. The class
 * <code>FilterIndexReader</code> itself simply implements all abstract methods
 * of <code>IndexReader</code> with versions that pass all requests to the
 * contained index reader. Subclasses of <code>FilterIndexReader</code> may
 * further override some of these methods and may also provide additional
 * methods and fields.
*/
public class FilterIndexReader extends IndexReader {

  /** Base class for filtering {@link TermDocs} implementations. */
  public static class FilterTermDocs implements TermDocs {
    protected TermDocs in;

    public FilterTermDocs(TermDocs in) { this.in = in; }

    public void seek(Term term) throws IOException { in.seek(term); }
    public void seek(TermEnum enum) throws IOException { in.seek(enum); }
    public int doc() { return in.doc(); }
    public int freq() { return in.freq(); }
    public boolean next() throws IOException { return in.next(); }
    public int read(int[] docs, int[] freqs) throws IOException {
      return in.read(docs, freqs);
    }
    public boolean skipTo(int i) throws IOException { return in.skipTo(i); }
    public void close() throws IOException { in.close(); }
  }

  /** Base class for filtering {@link TermPositions} implementations. */
  public static class FilterTermPositions
     extends FilterTermDocs implements TermPositions {

    public FilterTermPositions(TermPositions in) { super(in); }

    public int nextPosition() throws IOException {
      return ((TermPositions) this.in).nextPosition();
    }
  }

  /** Base class for filtering {@link TermEnum} implementations. */
  public static class FilterTermEnum extends TermEnum {
    protected TermEnum in;

    public FilterTermEnum(TermEnum in) { this.in = in; }

    public boolean next() throws IOException { return in.next(); }
    public Term term() { return in.term(); }
    public int docFreq() { return in.docFreq(); }
    public void close() throws IOException { in.close(); }
  }

  protected IndexReader in;

  public FilterIndexReader(IndexReader in) {
    super(in.directory());
    this.in = in;
  }

  public int numDocs() { return in.numDocs(); }
  public int maxDoc() { return in.maxDoc(); }

  public Document document(int n) throws IOException {return in.document(n);}

  public boolean isDeleted(int n) { return in.isDeleted(n); }
  public boolean hasDeletions() { return in.hasDeletions(); }
  public void undeleteAll() throws IOException { in.undeleteAll(); }

  public byte[] norms(String f) throws IOException { return in.norms(f); }

  public TermEnum terms() throws IOException { return in.terms(); }
  public TermEnum terms(Term t) throws IOException { return in.terms(t); }

  public int docFreq(Term t) throws IOException { return in.docFreq(t); }

  public TermDocs termDocs() throws IOException { return in.termDocs(); }
  public TermPositions termPositions() throws IOException {
    return in.termPositions();
  }

  protected void doDelete(int n) throws IOException { in.doDelete(n); }
  protected void doClose() throws IOException { in.doClose(); }

  public Collection getFieldNames() throws IOException {
    return in.getFieldNames();
  }
  public Collection getFieldNames(boolean indexed) throws IOException {
    return in.getFieldNames(indexed);
  }
}
