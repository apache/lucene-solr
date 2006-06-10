package org.apache.lucene.index;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;


import java.io.IOException;
import java.util.Collection;

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
    public void seek(TermEnum termEnum) throws IOException { in.seek(termEnum); }
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

  /**
   * <p>Construct a FilterIndexReader based on the specified base reader.
   * Directory locking for delete, undeleteAll, and setNorm operations is
   * left to the base reader.</p>
   * <p>Note that base reader is closed if this FilterIndexReader is closed.</p>
   * @param in specified base reader.
   */
  public FilterIndexReader(IndexReader in) {
    super(in.directory());
    this.in = in;
  }

  public TermFreqVector[] getTermFreqVectors(int docNumber)
          throws IOException {
    return in.getTermFreqVectors(docNumber);
  }

  public TermFreqVector getTermFreqVector(int docNumber, String field)
          throws IOException {
    return in.getTermFreqVector(docNumber, field);
  }

  public int numDocs() { return in.numDocs(); }
  public int maxDoc() { return in.maxDoc(); }

  public Document document(int n, FieldSelector fieldSelector) throws IOException { return in.document(n, fieldSelector); }

  public boolean isDeleted(int n) { return in.isDeleted(n); }
  public boolean hasDeletions() { return in.hasDeletions(); }
  protected void doUndeleteAll() throws IOException { in.undeleteAll(); }

  public boolean hasNorms(String field) throws IOException {
    return in.hasNorms(field);
  }

  public byte[] norms(String f) throws IOException { return in.norms(f); }
  public void norms(String f, byte[] bytes, int offset) throws IOException {
    in.norms(f, bytes, offset);
  }
  protected void doSetNorm(int d, String f, byte b) throws IOException {
    in.setNorm(d, f, b);
  }

  public TermEnum terms() throws IOException { return in.terms(); }
  public TermEnum terms(Term t) throws IOException { return in.terms(t); }

  public int docFreq(Term t) throws IOException { return in.docFreq(t); }

  public TermDocs termDocs() throws IOException { return in.termDocs(); }

  public TermPositions termPositions() throws IOException {
    return in.termPositions();
  }

  protected void doDelete(int n) throws IOException { in.deleteDocument(n); }
  protected void doCommit() throws IOException { in.commit(); }
  protected void doClose() throws IOException { in.close(); }


  public Collection getFieldNames(IndexReader.FieldOption fieldNames) {
    return in.getFieldNames(fieldNames);
  }

  public long getVersion() { return in.getVersion(); }
  public boolean isCurrent() throws IOException { return in.isCurrent(); }
}
