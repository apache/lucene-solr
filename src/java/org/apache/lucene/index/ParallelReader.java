package org.apache.lucene.index;

/**
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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.document.Fieldable;

import java.io.IOException;
import java.util.*;


/** An IndexReader which reads multiple, parallel indexes.  Each index added
 * must have the same number of documents, but typically each contains
 * different fields.  Each document contains the union of the fields of all
 * documents with the same document number.  When searching, matches for a
 * query term are from the first index added that has the field.
 *
 * <p>This is useful, e.g., with collections that have large fields which
 * change rarely and small fields that change more frequently.  The smaller
 * fields may be re-indexed in a new index and both indexes may be searched
 * together.
 *
 * <p><strong>Warning:</strong> It is up to you to make sure all indexes
 * are created and modified the same way. For example, if you add
 * documents to one index, you need to add the same documents in the
 * same order to the other indexes. <em>Failure to do so will result in
 * undefined behavior</em>.
 */
public class ParallelReader extends IndexReader {
  private List readers = new ArrayList();
  private List decrefOnClose = new ArrayList(); // remember which subreaders to decRef on close
  boolean incRefReaders = false;
  private SortedMap fieldToReader = new TreeMap();
  private Map readerToFields = new HashMap();
  private List storedFieldReaders = new ArrayList();

  private int maxDoc;
  private int numDocs;
  private boolean hasDeletions;

 /** Construct a ParallelReader. 
  * <p>Note that all subreaders are closed if this ParallelReader is closed.</p>
  */
  public ParallelReader() throws IOException { this(true); }
   
 /** Construct a ParallelReader. 
  * @param closeSubReaders indicates whether the subreaders should be closed
  * when this ParallelReader is closed
  */
  public ParallelReader(boolean closeSubReaders) throws IOException {
    super();
    this.incRefReaders = !closeSubReaders;
  }

 /** Add an IndexReader.
  * @throws IOException if there is a low-level IO error
  */
  public void add(IndexReader reader) throws IOException {
    ensureOpen();
    add(reader, false);
  }

 /** Add an IndexReader whose stored fields will not be returned.  This can
  * accellerate search when stored fields are only needed from a subset of
  * the IndexReaders.
  *
  * @throws IllegalArgumentException if not all indexes contain the same number
  *     of documents
  * @throws IllegalArgumentException if not all indexes have the same value
  *     of {@link IndexReader#maxDoc()}
  * @throws IOException if there is a low-level IO error
  */
  public void add(IndexReader reader, boolean ignoreStoredFields)
    throws IOException {

    ensureOpen();
    if (readers.size() == 0) {
      this.maxDoc = reader.maxDoc();
      this.numDocs = reader.numDocs();
      this.hasDeletions = reader.hasDeletions();
    }

    if (reader.maxDoc() != maxDoc)                // check compatibility
      throw new IllegalArgumentException
        ("All readers must have same maxDoc: "+maxDoc+"!="+reader.maxDoc());
    if (reader.numDocs() != numDocs)
      throw new IllegalArgumentException
        ("All readers must have same numDocs: "+numDocs+"!="+reader.numDocs());

    Collection fields = reader.getFieldNames(IndexReader.FieldOption.ALL);
    readerToFields.put(reader, fields);
    Iterator i = fields.iterator();
    while (i.hasNext()) {                         // update fieldToReader map
      String field = (String)i.next();
      if (fieldToReader.get(field) == null)
        fieldToReader.put(field, reader);
    }

    if (!ignoreStoredFields)
      storedFieldReaders.add(reader);             // add to storedFieldReaders
    readers.add(reader);
    
    if (incRefReaders) {
      reader.incRef();
    }
    decrefOnClose.add(Boolean.valueOf(incRefReaders));
  }

  /**
   * Tries to reopen the subreaders.
   * <br>
   * If one or more subreaders could be re-opened (i. e. subReader.reopen() 
   * returned a new instance != subReader), then a new ParallelReader instance 
   * is returned, otherwise this instance is returned.
   * <p>
   * A re-opened instance might share one or more subreaders with the old 
   * instance. Index modification operations result in undefined behavior
   * when performed before the old instance is closed.
   * (see {@link IndexReader#reopen()}).
   * <p>
   * If subreaders are shared, then the reference count of those
   * readers is increased to ensure that the subreaders remain open
   * until the last referring reader is closed.
   * 
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error 
   */
  public IndexReader reopen() throws CorruptIndexException, IOException {
    ensureOpen();
    
    boolean reopened = false;
    List newReaders = new ArrayList();
    List newDecrefOnClose = new ArrayList();
    
    boolean success = false;
    
    try {
    
      for (int i = 0; i < readers.size(); i++) {
        IndexReader oldReader = (IndexReader) readers.get(i);
        IndexReader newReader = oldReader.reopen();
        newReaders.add(newReader);
        // if at least one of the subreaders was updated we remember that
        // and return a new MultiReader
        if (newReader != oldReader) {
          reopened = true;
        }
      }
  
      if (reopened) {
        ParallelReader pr = new ParallelReader();
        for (int i = 0; i < readers.size(); i++) {
          IndexReader oldReader = (IndexReader) readers.get(i);
          IndexReader newReader = (IndexReader) newReaders.get(i);
          if (newReader == oldReader) {
            newDecrefOnClose.add(Boolean.TRUE);
            newReader.incRef();
          } else {
            // this is a new subreader instance, so on close() we don't
            // decRef but close it 
            newDecrefOnClose.add(Boolean.FALSE);
          }
          pr.add(newReader, !storedFieldReaders.contains(oldReader));
        }
        pr.decrefOnClose = newDecrefOnClose;
        pr.incRefReaders = incRefReaders;
        success = true;
        return pr;
      } else {
        success = true; 
       // No subreader was refreshed
        return this;
      }
    } finally {
      if (!success && reopened) {
        for (int i = 0; i < newReaders.size(); i++) {
          IndexReader r = (IndexReader) newReaders.get(i);
          if (r != null) {
            try {
              if (((Boolean) newDecrefOnClose.get(i)).booleanValue()) {
                r.decRef();
              } else {
                r.close();
              }
            } catch (IOException ignore) {
              // keep going - we want to clean up as much as possible
            }
          }
        }
      }
    }
  }


  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    return numDocs;
  }

  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return maxDoc;
  }

  public boolean hasDeletions() {
    // Don't call ensureOpen() here (it could affect performance)
    return hasDeletions;
  }

  // check first reader
  public boolean isDeleted(int n) {
    // Don't call ensureOpen() here (it could affect performance)
    if (readers.size() > 0)
      return ((IndexReader)readers.get(0)).isDeleted(n);
    return false;
  }

  // delete in all readers
  protected void doDelete(int n) throws CorruptIndexException, IOException {
    for (int i = 0; i < readers.size(); i++) {
      ((IndexReader)readers.get(i)).deleteDocument(n);
    }
    hasDeletions = true;
  }

  // undeleteAll in all readers
  protected void doUndeleteAll() throws CorruptIndexException, IOException {
    for (int i = 0; i < readers.size(); i++) {
      ((IndexReader)readers.get(i)).undeleteAll();
    }
    hasDeletions = false;
  }

  // append fields from storedFieldReaders
  public Document document(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
    ensureOpen();
    Document result = new Document();
    for (int i = 0; i < storedFieldReaders.size(); i++) {
      IndexReader reader = (IndexReader)storedFieldReaders.get(i);

      boolean include = (fieldSelector==null);
      if (!include) {
        Iterator it = ((Collection) readerToFields.get(reader)).iterator();
        while (it.hasNext())
          if (fieldSelector.accept((String)it.next())!=FieldSelectorResult.NO_LOAD) {
            include = true;
            break;
          }
      }
      if (include) {
        Iterator fieldIterator = reader.document(n, fieldSelector).getFields().iterator();
        while (fieldIterator.hasNext()) {
          result.add((Fieldable)fieldIterator.next());
        }
      }
    }
    return result;
  }

  // get all vectors
  public TermFreqVector[] getTermFreqVectors(int n) throws IOException {
    ensureOpen();
    ArrayList results = new ArrayList();
    Iterator i = fieldToReader.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry e = (Map.Entry)i.next();
      String field = (String)e.getKey();
      IndexReader reader = (IndexReader)e.getValue();
      TermFreqVector vector = reader.getTermFreqVector(n, field);
      if (vector != null)
        results.add(vector);
    }
    return (TermFreqVector[])
      results.toArray(new TermFreqVector[results.size()]);
  }

  public TermFreqVector getTermFreqVector(int n, String field)
    throws IOException {
    ensureOpen();
    IndexReader reader = ((IndexReader)fieldToReader.get(field));
    return reader==null ? null : reader.getTermFreqVector(n, field);
  }


  public void getTermFreqVector(int docNumber, String field, TermVectorMapper mapper) throws IOException {
    ensureOpen();
    IndexReader reader = ((IndexReader)fieldToReader.get(field));
    if (reader != null) {
      reader.getTermFreqVector(docNumber, field, mapper); 
    }
  }

  public void getTermFreqVector(int docNumber, TermVectorMapper mapper) throws IOException {
    ensureOpen();
    ensureOpen();

    Iterator i = fieldToReader.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry e = (Map.Entry)i.next();
      String field = (String)e.getKey();
      IndexReader reader = (IndexReader)e.getValue();
      reader.getTermFreqVector(docNumber, field, mapper);
    }

  }

  public boolean hasNorms(String field) throws IOException {
    ensureOpen();
    IndexReader reader = ((IndexReader)fieldToReader.get(field));
    return reader==null ? false : reader.hasNorms(field);
  }

  public byte[] norms(String field) throws IOException {
    ensureOpen();
    IndexReader reader = ((IndexReader)fieldToReader.get(field));
    return reader==null ? null : reader.norms(field);
  }

  public void norms(String field, byte[] result, int offset)
    throws IOException {
    ensureOpen();
    IndexReader reader = ((IndexReader)fieldToReader.get(field));
    if (reader!=null)
      reader.norms(field, result, offset);
  }

  protected void doSetNorm(int n, String field, byte value)
    throws CorruptIndexException, IOException {
    IndexReader reader = ((IndexReader)fieldToReader.get(field));
    if (reader!=null)
      reader.doSetNorm(n, field, value);
  }

  public TermEnum terms() throws IOException {
    ensureOpen();
    return new ParallelTermEnum();
  }

  public TermEnum terms(Term term) throws IOException {
    ensureOpen();
    return new ParallelTermEnum(term);
  }

  public int docFreq(Term term) throws IOException {
    ensureOpen();
    IndexReader reader = ((IndexReader)fieldToReader.get(term.field()));
    return reader==null ? 0 : reader.docFreq(term);
  }

  public TermDocs termDocs(Term term) throws IOException {
    ensureOpen();
    return new ParallelTermDocs(term);
  }

  public TermDocs termDocs() throws IOException {
    ensureOpen();
    return new ParallelTermDocs();
  }

  public TermPositions termPositions(Term term) throws IOException {
    ensureOpen();
    return new ParallelTermPositions(term);
  }

  public TermPositions termPositions() throws IOException {
    ensureOpen();
    return new ParallelTermPositions();
  }
  
  /**
   * Checks recursively if all subreaders are up to date. 
   */
  public boolean isCurrent() throws CorruptIndexException, IOException {
    for (int i = 0; i < readers.size(); i++) {
      if (!((IndexReader)readers.get(i)).isCurrent()) {
        return false;
      }
    }
    
    // all subreaders are up to date
    return true;
  }

  /**
   * Checks recursively if all subindexes are optimized 
   */
  public boolean isOptimized() {
    for (int i = 0; i < readers.size(); i++) {
      if (!((IndexReader)readers.get(i)).isOptimized()) {
        return false;
      }
    }
    
    // all subindexes are optimized
    return true;
  }

  
  /** Not implemented.
   * @throws UnsupportedOperationException
   */
  public long getVersion() {
    throw new UnsupportedOperationException("ParallelReader does not support this method.");
  }

  // for testing
  IndexReader[] getSubReaders() {
    return (IndexReader[]) readers.toArray(new IndexReader[readers.size()]);
  }

  protected void doCommit() throws IOException {
    for (int i = 0; i < readers.size(); i++)
      ((IndexReader)readers.get(i)).commit();
  }

  protected synchronized void doClose() throws IOException {
    for (int i = 0; i < readers.size(); i++) {
      if (((Boolean) decrefOnClose.get(i)).booleanValue()) {
        ((IndexReader)readers.get(i)).decRef();
      } else {
        ((IndexReader)readers.get(i)).close();
      }
    }
  }

  public Collection getFieldNames (IndexReader.FieldOption fieldNames) {
    ensureOpen();
    Set fieldSet = new HashSet();
    for (int i = 0; i < readers.size(); i++) {
      IndexReader reader = ((IndexReader)readers.get(i));
      Collection names = reader.getFieldNames(fieldNames);
      fieldSet.addAll(names);
    }
    return fieldSet;
  }

  private class ParallelTermEnum extends TermEnum {
    private String field;
    private Iterator fieldIterator;
    private TermEnum termEnum;

    public ParallelTermEnum() throws IOException {
      field = (String)fieldToReader.firstKey();
      if (field != null)
        termEnum = ((IndexReader)fieldToReader.get(field)).terms();
    }

    public ParallelTermEnum(Term term) throws IOException {
      field = term.field();
      IndexReader reader = ((IndexReader)fieldToReader.get(field));
      if (reader!=null)
        termEnum = reader.terms(term);
    }

    public boolean next() throws IOException {
      if (termEnum==null)
        return false;

      // another term in this field?
      if (termEnum.next() && termEnum.term().field()==field)
        return true;                              // yes, keep going

      termEnum.close();                           // close old termEnum

      // find the next field with terms, if any
      if (fieldIterator==null) {
        fieldIterator = fieldToReader.tailMap(field).keySet().iterator();
        fieldIterator.next();                     // Skip field to get next one
      }
      while (fieldIterator.hasNext()) {
        field = (String) fieldIterator.next();
        termEnum = ((IndexReader)fieldToReader.get(field)).terms(new Term(field));
        Term term = termEnum.term();
        if (term!=null && term.field()==field)
          return true;
        else
          termEnum.close();
      }
 
      return false;                               // no more fields
    }

    public Term term() {
      if (termEnum==null)
        return null;

      return termEnum.term();
    }

    public int docFreq() {
      if (termEnum==null)
        return 0;

      return termEnum.docFreq();
    }

    public void close() throws IOException {
      if (termEnum!=null)
        termEnum.close();
    }

  }

  // wrap a TermDocs in order to support seek(Term)
  private class ParallelTermDocs implements TermDocs {
    protected TermDocs termDocs;

    public ParallelTermDocs() {}
    public ParallelTermDocs(Term term) throws IOException { seek(term); }

    public int doc() { return termDocs.doc(); }
    public int freq() { return termDocs.freq(); }

    public void seek(Term term) throws IOException {
      IndexReader reader = ((IndexReader)fieldToReader.get(term.field()));
      termDocs = reader!=null ? reader.termDocs(term) : null;
    }

    public void seek(TermEnum termEnum) throws IOException {
      seek(termEnum.term());
    }

    public boolean next() throws IOException {
      if (termDocs==null)
        return false;

      return termDocs.next();
    }

    public int read(final int[] docs, final int[] freqs) throws IOException {
      if (termDocs==null)
        return 0;

      return termDocs.read(docs, freqs);
    }

    public boolean skipTo(int target) throws IOException {
      if (termDocs==null)
        return false;

      return termDocs.skipTo(target);
    }

    public void close() throws IOException {
      if (termDocs!=null)
        termDocs.close();
    }

  }

  private class ParallelTermPositions
    extends ParallelTermDocs implements TermPositions {

    public ParallelTermPositions() {}
    public ParallelTermPositions(Term term) throws IOException { seek(term); }

    public void seek(Term term) throws IOException {
      IndexReader reader = ((IndexReader)fieldToReader.get(term.field()));
      termDocs = reader!=null ? reader.termPositions(term) : null;
    }

    public int nextPosition() throws IOException {
      // It is an error to call this if there is no next position, e.g. if termDocs==null
      return ((TermPositions)termDocs).nextPosition();
    }

    public int getPayloadLength() {
      return ((TermPositions)termDocs).getPayloadLength();
    }

    public byte[] getPayload(byte[] data, int offset) throws IOException {
      return ((TermPositions)termDocs).getPayload(data, offset);
    }


    // TODO: Remove warning after API has been finalized
    public boolean isPayloadAvailable() {
      return ((TermPositions) termDocs).isPayloadAvailable();
    }
  }

}





