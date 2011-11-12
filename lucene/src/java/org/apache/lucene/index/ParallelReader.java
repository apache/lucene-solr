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
import org.apache.lucene.util.MapBackedSet;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


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
  private List<IndexReader> readers = new ArrayList<IndexReader>();
  private List<Boolean> decrefOnClose = new ArrayList<Boolean>(); // remember which subreaders to decRef on close
  boolean incRefReaders = false;
  private SortedMap<String,IndexReader> fieldToReader = new TreeMap<String,IndexReader>();
  private Map<IndexReader,Collection<String>> readerToFields = new HashMap<IndexReader,Collection<String>>();
  private List<IndexReader> storedFieldReaders = new ArrayList<IndexReader>();

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
    readerFinishedListeners = new MapBackedSet<ReaderFinishedListener>(new ConcurrentHashMap<ReaderFinishedListener,Boolean>());
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("ParallelReader(");
    final Iterator<IndexReader> iter = readers.iterator();
    if (iter.hasNext()) {
      buffer.append(iter.next());
    }
    while (iter.hasNext()) {
      buffer.append(", ").append(iter.next());
    }
    buffer.append(')');
    return buffer.toString();
  }

 /** Add an IndexReader.
  * @throws IOException if there is a low-level IO error
  */
  public void add(IndexReader reader) throws IOException {
    ensureOpen();
    add(reader, false);
  }

 /** Add an IndexReader whose stored fields will not be returned.  This can
  * accelerate search when stored fields are only needed from a subset of
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

    Collection<String> fields = reader.getFieldNames(IndexReader.FieldOption.ALL);
    readerToFields.put(reader, fields);
    for (final String field : fields) {                         // update fieldToReader map
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
  
  @Override
  public synchronized Object clone() {
    // doReopen calls ensureOpen
    try {
      return doReopen(true);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  /**
   * Tries to reopen the subreaders.
   * <br>
   * If one or more subreaders could be re-opened (i. e. subReader.reopen() 
   * returned a new instance != subReader), then a new ParallelReader instance 
   * is returned, otherwise null is returned.
   * <p>
   * A re-opened instance might share one or more subreaders with the old 
   * instance. Index modification operations result in undefined behavior
   * when performed before the old instance is closed.
   * (see {@link IndexReader#openIfChanged}).
   * <p>
   * If subreaders are shared, then the reference count of those
   * readers is increased to ensure that the subreaders remain open
   * until the last referring reader is closed.
   * 
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error 
   */
  @Override
  protected synchronized IndexReader doOpenIfChanged() throws CorruptIndexException, IOException {
    // doReopen calls ensureOpen
    return doReopen(false);
  }
    
  protected IndexReader doReopen(boolean doClone) throws CorruptIndexException, IOException {
    ensureOpen();
    
    boolean reopened = false;
    List<IndexReader> newReaders = new ArrayList<IndexReader>();
    
    boolean success = false;
    
    try {
      for (final IndexReader oldReader : readers) {
        IndexReader newReader = null;
        if (doClone) {
          newReader = (IndexReader) oldReader.clone();
          reopened = true;
        } else {
          newReader = IndexReader.openIfChanged(oldReader);
          if (newReader != null) {
            reopened = true;
          } else {
            newReader = oldReader;
          }
        }
        newReaders.add(newReader);
      }
      success = true;
    } finally {
      if (!success && reopened) {
        for (int i = 0; i < newReaders.size(); i++) {
          IndexReader r = newReaders.get(i);
          if (r != readers.get(i)) {
            try {
              r.close();
            } catch (IOException ignore) {
              // keep going - we want to clean up as much as possible
            }
          }
        }
      }
    }

    if (reopened) {
      List<Boolean> newDecrefOnClose = new ArrayList<Boolean>();
      ParallelReader pr = new ParallelReader();
      for (int i = 0; i < readers.size(); i++) {
        IndexReader oldReader = readers.get(i);
        IndexReader newReader = newReaders.get(i);
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
      return pr;
    } else {
      // No subreader was refreshed
      return null;
    }
  }


  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    return numDocs;
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return maxDoc;
  }

  @Override
  public boolean hasDeletions() {
    ensureOpen();
    return hasDeletions;
  }

  // check first reader
  @Override
  public boolean isDeleted(int n) {
    // Don't call ensureOpen() here (it could affect performance)
    if (readers.size() > 0)
      return readers.get(0).isDeleted(n);
    return false;
  }

  // delete in all readers
  @Override
  protected void doDelete(int n) throws CorruptIndexException, IOException {
    for (final IndexReader reader : readers) {
      reader.deleteDocument(n);
    }
    hasDeletions = true;
  }

  // undeleteAll in all readers
  @Override
  protected void doUndeleteAll() throws CorruptIndexException, IOException {
    for (final IndexReader reader : readers) {
      reader.undeleteAll();
    }
    hasDeletions = false;
  }

  // append fields from storedFieldReaders
  @Override
  public Document document(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
    ensureOpen();
    Document result = new Document();
    for (final IndexReader reader: storedFieldReaders) {

      boolean include = (fieldSelector==null);
      if (!include) {
        Collection<String> fields = readerToFields.get(reader);
        for (final String field : fields)
          if (fieldSelector.accept(field) != FieldSelectorResult.NO_LOAD) {
            include = true;
            break;
          }
      }
      if (include) {
        List<Fieldable> fields = reader.document(n, fieldSelector).getFields();
        for (Fieldable field : fields) {
          result.add(field);
        }
      }
    }
    return result;
  }

  // get all vectors
  @Override
  public TermFreqVector[] getTermFreqVectors(int n) throws IOException {
    ensureOpen();
    ArrayList<TermFreqVector> results = new ArrayList<TermFreqVector>();
    for (final Map.Entry<String,IndexReader> e: fieldToReader.entrySet()) {

      String field = e.getKey();
      IndexReader reader = e.getValue();
      TermFreqVector vector = reader.getTermFreqVector(n, field);
      if (vector != null)
        results.add(vector);
    }
    return results.toArray(new TermFreqVector[results.size()]);
  }

  @Override
  public TermFreqVector getTermFreqVector(int n, String field)
    throws IOException {
    ensureOpen();
    IndexReader reader = fieldToReader.get(field);
    return reader==null ? null : reader.getTermFreqVector(n, field);
  }


  @Override
  public void getTermFreqVector(int docNumber, String field, TermVectorMapper mapper) throws IOException {
    ensureOpen();
    IndexReader reader = fieldToReader.get(field);
    if (reader != null) {
      reader.getTermFreqVector(docNumber, field, mapper); 
    }
  }

  @Override
  public void getTermFreqVector(int docNumber, TermVectorMapper mapper) throws IOException {
    ensureOpen();

    for (final Map.Entry<String,IndexReader> e : fieldToReader.entrySet()) {

      String field = e.getKey();
      IndexReader reader = e.getValue();
      reader.getTermFreqVector(docNumber, field, mapper);
    }

  }

  @Override
  public boolean hasNorms(String field) throws IOException {
    ensureOpen();
    IndexReader reader = fieldToReader.get(field);
    return reader==null ? false : reader.hasNorms(field);
  }

  @Override
  public byte[] norms(String field) throws IOException {
    ensureOpen();
    IndexReader reader = fieldToReader.get(field);
    return reader==null ? null : reader.norms(field);
  }

  @Override
  public void norms(String field, byte[] result, int offset)
    throws IOException {
    ensureOpen();
    IndexReader reader = fieldToReader.get(field);
    if (reader!=null)
      reader.norms(field, result, offset);
  }

  @Override
  protected void doSetNorm(int n, String field, byte value)
    throws CorruptIndexException, IOException {
    IndexReader reader = fieldToReader.get(field);
    if (reader!=null)
      reader.doSetNorm(n, field, value);
  }

  @Override
  public TermEnum terms() throws IOException {
    ensureOpen();
    return new ParallelTermEnum();
  }

  @Override
  public TermEnum terms(Term term) throws IOException {
    ensureOpen();
    return new ParallelTermEnum(term);
  }

  @Override
  public int docFreq(Term term) throws IOException {
    ensureOpen();
    IndexReader reader = fieldToReader.get(term.field());
    return reader==null ? 0 : reader.docFreq(term);
  }

  @Override
  public TermDocs termDocs(Term term) throws IOException {
    ensureOpen();
    return new ParallelTermDocs(term);
  }

  @Override
  public TermDocs termDocs() throws IOException {
    ensureOpen();
    return new ParallelTermDocs();
  }

  @Override
  public TermPositions termPositions(Term term) throws IOException {
    ensureOpen();
    return new ParallelTermPositions(term);
  }

  @Override
  public TermPositions termPositions() throws IOException {
    ensureOpen();
    return new ParallelTermPositions();
  }
  
  /**
   * Checks recursively if all subreaders are up to date. 
   */
  @Override
  public boolean isCurrent() throws CorruptIndexException, IOException {
    ensureOpen();
    for (final IndexReader reader : readers) {
      if (!reader.isCurrent()) {
        return false;
      }
    }
    
    // all subreaders are up to date
    return true;
  }

  @Deprecated
  @Override
  public boolean isOptimized() {
    ensureOpen();
    for (final IndexReader reader : readers) {
      if (!reader.isOptimized()) {
        return false;
      }
    }
    
    // all subindexes are optimized
    return true;
  }

  /** Not implemented.
   * @throws UnsupportedOperationException
   */
  @Override
  public long getVersion() {
    throw new UnsupportedOperationException("ParallelReader does not support this method.");
  }

  // for testing
  IndexReader[] getSubReaders() {
    return readers.toArray(new IndexReader[readers.size()]);
  }

  @Override
  protected void doCommit(Map<String,String> commitUserData) throws IOException {
    for (final IndexReader reader : readers)
      reader.commit(commitUserData);
  }

  @Override
  protected synchronized void doClose() throws IOException {
    for (int i = 0; i < readers.size(); i++) {
      if (decrefOnClose.get(i).booleanValue()) {
        readers.get(i).decRef();
      } else {
        readers.get(i).close();
      }
    }
  }

  @Override
  public Collection<String> getFieldNames (IndexReader.FieldOption fieldNames) {
    ensureOpen();
    Set<String> fieldSet = new HashSet<String>();
    for (final IndexReader reader : readers) {
      Collection<String> names = reader.getFieldNames(fieldNames);
      fieldSet.addAll(names);
    }
    return fieldSet;
  }

  private class ParallelTermEnum extends TermEnum {
    private String field;
    private Iterator<String> fieldIterator;
    private TermEnum termEnum;

    public ParallelTermEnum() throws IOException {
      try {
        field = fieldToReader.firstKey();
      } catch(NoSuchElementException e) {
        // No fields, so keep field == null, termEnum == null
        return;
      }
      if (field != null)
        termEnum = fieldToReader.get(field).terms();
    }

    public ParallelTermEnum(Term term) throws IOException {
      field = term.field();
      IndexReader reader = fieldToReader.get(field);
      if (reader!=null)
        termEnum = reader.terms(term);
    }

    @Override
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
        field = fieldIterator.next();
        termEnum = fieldToReader.get(field).terms(new Term(field));
        Term term = termEnum.term();
        if (term!=null && term.field()==field)
          return true;
        else
          termEnum.close();
      }
 
      return false;                               // no more fields
    }

    @Override
    public Term term() {
      if (termEnum==null)
        return null;

      return termEnum.term();
    }

    @Override
    public int docFreq() {
      if (termEnum==null)
        return 0;

      return termEnum.docFreq();
    }

    @Override
    public void close() throws IOException {
      if (termEnum!=null)
        termEnum.close();
    }
  }

  // wrap a TermDocs in order to support seek(Term)
  private class ParallelTermDocs implements TermDocs {
    protected TermDocs termDocs;

    public ParallelTermDocs() {}
    public ParallelTermDocs(Term term) throws IOException {
      if (term == null)
        termDocs = readers.isEmpty() ? null : readers.get(0).termDocs(null);
      else
        seek(term);
    }

    public int doc() { return termDocs.doc(); }
    public int freq() { return termDocs.freq(); }

    public void seek(Term term) throws IOException {
      IndexReader reader = fieldToReader.get(term.field());
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

    @Override
    public void seek(Term term) throws IOException {
      IndexReader reader = fieldToReader.get(term.field());
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

  @Override
  public void addReaderFinishedListener(ReaderFinishedListener listener) {
    super.addReaderFinishedListener(listener);
    for (IndexReader reader : readers) {
      reader.addReaderFinishedListener(listener);
    }
  }

  @Override
  public void removeReaderFinishedListener(ReaderFinishedListener listener) {
    super.removeReaderFinishedListener(listener);
    for (IndexReader reader : readers) {
      reader.removeReaderFinishedListener(listener);
    }
  }
}





