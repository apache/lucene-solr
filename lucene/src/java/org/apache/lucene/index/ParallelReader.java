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

import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.index.values.IndexDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
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
  private Map<String,byte[]> normsCache = new HashMap<String,byte[]>();
  private final ReaderContext topLevelReaderContext = new AtomicReaderContext(this);
  private int maxDoc;
  private int numDocs;
  private boolean hasDeletions;

  private final ParallelFields fields = new ParallelFields();
  private final ParallelPerDocs perDocs = new ParallelPerDocs();

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
    for (final String field : fields) {               // update fieldToReader map
      if (fieldToReader.get(field) == null) {
        fieldToReader.put(field, reader);
        this.fields.addField(field, MultiFields.getFields(reader).terms(field));
        this.perDocs.addField(field, reader);
      }
    }

    if (!ignoreStoredFields)
      storedFieldReaders.add(reader);             // add to storedFieldReaders
    readers.add(reader);
    
    if (incRefReaders) {
      reader.incRef();
    }
    decrefOnClose.add(Boolean.valueOf(incRefReaders));
    synchronized(normsCache) {
      normsCache.clear(); // TODO: don't need to clear this for all fields really?
    }
  }

  private class ParallelFieldsEnum extends FieldsEnum {
    String currentField;
    Iterator<String> keys;
    private final Fields fields;

    ParallelFieldsEnum(Fields fields) {
      this.fields = fields;
      keys = fieldToReader.keySet().iterator();
    }

    @Override
    public String next() throws IOException {
      if (keys.hasNext()) {
        currentField = keys.next();
      } else {
        currentField = null;
      }
      return currentField;
    }

    @Override
    public Terms terms() throws IOException {
      return fields.terms(currentField);
    }

  }

  // Single instance of this, per ParallelReader instance
  private class ParallelFields extends Fields {
    final HashMap<String,Terms> fields = new HashMap<String,Terms>();

    public void addField(String fieldName, Terms terms) throws IOException {
      fields.put(fieldName, terms);
    }

    @Override
    public FieldsEnum iterator() throws IOException {
      return new ParallelFieldsEnum(this);
    }

    @Override
    public Terms terms(String field) throws IOException {
      return fields.get(field);
    }

    @Override
    public int getUniqueFieldCount() throws IOException {
      return fields.size();
    }
  }
  
  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return MultiFields.getLiveDocs(readers.get(0));
  }

  @Override
  public Fields fields() {
    ensureOpen();
    return fields;
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
      // TODO: maybe add a special reopen-ctor for norm-copying?
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

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws CorruptIndexException, IOException {
    ensureOpen();
    for (final IndexReader reader: storedFieldReaders) {
      reader.document(docID, visitor);
    }
  }

  // get all vectors
  @Override
  public Fields getTermVectors(int docID) throws IOException {
    ensureOpen();
    ParallelFields fields = new ParallelFields();
    for (Map.Entry<String,IndexReader> ent : fieldToReader.entrySet()) {
      String fieldName = ent.getKey();
      Terms vector = ent.getValue().getTermVector(docID, fieldName);
      if (vector != null) {
        fields.addField(fieldName, vector);
      }
    }

    return fields;
  }

  @Override
  public boolean hasNorms(String field) throws IOException {
    ensureOpen();
    IndexReader reader = fieldToReader.get(field);
    return reader==null ? false : reader.hasNorms(field);
  }

  @Override
  public synchronized byte[] norms(String field) throws IOException {
    ensureOpen();
    IndexReader reader = fieldToReader.get(field);

    if (reader==null)
      return null;
    
    byte[] bytes = normsCache.get(field);
    if (bytes != null)
      return bytes;
    if (!hasNorms(field))
      return null;
    if (normsCache.containsKey(field)) // cached omitNorms, not missing key
      return null;

    bytes = MultiNorms.norms(reader, field);
    normsCache.put(field, bytes);
    return bytes;
  }

  @Override
  public int docFreq(String field, BytesRef term) throws IOException {
    ensureOpen();
    IndexReader reader = fieldToReader.get(field);
    return reader == null? 0 : reader.docFreq(field, term);
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

  @Override
  public ReaderContext getTopReaderContext() {
    ensureOpen();
    return topLevelReaderContext;
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

  @Override
  public PerDocValues perDocValues() throws IOException {
    ensureOpen();
    return perDocs;
  }
  
  // Single instance of this, per ParallelReader instance
  private static final class ParallelPerDocs extends PerDocValues {
    final TreeMap<String,IndexDocValues> fields = new TreeMap<String,IndexDocValues>();

    void addField(String field, IndexReader r) throws IOException {
      PerDocValues perDocs = MultiPerDocValues.getPerDocs(r);
      if (perDocs != null) {
        fields.put(field, perDocs.docValues(field));
      }
    }

    @Override
    public void close() throws IOException {
      // nothing to do here
    }

    @Override
    public IndexDocValues docValues(String field) throws IOException {
      return fields.get(field);
    }

    @Override
    public Collection<String> fields() {
      return fields.keySet();
    }
  }
}





