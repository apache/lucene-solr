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

import java.io.IOException;
import java.util.*;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;


/** An AtomicIndexReader which reads multiple, parallel indexes.  Each index added
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
public class ParallelReader extends AtomicReader {
  private List<AtomicReader> readers = new ArrayList<AtomicReader>();
  private List<Boolean> decrefOnClose = new ArrayList<Boolean>(); // remember which subreaders to decRef on close
  boolean incRefReaders = false;
  private SortedMap<String,AtomicReader> fieldToReader = new TreeMap<String,AtomicReader>();
  private Map<AtomicReader,Collection<String>> readerToFields = new HashMap<AtomicReader,Collection<String>>();
  private List<AtomicReader> storedFieldReaders = new ArrayList<AtomicReader>();
  private Map<String, DocValues> normsCache = new HashMap<String,DocValues>();
  private int maxDoc;
  private int numDocs;
  private boolean hasDeletions;
  private final FieldInfos fieldInfos;

  private final ParallelFields fields = new ParallelFields();

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
    fieldInfos = new FieldInfos();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("ParallelReader(");
    final Iterator<AtomicReader> iter = readers.iterator();
    if (iter.hasNext()) {
      buffer.append(iter.next());
    }
    while (iter.hasNext()) {
      buffer.append(", ").append(iter.next());
    }
    buffer.append(')');
    return buffer.toString();
  }
  
 /** Add an AtomicIndexReader.
  * @throws IOException if there is a low-level IO error
  */
  public void add(AtomicReader reader) throws IOException {
    ensureOpen();
    add(reader, false);
  }

 /** Add an AtomicIndexReader whose stored fields will not be returned.  This can
  * accelerate search when stored fields are only needed from a subset of
  * the IndexReaders.
  *
  * @throws IllegalArgumentException if not all indexes contain the same number
  *     of documents
  * @throws IllegalArgumentException if not all indexes have the same value
  *     of {@link AtomicReader#maxDoc()}
  * @throws IOException if there is a low-level IO error
  */
  public void add(AtomicReader reader, boolean ignoreStoredFields)
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

    final FieldInfos readerFieldInfos = MultiFields.getMergedFieldInfos(reader);
    for(FieldInfo fieldInfo : readerFieldInfos) {   // update fieldToReader map
      // NOTE: first reader having a given field "wins":
      if (fieldToReader.get(fieldInfo.name) == null) {
        fieldInfos.add(fieldInfo);
        fieldToReader.put(fieldInfo.name, reader);
        this.fields.addField(fieldInfo.name, reader.terms(fieldInfo.name));
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
  public FieldInfos getFieldInfos() {
    return fieldInfos;
  }
  
  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return readers.get(0).getLiveDocs();
  }

  @Override
  public Fields fields() {
    ensureOpen();
    return fields;
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
    for (final AtomicReader reader: storedFieldReaders) {
      reader.document(docID, visitor);
    }
  }

  // get all vectors
  @Override
  public Fields getTermVectors(int docID) throws IOException {
    ensureOpen();
    ParallelFields fields = new ParallelFields();
    for (Map.Entry<String,AtomicReader> ent : fieldToReader.entrySet()) {
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
    AtomicReader reader = fieldToReader.get(field);
    return reader==null ? false : reader.hasNorms(field);
  }

  // for testing
  AtomicReader[] getSubReaders() {
    return readers.toArray(new AtomicReader[readers.size()]);
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

  // TODO: I suspect this is completely untested!!!!!
  @Override
  public DocValues docValues(String field) throws IOException {
    AtomicReader reader = fieldToReader.get(field);
    return reader == null ? null : reader.docValues(field);
  }
  
  // TODO: I suspect this is completely untested!!!!!
  @Override
  public synchronized DocValues normValues(String field) throws IOException {
    DocValues values = normsCache.get(field);
    if (values == null) {
      AtomicReader reader = fieldToReader.get(field);
      values = reader == null ? null : reader.normValues(field);
      normsCache.put(field, values);
    } 
    return values;
  }
}
