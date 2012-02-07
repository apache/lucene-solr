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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.util.Bits;


/** An {@link AtomicReader} which reads multiple, parallel indexes.  Each index added
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
 * <p>To create instances of {@code ParallelAtomicReader}, use the provided
 * {@link ParallelAtomicReader.Builder}.
 *
 * <p><strong>Warning:</strong> It is up to you to make sure all indexes
 * are created and modified the same way. For example, if you add
 * documents to one index, you need to add the same documents in the
 * same order to the other indexes. <em>Failure to do so will result in
 * undefined behavior</em>.
 */
public final class ParallelAtomicReader extends AtomicReader {
  private final FieldInfos fieldInfos = new FieldInfos();
  private final ParallelFields fields = new ParallelFields();
  private final AtomicReader[] parallelReaders, storedFieldReaders;
  private final boolean closeSubReaders;
  private final int maxDoc, numDocs;
  private final boolean hasDeletions;
  final SortedMap<String,AtomicReader> fieldToReader = new TreeMap<String,AtomicReader>();
  
  // only called from builder!!!
  ParallelAtomicReader(boolean closeSubReaders, AtomicReader[] readers, AtomicReader[] storedFieldReaders) throws IOException {
    this.closeSubReaders = closeSubReaders;
    assert readers.length >= storedFieldReaders.length;
    this.parallelReaders = readers;
    this.storedFieldReaders = storedFieldReaders;
    this.numDocs = (readers.length > 0) ? readers[0].numDocs() : 0;
    this.maxDoc = (readers.length > 0) ? readers[0].maxDoc() : 0;
    this.hasDeletions = (readers.length > 0) ? readers[0].hasDeletions() : false;
    
    for (final AtomicReader reader : readers) {
      final FieldInfos readerFieldInfos = reader.getFieldInfos();
      for(FieldInfo fieldInfo : readerFieldInfos) { // update fieldToReader map
        // NOTE: first reader having a given field "wins":
        if (fieldToReader.get(fieldInfo.name) == null) {
          fieldInfos.add(fieldInfo);
          fieldToReader.put(fieldInfo.name, reader);
          this.fields.addField(fieldInfo.name, reader.terms(fieldInfo.name));
        }
      }
      if (!closeSubReaders) {
        reader.incRef();
      }
    } 
  }
  
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("ParallelAtomicReader(");
    for (final Iterator<AtomicReader> iter = Arrays.asList(parallelReaders).iterator(); iter.hasNext();) {
      buffer.append(iter.next());
      if (iter.hasNext()) buffer.append(", ");
    }
    return buffer.append(')').toString();
  }
  
  private final class ParallelFieldsEnum extends FieldsEnum {
    private String currentField;
    private final Iterator<String> keys;
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
  private final class ParallelFields extends Fields {
    final HashMap<String,Terms> fields = new HashMap<String,Terms>();
    
    ParallelFields() {
    }
    
    void addField(String fieldName, Terms terms) throws IOException {
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
    return hasDeletions ? parallelReaders[0].getLiveDocs() : null;
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
  
  @Override
  protected synchronized void doClose() throws IOException {
    IOException ioe = null;
    for (AtomicReader reader : parallelReaders) {
      try {
        if (closeSubReaders) {
          reader.close();
        } else {
          reader.decRef();
        }
      } catch (IOException e) {
        if (ioe == null) ioe = e;
      }
    }
    // throw the first exception
    if (ioe != null) throw ioe;
  }
  
  @Override
  public DocValues docValues(String field) throws IOException {
    ensureOpen();
    AtomicReader reader = fieldToReader.get(field);
    return reader == null ? null : reader.docValues(field);
  }
  
  @Override
  public DocValues normValues(String field) throws IOException {
    ensureOpen();
    AtomicReader reader = fieldToReader.get(field);
    return reader == null ? null : reader.normValues(field);
  }
  
  /**
   * Builder implementation to create instances of {@link ParallelAtomicReader}.
   */
  public static final class Builder {
    private final boolean closeSubReaders;
    private final List<AtomicReader> parallelReaders = new ArrayList<AtomicReader>();
    private final List<AtomicReader> storedFieldReaders = new ArrayList<AtomicReader>();
    private int maxDoc, numDocs;
    
    /**
     * Create a new builder instance that automatically enables closing of all subreader
     * once the build reader is closed.
     */
    public Builder() {
      this(true);
    }
    
    /**
     * Create a new builder instance.
     */
    public Builder(boolean closeSubReaders) {
      this.closeSubReaders = closeSubReaders;
    }
    
    /** Add an AtomicReader.
     * @throws IOException if there is a low-level IO error
     */
    public Builder add(AtomicReader reader) throws IOException {
      return add(reader, false);
    }
    
    /** Add an AtomicReader whose stored fields will not be returned.  This can
     * accelerate search when stored fields are only needed from a subset of
     * the IndexReaders.
     *
     * @throws IllegalArgumentException if not all indexes contain the same number
     *     of documents
     * @throws IllegalArgumentException if not all indexes have the same value
     *     of {@link AtomicReader#maxDoc()}
     * @throws IOException if there is a low-level IO error
     */
    public Builder add(AtomicReader reader, boolean ignoreStoredFields) throws IOException {
      if (parallelReaders.isEmpty()) {
        this.maxDoc = reader.maxDoc();
        this.numDocs = reader.numDocs();
      } else {
        // check compatibility
        if (reader.maxDoc() != maxDoc)
          throw new IllegalArgumentException("All readers must have same maxDoc: "+maxDoc+"!="+reader.maxDoc());
        if (reader.numDocs() != numDocs)
          throw new IllegalArgumentException("All readers must have same numDocs: "+numDocs+"!="+reader.numDocs());
      }
      
      if (!ignoreStoredFields)
        storedFieldReaders.add(reader); // add to storedFieldReaders
      parallelReaders.add(reader);
      return this;
    }
    
    /**
     * Build the {@link ParallelAtomicReader} instance from the settings.
     */
    public ParallelAtomicReader build() throws IOException {
      return new ParallelAtomicReader(
        closeSubReaders,
        parallelReaders.toArray(new AtomicReader[parallelReaders.size()]),
        storedFieldReaders.toArray(new AtomicReader[storedFieldReaders.size()])
      );
    }
    
  }
}
