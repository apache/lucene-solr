package org.apache.lucene.index;

/*
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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.util.Bits;


/** An {@link AtomicReader} which reads multiple, parallel indexes.  Each index
 * added must have the same number of documents, but typically each contains
 * different fields. Deletions are taken from the first reader.
 * Each document contains the union of the fields of all documents
 * with the same document number.  When searching, matches for a
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
public class ParallelAtomicReader extends AtomicReader {
  private final FieldInfos fieldInfos;
  private final ParallelFields fields = new ParallelFields();
  private final AtomicReader[] parallelReaders, storedFieldsReaders;
  private final Set<AtomicReader> completeReaderSet =
    Collections.newSetFromMap(new IdentityHashMap<AtomicReader,Boolean>());
  private final boolean closeSubReaders;
  private final int maxDoc, numDocs;
  private final boolean hasDeletions;
  private final SortedMap<String,AtomicReader> fieldToReader = new TreeMap<String,AtomicReader>();
  private final SortedMap<String,AtomicReader> tvFieldToReader = new TreeMap<String,AtomicReader>();
  
  /** Create a ParallelAtomicReader based on the provided
   *  readers; auto-closes the given readers on {@link #close()}. */
  public ParallelAtomicReader(AtomicReader... readers) throws IOException {
    this(true, readers);
  }

  /** Create a ParallelAtomicReader based on the provided
   *  readers. */
  public ParallelAtomicReader(boolean closeSubReaders, AtomicReader... readers) throws IOException {
    this(closeSubReaders, readers, readers);
  }

  /** Expert: create a ParallelAtomicReader based on the provided
   *  readers and storedFieldReaders; when a document is
   *  loaded, only storedFieldsReaders will be used. */
  public ParallelAtomicReader(boolean closeSubReaders, AtomicReader[] readers, AtomicReader[] storedFieldsReaders) throws IOException {
    this.closeSubReaders = closeSubReaders;
    if (readers.length == 0 && storedFieldsReaders.length > 0)
      throw new IllegalArgumentException("There must be at least one main reader if storedFieldsReaders are used.");
    this.parallelReaders = readers.clone();
    this.storedFieldsReaders = storedFieldsReaders.clone();
    if (parallelReaders.length > 0) {
      final AtomicReader first = parallelReaders[0];
      this.maxDoc = first.maxDoc();
      this.numDocs = first.numDocs();
      this.hasDeletions = first.hasDeletions();
    } else {
      this.maxDoc = this.numDocs = 0;
      this.hasDeletions = false;
    }
    Collections.addAll(completeReaderSet, this.parallelReaders);
    Collections.addAll(completeReaderSet, this.storedFieldsReaders);
    
    // check compatibility:
    for(AtomicReader reader : completeReaderSet) {
      if (reader.maxDoc() != maxDoc) {
        throw new IllegalArgumentException("All readers must have same maxDoc: "+maxDoc+"!="+reader.maxDoc());
      }
    }
    
    // TODO: make this read-only in a cleaner way?
    FieldInfos.Builder builder = new FieldInfos.Builder();
    // build FieldInfos and fieldToReader map:
    for (final AtomicReader reader : this.parallelReaders) {
      final FieldInfos readerFieldInfos = reader.getFieldInfos();
      for (FieldInfo fieldInfo : readerFieldInfos) {
        // NOTE: first reader having a given field "wins":
        if (!fieldToReader.containsKey(fieldInfo.name)) {
          builder.add(fieldInfo);
          fieldToReader.put(fieldInfo.name, reader);
          if (fieldInfo.hasVectors()) {
            tvFieldToReader.put(fieldInfo.name, reader);
          }
        }
      }
    }
    fieldInfos = builder.finish();
    
    // build Fields instance
    for (final AtomicReader reader : this.parallelReaders) {
      final Fields readerFields = reader.fields();
      if (readerFields != null) {
        for (String field : readerFields) {
          // only add if the reader responsible for that field name is the current:
          if (fieldToReader.get(field) == reader) {
            this.fields.addField(field, readerFields.terms(field));
          }
        }
      }
    }

    // do this finally so any Exceptions occurred before don't affect refcounts:
    for (AtomicReader reader : completeReaderSet) {
      if (!closeSubReaders) {
        reader.incRef();
      }
      reader.registerParentReader(this);
    }
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("ParallelAtomicReader(");
    for (final Iterator<AtomicReader> iter = completeReaderSet.iterator(); iter.hasNext();) {
      buffer.append(iter.next());
      if (iter.hasNext()) buffer.append(", ");
    }
    return buffer.append(')').toString();
  }
  
  // Single instance of this, per ParallelReader instance
  private final class ParallelFields extends Fields {
    final Map<String,Terms> fields = new TreeMap<String,Terms>();
    
    ParallelFields() {
    }
    
    void addField(String fieldName, Terms terms) {
      fields.put(fieldName, terms);
    }
    
    @Override
    public Iterator<String> iterator() {
      return Collections.unmodifiableSet(fields.keySet()).iterator();
    }
    
    @Override
    public Terms terms(String field) {
      return fields.get(field);
    }
    
    @Override
    public int size() {
      return fields.size();
    }
  }
  
  /**
   * {@inheritDoc}
   * <p>
   * NOTE: the returned field numbers will likely not
   * correspond to the actual field numbers in the underlying
   * readers, and codec metadata ({@link FieldInfo#getAttribute(String)}
   * will be unavailable.
   */
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
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    ensureOpen();
    for (final AtomicReader reader: storedFieldsReaders) {
      reader.document(docID, visitor);
    }
  }
  
  @Override
  public Fields getTermVectors(int docID) throws IOException {
    ensureOpen();
    ParallelFields fields = null;
    for (Map.Entry<String,AtomicReader> ent : tvFieldToReader.entrySet()) {
      String fieldName = ent.getKey();
      Terms vector = ent.getValue().getTermVector(docID, fieldName);
      if (vector != null) {
        if (fields == null) {
          fields = new ParallelFields();
        }
        fields.addField(fieldName, vector);
      }
    }
    
    return fields;
  }
  
  @Override
  protected synchronized void doClose() throws IOException {
    IOException ioe = null;
    for (AtomicReader reader : completeReaderSet) {
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
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    ensureOpen();
    AtomicReader reader = fieldToReader.get(field);
    return reader == null ? null : reader.getNumericDocValues(field);
  }
  
  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    ensureOpen();
    AtomicReader reader = fieldToReader.get(field);
    return reader == null ? null : reader.getBinaryDocValues(field);
  }
  
  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    ensureOpen();
    AtomicReader reader = fieldToReader.get(field);
    return reader == null ? null : reader.getSortedDocValues(field);
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    ensureOpen();
    AtomicReader reader = fieldToReader.get(field);
    return reader == null ? null : reader.getSortedSetDocValues(field);
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    ensureOpen();
    AtomicReader reader = fieldToReader.get(field);
    NumericDocValues values = reader == null ? null : reader.getNormValues(field);
    return values;
  }
}
