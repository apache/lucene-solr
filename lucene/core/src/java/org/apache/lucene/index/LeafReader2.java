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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IOUtils;

/** "this is the new LeafReader" */
// nocommit: fold into LeafReader when all FilterReaders are converted
public abstract class LeafReader2 extends LeafReader {
  
  /** 
   * Expert: retrieve thread-private TermVectorsReader
   * @throws AlreadyClosedException if this reader is closed
   * @lucene.internal 
   */
  protected abstract TermVectorsReader getTermVectorsReader();

  /** 
   * Expert: retrieve thread-private StoredFieldsReader
   * @throws AlreadyClosedException if this reader is closed
   * @lucene.internal 
   */
  protected abstract StoredFieldsReader getFieldsReader();
  
  /** 
   * Expert: retrieve underlying NormsProducer
   * @throws AlreadyClosedException if this reader is closed
   * @lucene.internal 
   */
  protected abstract NormsProducer getNormsReader();
  
  /** 
   * Expert: retrieve underlying DocValuesProducer
   * @throws AlreadyClosedException if this reader is closed
   * @lucene.internal 
   */
  protected abstract DocValuesProducer getDocValuesReader();
  
  /** 
   * Expert: retrieve underlying FieldsProducer
   * @throws AlreadyClosedException if this reader is closed
   * @lucene.internal  
   */
  protected abstract FieldsProducer getPostingsReader();
  
  @Override
  public final Fields fields() {
    return getPostingsReader();
  }
  
  private final void checkBounds(int docID) {
    if (docID < 0 || docID >= maxDoc()) {       
      throw new IndexOutOfBoundsException("docID must be >= 0 and < maxDoc=" + maxDoc() + " (got docID=" + docID + ")");
    }
  }
  
  @Override
  public final void document(int docID, StoredFieldVisitor visitor) throws IOException {
    checkBounds(docID);
    getFieldsReader().visitDocument(docID, visitor);
  }
  
  @Override
  public final Fields getTermVectors(int docID) throws IOException {
    TermVectorsReader termVectorsReader = getTermVectorsReader();
    if (termVectorsReader == null) {
      return null;
    }
    checkBounds(docID);
    return termVectorsReader.get(docID);
  }
  
  final CloseableThreadLocal<Map<String,Object>> docValuesLocal = new CloseableThreadLocal<Map<String,Object>>() {
    @Override
    protected Map<String,Object> initialValue() {
      return new HashMap<>();
    }
  };

  final CloseableThreadLocal<Map<String,Bits>> docsWithFieldLocal = new CloseableThreadLocal<Map<String,Bits>>() {
    @Override
    protected Map<String,Bits> initialValue() {
      return new HashMap<>();
    }
  };
  

  final CloseableThreadLocal<Map<String,Object>> normsLocal = new CloseableThreadLocal<Map<String,Object>>() {
    @Override
    protected Map<String,Object> initialValue() {
      return new HashMap<>();
    }
  };
  
  // returns the FieldInfo that corresponds to the given field and type, or
  // null if the field does not exist, or not indexed as the requested
  // DovDocValuesType.
  private FieldInfo getDVField(String field, DocValuesType type) {
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() == DocValuesType.NONE) {
      // Field was not indexed with doc values
      return null;
    }
    if (fi.getDocValuesType() != type) {
      // Field DocValues are different than requested type
      return null;
    }

    return fi;
  }
  
  @Override
  public final NumericDocValues getNumericDocValues(String field) throws IOException {
    ensureOpen();
    Map<String,Object> dvFields = docValuesLocal.get();

    Object previous = dvFields.get(field);
    if (previous != null && previous instanceof NumericDocValues) {
      return (NumericDocValues) previous;
    } else {
      FieldInfo fi = getDVField(field, DocValuesType.NUMERIC);
      if (fi == null) {
        return null;
      }
      NumericDocValues dv = getDocValuesReader().getNumeric(fi);
      dvFields.put(field, dv);
      return dv;
    }
  }
  
  @Override
  public final Bits getDocsWithField(String field) throws IOException {
    ensureOpen();
    Map<String,Bits> dvFields = docsWithFieldLocal.get();

    Bits previous = dvFields.get(field);
    if (previous != null) {
      return previous;
    } else {
      FieldInfo fi = getFieldInfos().fieldInfo(field);
      if (fi == null) {
        // Field does not exist
        return null;
      }
      if (fi.getDocValuesType() == DocValuesType.NONE) {
        // Field was not indexed with doc values
        return null;
      }
      Bits dv = getDocValuesReader().getDocsWithField(fi);
      dvFields.put(field, dv);
      return dv;
    }
  }

  @Override
  public final BinaryDocValues getBinaryDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getDVField(field, DocValuesType.BINARY);
    if (fi == null) {
      return null;
    }

    Map<String,Object> dvFields = docValuesLocal.get();

    BinaryDocValues dvs = (BinaryDocValues) dvFields.get(field);
    if (dvs == null) {
      dvs = getDocValuesReader().getBinary(fi);
      dvFields.put(field, dvs);
    }

    return dvs;
  }

  @Override
  public final SortedDocValues getSortedDocValues(String field) throws IOException {
    ensureOpen();
    Map<String,Object> dvFields = docValuesLocal.get();
    
    Object previous = dvFields.get(field);
    if (previous != null && previous instanceof SortedDocValues) {
      return (SortedDocValues) previous;
    } else {
      FieldInfo fi = getDVField(field, DocValuesType.SORTED);
      if (fi == null) {
        return null;
      }
      SortedDocValues dv = getDocValuesReader().getSorted(fi);
      dvFields.put(field, dv);
      return dv;
    }
  }
  
  @Override
  public final SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    ensureOpen();
    Map<String,Object> dvFields = docValuesLocal.get();

    Object previous = dvFields.get(field);
    if (previous != null && previous instanceof SortedNumericDocValues) {
      return (SortedNumericDocValues) previous;
    } else {
      FieldInfo fi = getDVField(field, DocValuesType.SORTED_NUMERIC);
      if (fi == null) {
        return null;
      }
      SortedNumericDocValues dv = getDocValuesReader().getSortedNumeric(fi);
      dvFields.put(field, dv);
      return dv;
    }
  }

  @Override
  public final SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    ensureOpen();
    Map<String,Object> dvFields = docValuesLocal.get();
    
    Object previous = dvFields.get(field);
    if (previous != null && previous instanceof SortedSetDocValues) {
      return (SortedSetDocValues) previous;
    } else {
      FieldInfo fi = getDVField(field, DocValuesType.SORTED_SET);
      if (fi == null) {
        return null;
      }
      SortedSetDocValues dv = getDocValuesReader().getSortedSet(fi);
      dvFields.put(field, dv);
      return dv;
    }
  }
  
  @Override
  public final NumericDocValues getNormValues(String field) throws IOException {
    ensureOpen();
    Map<String,Object> normFields = normsLocal.get();

    NumericDocValues norms = (NumericDocValues) normFields.get(field);
    if (norms != null) {
      return norms;
    } else {
      FieldInfo fi = getFieldInfos().fieldInfo(field);
      if (fi == null || !fi.hasNorms()) {
        // Field does not exist or does not index norms
        return null;
      }
      norms = getNormsReader().getNorms(fi);
      normFields.put(field, norms);
      return norms;
    }
  }

  @Override
  protected void doClose() throws IOException {
    IOUtils.close(docValuesLocal, docsWithFieldLocal, normsLocal);
  }

  // nocommit
  @Override
  public void cutover() {}
  
  // nocommit: this is all cutover API
  // --------------------------------------------------------------------------
  public static LeafReader2 hack(final LeafReader reader) throws IOException {
    if (reader instanceof LeafReader2) {
      return (LeafReader2) reader;
    } else {
      // simulate it, over the old leafReader api      
      return new LeafReader2() {

        @Override
        protected TermVectorsReader getTermVectorsReader() {
          ensureOpen();
          return readerToTermVectorsReader(reader);
        }

        @Override
        protected StoredFieldsReader getFieldsReader() {
          ensureOpen();
          return readerToStoredFieldsReader(reader);
        }

        @Override
        protected NormsProducer getNormsReader() {
          ensureOpen();
          return readerToNormsProducer(reader);
        }

        @Override
        protected DocValuesProducer getDocValuesReader() {
          ensureOpen();
          return readerToDocValuesProducer(reader);
        }

        @Override
        protected FieldsProducer getPostingsReader() {
          ensureOpen();
          try {
            return readerToFieldsProducer(reader);
          } catch (IOException bogus) {
            throw new AssertionError(bogus);
          }
        }

        @Override
        public void addCoreClosedListener(CoreClosedListener listener) {
          reader.addCoreClosedListener(listener);
        }

        @Override
        public void removeCoreClosedListener(CoreClosedListener listener) {
          reader.removeCoreClosedListener(listener);
        }

        @Override
        public FieldInfos getFieldInfos() {
          return reader.getFieldInfos();
        }

        @Override
        public Bits getLiveDocs() {
          return reader.getLiveDocs();
        }

        @Override
        public int numDocs() {
          return reader.numDocs();
        }

        @Override
        public int maxDoc() {
          return reader.maxDoc();
        }

        @Override
        public Object getCoreCacheKey() {
          return reader.getCoreCacheKey();
        }
      };
    }
  }
  
  private static NormsProducer readerToNormsProducer(final LeafReader reader) {
    return new NormsProducer() {

      @Override
      public NumericDocValues getNorms(FieldInfo field) throws IOException {
        return reader.getNormValues(field.name);
      }

      @Override
      public void checkIntegrity() throws IOException {
        // We already checkIntegrity the entire reader up front in SegmentMerger
      }

      @Override
      public void close() {
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }

      @Override
      public Iterable<? extends Accountable> getChildResources() {
        return Collections.emptyList();
      }
    };
  }

  private static DocValuesProducer readerToDocValuesProducer(final LeafReader reader) {
    return new DocValuesProducer() {

      @Override
      public NumericDocValues getNumeric(FieldInfo field) throws IOException {  
        return reader.getNumericDocValues(field.name);
      }

      @Override
      public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return reader.getBinaryDocValues(field.name);
      }

      @Override
      public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return reader.getSortedDocValues(field.name);
      }

      @Override
      public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return reader.getSortedNumericDocValues(field.name);
      }

      @Override
      public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return reader.getSortedSetDocValues(field.name);
      }

      @Override
      public Bits getDocsWithField(FieldInfo field) throws IOException {
        return reader.getDocsWithField(field.name);
      }

      @Override
      public void checkIntegrity() throws IOException {
        // We already checkIntegrity the entire reader up front in SegmentMerger
      }

      @Override
      public void close() {
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }

      @Override
      public Iterable<? extends Accountable> getChildResources() {
        return Collections.emptyList();
      }
    };
  }

  private static StoredFieldsReader readerToStoredFieldsReader(final LeafReader reader) {
    return new StoredFieldsReader() {
      @Override
      public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {
        reader.document(docID, visitor);
      }

      @Override
      public StoredFieldsReader clone() {
        return readerToStoredFieldsReader(reader);
      }

      @Override
      public void checkIntegrity() throws IOException {
        // We already checkIntegrity the entire reader up front in SegmentMerger
      }

      @Override
      public void close() {
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }

      @Override
      public Iterable<? extends Accountable> getChildResources() {
        return Collections.emptyList();
      }
    };
  }

  private static TermVectorsReader readerToTermVectorsReader(final LeafReader reader) {
    return new TermVectorsReader() {
      @Override
      public Fields get(int docID) throws IOException {
        return reader.getTermVectors(docID);
      }

      @Override
      public TermVectorsReader clone() {
        return readerToTermVectorsReader(reader);
      }

      @Override
      public void checkIntegrity() throws IOException {
        // We already checkIntegrity the entire reader up front in SegmentMerger
      }

      @Override
      public void close() {
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }

      @Override
      public Iterable<? extends Accountable> getChildResources() {
        return Collections.emptyList();
      }
    };
  }

  private static FieldsProducer readerToFieldsProducer(final LeafReader reader) throws IOException {
    final Fields fields = reader.fields();
    return new FieldsProducer() {
      @Override
      public Iterator<String> iterator() {
        return fields.iterator();
      }

      @Override
      public Terms terms(String field) throws IOException {
        return fields.terms(field);
      }

      @Override
      public int size() {
        return fields.size();
      }

      @Override
      public void checkIntegrity() throws IOException {
        // We already checkIntegrity the entire reader up front in SegmentMerger
      }

      @Override
      public void close() {
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }

      @Override
      public Iterable<? extends Accountable> getChildResources() {
        return Collections.emptyList();
      }
    };
  }
}
