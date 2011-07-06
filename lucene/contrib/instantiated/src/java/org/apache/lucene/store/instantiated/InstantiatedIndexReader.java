package org.apache.lucene.store.instantiated;

/**
 * Copyright 2006 The Apache Software Foundation
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Comparator;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.*;
import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitVector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Bits;

/**
 * An InstantiatedIndexReader is not a snapshot in time, it is completely in
 * sync with the latest commit to the store!
 * <p>
 * Consider using InstantiatedIndex as if it was immutable.
 */
public class InstantiatedIndexReader extends IndexReader {

  private final InstantiatedIndex index;
  private ReaderContext context = new AtomicReaderContext(this);


  public InstantiatedIndexReader(InstantiatedIndex index) {
    super();
    this.index = index;
    readerFinishedListeners = Collections.synchronizedSet(new HashSet<ReaderFinishedListener>());
  }

  /**
   * @return always true.
   */
  @Override
  public boolean isOptimized() {
    return true;
  }

  /**
   * An InstantiatedIndexReader is not a snapshot in time, it is completely in
   * sync with the latest commit to the store!
   * 
   * @return output from {@link InstantiatedIndex#getVersion()} in associated instantiated index.
   */
  @Override
  public long getVersion() {
    return index.getVersion();
  }

  @Override
  public Directory directory() {
    throw new UnsupportedOperationException();
  }

  /**
   * An InstantiatedIndexReader is always current!
   * 
   * Check whether this IndexReader is still using the current (i.e., most
   * recently committed) version of the index. If a writer has committed any
   * changes to the index since this reader was opened, this will return
   * <code>false</code>, in which case you must open a new IndexReader in
   * order to see the changes. See the description of the <a
   * href="IndexWriter.html#autoCommit"><code>autoCommit</code></a> flag
   * which controls when the {@link IndexWriter} actually commits changes to the
   * index.
   * 
   * @return always true
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @throws UnsupportedOperationException unless overridden in subclass
   */
  @Override
  public boolean isCurrent() throws IOException {
    return true;
  }

  public InstantiatedIndex getIndex() {
    return index;
  }

  @Override
  public Bits getLiveDocs() {
    return new Bits() {
      public boolean get(int n) {
        return !(index.getDeletedDocuments() != null && index.getDeletedDocuments().get(n))
                   && !(uncommittedDeletedDocuments != null && uncommittedDeletedDocuments.get(n));
      }

      public int length() {
        return maxDoc();
      }
    };
  }

  private BitVector uncommittedDeletedDocuments;

  private Map<String,List<NormUpdate>> uncommittedNormsByFieldNameAndDocumentNumber = null;

  private class NormUpdate {
    private int doc;
    private byte value;

    public NormUpdate(int doc, byte value) {
      this.doc = doc;
      this.value = value;
    }
  }

  @Override
  public int numDocs() {
    // todo i suppose this value could be cached, but array#length and bitvector#count is fast.
    int numDocs = getIndex().getDocumentsByNumber().length;
    if (uncommittedDeletedDocuments != null) {
      numDocs -= uncommittedDeletedDocuments.count();
    }
    if (index.getDeletedDocuments() != null) {
      numDocs -= index.getDeletedDocuments().count();
    }
    return numDocs;
  }

  @Override
  public int maxDoc() {
    return getIndex().getDocumentsByNumber().length;
  }

  @Override
  public boolean hasDeletions() {
    return index.getDeletedDocuments() != null || uncommittedDeletedDocuments != null;
  }

  @Override
  protected void doDelete(int docNum) throws IOException {

    // dont delete if already deleted
    if ((index.getDeletedDocuments() != null && index.getDeletedDocuments().get(docNum))
        || (uncommittedDeletedDocuments != null && uncommittedDeletedDocuments.get(docNum))) {
      return;
    }

    if (uncommittedDeletedDocuments == null) {
      uncommittedDeletedDocuments = new BitVector(maxDoc());
    }

    uncommittedDeletedDocuments.set(docNum);
  }

  @Override
  protected void doUndeleteAll() throws IOException {
    // todo: read/write lock
    uncommittedDeletedDocuments = null;
    // todo: read/write unlock
  }

  @Override
  protected void doCommit(Map<String,String> commitUserData) throws IOException {
    // todo: read/write lock

    // 1. update norms
    if (uncommittedNormsByFieldNameAndDocumentNumber != null) {
      for (Map.Entry<String,List<NormUpdate>> e : uncommittedNormsByFieldNameAndDocumentNumber.entrySet()) {
        byte[] norms = getIndex().getNormsByFieldNameAndDocumentNumber().get(e.getKey());
        for (NormUpdate normUpdate : e.getValue()) {
          norms[normUpdate.doc] = normUpdate.value;
        }
      }
      uncommittedNormsByFieldNameAndDocumentNumber = null;
    }

    // 2. remove deleted documents
    if (uncommittedDeletedDocuments != null) {
      if (index.getDeletedDocuments() == null) {
        index.setDeletedDocuments(uncommittedDeletedDocuments);
      } else {
        for (int d = 0; d< uncommittedDeletedDocuments.size(); d++) {
          if (uncommittedDeletedDocuments.get(d)) {
            index.getDeletedDocuments().set(d);
          }
        }
      }
      uncommittedDeletedDocuments = null;
    }

    // todo unlock read/writelock
  }

  @Override
  protected void doClose() throws IOException {
    // ignored
    // todo perhaps release all associated instances?
  }

  @Override
  public Collection<String> getFieldNames(FieldOption fieldOption) {
    Set<String> fieldSet = new HashSet<String>();
    for (FieldSetting fi : index.getFieldSettings().values()) {
      if (fieldOption == IndexReader.FieldOption.ALL) {
        fieldSet.add(fi.fieldName);
      } else if (!fi.indexed && fieldOption == IndexReader.FieldOption.UNINDEXED) {
        fieldSet.add(fi.fieldName);
      } else if (fi.storePayloads && fieldOption == IndexReader.FieldOption.STORES_PAYLOADS) {
        fieldSet.add(fi.fieldName);
      } else if (fi.indexed && fieldOption == IndexReader.FieldOption.INDEXED) {
        fieldSet.add(fi.fieldName);
      } else if (fi.indexed && fi.storeTermVector == false && fieldOption == IndexReader.FieldOption.INDEXED_NO_TERMVECTOR) {
        fieldSet.add(fi.fieldName);
      } else if (fi.storeTermVector == true && fi.storePositionWithTermVector == false && fi.storeOffsetWithTermVector == false
          && fieldOption == IndexReader.FieldOption.TERMVECTOR) {
        fieldSet.add(fi.fieldName);
      } else if (fi.indexed && fi.storeTermVector && fieldOption == IndexReader.FieldOption.INDEXED_WITH_TERMVECTOR) {
        fieldSet.add(fi.fieldName);
      } else if (fi.storePositionWithTermVector && fi.storeOffsetWithTermVector == false
          && fieldOption == IndexReader.FieldOption.TERMVECTOR_WITH_POSITION) {
        fieldSet.add(fi.fieldName);
      } else if (fi.storeOffsetWithTermVector && fi.storePositionWithTermVector == false
          && fieldOption == IndexReader.FieldOption.TERMVECTOR_WITH_OFFSET) {
        fieldSet.add(fi.fieldName);
      } else if ((fi.storeOffsetWithTermVector && fi.storePositionWithTermVector)
          && fieldOption == IndexReader.FieldOption.TERMVECTOR_WITH_POSITION_OFFSET) {
        fieldSet.add(fi.fieldName);
      } 
    }
    return fieldSet;
  }

  /**
   * Return the {@link org.apache.lucene.document.Document} at the <code>n</code><sup>th</sup>
   * position.
     <p>
   * <b>Warning!</b>
   * The resulting document is the actual stored document instance
   * and not a deserialized clone as retuned by an IndexReader
   * over a {@link org.apache.lucene.store.Directory}.
   * I.e., if you need to touch the document, clone it first!
   * <p>
   * This can also be seen as a feature for live changes of stored values,
   * but be careful! Adding a field with an name unknown to the index
   * or to a field with previously no stored values will make
   * {@link org.apache.lucene.store.instantiated.InstantiatedIndexReader#getFieldNames(org.apache.lucene.index.IndexReader.FieldOption)}
   * out of sync, causing problems for instance when merging the
   * instantiated index to another index.
     <p>
   * This implementation ignores the field selector! All stored fields are always returned!
   * <p>
   *
   * @param n document number
   * @param fieldSelector ignored
   * @return The stored fields of the {@link org.apache.lucene.document.Document} at the nth position
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * 
   * @see org.apache.lucene.document.Fieldable
   * @see org.apache.lucene.document.FieldSelector
   * @see org.apache.lucene.document.SetBasedFieldSelector
   * @see org.apache.lucene.document.LoadFirstFieldSelector
   */
  @Override
  public Document document(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
    return document(n);
  }

  /**
   * Returns the stored fields of the <code>n</code><sup>th</sup>
   * <code>Document</code> in this index.
   * <p>
   * <b>Warning!</b>
   * The resulting document is the actual stored document instance
   * and not a deserialized clone as retuned by an IndexReader
   * over a {@link org.apache.lucene.store.Directory}.
   * I.e., if you need to touch the document, clone it first!
   * <p>
   * This can also be seen as a feature for live changes of stored values,
   * but be careful! Adding a field with an name unknown to the index
   * or to a field with previously no stored values will make
   * {@link org.apache.lucene.store.instantiated.InstantiatedIndexReader#getFieldNames(org.apache.lucene.index.IndexReader.FieldOption)}
   * out of sync, causing problems for instance when merging the
   * instantiated index to another index.
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */

  @Override
  public Document document(int n) throws IOException {
    return getIndex().getDocumentsByNumber()[n].getDocument();
  }

  /**
   * never ever touch these values. it is the true values, unless norms have
   * been touched.
   */
  @Override
  public byte[] norms(String field) throws IOException {
    byte[] norms = getIndex().getNormsByFieldNameAndDocumentNumber().get(field);
    if (norms == null) {
      return new byte[0]; // todo a static final zero length attribute?
    }
    if (uncommittedNormsByFieldNameAndDocumentNumber != null) {
      norms = norms.clone();
      List<NormUpdate> updated = uncommittedNormsByFieldNameAndDocumentNumber.get(field);
      if (updated != null) {
        for (NormUpdate normUpdate : updated) {
          norms[normUpdate.doc] = normUpdate.value;
        }
      }
    }
    return norms;
  }

  @Override
  protected void doSetNorm(int doc, String field, byte value) throws IOException {
    if (uncommittedNormsByFieldNameAndDocumentNumber == null) {
      uncommittedNormsByFieldNameAndDocumentNumber = new HashMap<String,List<NormUpdate>>(getIndex().getNormsByFieldNameAndDocumentNumber().size());
    }
    List<NormUpdate> list = uncommittedNormsByFieldNameAndDocumentNumber.get(field);
    if (list == null) {
      list = new LinkedList<NormUpdate>();
      uncommittedNormsByFieldNameAndDocumentNumber.put(field, list);
    }
    list.add(new NormUpdate(doc, value));
  }

  @Override
  public int docFreq(Term t) throws IOException {
    InstantiatedTerm term = getIndex().findTerm(t);
    if (term == null) {
      return 0;
    } else {
      return term.getAssociatedDocuments().length;
    }
  }

  @Override
  public Fields fields() {
    if (getIndex().getOrderedTerms().length == 0) {
      return null;
    }

    return new Fields() {
      @Override
      public FieldsEnum iterator() {
        final InstantiatedTerm[] orderedTerms = getIndex().getOrderedTerms();

        return new FieldsEnum() {
          int upto = -1;
          String currentField;

          @Override
          public String next() {
            do {
              upto++;
              if (upto >= orderedTerms.length) {
                return null;
              }
            } while(orderedTerms[upto].field() == currentField);
            
            currentField = orderedTerms[upto].field();
            return currentField;
          }

          @Override
          public TermsEnum terms() {
            return new InstantiatedTermsEnum(orderedTerms, upto, currentField);
          }
        };
      }

      @Override
      public Terms terms(final String field) {
        final InstantiatedTerm[] orderedTerms = getIndex().getOrderedTerms();
        int i = Arrays.binarySearch(orderedTerms, new Term(field), InstantiatedTerm.termComparator);
        if (i < 0) {
          i = -i - 1;
        }
        if (i >= orderedTerms.length || !orderedTerms[i].field().equals(field)) {
          // field does not exist
          return null;
        }
        final int startLoc = i;

        // TODO: heavy to do this here; would be better to
        // do it up front & cache
        long sum = 0;
        int upto = i;
        while(upto < orderedTerms.length && orderedTerms[i].field() == field) {
          sum += orderedTerms[i].getTotalTermFreq();
          upto++;
        }
        final long sumTotalTermFreq = sum;

        return new Terms() {
          @Override 
          public TermsEnum iterator() {
            return new InstantiatedTermsEnum(orderedTerms, startLoc, field);
          }

          @Override
          public long getSumTotalTermFreq() {
            return sumTotalTermFreq;
          }

          @Override
          public Comparator<BytesRef> getComparator() {
            return BytesRef.getUTF8SortedAsUnicodeComparator();
          }
        };
      }
    };
  }
  
  @Override
  public ReaderContext getTopReaderContext() {
    return context;
  }

  @Override
  public TermFreqVector[] getTermFreqVectors(int docNumber) throws IOException {
    InstantiatedDocument doc = getIndex().getDocumentsByNumber()[docNumber];
    if (doc.getVectorSpace() == null) {
      return null;
    }
    TermFreqVector[] ret = new TermFreqVector[doc.getVectorSpace().size()];
    Iterator<String> it = doc.getVectorSpace().keySet().iterator();
    for (int i = 0; i < ret.length; i++) {
      ret[i] = new InstantiatedTermPositionVector(getIndex().getDocumentsByNumber()[docNumber], it.next());
    }
    return ret;
  }

  @Override
  public TermFreqVector getTermFreqVector(int docNumber, String field) throws IOException {
    InstantiatedDocument doc = getIndex().getDocumentsByNumber()[docNumber];
    if (doc.getVectorSpace() == null || doc.getVectorSpace().get(field) == null) {
      return null;
    } else {
      return new InstantiatedTermPositionVector(doc, field);
    }
  }

  @Override
  public void getTermFreqVector(int docNumber, String field, TermVectorMapper mapper) throws IOException {
    InstantiatedDocument doc = getIndex().getDocumentsByNumber()[docNumber];
    if (doc.getVectorSpace() != null && doc.getVectorSpace().get(field) == null) {
      List<InstantiatedTermDocumentInformation> tv = doc.getVectorSpace().get(field);
      mapper.setExpectations(field, tv.size(), true, true);
      for (InstantiatedTermDocumentInformation tdi : tv) {
        mapper.map(tdi.getTerm().getTerm().bytes(), tdi.getTermPositions().length, tdi.getTermOffsets(), tdi.getTermPositions());
      }
    }
  }

  @Override
  public void getTermFreqVector(int docNumber, TermVectorMapper mapper) throws IOException {
    InstantiatedDocument doc = getIndex().getDocumentsByNumber()[docNumber];
    for (Map.Entry<String, List<InstantiatedTermDocumentInformation>> e : doc.getVectorSpace().entrySet()) {
      mapper.setExpectations(e.getKey(), e.getValue().size(), true, true);
      for (InstantiatedTermDocumentInformation tdi : e.getValue()) {
        mapper.map(tdi.getTerm().getTerm().bytes(), tdi.getTermPositions().length, tdi.getTermOffsets(), tdi.getTermPositions());
      }
    }
  }

  @Override
  public PerDocValues perDocValues() throws IOException {
    return null;
  }
}
