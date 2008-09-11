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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.store.Directory;

/**
 * An InstantiatedIndexReader is not a snapshot in time, it is completely in
 * sync with the latest commit to the store!
 * <p>
 * Consider using InstantiatedIndex as if it was immutable.
 */
public class InstantiatedIndexReader extends IndexReader {

  private final InstantiatedIndex index;

  public InstantiatedIndexReader(InstantiatedIndex index) {
    super();
    this.index = index;
  }

  /**
   * @return always true.
   */
  public boolean isOptimized() {
    return true;
  }

  /**
   * An InstantiatedIndexReader is not a snapshot in time, it is completely in
   * sync with the latest commit to the store!
   * 
   * @return output from {@link InstantiatedIndex#getVersion()} in associated instantiated index.
   */
  public long getVersion() {
    return index.getVersion();
  }

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
  public boolean isCurrent() throws IOException {
    return true;
  }

  public InstantiatedIndex getIndex() {
    return index;
  }

  private Set<InstantiatedDocument> deletedDocuments = new HashSet<InstantiatedDocument>();
  private Set<Integer> deletedDocumentNumbers = new HashSet<Integer>();
  private Map<String,List<NormUpdate>> updatedNormsByFieldNameAndDocumentNumber = null;

  private class NormUpdate {
    private int doc;
    private byte value;

    public NormUpdate(int doc, byte value) {
      this.doc = doc;
      this.value = value;
    }
  }

  public int numDocs() {
    return getIndex().getDocumentsByNumber().length - index.getDeletedDocuments().size() - deletedDocuments.size();
  }

  public int maxDoc() {
    return getIndex().getDocumentsByNumber().length;
  }

  public boolean isDeleted(int n) {
    return getIndex().getDeletedDocuments().contains(n) || deletedDocumentNumbers.contains(n);
  }

  public boolean hasDeletions() {
    return getIndex().getDeletedDocuments().size() > 0 || deletedDocumentNumbers.size() > 0;
  }

  protected void doDelete(int docNum) throws IOException {
    if (!getIndex().getDeletedDocuments().contains(docNum)) {
      if (deletedDocumentNumbers.add(docNum)) {
        deletedDocuments.add(getIndex().getDocumentsByNumber()[docNum]);
      }
    }
  }

  protected void doUndeleteAll() throws IOException {
    deletedDocumentNumbers.clear();
    deletedDocuments.clear();
  }

  protected void doCommit() throws IOException {
    // todo: read/write lock

    boolean updated = false;

    // 1. update norms
    if (updatedNormsByFieldNameAndDocumentNumber != null) {
      for (Map.Entry<String,List<NormUpdate>> e : updatedNormsByFieldNameAndDocumentNumber.entrySet()) {
        byte[] norms = getIndex().getNormsByFieldNameAndDocumentNumber().get(e.getKey());
        for (NormUpdate normUpdate : e.getValue()) {
          norms[normUpdate.doc] = normUpdate.value;
        }
      }
      updatedNormsByFieldNameAndDocumentNumber = null;

      updated = true;
    }

    // 2. remove deleted documents
    if (deletedDocumentNumbers.size() > 0) {
      for (Integer doc : deletedDocumentNumbers) {
        getIndex().getDeletedDocuments().add(doc);
      }
      deletedDocumentNumbers.clear();
      deletedDocuments.clear();

      updated = true;

    }

    // todo unlock read/writelock
  }

  protected void doClose() throws IOException {
    // ignored
    // todo perhaps release all associated instances?
  }

  public Collection getFieldNames(FieldOption fieldOption) {
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
   * This can also be seen as a feature for live canges of stored values,
   * but be carful! Adding a field with an name unknown to the index
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
   * This can also be seen as a feature for live canges of stored values,
   * but be carful! Adding a field with an name unknown to the index
   * or to a field with previously no stored values will make
   * {@link org.apache.lucene.store.instantiated.InstantiatedIndexReader#getFieldNames(org.apache.lucene.index.IndexReader.FieldOption)}
   * out of sync, causing problems for instance when merging the
   * instantiated index to another index.
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */

  public Document document(int n) throws IOException {
    return isDeleted(n) ? null : getIndex().getDocumentsByNumber()[n].getDocument();
  }

  /**
   * never ever touch these values. it is the true values, unless norms have
   * been touched.
   */
  public byte[] norms(String field) throws IOException {
    byte[] norms = getIndex().getNormsByFieldNameAndDocumentNumber().get(field);
    if (updatedNormsByFieldNameAndDocumentNumber != null) {
      norms = norms.clone();
      List<NormUpdate> updated = updatedNormsByFieldNameAndDocumentNumber.get(field);
      if (updated != null) {
        for (NormUpdate normUpdate : updated) {
          norms[normUpdate.doc] = normUpdate.value;
        }
      }
    }
    return norms;
  }

  public void norms(String field, byte[] bytes, int offset) throws IOException {
    byte[] norms = getIndex().getNormsByFieldNameAndDocumentNumber().get(field);
    System.arraycopy(norms, 0, bytes, offset, norms.length);
  }

  protected void doSetNorm(int doc, String field, byte value) throws IOException {
    if (updatedNormsByFieldNameAndDocumentNumber == null) {
      updatedNormsByFieldNameAndDocumentNumber = new HashMap<String,List<NormUpdate>>(getIndex().getNormsByFieldNameAndDocumentNumber().size());
    }
    List<NormUpdate> list = updatedNormsByFieldNameAndDocumentNumber.get(field);
    if (list == null) {
      list = new LinkedList<NormUpdate>();
      updatedNormsByFieldNameAndDocumentNumber.put(field, list);
    }
    list.add(new NormUpdate(doc, value));
  }

  public int docFreq(Term t) throws IOException {
    InstantiatedTerm term = getIndex().findTerm(t);
    if (term == null) {
      return 0;
    } else {
      return term.getAssociatedDocuments().length;
    }
  }

  public TermEnum terms() throws IOException {
    return new InstantiatedTermEnum(this);
  }

  public TermEnum terms(Term t) throws IOException {
    InstantiatedTerm it = getIndex().findTerm(t);
    if (it != null) {
      return new InstantiatedTermEnum(this, it.getTermIndex());
    } else {
      int startPos = Arrays.binarySearch(index.getOrderedTerms(), t, InstantiatedTerm.termComparator);
      if (startPos < 0) {
        startPos = -1 - startPos;
      }
      return new InstantiatedTermEnum(this, startPos);
    }
  }

  public TermDocs termDocs() throws IOException {
    return new InstantiatedTermDocs(this);
  }

  public TermPositions termPositions() throws IOException {
    return new InstantiatedTermPositions(this);
  }

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

  public TermFreqVector getTermFreqVector(int docNumber, String field) throws IOException {
    InstantiatedDocument doc = getIndex().getDocumentsByNumber()[docNumber];
    if (doc.getVectorSpace() == null || doc.getVectorSpace().get(field) == null) {
      return null;
    } else {
      return new InstantiatedTermPositionVector(doc, field);
    }
  }

  public void getTermFreqVector(int docNumber, String field, TermVectorMapper mapper) throws IOException {
    InstantiatedDocument doc = getIndex().getDocumentsByNumber()[docNumber];
    if (doc.getVectorSpace() != null && doc.getVectorSpace().get(field) == null) {
      List<InstantiatedTermDocumentInformation> tv = doc.getVectorSpace().get(field);
      mapper.setExpectations(field, tv.size(), true, true);
      for (InstantiatedTermDocumentInformation tdi : tv) {
        mapper.map(tdi.getTerm().text(), tdi.getTermPositions().length, tdi.getTermOffsets(), tdi.getTermPositions());
      }
    }
  }

  public void getTermFreqVector(int docNumber, TermVectorMapper mapper) throws IOException {
    InstantiatedDocument doc = getIndex().getDocumentsByNumber()[docNumber];
    for (Map.Entry<String,List<InstantiatedTermDocumentInformation>> e : doc.getVectorSpace().entrySet()) {
      mapper.setExpectations(e.getKey(), e.getValue().size(), true, true);
      for (InstantiatedTermDocumentInformation tdi : e.getValue()) {
        mapper.map(tdi.getTerm().text(), tdi.getTermPositions().length, tdi.getTermOffsets(), tdi.getTermPositions());
      }
    }
  }
}
