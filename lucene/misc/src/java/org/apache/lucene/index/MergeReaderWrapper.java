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
package org.apache.lucene.index;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.util.Bits;

/** this is a hack to make SortingMP fast! */
class MergeReaderWrapper extends LeafReader {
  final SegmentReader in;
  final FieldsProducer fields;
  final NormsProducer norms;
  final DocValuesProducer docValues;
  final StoredFieldsReader store;
  final TermVectorsReader vectors;
  
  MergeReaderWrapper(SegmentReader in) throws IOException {
    this.in = in;
    
    FieldsProducer fields = in.getPostingsReader();
    if (fields != null) {
      fields = fields.getMergeInstance();
    }
    this.fields = fields;
    
    NormsProducer norms = in.getNormsReader();
    if (norms != null) {
      norms = norms.getMergeInstance();
    }
    this.norms = norms;
    
    DocValuesProducer docValues = in.getDocValuesReader();
    if (docValues != null) {
      docValues = docValues.getMergeInstance();
    }
    this.docValues = docValues;
    
    StoredFieldsReader store = in.getFieldsReader();
    if (store != null) {
      store = store.getMergeInstance();
    }
    this.store = store;
    
    TermVectorsReader vectors = in.getTermVectorsReader();
    if (vectors != null) {
      vectors = vectors.getMergeInstance();
    }
    this.vectors = vectors;
  }

  @Override
  public void addCoreClosedListener(CoreClosedListener listener) {
    in.addCoreClosedListener(listener);
  }

  @Override
  public void removeCoreClosedListener(CoreClosedListener listener) {
    in.removeCoreClosedListener(listener);
  }

  @Override
  public Fields fields() throws IOException {
    return fields;
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() != DocValuesType.NUMERIC) {
      // Field was not indexed with doc values
      return null;
    }
    return docValues.getNumeric(fi);
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() != DocValuesType.BINARY) {
      // Field was not indexed with doc values
      return null;
    }
    return docValues.getBinary(fi);
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() != DocValuesType.SORTED) {
      // Field was not indexed with doc values
      return null;
    }
    return docValues.getSorted(fi);
  }

  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() != DocValuesType.SORTED_NUMERIC) {
      // Field was not indexed with doc values
      return null;
    }
    return docValues.getSortedNumeric(fi);
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() != DocValuesType.SORTED_SET) {
      // Field was not indexed with doc values
      return null;
    }
    return docValues.getSortedSet(fi);
  }

  @Override
  public Bits getDocsWithField(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() == DocValuesType.NONE) {
      // Field was not indexed with doc values
      return null;
    }
    return docValues.getDocsWithField(fi);
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null || !fi.hasNorms()) {
      // Field does not exist or does not index norms
      return null;
    }
    return norms.getNorms(fi);
  }

  @Override
  public FieldInfos getFieldInfos() {
    return in.getFieldInfos();
  }

  @Override
  public Bits getLiveDocs() {
    return in.getLiveDocs();
  }

  @Override
  public void checkIntegrity() throws IOException {
    in.checkIntegrity();
  }

  @Override
  public Fields getTermVectors(int docID) throws IOException {
    ensureOpen();
    checkBounds(docID);
    if (vectors == null) {
      return null;
    }
    return vectors.get(docID);
  }

  @Override
  public int numDocs() {
    return in.numDocs();
  }

  @Override
  public int maxDoc() {
    return in.maxDoc();
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    ensureOpen();
    checkBounds(docID);
    store.visitDocument(docID, visitor);
  }

  @Override
  protected void doClose() throws IOException {
    in.close();
  }

  @Override
  public Object getCoreCacheKey() {
    return in.getCoreCacheKey();
  }

  @Override
  public Object getCombinedCoreAndDeletesKey() {
    return in.getCombinedCoreAndDeletesKey();
  }
  
  private void checkBounds(int docID) {
    if (docID < 0 || docID >= maxDoc()) {       
      throw new IndexOutOfBoundsException("docID must be >= 0 and < maxDoc=" + maxDoc() + " (got docID=" + docID + ")");
    }
  }

  @Override
  public String toString() {
    return "MergeReaderWrapper(" + in + ")";
  }
}
