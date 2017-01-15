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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;

/**
 * LeafReader implemented by codec APIs.
 */
public abstract class CodecReader extends LeafReader implements Accountable {
  
  /** Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.) */
  protected CodecReader() {}
  
  /** 
   * Expert: retrieve thread-private StoredFieldsReader
   * @lucene.internal 
   */
  public abstract StoredFieldsReader getFieldsReader();
  
  /** 
   * Expert: retrieve thread-private TermVectorsReader
   * @lucene.internal 
   */
  public abstract TermVectorsReader getTermVectorsReader();
  
  /** 
   * Expert: retrieve underlying NormsProducer
   * @lucene.internal 
   */
  public abstract NormsProducer getNormsReader();
  
  /** 
   * Expert: retrieve underlying DocValuesProducer
   * @lucene.internal 
   */
  public abstract DocValuesProducer getDocValuesReader();
  
  /**
   * Expert: retrieve underlying FieldsProducer
   * @lucene.internal
   */
  public abstract FieldsProducer getPostingsReader();

  /**
   * Expert: retrieve underlying PointsReader
   * @lucene.internal
   */
  public abstract PointsReader getPointsReader();
  
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
  
  private void checkBounds(int docID) {
    if (docID < 0 || docID >= maxDoc()) {       
      throw new IndexOutOfBoundsException("docID must be >= 0 and < maxDoc=" + maxDoc() + " (got docID=" + docID + ")");
    }
  }
  
  @Override
  public final Fields fields() {
    return getPostingsReader();
  }
  
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
    FieldInfo fi = getDVField(field, DocValuesType.NUMERIC);
    if (fi == null) {
      return null;
    }
    return getDocValuesReader().getNumeric(fi);
  }

  @Override
  public final BinaryDocValues getBinaryDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getDVField(field, DocValuesType.BINARY);
    if (fi == null) {
      return null;
    }
    return getDocValuesReader().getBinary(fi);
  }

  @Override
  public final SortedDocValues getSortedDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getDVField(field, DocValuesType.SORTED);
    if (fi == null) {
      return null;
    }
    return getDocValuesReader().getSorted(fi);
  }
  
  @Override
  public final SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    ensureOpen();

    FieldInfo fi = getDVField(field, DocValuesType.SORTED_NUMERIC);
    if (fi == null) {
      return null;
    }
    return getDocValuesReader().getSortedNumeric(fi);
  }

  @Override
  public final SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getDVField(field, DocValuesType.SORTED_SET);
    if (fi == null) {
      return null;
    }
    return getDocValuesReader().getSortedSet(fi);
  }
  
  @Override
  public final NumericDocValues getNormValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null || fi.hasNorms() == false) {
      // Field does not exist or does not index norms
      return null;
    }

    return getNormsReader().getNorms(fi);
  }

  @Override
  public final PointValues getPointValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null || fi.getPointDimensionCount() == 0) {
      // Field does not exist or does not index points
      return null;
    }

    return getPointsReader().getValues(field);
  }

  @Override
  protected void doClose() throws IOException {
  }
  
  @Override
  public long ramBytesUsed() {
    ensureOpen();
    
    // terms/postings
    long ramBytesUsed = getPostingsReader().ramBytesUsed();
    
    // norms
    if (getNormsReader() != null) {
      ramBytesUsed += getNormsReader().ramBytesUsed();
    }
    
    // docvalues
    if (getDocValuesReader() != null) {
      ramBytesUsed += getDocValuesReader().ramBytesUsed();
    }
    
    // stored fields
    if (getFieldsReader() != null) {
      ramBytesUsed += getFieldsReader().ramBytesUsed();
    }
    
    // term vectors
    if (getTermVectorsReader() != null) {
      ramBytesUsed += getTermVectorsReader().ramBytesUsed();
    }

    // points
    if (getPointsReader() != null) {
      ramBytesUsed += getPointsReader().ramBytesUsed();
    }
    
    return ramBytesUsed;
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    ensureOpen();
    final List<Accountable> resources = new ArrayList<>(6);
    
    // terms/postings
    resources.add(Accountables.namedAccountable("postings", getPostingsReader()));
    
    // norms
    if (getNormsReader() != null) {
      resources.add(Accountables.namedAccountable("norms", getNormsReader()));
    }
    
    // docvalues
    if (getDocValuesReader() != null) {
      resources.add(Accountables.namedAccountable("docvalues", getDocValuesReader()));
    }
    
    // stored fields
    if (getFieldsReader() != null) {
      resources.add(Accountables.namedAccountable("stored fields", getFieldsReader()));
    }

    // term vectors
    if (getTermVectorsReader() != null) {
      resources.add(Accountables.namedAccountable("term vectors", getTermVectorsReader()));
    }

    // points
    if (getPointsReader() != null) {
      resources.add(Accountables.namedAccountable("points", getPointsReader()));
    }
    
    return Collections.unmodifiableList(resources);
  }

  @Override
  public void checkIntegrity() throws IOException {
    ensureOpen();
    
    // terms/postings
    getPostingsReader().checkIntegrity();
    
    // norms
    if (getNormsReader() != null) {
      getNormsReader().checkIntegrity();
    }
    
    // docvalues
    if (getDocValuesReader() != null) {
      getDocValuesReader().checkIntegrity();
    }

    // stored fields
    if (getFieldsReader() != null) {
      getFieldsReader().checkIntegrity();
    }
    
    // term vectors
    if (getTermVectorsReader() != null) {
      getTermVectorsReader().checkIntegrity();
    }

    // points
    if (getPointsReader() != null) {
      getPointsReader().checkIntegrity();
    }
  }
}
