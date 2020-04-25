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

package org.apache.lucene.luke.models.documents;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.BytesRef;

/**
 * Holder for a document field's information and data.
 */
public final class DocumentField {

  // field name
  private String name;

  // index options
  private IndexOptions idxOptions;
  private boolean hasTermVectors;
  private boolean hasPayloads;
  private boolean hasNorms;
  private long norm;

  // stored value
  private boolean isStored;
  private String stringValue;
  private BytesRef binaryValue;
  private Number numericValue;

  // doc values
  private DocValuesType dvType;

  // point values
  private int pointDimensionCount;
  private int pointNumBytes;

  static DocumentField of(FieldInfo finfo, IndexReader reader, int docId)
      throws IOException {
    return of(finfo, null, reader, docId);
  }

  static DocumentField of(FieldInfo finfo, IndexableField field, IndexReader reader, int docId)
      throws IOException {

    Objects.requireNonNull(finfo);
    Objects.requireNonNull(reader);

    DocumentField dfield = new DocumentField();

    dfield.name = finfo.name;
    dfield.idxOptions = finfo.getIndexOptions();
    dfield.hasTermVectors = finfo.hasVectors();
    dfield.hasPayloads = finfo.hasPayloads();
    dfield.hasNorms = finfo.hasNorms();

    if (finfo.hasNorms()) {
      NumericDocValues norms = MultiDocValues.getNormValues(reader, finfo.name);
      if (norms.advanceExact(docId)) {
        dfield.norm = norms.longValue();
      }
    }

    dfield.dvType = finfo.getDocValuesType();

    dfield.pointDimensionCount = finfo.getPointDimensionCount();
    dfield.pointNumBytes = finfo.getPointNumBytes();

    if (field != null) {
      dfield.isStored = field.fieldType().stored();
      dfield.stringValue = field.stringValue();
      if (field.binaryValue() != null) {
        dfield.binaryValue = BytesRef.deepCopyOf(field.binaryValue());
      }
      dfield.numericValue = field.numericValue();
    }

    return dfield;
  }

  public String getName() {
    return name;
  }

  public IndexOptions getIdxOptions() {
    return idxOptions;
  }

  public boolean hasTermVectors() {
    return hasTermVectors;
  }

  public boolean hasPayloads() {
    return hasPayloads;
  }

  public boolean hasNorms() {
    return hasNorms;
  }

  public long getNorm() {
    return norm;
  }

  public boolean isStored() {
    return isStored;
  }

  public String getStringValue() {
    return stringValue;
  }

  public BytesRef getBinaryValue() {
    return binaryValue;
  }

  public Number getNumericValue() {
    return numericValue;
  }

  public DocValuesType getDvType() {
    return dvType;
  }

  public int getPointDimensionCount() {
    return pointDimensionCount;
  }

  public int getPointNumBytes() {
    return pointNumBytes;
  }

  @Override
  public String toString() {
    return "DocumentField{" +
        "name='" + name + '\'' +
        ", idxOptions=" + idxOptions +
        ", hasTermVectors=" + hasTermVectors +
        ", isStored=" + isStored +
        ", dvType=" + dvType +
        ", pointDimensionCount=" + pointDimensionCount +
        '}';
  }

  private DocumentField() {
  }
}
