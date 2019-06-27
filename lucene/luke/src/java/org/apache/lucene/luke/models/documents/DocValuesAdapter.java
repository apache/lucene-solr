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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.luke.models.util.IndexUtils;
import org.apache.lucene.util.BytesRef;

/**
 * An utility class to access to the doc values.
 */
final class DocValuesAdapter {

  private final IndexReader reader;

  DocValuesAdapter(IndexReader reader) {
    this.reader = Objects.requireNonNull(reader);
  }

  /**
   * Returns the doc values for the specified field in the specified document.
   * Empty Optional instance is returned if no doc values is available for the field.
   *
   * @param docid - document id
   * @param field - field name
   * @return doc values, if exists, or empty
   * @throws IOException - if there is a low level IO error.
   */
  Optional<DocValues> getDocValues(int docid, String field) throws IOException {
    DocValuesType dvType = IndexUtils.getFieldInfo(reader, field).getDocValuesType();

    switch (dvType) {
      case BINARY:
        return createBinaryDocValues(docid, field, DocValuesType.BINARY);
      case NUMERIC:
        return createNumericDocValues(docid, field, DocValuesType.NUMERIC);
      case SORTED_NUMERIC:
        return createSortedNumericDocValues(docid, field, DocValuesType.SORTED_NUMERIC);
      case SORTED:
        return createSortedDocValues(docid, field, DocValuesType.SORTED);
      case SORTED_SET:
        return createSortedSetDocValues(docid, field, DocValuesType.SORTED_SET);
      default:
        return Optional.empty();
    }
  }

  private Optional<DocValues> createBinaryDocValues(int docid, String field, DocValuesType dvType)
      throws IOException {
    BinaryDocValues bvalues = IndexUtils.getBinaryDocValues(reader, field);

    if (bvalues.advanceExact(docid)) {
      DocValues dv = DocValues.of(
          dvType,
          Collections.singletonList(BytesRef.deepCopyOf(bvalues.binaryValue())),
          Collections.emptyList());
      return Optional.of(dv);
    }

    return Optional.empty();
  }

  private Optional<DocValues> createNumericDocValues(int docid, String field, DocValuesType dvType)
      throws IOException{
    NumericDocValues nvalues = IndexUtils.getNumericDocValues(reader, field);

    if (nvalues.advanceExact(docid)) {
      DocValues dv = DocValues.of(
          dvType,
          Collections.emptyList(),
          Collections.singletonList(nvalues.longValue())
      );
      return Optional.of(dv);
    }

    return Optional.empty();
  }

  private Optional<DocValues> createSortedNumericDocValues(int docid, String field, DocValuesType dvType)
      throws IOException {
    SortedNumericDocValues snvalues = IndexUtils.getSortedNumericDocValues(reader, field);

    if (snvalues.advanceExact(docid)) {
      List<Long> numericValues = new ArrayList<>();

      int dvCount = snvalues.docValueCount();
      for (int i = 0; i < dvCount; i++) {
        numericValues.add(snvalues.nextValue());
      }

      DocValues dv = DocValues.of(
          dvType,
          Collections.emptyList(),
          numericValues
      );
      return Optional.of(dv);
    }

    return Optional.empty();
  }

  private Optional<DocValues> createSortedDocValues(int docid, String field, DocValuesType dvType)
      throws IOException {
    SortedDocValues svalues = IndexUtils.getSortedDocValues(reader, field);

    if (svalues.advanceExact(docid)) {
      DocValues dv = DocValues.of(
          dvType,
          Collections.singletonList(BytesRef.deepCopyOf(svalues.binaryValue())),
          Collections.emptyList()
      );
      return Optional.of(dv);
    }

    return Optional.empty();
  }

  private Optional<DocValues> createSortedSetDocValues(int docid, String field, DocValuesType dvType)
      throws IOException {
    SortedSetDocValues ssvalues = IndexUtils.getSortedSetDocvalues(reader, field);

    if (ssvalues.advanceExact(docid)) {
      List<BytesRef> values = new ArrayList<>();

      long ord;
      while ((ord = ssvalues.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        values.add(BytesRef.deepCopyOf(ssvalues.lookupOrd(ord)));
      }

      DocValues dv = DocValues.of(
          dvType,
          values,
          Collections.emptyList()
      );
      return Optional.of(dv);
    }

    return Optional.empty();
  }
}
