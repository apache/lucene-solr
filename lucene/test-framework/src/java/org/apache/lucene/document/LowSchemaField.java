package org.apache.lucene.document;

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
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;

/**
 * A minimal implementation of Lucene's low-schema API, that does absolutely no "user space" validation, for abusing your schema and
 * stressing out Lucene's IndexWriter to ensure it never corrupts the index on abuse.
 */

public class LowSchemaField implements IndexableFieldType, IndexableField {
  private final String fieldName;
  private final Object value;
  private final boolean tokenized;
  private final IndexOptions indexOptions;
  private DocValuesType docValuesType = DocValuesType.NONE;
  private boolean stored = true;
  private boolean termVectors;
  private boolean termVectorPositions;
  private boolean termVectorOffsets;
  private boolean termVectorPayloads;
  private TokenStream tokenStream;
  private float boost = 1.0f;
  private boolean omitNorms;

  public LowSchemaField(String fieldName, Object value, IndexOptions indexOptions, boolean tokenized) {
    this.fieldName = fieldName;
    this.value = value;
    this.indexOptions = indexOptions;
    this.tokenized = tokenized;
  }

  public void doNotStore() {
    this.stored = false;
  }

  public void disableNorms() {
    this.omitNorms = true;
  }

  public void setDocValuesType(DocValuesType docValuesType) {
    this.docValuesType = docValuesType;
  }

  public void enableTermVectors(boolean positions, boolean offsets, boolean payloads) {
    termVectors = true;
    termVectorPositions = positions;
    termVectorOffsets = offsets;
    termVectorPayloads = payloads;
  }

  public void setTokenStream(TokenStream tokenStream) {
    this.tokenStream = tokenStream;
  }

  public void setBoost(float boost) {
    this.boost = boost;
  }

  @Override
  public boolean stored() {
    return stored;
  }

  @Override
  public boolean storeTermVectors() {
    return termVectors;
  }

  @Override
  public boolean storeTermVectorPositions() {
    return termVectorPositions;
  }

  @Override
  public boolean storeTermVectorOffsets() {
    return termVectorOffsets;
  }

  @Override
  public boolean storeTermVectorPayloads() {
    return termVectorPayloads;
  }

  @Override
  public boolean omitNorms() {
    return omitNorms;
  }

  @Override
  public IndexOptions indexOptions() {
    return indexOptions;
  }

  @Override
  public DocValuesType docValuesType() {
    return docValuesType;
  }

  @Override
  public String name() {
    return fieldName;
  }

  @Override
  public IndexableFieldType fieldType() {
    return this;
  }

  // nocommit need test trying to do index field with tokenized=false, and Reader value

  @Override
  public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) throws IOException {
    if (tokenStream != null) {
      return tokenStream;
    } else if (indexOptions != IndexOptions.NONE) {
      if (value instanceof String) {
        String s = (String) value;
        if (tokenized == false) {
          if (!(reuse instanceof StringTokenStream)) {
            // lazy init the TokenStream as it is heavy to instantiate
            // (attributes,...) if not needed (stored field loading)
            reuse = new StringTokenStream();
          }
          ((StringTokenStream) reuse).setValue(s);
          return reuse;
        } else {
          return analyzer.tokenStream(fieldName, s);
        }
      } else if (value instanceof Reader) {
        return analyzer.tokenStream(fieldName, (Reader) value);
      }
    }

    return null;
  }

  @Override
  public float boost() {
    return boost;
  }

  @Override
  public BytesRef binaryValue() {
    if (stored && value instanceof BytesRef) {
      return (BytesRef) value;
    } else {
      return null;
    }
  }

  @Override
  public BytesRef binaryDocValue() {
    if (docValuesType != DocValuesType.NONE && value instanceof BytesRef) {
      return (BytesRef) value;
    } else {
      return null;
    }
  }

  @Override
  public String stringValue() {
    if (value instanceof String) {
      return (String) value;
    } else {
      return null;
    }
  }

  @Override
  public Number numericValue() {
    if (value instanceof Number) {
      return (Number) value;
    } else {
      return null;
    }
  }

  @Override
  public Number numericDocValue() {
    if (docValuesType != DocValuesType.NONE && value instanceof Number) {
      return (Number) value;
    } else {
      return null;
    }
  }
}
