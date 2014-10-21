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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.FieldTypes.FieldType;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.IndexDocument;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FilterIterator;

/** A simpler API for building a document for indexing,
 *  that also tracks field properties implied by the
 *  fields being added. */

public class Document2 implements IndexDocument {

  private static final float DEFAULT_BOOST = 1.0f;

  private final FieldTypes fieldTypes;
  private final List<FieldValue> fields = new ArrayList<>();

  private class FieldValue implements IndexableField, StorableField {
    final String fieldName;
    final Object value;
    final float boost;
    final FieldType fieldType;

    public FieldValue(String name, Object value) {
      this(name, value, DEFAULT_BOOST);
    }

    public FieldValue(String fieldName, Object value, float boost) {
      this.fieldName = fieldName;
      this.value = value;
      this.boost = boost;
      this.fieldType = fieldTypes.getFieldType(fieldName);
    }
    
    @Override
    public String name() {
      return fieldName;
    }

    @Override
    public IndexableFieldType fieldType() {
      return fieldType;
    }

    @Override
    public float boost() {
      return boost;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzerIn, TokenStream reuse) throws IOException {
      Analyzer analyzer = fieldTypes.getAnalyzer();
      if (analyzerIn != analyzer) {
        // TODO: remove analyzer from IW APIs
        throw new IllegalArgumentException("analyzer must be the instance from FieldTypes");
      }

      FieldTypes.FieldType fieldType = fieldTypes.getFieldType(fieldName);
      switch (fieldType.valueType) {
      case INT:
      case FLOAT:
      case LONG:
      case DOUBLE:
        NumericTokenStream nts;
        if (reuse != null) {
          if (reuse instanceof NumericTokenStream == false) {
            FieldTypes.illegalState(fieldName, "should have had NumericTokenStream for reuse, but got " + reuse);
          }
          nts = (NumericTokenStream) reuse;
          if (fieldType.numericPrecisionStep == null || nts.getPrecisionStep() != fieldType.numericPrecisionStep.intValue()) {
            FieldTypes.illegalState(fieldName, "reused NumericTokenStream has precisionStep " + nts.getPrecisionStep() + ", which is different from FieldType's " + fieldType.numericPrecisionStep);
          }
        } else {
          nts = new NumericTokenStream(fieldType.numericPrecisionStep);
        }
        // initialize value in TokenStream
        final Number number = (Number) value;
        switch (fieldType.valueType) {
        case INT:
          nts.setIntValue(number.intValue());
          break;
        case LONG:
          nts.setLongValue(number.longValue());
          break;
        case FLOAT:
          nts.setFloatValue(number.floatValue());
          break;
        case DOUBLE:
          nts.setDoubleValue(number.doubleValue());
          break;
        default:
          throw new AssertionError("Should never get here");
        }
        return nts;

      case ATOM:
        if (value instanceof String) {
          StringTokenStream sts;
          if (reuse != null) {
            if (reuse instanceof StringTokenStream == false) {
              FieldTypes.illegalState(fieldName, "should have had StringTokenStream for reuse, but got " + reuse);
            }
            sts = (StringTokenStream) reuse;
          } else {
            sts = new StringTokenStream();
          }
          sts.setValue((String) value);
          return sts;
        } else {
          assert value instanceof byte[];
          BinaryTokenStream bts;
          if (reuse != null) {
            if (reuse instanceof BinaryTokenStream == false) {
              FieldTypes.illegalState(fieldName, "should have had BinaryTokenStream for reuse, but got " + reuse);
            }
            bts = (BinaryTokenStream) reuse;
          } else {
            bts = new BinaryTokenStream();
          }
          bts.setValue(new BytesRef((byte[]) value));
          return bts;
        }

      case BINARY:
        assert value instanceof byte[];
        BinaryTokenStream bts;
        if (reuse != null) {
          if (reuse instanceof BinaryTokenStream == false) {
            FieldTypes.illegalState(fieldName, "should have had BinaryTokenStream for reuse, but got " + reuse);
          }
          bts = (BinaryTokenStream) reuse;
        } else {
          bts = new BinaryTokenStream();
        }
        bts.setValue(new BytesRef((byte[]) value));
        return bts;

      case SHORT_TEXT:
      case TEXT:
        if (value instanceof TokenStream) {
          return (TokenStream) value;
        } else if (value instanceof Reader) {
          return analyzer.tokenStream(name(), (Reader) value);
        } else {
          return analyzer.tokenStream(name(), (String) value);
        }

      default:
        FieldTypes.illegalState(fieldName, "valueType=" + fieldType.valueType + " cannot be indexed");

        // Dead code but javac disagrees:
        return null;
      }
    }

    @Override
    public Number numericValue() {
      switch (fieldType.valueType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return (Number) value;
      default:
        return null;
      }
    }

    @Override
    public Number numericDocValue() {
      switch (fieldType.valueType) {
      case INT:
        return (Number) value;
      case LONG:
        return (Number) value;
      case FLOAT:
        return Integer.valueOf(Float.floatToIntBits((Float) value));
      case DOUBLE:
        return Long.valueOf(Double.doubleToLongBits((Double) value));
      default:
        return null;
      }
    }

    @Override
    public String stringValue() {
      switch (fieldType.valueType) {
      case SHORT_TEXT:
      case TEXT:
        return (String) value;
      case ATOM:
        if (value instanceof String) {
          return (String) value;
        } else {
          return null;
        }
      default:
        return null;
      }
    }

    @Override
    public BytesRef binaryValue() {
      if (value instanceof byte[]) {
        return new BytesRef((byte[]) value);
      } else {
        return null;
      }
    }

    @Override
    public BytesRef binaryDocValue() {
      if (value instanceof byte[]) {
        return new BytesRef((byte[]) value);
      } else if (value instanceof String && (fieldType.docValuesType == DocValuesType.BINARY || fieldType.docValuesType == DocValuesType.SORTED || fieldType.docValuesType == DocValuesType.SORTED_SET)) {
        // nocommit somewhat evil we utf8-encode your string?
        return new BytesRef((String) value);
      }

      return null;
    }
  }

  public Document2(FieldTypes fieldTypes) {
    this.fieldTypes = fieldTypes;
  }

  @Override
  public Iterable<IndexableField> indexableFields() {
    return new Iterable<IndexableField>() {
      @Override
      public Iterator<IndexableField> iterator() {
        return Document2.this.indexedFieldsIterator();
      }
    };
  }

  @Override
  public Iterable<StorableField> storableFields() {
    return new Iterable<StorableField>() {
      @Override
      public Iterator<StorableField> iterator() {
        return Document2.this.storedFieldsIterator();
      }
    };
  }

  private Iterator<StorableField> storedFieldsIterator() {
    return new FilterIterator<StorableField,FieldValue>(fields.iterator()) {
      @Override
      protected boolean predicateFunction(FieldValue field) {
        return field.fieldType.stored() || field.fieldType.docValueType() != null;
      }
    };
  }
  
  private Iterator<IndexableField> indexedFieldsIterator() {
    return new FilterIterator<IndexableField,FieldValue>(fields.iterator()) {
      @Override
      protected boolean predicateFunction(FieldValue field) {
        return field.fieldType.indexOptions() != null;
      }
    };
  }

  /** E.g. a "country" field.  Default: indexes this value as a single token, and disables norms and freqs, and also enables sorting (indexes doc values) and stores it. */
  public void addAtom(String fieldName, String value) {
    fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.ATOM);
    fields.add(new FieldValue(fieldName, value));
  }

  /** E.g. an "id" (primary key) field.  Default: indexes this value as a single token, and disables norms and freqs. */
  public void addAtom(String fieldName, byte[] value) {
    fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.ATOM);
    fields.add(new FieldValue(fieldName, value));
  }

  /** E.g. a "title" field.  Default: indexes this value as multiple tokens from analyzer, and disables norms and freqs, and also enables
   *  sorting (indexes sorted doc values). */
  public void addShortText(String fieldName, String value) {
    fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.SHORT_TEXT);
    fields.add(new FieldValue(fieldName, value));
  }

  /** Default: store this value. */
  public void addStored(String fieldName, byte[] value) {
    // nocommit akward we inferred binary here?
    fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.BINARY);
    fields.add(new FieldValue(fieldName, value));
  }

  /** Default: store this value. */
  public void addStored(String fieldName, String value) {
    // nocommit akward we inferred large_text here?
    fieldTypes.recordLargeTextType(fieldName, true);
    fields.add(new FieldValue(fieldName, value));
  }

  /** E.g. a "body" field.  Default: indexes this value as multiple tokens from analyzer and stores the value. */
  public void addLargeText(String fieldName, String value) {
    addLargeText(fieldName, value, DEFAULT_BOOST);
  }

  /** E.g. a "body" field.  Default: indexes this value as multiple tokens from analyzer and stores the value. */
  public void addLargeText(String fieldName, String value, float boost) {
    fieldTypes.recordLargeTextType(fieldName, true);
    fields.add(new FieldValue(fieldName, value, boost));
  }

  /** E.g. a "body" field.  Default: indexes this value as multiple tokens from analyzer. */
  public void addLargeText(String fieldName, TokenStream value) {
    addLargeText(fieldName, value, DEFAULT_BOOST);
  }

  /** E.g. a "body" field.  Default: indexes this value as multiple tokens from analyzer. */
  public void addLargeText(String fieldName, TokenStream value, float boost) {
    fieldTypes.recordLargeTextType(fieldName, false);
    fields.add(new FieldValue(fieldName, value, boost));
  }

  /** E.g. a "body" field.  Default: indexes this value as multiple tokens from analyzer. */
  public void addLargeText(String fieldName, Reader reader) {
    addLargeText(fieldName, reader, DEFAULT_BOOST);
  }

  /** E.g. a "body" field.  Default: indexes this value as multiple tokens from analyzer. */
  public void addLargeText(String fieldName, Reader value, float boost) {
    fieldTypes.recordLargeTextType(fieldName, false);
    fields.add(new FieldValue(fieldName, value, boost));
  }

  // addLongArray, addIntArray

  // nocommit don't use overloadign here ... change to addLong, addFloat, etc.
  /** Default: support for range filtering/querying and sorting (using numeric doc values). */
  public void addNumber(String fieldName, int value) {
    fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.INT);
    fields.add(new FieldValue(fieldName, Integer.valueOf(value)));
  }

  /** Default: support for range filtering/querying and sorting (using numeric doc values). */
  public void addNumber(String fieldName, float value) {
    fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.FLOAT);
    fields.add(new FieldValue(fieldName, Float.valueOf(value)));
  }

  /** Default: support for range filtering/querying and sorting (using numeric doc values). */
  public void addNumber(String fieldName, long value) {
    fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.LONG);
    fields.add(new FieldValue(fieldName, Long.valueOf(value)));
  }

  /** Default: support for range filtering/querying and sorting (using numeric doc values). */
  public void addNumber(String fieldName, double value) {
    fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.DOUBLE);
    fields.add(new FieldValue(fieldName, Double.valueOf(value)));
  }
}
