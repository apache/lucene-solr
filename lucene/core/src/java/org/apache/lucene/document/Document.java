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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.FieldTypes.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.HalfFloat;
import org.apache.lucene.util.NumericUtils;

/** Holds one document, either created anew for indexing, or retrieved a search time.
 *  When you add fields, their type and properties are tracked by an instance of
 *  {@link FieldTypes} held by the {@link IndexWriter#getFieldTypes()}. */

public class Document implements Iterable<IndexableField> {

  private static final float DEFAULT_BOOST = 1.0f;

  private final FieldTypes fieldTypes;
  private final List<IndexableField> fields = new ArrayList<>();
  private final boolean changeSchema;
  private final Set<String> seenFields;

  private class FieldValue implements IndexableField {
    final String fieldName;
    final Object value;
    final float boost;
    final FieldType fieldType;

    public FieldValue(String name, Object value) {
      this(name, value, DEFAULT_BOOST);
    }

    public FieldValue(String fieldName, Object value, float boost) {
      if (fieldName == null) {
        throw new IllegalArgumentException("field name cannot be null");
      }
      if (value == null) {
        throw new IllegalArgumentException("field=\"" + fieldName + "\": value cannot be null");
      }
      this.fieldName = fieldName;
      this.value = value;
      this.boost = boost;
      FieldType curFieldType;
      if (changeSchema == false) {
        if (fieldTypes != null) {
          try {
            curFieldType = fieldTypes.getFieldType(fieldName);
          } catch (IllegalArgumentException iae) {
            curFieldType = null;
          }
        } else {
          curFieldType = null;
        }
      } else {
        curFieldType = fieldTypes.getFieldType(fieldName);
      }
      this.fieldType = curFieldType;
      if (seenFields != null && seenFields.add(fieldName) == false && fieldType.multiValued != Boolean.TRUE) {
        throw new IllegalArgumentException("field=\"" + fieldName + "\": this field is added more than once but is not multiValued");
      }
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

    private TokenStream getReusedBinaryTokenStream(BytesRef value, TokenStream reuse) {
      BinaryTokenStream bts;
      // It might be non-null and not a BinaryTokenStream if this is an atom field that just add a too small or too big term:
      if (reuse != null && reuse instanceof BinaryTokenStream) {
        bts = (BinaryTokenStream) reuse;
      } else {
        bts = new BinaryTokenStream();
      }
      bts.setValue(value);
      return bts;
    }

    private TokenStream getReusedStringTokenStream(String value, TokenStream reuse) {
      StringTokenStream sts;
      // It might be non-null and not a StringTokenStream if this is an atom field that just add a too small or too big term:
      if (reuse != null && reuse instanceof StringTokenStream) {
        sts = (StringTokenStream) reuse;
      } else {
        sts = new StringTokenStream();
      }
      sts.setValue(value);
      return sts;
    }
    
    @Override
    public String toString() {
      return fieldName + ": " + value;
    }

    @Override
    public TokenStream tokenStream(TokenStream reuse) throws IOException {
      Analyzer analyzer = fieldTypes.getIndexAnalyzer();

      assert fieldTypes.getIndexOptions(fieldName) != IndexOptions.NONE;

      FieldTypes.FieldType fieldType = fieldTypes.getFieldType(fieldName);

      switch (fieldType.valueType) {
      case INT:
        return getReusedBinaryTokenStream(NumericUtils.intToBytes(((Number) value).intValue()), reuse);
      case HALF_FLOAT:
        return getReusedBinaryTokenStream(NumericUtils.halfFloatToBytes(((Number) value).floatValue()), reuse);
      case FLOAT:
        return getReusedBinaryTokenStream(NumericUtils.floatToBytes(((Number) value).floatValue()), reuse);
      case LONG:
        return getReusedBinaryTokenStream(NumericUtils.longToBytes(((Number) value).longValue()), reuse);
      case DOUBLE:
        return getReusedBinaryTokenStream(NumericUtils.doubleToBytes(((Number) value).doubleValue()), reuse);
      case BIG_INT:
        {
          BytesRef bytes;
          try {
            bytes = NumericUtils.bigIntToBytes((BigInteger) value, fieldType.bigIntByteWidth.intValue());
          } catch (IllegalArgumentException iae) {
            FieldTypes.illegalState(fieldName, iae.getMessage());
            // Dead code but compile disagrees:
            bytes = null;
          }
          return getReusedBinaryTokenStream(bytes, reuse);
        }
      case BIG_DECIMAL:
        {
          BigDecimal dec = (BigDecimal) value;
          if (dec.scale() != fieldType.bigDecimalScale.intValue()) {
            FieldTypes.illegalState(fieldName, "BIG_DECIMAL was configured with scale=" + fieldType.bigDecimalScale + ", but indexed value has scale=" + dec.scale());
          }
          BytesRef bytes;
          try {
            bytes = NumericUtils.bigIntToBytes(dec.unscaledValue(), fieldType.bigIntByteWidth.intValue());
          } catch (IllegalArgumentException iae) {
            FieldTypes.illegalState(fieldName, iae.getMessage());
            // Dead code but compile disagrees:
            bytes = null;
          }
          return getReusedBinaryTokenStream(bytes, reuse);
        }
      case DATE:
        return getReusedBinaryTokenStream(NumericUtils.longToBytes(((Date) value).getTime()), reuse);
      case ATOM:
        // TODO: we could/should just wrap a SingleTokenTokenizer here?:
        if (fieldType.minTokenLength != null) {
          if (value instanceof String) {
            String s = (String) value;
            if (s.length() < fieldType.minTokenLength.intValue() ||
                s.length() > fieldType.maxTokenLength.intValue()) {
              return EMPTY_TOKEN_STREAM;
            }
          } else if (value instanceof BytesRef) {
            BytesRef b = (BytesRef) value;
            if (b.length < fieldType.minTokenLength.intValue() ||
                b.length > fieldType.maxTokenLength.intValue()) {
              return EMPTY_TOKEN_STREAM;
            }
          }
        }

        Object indexValue;

        if (fieldType.reversedTerms == Boolean.TRUE) {
          if (value instanceof String) {
            indexValue = new StringBuilder((String) value).reverse().toString();
          } else {
            BytesRef valueBR = (BytesRef) value;
            BytesRef br = new BytesRef(valueBR.length);
            for(int i=0;i<valueBR.length;i++) {
              br.bytes[i] = valueBR.bytes[valueBR.offset+valueBR.length-i-1];
            }
            br.length = valueBR.length;
            indexValue = br;
          }
        } else {
          indexValue = value;
        }
        if (value instanceof String) {
          return getReusedStringTokenStream((String) indexValue, reuse);
        } else {
          assert value instanceof BytesRef;
          return getReusedBinaryTokenStream((BytesRef) indexValue, reuse);
        }

      case BINARY:
        {
          assert value instanceof BytesRef;
          BinaryTokenStream bts;
          if (reuse != null) {
            if (reuse instanceof BinaryTokenStream == false) {
              FieldTypes.illegalState(fieldName, "should have had BinaryTokenStream for reuse, but got " + reuse);
            }
            bts = (BinaryTokenStream) reuse;
          } else {
            bts = new BinaryTokenStream();
          }
          bts.setValue((BytesRef) value);
          return bts;
        }

      case INET_ADDRESS:
        {
          assert value instanceof InetAddress;
          BinaryTokenStream bts;
          if (reuse != null) {
            if (reuse instanceof BinaryTokenStream == false) {
              FieldTypes.illegalState(fieldName, "should have had BinaryTokenStream for reuse, but got " + reuse);
            }
            bts = (BinaryTokenStream) reuse;
          } else {
            bts = new BinaryTokenStream();
          }
          bts.setValue(new BytesRef(((InetAddress) value).getAddress()));
          return bts;
        }

      case SHORT_TEXT:
      case TEXT:
        if (value instanceof TokenStream) {
          return (TokenStream) value;
        } else if (value instanceof StringAndTokenStream) {
          return ((StringAndTokenStream) value).tokens;
        } else if (value instanceof Reader) {
          return analyzer.tokenStream(name(), (Reader) value);
        } else {
          return analyzer.tokenStream(name(), (String) value);
        }

      case BOOLEAN:
        byte[] token = new byte[1];
        if (value == Boolean.TRUE) {
          token[0] = 1;
        }
        return getReusedBinaryTokenStream(new BytesRef(token), reuse);

      default:
        FieldTypes.illegalState(fieldName, "valueType=" + fieldType.valueType + " cannot be indexed");

        // Dead code but javac disagrees:
        return null;
      }
    }

    @Override
    public Number numericValue() {
      if (fieldType == null) {
        if (value instanceof Number) {
          return (Number) value;
        } else {
          return null;
        }
      }
      switch (fieldType.valueType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return (Number) value;
      case HALF_FLOAT:
        return Short.valueOf(NumericUtils.halfFloatToShort((Float) value));
      case DATE:
        return ((Date) value).getTime();
      case BOOLEAN:
        if (value == Boolean.TRUE) {
          return Integer.valueOf(1);
        } else {
          return Integer.valueOf(0);
        }
      default:
        return null;
      }
    }

    @Override
    public Number numericDocValue() {
      if (fieldType == null) {
        return null;
      }
      switch (fieldType.valueType) {
      case INT:
        return (Number) value;
      case LONG:
        return (Number) value;
      case HALF_FLOAT:
        short shortBits = HalfFloat.floatToShort((Float) value);
        shortBits = NumericUtils.sortableHalfFloatBits(shortBits);
        return Short.valueOf(shortBits);
      case FLOAT:
        return Integer.valueOf(NumericUtils.floatToInt((Float) value));
      case DOUBLE:
        return Long.valueOf(NumericUtils.doubleToLong((Double) value));
      case DATE:
        return Long.valueOf(((Date) value).getTime());
      case BOOLEAN:
        if (value == Boolean.TRUE) {
          return Integer.valueOf(1);
        } else {
          return Integer.valueOf(0);
        }
      default:
        return null;
      }
    }

    @Override
    public String stringValue() {
      if (fieldType == null) {
        if (value instanceof String) {
          return (String) value;
        } else {
          return null;
        }
      }

      switch (fieldType.valueType) {
      case SHORT_TEXT:
      case TEXT:
        if (value instanceof String) {
          return (String) value;
        } else if (value instanceof StringAndTokenStream) {
          return ((StringAndTokenStream) value).value;
        } else {
          return null;
        }
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
      if (fieldType == null) {
        if (value instanceof BytesRef) {
          return (BytesRef) value;
        } else {
          return null;
        }
      }

      if (fieldType.valueType == FieldTypes.ValueType.BOOLEAN) {
        byte[] bytes = new byte[1];
        if (value == Boolean.TRUE) {
          bytes[0] = 1;
        }
        return new BytesRef(bytes);
      } else if (fieldType.valueType == FieldTypes.ValueType.INET_ADDRESS) {
        return new BytesRef(((InetAddress) value).getAddress());
      } else if (fieldType.valueType == FieldTypes.ValueType.BIG_INT) { 
        return new BytesRef(((BigInteger) value).toByteArray());
      } else if (fieldType.valueType == FieldTypes.ValueType.BIG_DECIMAL) { 
        BigDecimal dec = (BigDecimal) value;
        if (dec.scale() != fieldType.bigDecimalScale) {
          FieldTypes.illegalState(fieldName, "BIG_DECIMAL was configured with scale=" + fieldType.bigDecimalScale + ", but stored value has scale=" + dec.scale());
        }
        return new BytesRef(dec.unscaledValue().toByteArray());
      } else if (value instanceof BytesRef) {
        return (BytesRef) value;
      } else {
        return null;
      }
    }

    @Override
    public BytesRef binaryDocValue() {
      if (value instanceof BytesRef) {
        return (BytesRef) value;
      } else if (fieldType.docValuesType == DocValuesType.BINARY || fieldType.docValuesType == DocValuesType.SORTED || fieldType.docValuesType == DocValuesType.SORTED_SET) {
        if (fieldType.valueType == FieldTypes.ValueType.INET_ADDRESS) {
          return new BytesRef(((InetAddress) value).getAddress());
        } else if (fieldType.valueType == FieldTypes.ValueType.BIG_INT) {
          // TODO: can we do this only once, if it's DV'd & indexed?
          return NumericUtils.bigIntToBytes((BigInteger) value, fieldType.bigIntByteWidth);
        } else if (fieldType.valueType == FieldTypes.ValueType.BIG_DECIMAL) { 
          // TODO: can we do this only once, if it's DV'd & indexed?
          return NumericUtils.bigIntToBytes(((BigDecimal) value).unscaledValue(), fieldType.bigIntByteWidth);
        } else if (value instanceof String) {
          String s = (String) value;
          BytesRef br;
          if (fieldType.sortCollator != null) {
            // TOOD: thread local clones instead of sync'd on one instance?
            byte[] bytes;
            synchronized (fieldType.sortCollator) {
              bytes = fieldType.sortCollator.getCollationKey(s).toByteArray();
            }
            br = new BytesRef(bytes);
          } else {
            // TODO: somewhat evil we utf8-encode your string?
            br = new BytesRef(s);
          }

          return br;
        }
      }

      return null;
    }
  }

  private static class StringAndTokenStream {
    public final String value;
    public final TokenStream tokens;
    public StringAndTokenStream(String value, TokenStream tokens) {
      this.value = value;
      this.tokens = tokens;
    }
  }

  public Document(FieldTypes fieldTypes) {
    this(fieldTypes, true);
  }

  public Document(Document other) {
    this.fieldTypes = other.fieldTypes;
    this.changeSchema = other.changeSchema;
    if (changeSchema) {
      seenFields = new HashSet<>();
    } else {
      seenFields = null;
    }
    addAll(other);
  }

  Document(FieldTypes fieldTypes, boolean changeSchema) {
    this.fieldTypes = fieldTypes;
    this.changeSchema = changeSchema;
    if (changeSchema) {
      seenFields = new HashSet<>();
    } else {
      seenFields = null;
    }
  }

  private boolean enableExistsField = true;
  
  /** Disables indexing of field names for this one document.
   *  To disable globally use {@link FieldTypes#disableExistsFilters}. */
  public void disableExistsField() {
    enableExistsField = false;
  }

  @Override
  public Iterator<IndexableField> iterator() {
    if (fieldTypes != null) {
      assert fieldTypes.getStored(FieldTypes.FIELD_NAMES_FIELD) == false;
    }

    return new Iterator<IndexableField>() {
      int index;
      int fieldNamesIndex;

      public boolean hasNext() {
        return index < fields.size() || (enableExistsField && changeSchema && fieldTypes != null && fieldTypes.enableExistsFilters && fieldNamesIndex < fields.size());
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }

      public IndexableField next() {
        if (index < fields.size()) {
          return fields.get(index++);
        } else if (enableExistsField && fieldTypes != null && changeSchema && fieldTypes.enableExistsFilters && fieldNamesIndex < fields.size()) {
          // TODO: maybe single method call to add multiple atoms?  addAtom(String...)
          return new FieldValue(FieldTypes.FIELD_NAMES_FIELD, fields.get(fieldNamesIndex++).name());
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  public List<IndexableField> getFields() {
    return fields;
  }

  public IndexableField getField(String name) {
    for (IndexableField field : fields) {
      if (field.name().equals(name)) {
        return field;
      }
    }
    return null;
  }

  public List<IndexableField> getFields(String name) {
    List<IndexableField> result = new ArrayList<>();
    for (IndexableField field : fields) {
      if (field.name().equals(name)) {
        result.add(field);
      }
    }

    return result;
  }

  /** E.g. a "country" field.  Default: indexes this value as a single token, and disables norms and freqs, and also enables sorting (indexes doc values) and stores it. */
  public void addAtom(String fieldName, String value) {
    if (changeSchema) {
      fieldTypes.recordStringAtomValueType(fieldName, false);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** E.g. a binary single-token field. */
  public void addAtom(String fieldName, byte[] value) {
    addAtom(fieldName, new BytesRef(value));
  }

  public void addAtom(String fieldName, BytesRef value) {
    if (changeSchema) {
      fieldTypes.recordBinaryAtomValueType(fieldName, false);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  // nocommit explain/verify how there can be only one unique field per doc

  /** E.g. a primary key field. */
  public void addUniqueAtom(String fieldName, String value) {
    if (changeSchema) {
      fieldTypes.recordStringAtomValueType(fieldName, true);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** E.g. a primary key field. */
  public void addUniqueAtom(String fieldName, byte[] value) {
    addUniqueAtom(fieldName, new BytesRef(value));
  }

  /** E.g. a primary key field. */
  public void addUniqueAtom(String fieldName, BytesRef value) {
    if (changeSchema) {
      fieldTypes.recordBinaryAtomValueType(fieldName, true);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** E.g. a "title" field.  Default: indexes this value as multiple tokens from analyzer, and disables norms and freqs, and also enables
   *  sorting (indexes sorted doc values). */
  public void addShortText(String fieldName, String value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.SHORT_TEXT);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Only store this value. */
  public void addStoredBinary(String fieldName, BytesRef value) {
    if (changeSchema) {
      fieldTypes.recordStoredValueType(fieldName, FieldTypes.ValueType.BINARY);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Only store this value. */
  public void addStoredBinary(String fieldName, byte[] value) {
    addStoredBinary(fieldName, new BytesRef(value));
  }

  /** Only store this value. */
  public void addStoredString(String fieldName, String value) {
    if (changeSchema) {
      fieldTypes.recordLargeTextType(fieldName, true, false);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Only store this value. */
  public void addStoredInt(String fieldName, int value) {
    if (changeSchema) {
      fieldTypes.recordStoredValueType(fieldName, FieldTypes.ValueType.INT);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Only store this value. */
  public void addStoredLong(String fieldName, long value) {
    if (changeSchema) {
      fieldTypes.recordStoredValueType(fieldName, FieldTypes.ValueType.LONG);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Only store this value. */
  public void addStoredFloat(String fieldName, float value) {
    if (changeSchema) {
      fieldTypes.recordStoredValueType(fieldName, FieldTypes.ValueType.FLOAT);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Only store this value. */
  public void addStoredHalfFloat(String fieldName, float value) {
    if (changeSchema) {
      fieldTypes.recordStoredValueType(fieldName, FieldTypes.ValueType.HALF_FLOAT);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Only store this value. */
  public void addStoredDouble(String fieldName, double value) {
    if (changeSchema) {
      fieldTypes.recordStoredValueType(fieldName, FieldTypes.ValueType.DOUBLE);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Only store this value. */
  public void addStoredDate(String fieldName, Date value) {
    if (changeSchema) {
      fieldTypes.recordStoredValueType(fieldName, FieldTypes.ValueType.DATE);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Only store this value. */
  public void addStoredInetAddress(String fieldName, InetAddress value) {
    if (changeSchema) {
      fieldTypes.recordStoredValueType(fieldName, FieldTypes.ValueType.INET_ADDRESS);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Not indexed, stored, doc values. */
  public void addBinary(String fieldName, BytesRef value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.BINARY);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Only store this value. */
  public void addStoredBigInteger(String fieldName, BigInteger value) {
    if (changeSchema) {
      fieldTypes.recordStoredValueType(fieldName, FieldTypes.ValueType.BIG_INT);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Only store this value. */
  public void addStoredBigDecimal(String fieldName, BigDecimal value) {
    if (changeSchema) {
      fieldTypes.recordStoredValueType(fieldName, FieldTypes.ValueType.BIG_DECIMAL);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Default: store this value. */
  public void addBinary(String fieldName, byte[] value) {
    addBinary(fieldName, new BytesRef(value));
  }

  /** E.g. a "body" field.  Default: indexes this value as multiple tokens from analyzer and stores the value. */
  public void addLargeText(String fieldName, String value) {
    addLargeText(fieldName, value, DEFAULT_BOOST);
  }

  /** E.g. a "body" field.  Default: indexes this value as multiple tokens from analyzer and stores the value. */
  public void addLargeText(String fieldName, String value, float boost) {
    if (changeSchema) {
      fieldTypes.recordLargeTextType(fieldName, true, true);
    }
    fields.add(new FieldValue(fieldName, value, boost));
  }

  /** E.g. a "body" field.  Default: indexes this value as multiple tokens from analyzer. */
  public void addLargeText(String fieldName, TokenStream value) {
    addLargeText(fieldName, value, DEFAULT_BOOST);
  }

  /** E.g. a "body" field.  Default: indexes this value as multiple tokens from analyzer. */
  public void addLargeText(String fieldName, TokenStream value, float boost) {
    if (changeSchema) {
      fieldTypes.recordLargeTextType(fieldName, false, true);
    }
    fields.add(new FieldValue(fieldName, value, boost));
  }

  public void addLargeText(String fieldName, String value, TokenStream tokens, float boost) {
    if (changeSchema) {
      fieldTypes.recordLargeTextType(fieldName, true, true);
    }
    fields.add(new FieldValue(fieldName, new StringAndTokenStream(value, tokens), boost));
  }

  /** E.g. a "body" field.  Default: indexes this value as multiple tokens from analyzer. */
  public void addLargeText(String fieldName, Reader reader) {
    addLargeText(fieldName, reader, DEFAULT_BOOST);
  }

  /** E.g. a "body" field.  Default: indexes this value as multiple tokens from analyzer. */
  public void addLargeText(String fieldName, Reader value, float boost) {
    if (changeSchema) {
      fieldTypes.recordLargeTextType(fieldName, false, true);
    }
    fields.add(new FieldValue(fieldName, value, boost));
  }

  /** Default: support for range filtering/querying and sorting (using numeric doc values). */
  public void addInt(String fieldName, int value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.INT);
    }
    fields.add(new FieldValue(fieldName, Integer.valueOf(value)));
  }

  /** Default: support for range filtering/querying and sorting (using numeric doc values). */
  public void addUniqueInt(String fieldName, int value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.INT, true);
    }
    fields.add(new FieldValue(fieldName, Integer.valueOf(value)));
  }

  /** Default: support for range filtering/querying and sorting (using numeric doc values). */
  public void addFloat(String fieldName, float value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.FLOAT);
    }
    fields.add(new FieldValue(fieldName, Float.valueOf(value)));
  }

  /** Adds half precision (2 bytes) float.  Note that the value is stored with 2 bytes in doc values, but in stored fields it's stored as an
   *  ordinary 4 byte float.  Default: support for range filtering/querying and sorting (using numeric doc values). */
  public void addHalfFloat(String fieldName, float value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.HALF_FLOAT);
    }
    fields.add(new FieldValue(fieldName, Float.valueOf(value)));
  }

  /** Default: support for range filtering/querying and sorting (using numeric doc values). */
  public void addLong(String fieldName, long value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.LONG);
    }
    fields.add(new FieldValue(fieldName, Long.valueOf(value)));
  }

  /** Default: support for range filtering/querying and sorting (using numeric doc values). */
  public void addUniqueLong(String fieldName, long value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.LONG, true);
    }
    fields.add(new FieldValue(fieldName, Long.valueOf(value)));
  }

  /** Default: support for range filtering/querying and sorting (using numeric doc values). */
  public void addDouble(String fieldName, double value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.DOUBLE);
    }
    fields.add(new FieldValue(fieldName, Double.valueOf(value)));
  }

  // TODO: addUniqueBigInteger?

  /** Default: support for range filtering/querying and sorting (using sorted doc values). */
  public void addBigInteger(String fieldName, BigInteger value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.BIG_INT);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Default: support for range filtering/querying and sorting (using sorted doc values). */
  public void addBigDecimal(String fieldName, BigDecimal value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.BIG_DECIMAL);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  public void addBoolean(String fieldName, boolean value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.BOOLEAN);
    }
    fields.add(new FieldValue(fieldName, Boolean.valueOf(value)));
  }

  public void addDate(String fieldName, Date value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.DATE);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  // nocommit should we map v4 addresses like this: http://en.wikipedia.org/wiki/IPv6#IPv4-mapped_IPv6_addresses

  /** Add an {@code InetAddress} field.  This is indexed as a binary atom under the hood, for sorting,
   *  range filtering and stored. */
  public void addInetAddress(String fieldName, InetAddress value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.INET_ADDRESS);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Adds an abitrary external, opaque {@link IndexableField} without updating the field types. */
  public void add(IndexableField field) {
    fields.add(field);
  }

  static {
    assert FieldTypes.ValueType.values().length == 15: "missing case for switch statement below";
  }

  /** Note: the FieldTypes must already know about all the fields in the incoming doc. */
  public void addAll(Document other) {
    for (IndexableField indexableField : other.fields) {
      String fieldName = indexableField.name();
      if (indexableField instanceof FieldValue) {
        FieldValue field = (FieldValue) indexableField;
        FieldType fieldType = other.fieldTypes.getFieldType(fieldName);
        switch (fieldType.valueType) {
        case TEXT:
          addLargeText(fieldName, field.stringValue());
          break;
        case SHORT_TEXT:
          addShortText(fieldName, field.stringValue());
          break;
        case ATOM:
          if (field.value instanceof BytesRef) {
            if (fieldType.isUnique == Boolean.TRUE) {
              addUniqueAtom(fieldName, (BytesRef) field.value);
            } else {
              addAtom(fieldName, (BytesRef) field.value);
            }
          } else {
            if (fieldType.isUnique == Boolean.TRUE) {
              addUniqueAtom(fieldName, (String) field.value);
            } else {
              addAtom(fieldName, (String) field.value);
            }
          }
          break;
        case INT:
          if (fieldType.isUnique == Boolean.TRUE) {
            addUniqueInt(fieldName, field.numericValue().intValue());
          } else {
            addInt(fieldName, field.numericValue().intValue());
          }
          break;
        case HALF_FLOAT:
          addHalfFloat(fieldName, field.numericValue().floatValue());
          break;
        case FLOAT:
          addFloat(fieldName, field.numericValue().floatValue());
          break;
        case LONG:
          if (fieldType.isUnique == Boolean.TRUE) {
            addUniqueLong(fieldName, field.numericValue().longValue());
          } else {
            addLong(fieldName, field.numericValue().longValue());
          }
          break;
        case DOUBLE:
          addDouble(fieldName, field.numericValue().doubleValue());
          break;
        case BIG_INT:
          addBigInteger(fieldName, (BigInteger) field.value);
          break;
        case BIG_DECIMAL:
          addBigDecimal(fieldName, (BigDecimal) field.value);
          break;
        case BINARY:
          addStoredBinary(fieldName, field.binaryValue());
          break;
        case BOOLEAN:
          addBoolean(fieldName, ((Boolean) field.value).booleanValue());
          break;
        case DATE:
          addDate(fieldName, (Date) field.value);
          break;
        case INET_ADDRESS:
          addInetAddress(fieldName, (InetAddress) field.value);
          break;
        default:
          // BUG:
          throw new AssertionError("missing valueType=" + fieldType.valueType + " in switch");
        }
      } else {
        add(indexableField);
      }
    }
  }

  public Boolean getBoolean(String fieldName) {
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (Boolean) fieldValue.value;
    }
  }

  public Date getDate(String fieldName) {
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (Date) fieldValue.value;
    }
  }

  public InetAddress getInetAddress(String fieldName) {
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (InetAddress) fieldValue.value;
    }
  }

  public BigInteger getBigInteger(String fieldName) {
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (BigInteger) fieldValue.value;
    }
  }

  public BigDecimal getBigDecimal(String fieldName) {
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (BigDecimal) fieldValue.value;
    }
  }

  public String getString(String fieldName) {
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (String) fieldValue.value;
    }
  }

  public String[] getStrings(String fieldName) {
    List<String> values = new ArrayList<>();
    for(IndexableField fieldValue : fields) {
      if (fieldValue.name().equals(fieldName) && fieldValue instanceof FieldValue) {
        values.add((String) ((FieldValue) fieldValue).value);
      }
    }

    return values.toArray(new String[values.size()]);
  }

  public BytesRef getBinary(String fieldName) {
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (BytesRef) fieldValue.value;
    }
  }

  public Integer getInt(String fieldName) {
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (Integer) fieldValue.value;
    }
  }

  public Long getLong(String fieldName) {
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (Long) fieldValue.value;
    }
  }

  public Float getHalfFloat(String fieldName) {
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (Float) fieldValue.value;
    }
  }

  public Float getFloat(String fieldName) {
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (Float) fieldValue.value;
    }
  }

  public Double getDouble(String fieldName) {
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (Double) fieldValue.value;
    }
  }

  public Object get(String fieldName) {
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return fieldValue.value;
    }
  }

  private FieldValue getFirstFieldValue(String name) {
    for(IndexableField fieldValue : fields) {
      if (fieldValue.name().equals(name) && fieldValue instanceof FieldValue) {
        return (FieldValue) fieldValue;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    for(IndexableField field : fields) {
      b.append("\n  ");
      b.append(field.name());
      b.append(": ");
      String s;
      if (field instanceof FieldValue) {
        s = ((FieldValue) field).value.toString();
      } else {
        s = field.toString();
      }
      if (s.length() > 20) {
        b.append(s.substring(0, 20));
        b.append("...");
      } else {
        b.append(s);
      }
    }
    return b.toString();
  }

  public FieldTypes getFieldTypes() {
    return fieldTypes;
  }

  private static final TokenStream EMPTY_TOKEN_STREAM = new TokenStream() {
      @Override
      public final boolean incrementToken() {
        return false;
      }
    };
}
