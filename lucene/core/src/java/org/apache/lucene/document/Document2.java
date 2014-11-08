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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.FieldTypes.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;

// nocommit clearly spell out which field defaults to what settings, e.g. that atom is not sorted by default

/** A simpler API for building a document for indexing,
 *  that also tracks field properties implied by the
 *  fields being added. */

public class Document2 implements Iterable<IndexableField> {

  private static final float DEFAULT_BOOST = 1.0f;

  private final FieldTypes fieldTypes;
  private final List<FieldValue> fields = new ArrayList<>();
  private final boolean changeSchema;

  // nocommit make private again and somehow deal w/ generics
  public class FieldValue implements IndexableField {
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
    public TokenStream tokenStream(Analyzer analyzerIn, TokenStream reuse) throws IOException {
      Analyzer analyzer = fieldTypes.getIndexAnalyzer();
      if (analyzerIn != analyzer) {
        // TODO: remove analyzer from IW APIs
        throw new IllegalArgumentException("analyzer must be the instance from FieldTypes: got " + analyzerIn + " vs " + analyzer);
      }

      assert fieldTypes.getIndexOptions(fieldName) != IndexOptions.NONE;

      FieldTypes.FieldType fieldType = fieldTypes.getFieldType(fieldName);

      switch (fieldType.valueType) {
      case INT:
        return getReusedBinaryTokenStream(intToBytes(((Number) value).intValue()), reuse);
      case FLOAT:
        return getReusedBinaryTokenStream(intToBytes(Float.floatToIntBits(((Number) value).floatValue())), reuse);
      case LONG:
        return getReusedBinaryTokenStream(longToBytes(((Number) value).longValue()), reuse);
      case DOUBLE:
        return getReusedBinaryTokenStream(longToBytes(Double.doubleToLongBits(((Number) value).doubleValue())), reuse);
      case DATE:
        return getReusedBinaryTokenStream(longToBytes(((Date) value).getTime()), reuse);
      case ATOM:
      case UNIQUE_ATOM:
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
        if (value instanceof String) {
          return getReusedStringTokenStream((String) value, reuse);
        } else {
          assert value instanceof BytesRef;
          return getReusedBinaryTokenStream((BytesRef) value, reuse);
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
      case FLOAT:
        return Integer.valueOf(Float.floatToIntBits((Float) value));
      case DOUBLE:
        return Long.valueOf(Double.doubleToLongBits((Double) value));
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
        } else if (value instanceof String) {
          // nocommit somewhat evil we utf8-encode your string?
          return new BytesRef((String) value);
        }
      }

      return null;
    }
  }

  public Document2(FieldTypes fieldTypes) {
    this(fieldTypes, true);
  }

  Document2(FieldTypes fieldTypes, boolean changeSchema) {
    this.fieldTypes = fieldTypes;
    this.changeSchema = changeSchema;
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
        return index < fields.size() || (changeSchema && fieldTypes != null && fieldTypes.enableExistsFilters && fieldNamesIndex < fields.size());
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }

      public IndexableField next() {
        if (index < fields.size()) {
          return fields.get(index++);
        } else if (fieldTypes != null && changeSchema && fieldTypes.enableExistsFilters && fieldNamesIndex < fields.size()) {
          // nocommit make a more efficient version?  e.g. a single field that takes a list and iterates each via TokenStream.  maybe we
          // should addAtom(String...)?
          return new FieldValue(FieldTypes.FIELD_NAMES_FIELD, fields.get(fieldNamesIndex++).fieldName);
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  public List<FieldValue> getFieldValues() {
    return fields;
  }

  public List<IndexableField> getFields() {
    List<IndexableField> result = new ArrayList<>();
    result.addAll(fields);
    return result;
  }

  public IndexableField getField(String name) {
    for (FieldValue field : fields) {
      if (field.name().equals(name)) {
        return field;
      }
    }
    return null;
  }

  public List<IndexableField> getFields(String name) {
    List<IndexableField> result = new ArrayList<>();
    for (FieldValue field : fields) {
      if (field.name().equals(name)) {
        result.add(field);
      }
    }

    return result;
  }

  /** E.g. a "country" field.  Default: indexes this value as a single token, and disables norms and freqs, and also enables sorting (indexes doc values) and stores it. */
  public void addAtom(String fieldName, String value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.ATOM);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** E.g. a binary single-token field. */
  public void addAtom(String fieldName, byte[] value) {
    addAtom(fieldName, new BytesRef(value));
  }

  public void addAtom(String fieldName, BytesRef value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.ATOM);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** E.g. a primary key field. */
  public void addUniqueAtom(String fieldName, String value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.UNIQUE_ATOM);
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
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.UNIQUE_ATOM);
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

  /** Default: store this value. */
  public void addStored(String fieldName, BytesRef value) {
    // nocommit akward we inferred binary here?
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.BINARY);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Default: store this value. */
  public void addBinary(String fieldName, BytesRef value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.BINARY);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Default: store this value. */
  public void addStored(String fieldName, String value) {
    // nocommit akward we inferred large_text here?
    if (changeSchema) {
      fieldTypes.recordLargeTextType(fieldName, true, false);
    }
    fields.add(new FieldValue(fieldName, value));
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

  // nocommit: addLongArray, addIntArray

  /** Default: support for range filtering/querying and sorting (using numeric doc values). */
  public void addInt(String fieldName, int value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.INT);
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

  /** Default: support for range filtering/querying and sorting (using numeric doc values). */
  public void addLong(String fieldName, long value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.LONG);
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

  /** Add an {@code InetAddress} field.  This is indexed as a binary atom under the hood, for sorting,
   *  range filtering and stored. */
  public void addInetAddress(String fieldName, InetAddress value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.INET_ADDRESS);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  static {
    // nocommit is there a cleaner/general way to detect missing enum value in case switch statically?  must we use ecj?
    assert FieldTypes.ValueType.values().length == 13: "missing case for switch statement below";
  }

  /** Note: this FieldTypes must already know about all the fields in the incoming doc. */
  public void addAll(Document2 other) {
    // nocommit should we insist other.fieldTypes == this.fieldTypes?  or, that they are "congruent"?
    for (FieldValue field : other.fields) {
      String fieldName = field.name();
      FieldType fieldType = fieldTypes.getFieldType(fieldName);
      // nocommit need more checking here ... but then, we should somehow remove StoredDocument, sicne w/ FieldTypes we can now fully
      // reconstruct (as long as all fields were stored) what was indexed:
      switch (fieldType.valueType) {
      case TEXT:
        addLargeText(fieldName, field.stringValue());
        break;
      case SHORT_TEXT:
        addShortText(fieldName, field.stringValue());
        break;
      case ATOM:
        if (field.value instanceof BytesRef) {
          addAtom(fieldName, (BytesRef) field.value);
        } else {
          addAtom(fieldName, (String) field.value);
        }
        break;
      case UNIQUE_ATOM:
        if (field.value instanceof BytesRef) {
          addUniqueAtom(fieldName, (BytesRef) field.value);
        } else {
          addUniqueAtom(fieldName, (String) field.value);
        }
        break;
      case INT:
        addInt(fieldName, field.numericValue().intValue());
        break;
      case FLOAT:
        addFloat(fieldName, field.numericValue().floatValue());
        break;
      case LONG:
        addLong(fieldName, field.numericValue().longValue());
        break;
      case DOUBLE:
        addDouble(fieldName, field.numericValue().doubleValue());
        break;
      case BINARY:
        addStored(fieldName, field.binaryValue());
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
    }
  }

  // nocommit i don't like that we have this ... it's linear cost, and this class is not supposed to be a generic container
  public void removeField(String name) {
    Iterator<FieldValue> it = fields.iterator();
    while (it.hasNext()) {
      FieldValue field = it.next();
      if (field.name().equals(name)) {
        it.remove();
        return;
      }
    }
  }

  static BytesRef intToBytes(int v) {
    int sortableBits = v ^ 0x80000000;
    BytesRef token = new BytesRef(4);
    token.length = 4;
    int index = 3;
    while (index >= 0) {
      token.bytes[index] = (byte) (sortableBits & 0xff);
      index--;
      sortableBits >>>= 8;
    }
    return token;
  }

  static BytesRef longToBytes(long v) {
    long sortableBits = v ^ 0x8000000000000000L;
    BytesRef token = new BytesRef(8);
    token.length = 8;
    int index = 7;
    while (index >= 0) {
      token.bytes[index] = (byte) (sortableBits & 0xff);
      index--;
      sortableBits >>>= 8;
    }
    return token;
  }

  public Boolean getBoolean(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    for(FieldValue fieldValue : fields) {
      if (fieldValue.fieldName.equals(fieldName)) {
        return (Boolean) fieldValue.value;
      }
    }

    return null;
  }

  public Date getDate(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    for(FieldValue fieldValue : fields) {
      if (fieldValue.fieldName.equals(fieldName)) {
        return (Date) fieldValue.value;
      }
    }

    return null;
  }

  public InetAddress getInetAddress(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    for(FieldValue fieldValue : fields) {
      if (fieldValue.fieldName.equals(fieldName)) {
        return (InetAddress) fieldValue.value;
      }
    }

    return null;
  }

  public String getString(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    for(FieldValue fieldValue : fields) {
      if (fieldValue.fieldName.equals(fieldName)) {
        return fieldValue.value.toString();
      }
    }

    return null;
  }

  public String[] getStrings(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    List<String> values = new ArrayList<>();
    for(FieldValue fieldValue : fields) {
      if (fieldValue.fieldName.equals(fieldName)) {
        values.add(fieldValue.value.toString());
      }
    }

    return values.toArray(new String[values.size()]);
  }

  public BytesRef getBinary(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    for(FieldValue fieldValue : fields) {
      if (fieldValue.fieldName.equals(fieldName)) {
        return (BytesRef) fieldValue.value;
      }
    }

    return null;
  }

  public Integer getInt(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    for(FieldValue fieldValue : fields) {
      if (fieldValue.fieldName.equals(fieldName)) {
        return (Integer) fieldValue.value;
      }
    }

    return null;
  }

  public Object get(String fieldName) {
    for(FieldValue fieldValue : fields) {
      if (fieldValue.fieldName.equals(fieldName)) {
        return fieldValue.value;
      }
    }

    return null;
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    for(FieldValue fieldValue : fields) {
      b.append("\n  ");
      b.append(fieldValue.fieldName);
      b.append(": ");
      String s = fieldValue.value.toString();
      if (s.length() > 20) {
        b.append(s.substring(0, 20));
        b.append("...");
      } else {
        b.append(s);
      }
    }
    return b.toString();
  }

  private static final TokenStream EMPTY_TOKEN_STREAM = new TokenStream() {
      @Override
      public final boolean incrementToken() {
        return false;
      }
    };
}
