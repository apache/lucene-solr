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

// nocommit clearly spell out which field defaults to what settings, e.g. that atom is not sorted by default

/** A simpler API for building a document for indexing,
 *  that also tracks field properties implied by the
 *  fields being added. */

public class Document2 implements Iterable<IndexableField> {

  private static final float DEFAULT_BOOST = 1.0f;

  private final FieldTypes fieldTypes;
  private final List<IndexableField> fields = new ArrayList<>();
  private final boolean changeSchema;
  private final Set<String> seenFields;

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
        // nocommit testme
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
    public TokenStream tokenStream(Analyzer analyzerIn, TokenStream reuse) throws IOException {
      Analyzer analyzer = fieldTypes.getIndexAnalyzer();
      if (analyzerIn != analyzer) {
        // TODO: remove analyzer from IW APIs
        throw new IllegalArgumentException("analyzer must be the instance from FieldTypes: got " + analyzerIn + " vs " + analyzer);
      }

      assert fieldTypes.getIndexOptions(fieldName) != IndexOptions.NONE;

      FieldTypes.FieldType fieldType = fieldTypes.getFieldType(fieldName);
      // nocommit should we be using Double.doubleToRawLongBits / Float.floatToRawIntBits?

      switch (fieldType.valueType) {
      case INT:
        return getReusedBinaryTokenStream(intToBytes(((Number) value).intValue()), reuse);
      case FLOAT:
        return getReusedBinaryTokenStream(floatToBytes(((Number) value).floatValue()), reuse);
      case LONG:
        return getReusedBinaryTokenStream(longToBytes(((Number) value).longValue()), reuse);
      case DOUBLE:
        return getReusedBinaryTokenStream(doubleToBytes(((Number) value).doubleValue()), reuse);
      case DATE:
        return getReusedBinaryTokenStream(longToBytes(((Date) value).getTime()), reuse);
      case ATOM:
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
        // nocommit i shouldn't do sortableFloatBits?  but why does ot TestSortedNumericSortField.testFloat fail?
        int intBits = Float.floatToIntBits((Float) value);
        if (fieldType.multiValued) {
          // nocommit this is weird?
          intBits = sortableFloatBits(intBits);
        }
        return Integer.valueOf(intBits);
        //return Integer.valueOf(Float.floatToRawIntBits((Float) value));
      case DOUBLE:
        // nocommit i shouldn't do sortableDoubleBits?
        long longBits = Double.doubleToLongBits((Double) value);
        if (fieldType.multiValued) {
          // nocommit this is weird?
          longBits = sortableDoubleBits(longBits);
        }
        return Long.valueOf(longBits);
        //return Long.valueOf(Double.doubleToRawLongBits((Double) value));
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

  private static class StringAndTokenStream {
    public final String value;
    public final TokenStream tokens;
    public StringAndTokenStream(String value, TokenStream tokens) {
      this.value = value;
      this.tokens = tokens;
    }
  }

  public Document2(FieldTypes fieldTypes) {
    this(fieldTypes, true);
  }

  public Document2(Document2 other) {
    this.fieldTypes = other.fieldTypes;
    this.changeSchema = other.changeSchema;
    if (changeSchema) {
      seenFields = new HashSet<>();
    } else {
      seenFields = null;
    }
    addAll(other);
  }

  Document2(FieldTypes fieldTypes, boolean changeSchema) {
    this.fieldTypes = fieldTypes;
    this.changeSchema = changeSchema;
    if (changeSchema) {
      seenFields = new HashSet<>();
    } else {
      seenFields = null;
    }
  }

  private boolean enableExistsField = true;
  
  // nocommit only needed for dv updates ... is there a simple way?
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
          // nocommit make a more efficient version?  e.g. a single field that takes a list and iterates each via TokenStream.  maybe we
          // should addAtom(String...)?
          return new FieldValue(FieldTypes.FIELD_NAMES_FIELD, fields.get(fieldNamesIndex++).name());
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  /*
  public List<FieldValue> getFieldValues() {
    return fields;
  }
  */

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
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.ATOM, true);
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
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.ATOM, true);
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

  // nocommit throw exc if this field was already indexed/dvd?
  /** Default: store this value. */
  public void addStored(String fieldName, BytesRef value) {
    // nocommit akward we inferred binary here?
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.BINARY);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  // nocommit throw exc if this field was already indexed/dvd?
  /** Default: store this value. */
  public void addStored(String fieldName, byte[] value) {
    addStored(fieldName, new BytesRef(value));
  }

  // nocommit throw exc if this field was already indexed/dvd?
  /** Default: store this value. */
  public void addStored(String fieldName, String value) {
    // nocommit akward we inferred large_text here?
    if (changeSchema) {
      fieldTypes.recordLargeTextType(fieldName, true, false);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  // nocommit throw exc if this field was already indexed/dvd?
  /** Default: store this value. */
  // nocommit testme, or remove?
  public void addStoredInt(String fieldName, int value) {
    // nocommit akward we inferred large_text here?
    if (changeSchema) {
      fieldTypes.recordStoredValueType(fieldName, FieldTypes.ValueType.INT);
    }
    fields.add(new FieldValue(fieldName, value));
  }

  /** Default: store & DV this value. */
  public void addBinary(String fieldName, BytesRef value) {
    if (changeSchema) {
      fieldTypes.recordValueType(fieldName, FieldTypes.ValueType.BINARY);
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

  // nocommit: addLongArray, addIntArray

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

  // nocommit mmmmmm
  public void add(IndexableField field) {
    fields.add(field);
  }

  static {
    // nocommit is there a cleaner/general way to detect missing enum value in case switch statically?  must we use ecj?
    assert FieldTypes.ValueType.values().length == 12: "missing case for switch statement below";
  }

  /** Note: this FieldTypes must already know about all the fields in the incoming doc. */
  public void addAll(Document2 other) {
    // nocommit should we insist other.fieldTypes == this.fieldTypes?  or, that they are "congruent"?
    for (IndexableField indexableField : other.fields) {
      String fieldName = indexableField.name();
      if (indexableField instanceof FieldValue) {
        FieldValue field = (FieldValue) indexableField;
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
      } else {
        add(indexableField);
      }
    }
  }

  // nocommit i don't like that we have this ... it's linear cost, and this class is not supposed to be a generic container
  public void removeField(String name) {
    Iterator<IndexableField> it = fields.iterator();
    while (it.hasNext()) {
      IndexableField field = it.next();
      if (field.name().equals(name)) {
        it.remove();
        return;
      }
    }
  }

  // nocommit public just for TestBlockJoin ...
  public static BytesRef intToBytes(int v) {
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

  public static BytesRef floatToBytes(float value) {
    return intToBytes(sortableFloatBits(Float.floatToIntBits(value)));
  }

  /** Converts numeric DV field back to double. */
  public static double sortableLongToDouble(long v) {
    return Double.longBitsToDouble(sortableDoubleBits(v));
  }

  /** Converts numeric DV field back to double. */
  public static double longToDouble(long v) {
    return Double.longBitsToDouble(v);
  }

  /** Converts numeric DV field back to float. */
  public static float sortableIntToFloat(int v) {
    return Float.intBitsToFloat(sortableFloatBits(v));
  }

  /** Converts numeric DV field back to float. */
  public static float intToFloat(int v) {
    return Float.intBitsToFloat(v);
  }

  // nocommit move elsewhere?
  public static int bytesToInt(BytesRef bytes) {
    if (bytes.length != 4) {
      throw new IllegalArgumentException("incoming bytes should be length=4; got length=" + bytes.length);
    }
    int sortableBits = 0;
    for(int i=0;i<4;i++) {
      sortableBits = (sortableBits << 8) | bytes.bytes[bytes.offset + i] & 0xff;
    }

    return sortableBits ^ 0x80000000;
  }

  public static BytesRef longToBytes(long v) {
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

  public static BytesRef doubleToBytes(double value) {
    return longToBytes(sortableDoubleBits(Double.doubleToLongBits(value)));
  }

  // nocommit move elsewhere?
  public static long bytesToLong(BytesRef bytes) {
    if (bytes.length != 8) {
      throw new IllegalArgumentException("incoming bytes should be length=8; got length=" + bytes.length);
    }
    long sortableBits = 0;
    for(int i=0;i<8;i++) {
      sortableBits = (sortableBits << 8) | bytes.bytes[bytes.offset + i] & 0xff;
    }

    return sortableBits ^ 0x8000000000000000L;
  }

  // nocommit move elsewhere?
  public static float bytesToFloat(BytesRef bytes) {
    return Float.intBitsToFloat(sortableFloatBits(bytesToInt(bytes)));
  }

  // nocommit move elsewhere?
  public static double bytesToDouble(BytesRef bytes) {
    return Double.longBitsToDouble(sortableDoubleBits(bytesToLong(bytes)));
  }

  /** Converts IEEE 754 representation of a double to sortable order (or back to the original) */
  public static long sortableDoubleBits(long bits) {
    return bits ^ (bits >> 63) & 0x7fffffffffffffffL;
  }
  
  /** Converts IEEE 754 representation of a float to sortable order (or back to the original) */
  public static int sortableFloatBits(int bits) {
    return bits ^ (bits >> 31) & 0x7fffffff;
  }

  public Boolean getBoolean(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (Boolean) fieldValue.value;
    }
  }

  // nocommit getFloat, getDouble, getLong

  public Date getDate(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (Date) fieldValue.value;
    }
  }

  public InetAddress getInetAddress(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (InetAddress) fieldValue.value;
    }
  }

  public String getString(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (String) fieldValue.value;
    }
  }

  public String[] getStrings(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    List<String> values = new ArrayList<>();
    for(IndexableField fieldValue : fields) {
      if (fieldValue.name().equals(fieldName) && fieldValue instanceof FieldValue) {
        values.add((String) ((FieldValue) fieldValue).value);
      }
    }

    return values.toArray(new String[values.size()]);
  }

  public BytesRef getBinary(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (BytesRef) fieldValue.value;
    }
  }

  public Integer getInt(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (Integer) fieldValue.value;
    }
  }

  public Long getLong(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (Long) fieldValue.value;
    }
  }

  public Float getFloat(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
    FieldValue fieldValue = getFirstFieldValue(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      return (Float) fieldValue.value;
    }
  }

  public Double getDouble(String fieldName) {
    // nocommit can we assert this is a known field and that its type is correct?
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
