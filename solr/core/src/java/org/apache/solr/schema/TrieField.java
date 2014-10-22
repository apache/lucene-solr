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
package org.apache.solr.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesRangeFilter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.uninverting.UninvertingReader.Type;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.mutable.MutableValueDate;
import org.apache.lucene.util.mutable.MutableValueLong;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;

/**
 * Provides field types to support for Lucene's {@link
 * IntField}, {@link LongField}, {@link FloatField} and
 * {@link DoubleField}.
 * See {@link org.apache.lucene.search.NumericRangeQuery} for more details.
 * It supports integer, float, long, double and date types.
 * <p/>
 * For each number being added to this field, multiple terms are generated as per the algorithm described in the above
 * link. The possible number of terms increases dramatically with lower precision steps. For
 * the fast range search to work, trie fields must be indexed.
 * <p/>
 * Trie fields are sortable in numerical order and can be used in function queries.
 * <p/>
 * Note that if you use a precisionStep of 32 for int/float and 64 for long/double/date, then multiple terms will not be
 * generated, range search will be no faster than any other number field, but sorting will still be possible.
 *
 *
 * @see org.apache.lucene.search.NumericRangeQuery
 * @since solr 1.4
 */
public class TrieField extends PrimitiveFieldType {
  public static final int DEFAULT_PRECISION_STEP = 8;

  protected int precisionStepArg = TrieField.DEFAULT_PRECISION_STEP;  // the one passed in or defaulted
  protected int precisionStep;     // normalized
  protected TrieTypes type;
  protected Object missingValue;

  
  /**
   * Used for handling date types
   */
  static final TrieDateField dateField = new TrieDateField();

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    String p = args.remove("precisionStep");
    if (p != null) {
       precisionStepArg = Integer.parseInt(p);
    }
    // normalize the precisionStep
    precisionStep = precisionStepArg;
    if (precisionStep<=0 || precisionStep>=64) precisionStep=Integer.MAX_VALUE;
    String t = args.remove("type");

    if (t != null) {
      try {
        type = TrieTypes.valueOf(t.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Invalid type specified in schema.xml for field: " + args.get("name"), e);
      }
    }
  }

  @Override
  public Object toObject(StorableField f) {
    final Number val = f.numericValue();
    if (val != null) {
      return (type == TrieTypes.DATE) ? new Date(val.longValue()) : val;
    } else {
      // the following code is "deprecated" and only to support pre-3.2 indexes using the old BinaryField encoding:
      final BytesRef bytes = f.binaryValue();
      if (bytes==null) return badFieldString(f);
      switch (type) {
        case INTEGER:
          return toInt(bytes.bytes, bytes.offset);
        case FLOAT:
          return Float.intBitsToFloat(toInt(bytes.bytes, bytes.offset));
        case LONG:
          return toLong(bytes.bytes, bytes.offset);
        case DOUBLE:
          return Double.longBitsToDouble(toLong(bytes.bytes, bytes.offset));
        case DATE:
          return new Date(toLong(bytes.bytes, bytes.offset));
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + f.name());
      }
    }
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    field.checkSortability();

    Object missingValue = null;
    boolean sortMissingLast  = field.sortMissingLast();
    boolean sortMissingFirst = field.sortMissingFirst();

    SortField sf;

    switch (type) {
      case INTEGER:
        if( sortMissingLast ) {
          missingValue = top ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        }
        else if( sortMissingFirst ) {
          missingValue = top ? Integer.MAX_VALUE : Integer.MIN_VALUE;
        }
        sf = new SortField( field.getName(), SortField.Type.INT, top);
        sf.setMissingValue(missingValue);
        return sf;
      
      case FLOAT:
        if( sortMissingLast ) {
          missingValue = top ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
        }
        else if( sortMissingFirst ) {
          missingValue = top ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY;
        }
        sf = new SortField( field.getName(), SortField.Type.FLOAT, top);
        sf.setMissingValue(missingValue);
        return sf;
      
      case DATE: // fallthrough
      case LONG:
        if( sortMissingLast ) {
          missingValue = top ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
        else if( sortMissingFirst ) {
          missingValue = top ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
        sf = new SortField( field.getName(), SortField.Type.LONG, top);
        sf.setMissingValue(missingValue);
        return sf;
        
      case DOUBLE:
        if( sortMissingLast ) {
          missingValue = top ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        }
        else if( sortMissingFirst ) {
          missingValue = top ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
        }
        sf = new SortField( field.getName(), SortField.Type.DOUBLE, top);
        sf.setMissingValue(missingValue);
        return sf;
        
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + field.name);
    }
  }
  
  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      switch (type) {
        case INTEGER:
          return Type.SORTED_SET_INTEGER;
        case LONG:
        case DATE:
          return Type.SORTED_SET_LONG;
        case FLOAT:
          return Type.SORTED_SET_FLOAT;
        case DOUBLE:
          return Type.SORTED_SET_DOUBLE;
        default:
          throw new AssertionError();
      }
    } else {
      switch (type) {
        case INTEGER:
          return Type.INTEGER;
        case LONG:
        case DATE:
          return Type.LONG;
        case FLOAT:
          return Type.FLOAT;
        case DOUBLE:
          return Type.DOUBLE;
        default:
          throw new AssertionError();
      }
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource(qparser);
    switch (type) {
      case INTEGER:
        return new IntFieldSource( field.getName());
      case FLOAT:
        return new FloatFieldSource( field.getName());
      case DATE:
        return new TrieDateFieldSource( field.getName());        
      case LONG:
        return new LongFieldSource( field.getName());
      case DOUBLE:
        return new DoubleFieldSource( field.getName());
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + field.name);
    }
  }


  @Override
  public void write(TextResponseWriter writer, String name, StorableField f) throws IOException {
    writer.writeVal(name, toObject(f));
  }

  @Override
  public boolean isTokenized() {
    return false;
  }

  @Override
  public boolean multiValuedFieldCache() {
    return false;
  }

  /**
   * @return the precisionStep used to index values into the field
   */
  public int getPrecisionStep() {
    return precisionStepArg;
  }

  /**
   * @return the type of this field
   */
  public TrieTypes getType() {
    return type;
  }

  @Override
  public NumericType getNumericType() {
    switch (type) {
      case INTEGER:
        return NumericType.INT;
      case LONG:
      case DATE:
        return NumericType.LONG;
      case FLOAT:
        return NumericType.FLOAT;
      case DOUBLE:
        return NumericType.DOUBLE;
      default:
        throw new AssertionError();
    }
  }

  @Override
  public Query getRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive, boolean maxInclusive) {
    if (field.multiValued() && field.hasDocValues() && !field.indexed()) {
      // for the multi-valued dv-case, the default rangeimpl over toInternal is correct
      return super.getRangeQuery(parser, field, min, max, minInclusive, maxInclusive);
    }
    int ps = precisionStep;
    Query query = null;
    final boolean matchOnly = field.hasDocValues() && !field.indexed();
    switch (type) {
      case INTEGER:
        if (matchOnly) {
          query = new ConstantScoreQuery(DocValuesRangeFilter.newIntRange(field.getName(),
                min == null ? null : Integer.parseInt(min),
                max == null ? null : Integer.parseInt(max),
                minInclusive, maxInclusive));
        } else {
          query = NumericRangeQuery.newIntRange(field.getName(), ps,
                min == null ? null : Integer.parseInt(min),
                max == null ? null : Integer.parseInt(max),
                minInclusive, maxInclusive);
        }
        break;
      case FLOAT:
        if (matchOnly) {
          query = new ConstantScoreQuery(DocValuesRangeFilter.newFloatRange(field.getName(),
                min == null ? null : Float.parseFloat(min),
                max == null ? null : Float.parseFloat(max),
                minInclusive, maxInclusive));
        } else {
          query = NumericRangeQuery.newFloatRange(field.getName(), ps,
                min == null ? null : Float.parseFloat(min),
                max == null ? null : Float.parseFloat(max),
                minInclusive, maxInclusive);
        }
        break;
      case LONG:
        if (matchOnly) {
          query = new ConstantScoreQuery(DocValuesRangeFilter.newLongRange(field.getName(),
                min == null ? null : Long.parseLong(min),
                max == null ? null : Long.parseLong(max),
                minInclusive, maxInclusive));
        } else {
          query = NumericRangeQuery.newLongRange(field.getName(), ps,
                min == null ? null : Long.parseLong(min),
                max == null ? null : Long.parseLong(max),
                minInclusive, maxInclusive);
        }
        break;
      case DOUBLE:
        if (matchOnly) {
          query = new ConstantScoreQuery(DocValuesRangeFilter.newDoubleRange(field.getName(),
                min == null ? null : Double.parseDouble(min),
                max == null ? null : Double.parseDouble(max),
                minInclusive, maxInclusive));
        } else {
          query = NumericRangeQuery.newDoubleRange(field.getName(), ps,
                min == null ? null : Double.parseDouble(min),
                max == null ? null : Double.parseDouble(max),
                minInclusive, maxInclusive);
        }
        break;
      case DATE:
        if (matchOnly) {
          query = new ConstantScoreQuery(DocValuesRangeFilter.newLongRange(field.getName(),
                min == null ? null : dateField.parseMath(null, min).getTime(),
                max == null ? null : dateField.parseMath(null, max).getTime(),
                minInclusive, maxInclusive));
        } else {
          query = NumericRangeQuery.newLongRange(field.getName(), ps,
                min == null ? null : dateField.parseMath(null, min).getTime(),
                max == null ? null : dateField.parseMath(null, max).getTime(),
                minInclusive, maxInclusive);
        }
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field");
    }

    return query;
  }
  
  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    if (!field.indexed() && field.hasDocValues()) {
      // currently implemented as singleton range
      return getRangeQuery(parser, field, externalVal, externalVal, true, true);
    } else {
      return super.getFieldQuery(parser, field, externalVal);
    }
  }

  @Deprecated
  static int toInt(byte[] arr, int offset) {
    return (arr[offset]<<24) | ((arr[offset+1]&0xff)<<16) | ((arr[offset+2]&0xff)<<8) | (arr[offset+3]&0xff);
  }
  
  @Deprecated
  static long toLong(byte[] arr, int offset) {
    int high = (arr[offset]<<24) | ((arr[offset+1]&0xff)<<16) | ((arr[offset+2]&0xff)<<8) | (arr[offset+3]&0xff);
    int low = (arr[offset+4]<<24) | ((arr[offset+5]&0xff)<<16) | ((arr[offset+6]&0xff)<<8) | (arr[offset+7]&0xff);
    return (((long)high)<<32) | (low&0x0ffffffffL);
  }

  @Override
  public String storedToReadable(StorableField f) {
    return toExternal(f);
  }

  @Override
  public String readableToIndexed(String val) {
    // TODO: Numeric should never be handled as String, that may break in future lucene versions! Change to use BytesRef for term texts!
    final BytesRefBuilder bytes = new BytesRefBuilder();
    readableToIndexed(val, bytes);
    return bytes.get().utf8ToString();
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    String s = val.toString();
    try {
      switch (type) {
        case INTEGER:
          NumericUtils.intToPrefixCodedBytes(Integer.parseInt(s), 0, result);
          break;
        case FLOAT:
          NumericUtils.intToPrefixCodedBytes(NumericUtils.floatToSortableInt(Float.parseFloat(s)), 0, result);
          break;
        case LONG:
          NumericUtils.longToPrefixCodedBytes(Long.parseLong(s), 0, result);
          break;
        case DOUBLE:
          NumericUtils.longToPrefixCodedBytes(NumericUtils.doubleToSortableLong(Double.parseDouble(s)), 0, result);
          break;
        case DATE:
          NumericUtils.longToPrefixCodedBytes(dateField.parseMath(null, s).getTime(), 0, result);
          break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
      }
    } catch (NumberFormatException nfe) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
                              "Invalid Number: " + val);
    }
  }

  @Override
  public String toInternal(String val) {
    return readableToIndexed(val);
  }

  static String badFieldString(StorableField f) {
    String s = f.stringValue();
    return "ERROR:SCHEMA-INDEX-MISMATCH,stringValue="+s;
  }

  @Override
  public String toExternal(StorableField f) {
    return (type == TrieTypes.DATE)
      ? dateField.toExternal((Date) toObject(f))
      : toObject(f).toString();
  }

  @Override
  public String indexedToReadable(String _indexedForm) {
    final BytesRef indexedForm = new BytesRef(_indexedForm);
    switch (type) {
      case INTEGER:
        return Integer.toString( NumericUtils.prefixCodedToInt(indexedForm) );
      case FLOAT:
        return Float.toString( NumericUtils.sortableIntToFloat(NumericUtils.prefixCodedToInt(indexedForm)) );
      case LONG:
        return Long.toString( NumericUtils.prefixCodedToLong(indexedForm) );
      case DOUBLE:
        return Double.toString( NumericUtils.sortableLongToDouble(NumericUtils.prefixCodedToLong(indexedForm)) );
      case DATE:
        return dateField.toExternal( new Date(NumericUtils.prefixCodedToLong(indexedForm)) );
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }
  }

  @Override
  public CharsRef indexedToReadable(BytesRef indexedForm, CharsRefBuilder charsRef) {
    final String value;
    switch (type) {
      case INTEGER:
        value = Integer.toString( NumericUtils.prefixCodedToInt(indexedForm) );
        break;
      case FLOAT:
        value = Float.toString( NumericUtils.sortableIntToFloat(NumericUtils.prefixCodedToInt(indexedForm)) );
        break;
      case LONG:
        value = Long.toString( NumericUtils.prefixCodedToLong(indexedForm) );
        break;
      case DOUBLE:
        value = Double.toString( NumericUtils.sortableLongToDouble(NumericUtils.prefixCodedToLong(indexedForm)) );
        break;
      case DATE:
        value = dateField.toExternal( new Date(NumericUtils.prefixCodedToLong(indexedForm)) );
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }
    charsRef.grow(value.length());
    charsRef.setLength(value.length());
    value.getChars(0, charsRef.length(), charsRef.chars(), 0);
    return charsRef.get();
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    switch (type) {
      case INTEGER:
        return NumericUtils.prefixCodedToInt(term);
      case FLOAT:
        return NumericUtils.sortableIntToFloat(NumericUtils.prefixCodedToInt(term));
      case LONG:
        return NumericUtils.prefixCodedToLong(term);
      case DOUBLE:
        return NumericUtils.sortableLongToDouble(NumericUtils.prefixCodedToLong(term));
      case DATE:
        return new Date(NumericUtils.prefixCodedToLong(term));
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }
  }

  @Override
  public String storedToIndexed(StorableField f) {
    final BytesRefBuilder bytes = new BytesRefBuilder();
    final Number val = f.numericValue();
    if (val != null) {
      switch (type) {
        case INTEGER:
          NumericUtils.intToPrefixCodedBytes(val.intValue(), 0, bytes);
          break;
        case FLOAT:
          NumericUtils.intToPrefixCodedBytes(NumericUtils.floatToSortableInt(val.floatValue()), 0, bytes);
          break;
        case LONG: //fallthrough!
        case DATE:
          NumericUtils.longToPrefixCodedBytes(val.longValue(), 0, bytes);
          break;
        case DOUBLE:
          NumericUtils.longToPrefixCodedBytes(NumericUtils.doubleToSortableLong(val.doubleValue()), 0, bytes);
          break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + f.name());
      }
    } else {
      // the following code is "deprecated" and only to support pre-3.2 indexes using the old BinaryField encoding:
      final BytesRef bytesRef = f.binaryValue();
      if (bytesRef==null)
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid field contents: "+f.name());
      switch (type) {
        case INTEGER:
          NumericUtils.intToPrefixCodedBytes(toInt(bytesRef.bytes, bytesRef.offset), 0, bytes);
          break;
        case FLOAT: {
          // WARNING: Code Duplication! Keep in sync with o.a.l.util.NumericUtils!
          // copied from NumericUtils to not convert to/from float two times
          // code in next 2 lines is identical to: int v = NumericUtils.floatToSortableInt(Float.intBitsToFloat(toInt(arr)));
          int v = toInt(bytesRef.bytes, bytesRef.offset);
          if (v<0) v ^= 0x7fffffff;
          NumericUtils.intToPrefixCodedBytes(v, 0, bytes);
          break;
        }
        case LONG: //fallthrough!
        case DATE:
          NumericUtils.longToPrefixCodedBytes(toLong(bytesRef.bytes, bytesRef.offset), 0, bytes);
          break;
        case DOUBLE: {
          // WARNING: Code Duplication! Keep in sync with o.a.l.util.NumericUtils!
          // copied from NumericUtils to not convert to/from double two times
          // code in next 2 lines is identical to: long v = NumericUtils.doubleToSortableLong(Double.longBitsToDouble(toLong(arr)));
          long v = toLong(bytesRef.bytes, bytesRef.offset);
          if (v<0) v ^= 0x7fffffffffffffffL;
          NumericUtils.longToPrefixCodedBytes(v, 0, bytes);
          break;
        }
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + f.name());
      }
    }
    return bytes.get().utf8ToString();
  }
  
  @Override
  public StorableField createField(SchemaField field, Object value, float boost) {
    boolean indexed = field.indexed();
    boolean stored = field.stored();
    boolean docValues = field.hasDocValues();

    if (!indexed && !stored && !docValues) {
      if (log.isTraceEnabled())
        log.trace("Ignoring unindexed/unstored field: " + field);
      return null;
    }
    
    FieldType ft = new FieldType();
    ft.setStored(stored);
    ft.setTokenized(true);
    ft.setOmitNorms(field.omitNorms());
    ft.setIndexOptions(indexed ? getIndexOptions(field, value.toString()) : null);

    switch (type) {
      case INTEGER:
        ft.setNumericType(NumericType.INT);
        break;
      case FLOAT:
        ft.setNumericType(NumericType.FLOAT);
        break;
      case LONG:
        ft.setNumericType(NumericType.LONG);
        break;
      case DOUBLE:
        ft.setNumericType(NumericType.DOUBLE);
        break;
      case DATE:
        ft.setNumericType(NumericType.LONG);
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }
    ft.setNumericPrecisionStep(precisionStep);

    final org.apache.lucene.document.Field f;

    switch (type) {
      case INTEGER:
        int i = (value instanceof Number)
          ? ((Number)value).intValue()
          : Integer.parseInt(value.toString());
        f = new org.apache.lucene.document.IntField(field.getName(), i, ft);
        break;
      case FLOAT:
        float fl = (value instanceof Number)
          ? ((Number)value).floatValue()
          : Float.parseFloat(value.toString());
        f = new org.apache.lucene.document.FloatField(field.getName(), fl, ft);
        break;
      case LONG:
        long l = (value instanceof Number)
          ? ((Number)value).longValue()
          : Long.parseLong(value.toString());
        f = new org.apache.lucene.document.LongField(field.getName(), l, ft);
        break;
      case DOUBLE:
        double d = (value instanceof Number)
          ? ((Number)value).doubleValue()
          : Double.parseDouble(value.toString());
        f = new org.apache.lucene.document.DoubleField(field.getName(), d, ft);
        break;
      case DATE:
        Date date = (value instanceof Date)
          ? ((Date)value)
          : dateField.parseMath(null, value.toString());
        f = new org.apache.lucene.document.LongField(field.getName(), date.getTime(), ft);
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }

    f.setBoost(boost);
    return f;
  }

  @Override
  public List<StorableField> createFields(SchemaField sf, Object value, float boost) {
    if (sf.hasDocValues()) {
      List<StorableField> fields = new ArrayList<>();
      final StorableField field = createField(sf, value, boost);
      fields.add(field);
      
      if (sf.multiValued()) {
        BytesRefBuilder bytes = new BytesRefBuilder();
        readableToIndexed(value.toString(), bytes);
        fields.add(new SortedSetDocValuesField(sf.getName(), bytes.get()));
      } else {
        final long bits;
        if (field.numericValue() instanceof Integer || field.numericValue() instanceof Long) {
          bits = field.numericValue().longValue();
        } else if (field.numericValue() instanceof Float) {
          bits = Float.floatToIntBits(field.numericValue().floatValue());
        } else {
          assert field.numericValue() instanceof Double;
          bits = Double.doubleToLongBits(field.numericValue().doubleValue());
        }
        fields.add(new NumericDocValuesField(sf.getName(), bits));
      }
      
      return fields;
    } else {
      return Collections.singletonList(createField(sf, value, boost));
    }
  }

  public enum TrieTypes {
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    DATE
  }


  static final String INT_PREFIX = new String(new char[]{NumericUtils.SHIFT_START_INT});
  static final String LONG_PREFIX = new String(new char[]{NumericUtils.SHIFT_START_LONG});

  /** expert internal use, subject to change.
   * Returns null if no prefix or prefix not needed, or the prefix of the main value of a trie field
   * that indexes multiple precisions per value.
   */
  public static String getMainValuePrefix(org.apache.solr.schema.FieldType ft) {
    if (ft instanceof TrieField) {
      final TrieField trie = (TrieField)ft;
      if (trie.precisionStep  == Integer.MAX_VALUE)
        return null;
      switch (trie.type) {
        case INTEGER:
        case FLOAT:
          return INT_PREFIX;
        case LONG:
        case DOUBLE:
        case DATE:
          return LONG_PREFIX;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + trie.type);
      }
    }
    return null;
  }

  @Override
  public void checkSchemaField(final SchemaField field) {
  }
}

class TrieDateFieldSource extends LongFieldSource {

  public TrieDateFieldSource(String field) {
    super(field);
  }

  @Override
  public String description() {
    return "date(" + field + ')';
  }

  @Override
  protected MutableValueLong newMutableValueLong() {
    return new MutableValueDate();
  }

  @Override
  public Date longToObject(long val) {
    return new Date(val);
  }

  @Override
  public String longToString(long val) {
    return TrieField.dateField.toExternal(longToObject(val));
  }

  @Override
  public long externalToLong(String extVal) {
    return TrieField.dateField.parseMath(null, extVal).getTime();
  }

}


