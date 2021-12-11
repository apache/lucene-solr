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
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.legacy.LegacyDoubleField;
import org.apache.solr.legacy.LegacyFieldType;
import org.apache.solr.legacy.LegacyFloatField;
import org.apache.solr.legacy.LegacyIntField;
import org.apache.solr.legacy.LegacyLongField;
import org.apache.solr.legacy.LegacyNumericRangeQuery;
import org.apache.solr.legacy.LegacyNumericType;
import org.apache.solr.legacy.LegacyNumericUtils;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
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
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.apache.solr.util.DateMathParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides field types to support for Lucene's {@link
 * org.apache.solr.legacy.LegacyIntField}, {@link org.apache.solr.legacy.LegacyLongField}, {@link org.apache.solr.legacy.LegacyFloatField} and
 * {@link org.apache.solr.legacy.LegacyDoubleField}.
 * See {@link org.apache.solr.legacy.LegacyNumericRangeQuery} for more details.
 * It supports integer, float, long, double and date types.
 * <p>
 * For each number being added to this field, multiple terms are generated as per the algorithm described in the above
 * link. The possible number of terms increases dramatically with lower precision steps. For
 * the fast range search to work, trie fields must be indexed.
 * <p>
 * Trie fields are sortable in numerical order and can be used in function queries.
 * <p>
 * Note that if you use a precisionStep of 32 for int/float and 64 for long/double/date, then multiple terms will not be
 * generated, range search will be no faster than any other number field, but sorting will still be possible.
 *
 *
 * @see org.apache.solr.legacy.LegacyNumericRangeQuery
 * @since solr 1.4
 * @deprecated Trie fields are deprecated as of Solr 7.0
 */
@Deprecated
public class TrieField extends NumericFieldType {
  public static final int DEFAULT_PRECISION_STEP = 8;

  protected int precisionStepArg = TrieField.DEFAULT_PRECISION_STEP;  // the one passed in or defaulted
  protected int precisionStep;     // normalized

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
        type = NumberType.valueOf(t.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Invalid type specified in schema.xml for field: " + args.get("name"), e);
      }
    }
  }

  @Override
  public Object toObject(IndexableField f) {
    final Number val = f.numericValue();
    if (val != null) {

      if (f.fieldType().stored() == false && f.fieldType().docValuesType() == DocValuesType.NUMERIC ) {
        long bits = val.longValue();
        switch (type) {
          case INTEGER:
            return (int)bits;
          case FLOAT:
            return Float.intBitsToFloat((int)bits);
          case LONG:
            return bits;
          case DOUBLE:
            return Double.longBitsToDouble(bits);
          case DATE:
            return new Date(bits);
          default:
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + f.name());
        }
      }

      // normal stored case
      return (type == NumberType.DATE) ? new Date(val.longValue()) : val;
    } else {
      // multi-valued numeric docValues currently use SortedSet on the indexed terms.
      BytesRef term = f.binaryValue();
      switch (type) {
        case INTEGER:
          return LegacyNumericUtils.prefixCodedToInt(term);
        case FLOAT:
          return NumericUtils.sortableIntToFloat(LegacyNumericUtils.prefixCodedToInt(term));
        case LONG:
          return LegacyNumericUtils.prefixCodedToLong(term);
        case DOUBLE:
          return NumericUtils.sortableLongToDouble(LegacyNumericUtils.prefixCodedToLong(term));
        case DATE:
          return new Date(LegacyNumericUtils.prefixCodedToLong(term));
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + f.name());
      }
    }

  }

  @Override
  public SortField getSortField(SchemaField field, boolean reverse) {
    // NOTE: can't use getNumericSort because our multivalued case is special: we use SortedSet

    if (field.multiValued()) {
      MultiValueSelector selector = field.type.getDefaultMultiValueSelectorForSort(field, reverse);
      if (null != selector) {
        return getSortedSetSortField(field, selector.getSortedSetSelectorType(),
                                     // yes: we really want Strings here, regardless of NumberType
                                     reverse, SortField.STRING_FIRST, SortField.STRING_LAST);
      }
    }
    
    // else...
    // either single valued, or don't support implicit multi selector
    // (in which case let getSortField() give the error)
    NumberType type = getNumberType();
    return getSortField(field, type.sortType, reverse, type.sortMissingLow, type.sortMissingHigh);
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
          return Type.LEGACY_INTEGER;
        case LONG:
        case DATE:
          return Type.LEGACY_LONG;
        case FLOAT:
          return Type.LEGACY_FLOAT;
        case DOUBLE:
          return Type.LEGACY_DOUBLE;
        default:
          throw new AssertionError();
      }
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
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
  public final ValueSource getSingleValueSource(MultiValueSelector choice, SchemaField field, QParser parser) {
    // trivial base case
    if (!field.multiValued()) {
      // single value matches any selector
      return getValueSource(field, parser);
    }

    // See LUCENE-6709
    if (! field.hasDocValues()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              "docValues='true' is required to select '" + choice.toString() +
                              "' value from multivalued field ("+ field.getName() +") at query time");
    }
    
    // multivalued Trie fields all use SortedSetDocValues, so we give a clean error if that's
    // not supported by the specified choice, else we delegate to a helper
    SortedSetSelector.Type selectorType = choice.getSortedSetSelectorType();
    if (null == selectorType) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              choice.toString() + " is not a supported option for picking a single value"
                              + " from the multivalued field: " + field.getName() +
                              " (type: " + this.getTypeName() + ")");
    }
    
    return getSingleValueSource(selectorType, field);
  }

  /**
   * Helper method that will only be called for multivalued Trie fields that have doc values.
   * Default impl throws an error indicating that selecting a single value from this multivalued 
   * field is not supported for this field type
   *
   * @param choice the selector Type to use, will never be null
   * @param field the field to use, guaranteed to be multivalued.
   * @see #getSingleValueSource(MultiValueSelector,SchemaField,QParser) 
   */
  protected ValueSource getSingleValueSource(SortedSetSelector.Type choice, SchemaField field) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                            "Can not select a single value for multivalued field: " + field.getName()
                            + " (single valued field selection not supported for type: " + this.getTypeName()
                            + ")");
  }
  
  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
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

  @Override
  protected Query getSpecializedRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive, boolean maxInclusive) {
    if (field.multiValued() && field.hasDocValues() && !field.indexed()) {
      // for the multi-valued dv-case, the default rangeimpl over toInternal is correct
      return super.getSpecializedRangeQuery(parser, field, min, max, minInclusive, maxInclusive);
    }
    int ps = precisionStep;
    Query query;
    
    if (field.hasDocValues() && !field.indexed()) {
      return getDocValuesRangeQuery(parser, field, min, max, minInclusive, maxInclusive);
    }

    switch (type) {
      case INTEGER:
        query = LegacyNumericRangeQuery.newIntRange(field.getName(), ps,
            min == null ? null : parseIntFromUser(field.getName(), min),
            max == null ? null : parseIntFromUser(field.getName(), max),
            minInclusive, maxInclusive);
        break;
      case FLOAT:
        query = LegacyNumericRangeQuery.newFloatRange(field.getName(), ps,
            min == null ? null : parseFloatFromUser(field.getName(), min),
            max == null ? null : parseFloatFromUser(field.getName(), max),
            minInclusive, maxInclusive);
        break;
      case LONG:
        query = LegacyNumericRangeQuery.newLongRange(field.getName(), ps,
            min == null ? null : parseLongFromUser(field.getName(), min),
            max == null ? null : parseLongFromUser(field.getName(), max),
            minInclusive, maxInclusive);
        break;
      case DOUBLE:
        query = LegacyNumericRangeQuery.newDoubleRange(field.getName(), ps,
            min == null ? null : parseDoubleFromUser(field.getName(), min),
            max == null ? null : parseDoubleFromUser(field.getName(), max),
            minInclusive, maxInclusive);
        break;
      case DATE:
        query = LegacyNumericRangeQuery.newLongRange(field.getName(), ps,
            min == null ? null : DateMathParser.parseMath(null, min).getTime(),
            max == null ? null : DateMathParser.parseMath(null, max).getTime(),
            minInclusive, maxInclusive);
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field");
    }
    return query;
  }

  @Override
  public String storedToReadable(IndexableField f) {
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
    switch (type) {
      case INTEGER:
        LegacyNumericUtils.intToPrefixCoded(parseIntFromUser(null, s), 0, result);
        break;
      case FLOAT:
        LegacyNumericUtils.intToPrefixCoded(NumericUtils.floatToSortableInt(parseFloatFromUser(null, s)), 0, result);
        break;
      case LONG:
        LegacyNumericUtils.longToPrefixCoded(parseLongFromUser(null, s), 0, result);
        break;
      case DOUBLE:
        LegacyNumericUtils.longToPrefixCoded(NumericUtils.doubleToSortableLong(parseDoubleFromUser(null, s)), 0, result);
        break;
      case DATE:
        LegacyNumericUtils.longToPrefixCoded(DateMathParser.parseMath(null, s).getTime(), 0, result);
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }
  }

  @Override
  public String toInternal(String val) {
    return readableToIndexed(val);
  }

  static String badFieldString(IndexableField f) {
    String s = f.stringValue();
    return "ERROR:SCHEMA-INDEX-MISMATCH,stringValue="+s;
  }

  @Override
  public String toExternal(IndexableField f) {
    return (type == NumberType.DATE)
      ? ((Date) toObject(f)).toInstant().toString()
      : toObject(f).toString();
  }

  @Override
  public String indexedToReadable(String _indexedForm) {
    final BytesRef indexedForm = new BytesRef(_indexedForm);
    switch (type) {
      case INTEGER:
        return Integer.toString( LegacyNumericUtils.prefixCodedToInt(indexedForm) );
      case FLOAT:
        return Float.toString( NumericUtils.sortableIntToFloat(LegacyNumericUtils.prefixCodedToInt(indexedForm)) );
      case LONG:
        return Long.toString( LegacyNumericUtils.prefixCodedToLong(indexedForm) );
      case DOUBLE:
        return Double.toString( NumericUtils.sortableLongToDouble(LegacyNumericUtils.prefixCodedToLong(indexedForm)) );
      case DATE:
        return Instant.ofEpochMilli(LegacyNumericUtils.prefixCodedToLong(indexedForm)).toString();
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }
  }

  @Override
  public CharsRef indexedToReadable(BytesRef indexedForm, CharsRefBuilder charsRef) {
    final String value;
    switch (type) {
      case INTEGER:
        value = Integer.toString( LegacyNumericUtils.prefixCodedToInt(indexedForm) );
        break;
      case FLOAT:
        value = Float.toString( NumericUtils.sortableIntToFloat(LegacyNumericUtils.prefixCodedToInt(indexedForm)) );
        break;
      case LONG:
        value = Long.toString( LegacyNumericUtils.prefixCodedToLong(indexedForm) );
        break;
      case DOUBLE:
        value = Double.toString( NumericUtils.sortableLongToDouble(LegacyNumericUtils.prefixCodedToLong(indexedForm)) );
        break;
      case DATE:
        value = Instant.ofEpochMilli(LegacyNumericUtils.prefixCodedToLong(indexedForm)).toString();
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
        return LegacyNumericUtils.prefixCodedToInt(term);
      case FLOAT:
        return NumericUtils.sortableIntToFloat(LegacyNumericUtils.prefixCodedToInt(term));
      case LONG:
        return LegacyNumericUtils.prefixCodedToLong(term);
      case DOUBLE:
        return NumericUtils.sortableLongToDouble(LegacyNumericUtils.prefixCodedToLong(term));
      case DATE:
        return new Date(LegacyNumericUtils.prefixCodedToLong(term));
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }
  }

  @Override
  public String storedToIndexed(IndexableField f) {
    final BytesRefBuilder bytes = new BytesRefBuilder();
    storedToIndexed(f, bytes);
    return bytes.get().utf8ToString();
  }

  private void storedToIndexed(IndexableField f, final BytesRefBuilder bytes) {
    final Number val = f.numericValue();
    if (val != null) {
      switch (type) {
        case INTEGER:
          LegacyNumericUtils.intToPrefixCoded(val.intValue(), 0, bytes);
          break;
        case FLOAT:
          LegacyNumericUtils.intToPrefixCoded(NumericUtils.floatToSortableInt(val.floatValue()), 0, bytes);
          break;
        case LONG: //fallthrough!
        case DATE:
          LegacyNumericUtils.longToPrefixCoded(val.longValue(), 0, bytes);
          break;
        case DOUBLE:
          LegacyNumericUtils.longToPrefixCoded(NumericUtils.doubleToSortableLong(val.doubleValue()), 0, bytes);
          break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + f.name());
      }
    } else {
      // the old BinaryField encoding is no longer supported
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid field contents: "+f.name());
    }
  }
  
  @Override
  public IndexableField createField(SchemaField field, Object value) {
    boolean indexed = field.indexed();
    boolean stored = field.stored();
    boolean docValues = field.hasDocValues();

    if (!indexed && !stored && !docValues) {
      if (log.isTraceEnabled())
        log.trace("Ignoring unindexed/unstored field: {}", field);
      return null;
    }
    
    LegacyFieldType ft = new LegacyFieldType();
    ft.setStored(stored);
    ft.setTokenized(true);
    ft.setOmitNorms(field.omitNorms());
    ft.setIndexOptions(field.indexOptions());

    switch (type) {
      case INTEGER:
        ft.setNumericType(LegacyNumericType.INT);
        break;
      case FLOAT:
        ft.setNumericType(LegacyNumericType.FLOAT);
        break;
      case LONG:
        ft.setNumericType(LegacyNumericType.LONG);
        break;
      case DOUBLE:
        ft.setNumericType(LegacyNumericType.DOUBLE);
        break;
      case DATE:
        ft.setNumericType(LegacyNumericType.LONG);
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
        f = new LegacyIntField(field.getName(), i, ft);
        break;
      case FLOAT:
        float fl = (value instanceof Number)
          ? ((Number)value).floatValue()
          : Float.parseFloat(value.toString());
        f = new LegacyFloatField(field.getName(), fl, ft);
        break;
      case LONG:
        long l = (value instanceof Number)
          ? ((Number)value).longValue()
          : Long.parseLong(value.toString());
        f = new LegacyLongField(field.getName(), l, ft);
        break;
      case DOUBLE:
        double d = (value instanceof Number)
          ? ((Number)value).doubleValue()
          : Double.parseDouble(value.toString());
        f = new LegacyDoubleField(field.getName(), d, ft);
        break;
      case DATE:
        Date date = (value instanceof Date)
          ? ((Date)value)
          : DateMathParser.parseMath(null, value.toString());
        f = new LegacyLongField(field.getName(), date.getTime(), ft);
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }

    return f;
  }

  @Override
  public List<IndexableField> createFields(SchemaField sf, Object value) {
    if (sf.hasDocValues()) {
      List<IndexableField> fields = new ArrayList<>();
      final IndexableField field = createField(sf, value);
      fields.add(field);
      
      if (sf.multiValued()) {
        BytesRefBuilder bytes = new BytesRefBuilder();
        storedToIndexed(field, bytes);
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
      return Collections.singletonList(createField(sf, value));
    }
  }

  static final String INT_PREFIX = new String(new char[]{LegacyNumericUtils.SHIFT_START_INT});
  static final String LONG_PREFIX = new String(new char[]{LegacyNumericUtils.SHIFT_START_LONG});

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

}

@Deprecated
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
    return longToObject(val).toInstant().toString();
  }

  @Override
  public long externalToLong(String extVal) {
    return DateMathParser.parseMath(null, extVal).getTime();
  }
}


