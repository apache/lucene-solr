/**
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

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.search.*;
import org.apache.lucene.search.cache.CachedArrayCreator;
import org.apache.lucene.search.cache.DoubleValuesCreator;
import org.apache.lucene.search.cache.FloatValuesCreator;
import org.apache.lucene.search.cache.IntValuesCreator;
import org.apache.lucene.search.cache.LongValuesCreator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.mutable.MutableValueDate;
import org.apache.lucene.util.mutable.MutableValueLong;
import org.apache.solr.analysis.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.function.*;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Date;

/**
 * Provides field types to support for Lucene's {@link NumericField}.
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
public class TrieField extends FieldType {
  public static final int DEFAULT_PRECISION_STEP = 8;

  protected int precisionStepArg = TrieField.DEFAULT_PRECISION_STEP;  // the one passed in or defaulted
  protected int precisionStep;     // normalized
  protected TrieTypes type;
  protected Object missingValue;

  
  /**
   * Used for handling date types following the same semantics as DateField
   */
  static final DateField dateField = new DateField();

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
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
        type = TrieTypes.valueOf(t.toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Invalid type specified in schema.xml for field: " + args.get("name"), e);
      }
    }
  
    
    CharFilterFactory[] filterFactories = new CharFilterFactory[0];
    TokenFilterFactory[] tokenFilterFactories = new TokenFilterFactory[0];
    analyzer = new TokenizerChain(filterFactories, new TrieTokenizerFactory(type, precisionStep), tokenFilterFactories);
    // for query time we only need one token, so we use the biggest possible precisionStep:
    queryAnalyzer = new TokenizerChain(filterFactories, new TrieTokenizerFactory(type, Integer.MAX_VALUE), tokenFilterFactories);
  }

  @Override
  public Object toObject(Fieldable f) {
    if (f instanceof NumericField) {
      final Number val = ((NumericField) f).getNumericValue();
      if (val==null) return badFieldString(f);
      return (type == TrieTypes.DATE) ? new Date(val.longValue()) : val;
    } else {
      // the following code is "deprecated" and only to support pre-3.2 indexes using the old BinaryField encoding:
      final byte[] arr = f.getBinaryValue();
      if (arr==null) return badFieldString(f);
      switch (type) {
        case INTEGER:
          return toInt(arr);
        case FLOAT:
          return Float.intBitsToFloat(toInt(arr));
        case LONG:
          return toLong(arr);
        case DOUBLE:
          return Double.longBitsToDouble(toLong(arr));
        case DATE:
          return new Date(toLong(arr));
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + f.name());
      }
    }
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    field.checkSortability();

    int flags = CachedArrayCreator.CACHE_VALUES_AND_BITS;
    Object missingValue = null;
    boolean sortMissingLast  = field.sortMissingLast();
    boolean sortMissingFirst = field.sortMissingFirst();
    
    switch (type) {
      case INTEGER:
        if( sortMissingLast ) {
          missingValue = top ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        }
        else if( sortMissingFirst ) {
          missingValue = top ? Integer.MAX_VALUE : Integer.MIN_VALUE;
        }
        return new SortField( new IntValuesCreator( field.getName(), 
            FieldCache.NUMERIC_UTILS_INT_PARSER, flags ), top).setMissingValue( missingValue );
      
      case FLOAT:
        if( sortMissingLast ) {
          missingValue = top ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
        }
        else if( sortMissingFirst ) {
          missingValue = top ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY;
        }
        return new SortField( new FloatValuesCreator( field.getName(), 
            FieldCache.NUMERIC_UTILS_FLOAT_PARSER, flags ), top).setMissingValue( missingValue );
      
      case DATE: // fallthrough
      case LONG:
        if( sortMissingLast ) {
          missingValue = top ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
        else if( sortMissingFirst ) {
          missingValue = top ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
        return new SortField( new LongValuesCreator( field.getName(), 
            FieldCache.NUMERIC_UTILS_LONG_PARSER, flags ), top).setMissingValue( missingValue );
        
      case DOUBLE:
        if( sortMissingLast ) {
          missingValue = top ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        }
        else if( sortMissingFirst ) {
          missingValue = top ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
        }
        return new SortField( new DoubleValuesCreator( field.getName(), 
            FieldCache.NUMERIC_UTILS_DOUBLE_PARSER, flags ), top).setMissingValue( missingValue );
        
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + field.name);
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource(qparser);
    int flags = CachedArrayCreator.CACHE_VALUES_AND_BITS;
    switch (type) {
      case INTEGER:
        return new IntFieldSource( new IntValuesCreator( field.getName(), FieldCache.NUMERIC_UTILS_INT_PARSER, flags ) );
      case FLOAT:
        return new FloatFieldSource( new FloatValuesCreator( field.getName(), FieldCache.NUMERIC_UTILS_FLOAT_PARSER, flags ));
      case DATE:
        return new TrieDateFieldSource( new LongValuesCreator( field.getName(), FieldCache.NUMERIC_UTILS_LONG_PARSER, flags ));        
      case LONG:
        return new LongFieldSource( new LongValuesCreator( field.getName(), FieldCache.NUMERIC_UTILS_LONG_PARSER, flags ) );
      case DOUBLE:
        return new DoubleFieldSource( new DoubleValuesCreator( field.getName(), FieldCache.NUMERIC_UTILS_DOUBLE_PARSER, flags ));
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + field.name);
    }
  }


  @Override
  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException {
    writer.writeVal(name, toObject(f));
  }

  @Override
  public boolean isTokenized() {
    return true;
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
  public Query getRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive, boolean maxInclusive) {
    int ps = precisionStep;
    Query query = null;
    switch (type) {
      case INTEGER:
        query = NumericRangeQuery.newIntRange(field.getName(), ps,
                min == null ? null : Integer.parseInt(min),
                max == null ? null : Integer.parseInt(max),
                minInclusive, maxInclusive);
        break;
      case FLOAT:
        query = NumericRangeQuery.newFloatRange(field.getName(), ps,
                min == null ? null : Float.parseFloat(min),
                max == null ? null : Float.parseFloat(max),
                minInclusive, maxInclusive);
        break;
      case LONG:
        query = NumericRangeQuery.newLongRange(field.getName(), ps,
                min == null ? null : Long.parseLong(min),
                max == null ? null : Long.parseLong(max),
                minInclusive, maxInclusive);
        break;
      case DOUBLE:
        query = NumericRangeQuery.newDoubleRange(field.getName(), ps,
                min == null ? null : Double.parseDouble(min),
                max == null ? null : Double.parseDouble(max),
                minInclusive, maxInclusive);
        break;
      case DATE:
        query = NumericRangeQuery.newLongRange(field.getName(), ps,
                min == null ? null : dateField.parseMath(null, min).getTime(),
                max == null ? null : dateField.parseMath(null, max).getTime(),
                minInclusive, maxInclusive);
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field");
    }

    return query;
  }

  @Deprecated
  static int toInt(byte[] arr) {
    return (arr[0]<<24) | ((arr[1]&0xff)<<16) | ((arr[2]&0xff)<<8) | (arr[3]&0xff);
  }
  
  @Deprecated
  static long toLong(byte[] arr) {
    int high = (arr[0]<<24) | ((arr[1]&0xff)<<16) | ((arr[2]&0xff)<<8) | (arr[3]&0xff);
    int low = (arr[4]<<24) | ((arr[5]&0xff)<<16) | ((arr[6]&0xff)<<8) | (arr[7]&0xff);
    return (((long)high)<<32) | (low&0x0ffffffffL);
  }

  @Override
  public String storedToReadable(Fieldable f) {
    return toExternal(f);
  }

  @Override
  public String readableToIndexed(String val) {
    // TODO: Numeric should never be handled as String, that may break in future lucene versions! Change to use BytesRef for term texts!
    final BytesRef bytes = new BytesRef(NumericUtils.BUF_SIZE_LONG);
    readableToIndexed(val, bytes);
    return bytes.utf8ToString();
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRef result) {
    String s = val.toString();
    switch (type) {
      case INTEGER:
        NumericUtils.intToPrefixCoded(Integer.parseInt(s), 0, result);
        break;
      case FLOAT:
        NumericUtils.intToPrefixCoded(NumericUtils.floatToSortableInt(Float.parseFloat(s)), 0, result);
        break;
      case LONG:
        NumericUtils.longToPrefixCoded(Long.parseLong(s), 0, result);
        break;
      case DOUBLE:
        NumericUtils.longToPrefixCoded(NumericUtils.doubleToSortableLong(Double.parseDouble(s)), 0, result);
        break;
      case DATE:
        NumericUtils.longToPrefixCoded(dateField.parseMath(null, s).getTime(), 0, result);
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }
  }

  @Override
  public String toInternal(String val) {
    return readableToIndexed(val);
  }


  static String badFieldString(Fieldable f) {
    String s = f.stringValue();
    return "ERROR:SCHEMA-INDEX-MISMATCH,stringValue="+s;
  }

  @Override
  public String toExternal(Fieldable f) {
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
  public CharsRef indexedToReadable(BytesRef indexedForm, CharsRef charsRef) {
    final char[] value;
    switch (type) {
      case INTEGER:
        value = Integer.toString( NumericUtils.prefixCodedToInt(indexedForm) ).toCharArray();
        break;
      case FLOAT:
        value = Float.toString( NumericUtils.sortableIntToFloat(NumericUtils.prefixCodedToInt(indexedForm)) ).toCharArray();
        break;
      case LONG:
        value = Long.toString( NumericUtils.prefixCodedToLong(indexedForm) ).toCharArray();
        break;
      case DOUBLE:
        value = Double.toString( NumericUtils.sortableLongToDouble(NumericUtils.prefixCodedToLong(indexedForm)) ).toCharArray();
        break;
      case DATE:
        value = dateField.toExternal( new Date(NumericUtils.prefixCodedToLong(indexedForm)) ).toCharArray();
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }
    charsRef.copy(value, 0, value.length);
    return charsRef;
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
  public String storedToIndexed(Fieldable f) {
    final BytesRef bytes = new BytesRef(NumericUtils.BUF_SIZE_LONG);
    if (f instanceof NumericField) {
      final Number val = ((NumericField) f).getNumericValue();
      if (val==null)
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid field contents: "+f.name());
      switch (type) {
        case INTEGER:
          NumericUtils.intToPrefixCoded(val.intValue(), 0, bytes);
          break;
        case FLOAT:
          NumericUtils.intToPrefixCoded(NumericUtils.floatToSortableInt(val.floatValue()), 0, bytes);
          break;
        case LONG: //fallthrough!
        case DATE:
          NumericUtils.longToPrefixCoded(val.longValue(), 0, bytes);
          break;
        case DOUBLE:
          NumericUtils.longToPrefixCoded(NumericUtils.doubleToSortableLong(val.doubleValue()), 0, bytes);
          break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + f.name());
      }
    } else {
      // the following code is "deprecated" and only to support pre-3.2 indexes using the old BinaryField encoding:
      final byte[] arr = f.getBinaryValue();
      if (arr==null)
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid field contents: "+f.name());
      switch (type) {
        case INTEGER:
          NumericUtils.intToPrefixCoded(toInt(arr), 0, bytes);
          break;
        case FLOAT: {
          // WARNING: Code Duplication! Keep in sync with o.a.l.util.NumericUtils!
          // copied from NumericUtils to not convert to/from float two times
          // code in next 2 lines is identical to: int v = NumericUtils.floatToSortableInt(Float.intBitsToFloat(toInt(arr)));
          int v = toInt(arr);
          if (v<0) v ^= 0x7fffffff;
          NumericUtils.intToPrefixCoded(v, 0, bytes);
          break;
        }
        case LONG: //fallthrough!
        case DATE:
          NumericUtils.longToPrefixCoded(toLong(arr), 0, bytes);
          break;
        case DOUBLE: {
          // WARNING: Code Duplication! Keep in sync with o.a.l.util.NumericUtils!
          // copied from NumericUtils to not convert to/from double two times
          // code in next 2 lines is identical to: long v = NumericUtils.doubleToSortableLong(Double.longBitsToDouble(toLong(arr)));
          long v = toLong(arr);
          if (v<0) v ^= 0x7fffffffffffffffL;
          NumericUtils.longToPrefixCoded(v, 0, bytes);
          break;
        }
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + f.name());
      }
    }
    return bytes.utf8ToString();
  }

  @Override
  public Fieldable createField(SchemaField field, Object value, float boost) {
    boolean indexed = field.indexed();
    boolean stored = field.stored();

    if (!indexed && !stored) {
      if (log.isTraceEnabled())
        log.trace("Ignoring unindexed/unstored field: " + field);
      return null;
    }

    final NumericField f = new NumericField(field.getName(), precisionStep, stored ? Field.Store.YES : Field.Store.NO, indexed);
    switch (type) {
      case INTEGER:
        int i = (value instanceof Number)
          ? ((Number)value).intValue()
          : Integer.parseInt(value.toString());
        f.setIntValue(i);
        break;
      case FLOAT:
        float fl = (value instanceof Number)
          ? ((Number)value).floatValue()
          : Float.parseFloat(value.toString());
        f.setFloatValue(fl);
        break;
      case LONG:
        long l = (value instanceof Number)
          ? ((Number)value).longValue()
          : Long.parseLong(value.toString());
        f.setLongValue(l);
        break;
      case DOUBLE:
        double d = (value instanceof Number)
          ? ((Number)value).doubleValue()
          : Double.parseDouble(value.toString());
        f.setDoubleValue(d);
        break;
      case DATE:
        Date date = (value instanceof Date)
          ? ((Date)value)
          : dateField.parseMath(null, value.toString());
        f.setLongValue(date.getTime());
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }

    f.setOmitNorms(field.omitNorms());
    f.setIndexOptions(getIndexOptions(field, value.toString()));
    f.setBoost(boost);
    return f;
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
  public static String getMainValuePrefix(FieldType ft) {
    if (ft instanceof TrieDateField)
      ft = ((TrieDateField) ft).wrappedField;
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

class TrieDateFieldSource extends LongFieldSource {

  public TrieDateFieldSource(LongValuesCreator creator) {
    super(creator);
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
  public Object longToObject(long val) {
    return new Date(val);
  }

  @Override
  public long externalToLong(String extVal) {
    return TrieField.dateField.parseMath(null, extVal).getTime();
  }
}


