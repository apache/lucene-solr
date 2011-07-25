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
import org.apache.lucene.search.*;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.solr.analysis.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.response.XMLWriter;
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
 * @version $Id$
 * @see org.apache.lucene.search.NumericRangeQuery
 * @since solr 1.4
 */
public class TrieField extends FieldType {
  public static final int DEFAULT_PRECISION_STEP = 8;

  protected int precisionStepArg = TrieField.DEFAULT_PRECISION_STEP;  // the one passed in or defaulted
  protected int precisionStep;     // normalized
  protected TrieTypes type;

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

    switch (type) {
      case INTEGER:
        return new SortField(field.getName(), FieldCache.NUMERIC_UTILS_INT_PARSER, top);
      case FLOAT:
        return new SortField(field.getName(), FieldCache.NUMERIC_UTILS_FLOAT_PARSER, top);
      case DATE: // fallthrough
      case LONG:
        return new SortField(field.getName(), FieldCache.NUMERIC_UTILS_LONG_PARSER, top);
      case DOUBLE:
        return new SortField(field.getName(), FieldCache.NUMERIC_UTILS_DOUBLE_PARSER, top);
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + field.name);
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource(qparser);
    switch (type) {
      case INTEGER:
        return new IntFieldSource(field.getName(), FieldCache.NUMERIC_UTILS_INT_PARSER);
      case FLOAT:
        return new FloatFieldSource(field.getName(), FieldCache.NUMERIC_UTILS_FLOAT_PARSER);
      case DATE:
        return new TrieDateFieldSource(field.getName(), FieldCache.NUMERIC_UTILS_LONG_PARSER);        
      case LONG:
        return new LongFieldSource(field.getName(), FieldCache.NUMERIC_UTILS_LONG_PARSER);
      case DOUBLE:
        return new DoubleFieldSource(field.getName(), FieldCache.NUMERIC_UTILS_DOUBLE_PARSER);
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + field.name);
    }
  }

  @Override
  public void write(XMLWriter xmlWriter, String name, Fieldable f) throws IOException {
    xmlWriter.writeVal(name, toObject(f));
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
    switch (type) {
      case INTEGER:
        return NumericUtils.intToPrefixCoded(Integer.parseInt(val));
      case FLOAT:
        return NumericUtils.intToPrefixCoded(NumericUtils.floatToSortableInt(Float.parseFloat(val)));
      case LONG:
        return NumericUtils.longToPrefixCoded(Long.parseLong(val));
      case DOUBLE:
        return NumericUtils.longToPrefixCoded(NumericUtils.doubleToSortableLong(Double.parseDouble(val)));
      case DATE:
        return NumericUtils.longToPrefixCoded(dateField.parseMath(null, val).getTime());
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
  public String indexedToReadable(String indexedForm) {
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
  public String storedToIndexed(Fieldable f) {
    if (f instanceof NumericField) {
      final Number val = ((NumericField) f).getNumericValue();
      if (val==null)
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid field contents: "+f.name());
      switch (type) {
        case INTEGER:
          return NumericUtils.intToPrefixCoded(val.intValue());
        case FLOAT:
          return NumericUtils.intToPrefixCoded(NumericUtils.floatToSortableInt(val.floatValue()));
        case LONG: //fallthrough!
        case DATE:
          return NumericUtils.longToPrefixCoded(val.longValue());
        case DOUBLE:
          return NumericUtils.longToPrefixCoded(NumericUtils.doubleToSortableLong(val.doubleValue()));
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
          return NumericUtils.intToPrefixCoded(toInt(arr));
        case FLOAT: {
          // WARNING: Code Duplication! Keep in sync with o.a.l.util.NumericUtils!
          // copied from NumericUtils to not convert to/from float two times
          // code in next 2 lines is identical to: int v = NumericUtils.floatToSortableInt(Float.intBitsToFloat(toInt(arr)));
          int v = toInt(arr);
          if (v<0) v ^= 0x7fffffff;
          return NumericUtils.intToPrefixCoded(v);
        }
        case LONG: //fallthrough!
        case DATE:
          return NumericUtils.longToPrefixCoded(toLong(arr));
        case DOUBLE: {
          // WARNING: Code Duplication! Keep in sync with o.a.l.util.NumericUtils!
          // copied from NumericUtils to not convert to/from double two times
          // code in next 2 lines is identical to: long v = NumericUtils.doubleToSortableLong(Double.longBitsToDouble(toLong(arr)));
          long v = toLong(arr);
          if (v<0) v ^= 0x7fffffffffffffffL;
          return NumericUtils.longToPrefixCoded(v);
        }
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + f.name());
      }
    }
  }

  @Override
  public Fieldable createField(SchemaField field, String externalVal, float boost) {
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
        f.setIntValue(Integer.parseInt(externalVal));
        break;
      case FLOAT:
        f.setFloatValue(Float.parseFloat(externalVal));
        break;
      case LONG:
        f.setLongValue(Long.parseLong(externalVal));
        break;
      case DOUBLE:
        f.setDoubleValue(Double.parseDouble(externalVal));
        break;
      case DATE:
        f.setLongValue(dateField.parseMath(null, externalVal).getTime());
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }

    f.setOmitNorms(field.omitNorms());
    f.setIndexOptions(getIndexOptions(field, externalVal));
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

  public TrieDateFieldSource(String field, FieldCache.LongParser parser) {
    super(field, parser);
  }

  public TrieDateFieldSource(String field) {
    super(field);
  }

  @Override
  public String description() {
    return "date(" + field + ')';
  }

  @Override
  public long externalToLong(String extVal) {
    return TrieField.dateField.parseMath(null, extVal).getTime();
  }
}


