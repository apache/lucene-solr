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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedIntFieldSource;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Field type for support of string values with custom sort order.
 */
public class EnumFieldType extends AbstractEnumField {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public Type getUninversionType(SchemaField sf) {
    return null;
  }
  
  @Override
  protected Query getSpecializedRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive, boolean maxInclusive) {
    Integer minValue = enumMapping.stringValueToIntValue(min);
    Integer maxValue = enumMapping.stringValueToIntValue(max);

    if (field.indexed()) {
      BytesRef minBytes = null;
      if (min != null) {
        byte[] bytes = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(minValue, bytes, 0);
        minBytes = new BytesRef(bytes);
      }
      BytesRef maxBytes = null;
      if (max != null) {
        byte[] bytes = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(maxValue, bytes, 0);
        maxBytes = new BytesRef(bytes);
      }
      return new TermRangeQuery(field.getName(), minBytes, maxBytes, minInclusive, maxInclusive);

    } else {
      long lowerValue = Long.MIN_VALUE;
      long upperValue = Long.MAX_VALUE;
      if (minValue != null) {
        lowerValue = minValue.longValue();
        if (minInclusive == false) {
          ++lowerValue;
        }
      }
      if (maxValue != null) {
        upperValue = maxValue.longValue();
        if (maxInclusive == false) {
          --upperValue;
        }
      }
      if (field.multiValued()) {
        return new ConstantScoreQuery(SortedNumericDocValuesField.newSlowRangeQuery
                                      (field.getName(), lowerValue, upperValue));
      } else {
        return new ConstantScoreQuery(NumericDocValuesField.newSlowRangeQuery
                                      (field.getName(), lowerValue, upperValue));
      }
    }
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    final String s = val.toString();
    if (s == null)
      return;

    result.grow(Integer.BYTES);
    result.setLength(Integer.BYTES);
    final Integer intValue = enumMapping.stringValueToIntValue(s);
    NumericUtils.intToSortableBytes(intValue, result.bytes(), 0);
  }

  @Override
  public String indexedToReadable(String indexedForm) {
    if (indexedForm == null)
      return null;
    final BytesRef bytesRef = new BytesRef(indexedForm);
    final Integer intValue = NumericUtils.sortableBytesToInt(bytesRef.bytes, 0);
    return enumMapping.intValueToStringValue(intValue);
  }

  @Override
  public CharsRef indexedToReadable(BytesRef input, CharsRefBuilder output) {
    final Integer intValue = NumericUtils.sortableBytesToInt(input.bytes, 0);
    final String stringValue = enumMapping.intValueToStringValue(intValue);
    output.grow(stringValue.length());
    output.setLength(stringValue.length());
    stringValue.getChars(0, output.length(), output.chars(), 0);
    return output.get();
  }

  @Override
  public EnumFieldValue toObject(SchemaField sf, BytesRef term) {
    final Integer intValue = NumericUtils.sortableBytesToInt(term.bytes, 0);
    final String stringValue = enumMapping.intValueToStringValue(intValue);
    return new EnumFieldValue(intValue, stringValue);
  }

  @Override
  public String storedToIndexed(IndexableField f) {
    final Number val = f.numericValue();
    if (val == null)
      return null;
    final BytesRefBuilder bytes = new BytesRefBuilder();
    bytes.grow(Integer.BYTES);
    bytes.setLength(Integer.BYTES);
    NumericUtils.intToSortableBytes(val.intValue(), bytes.bytes(), 0);
    return bytes.get().utf8ToString();
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    final Integer intValue = enumMapping.stringValueToIntValue(value.toString());
    if (intValue == null || intValue.equals(EnumMapping.DEFAULT_VALUE)) {
      String exceptionMessage = String.format(Locale.ENGLISH, "Unknown value for enum field: %s, value: %s",
          field.getName(), value.toString());
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,  exceptionMessage);
    }

    org.apache.lucene.document.FieldType newType = new org.apache.lucene.document.FieldType();
    newType.setTokenized(false);
    newType.setStored(field.stored());
    newType.setOmitNorms(field.omitNorms());
    newType.setIndexOptions(field.indexOptions());
    newType.setStoreTermVectors(field.storeTermVector());
    newType.setStoreTermVectorOffsets(field.storeTermOffsets());
    newType.setStoreTermVectorPositions(field.storeTermPositions());
    newType.setStoreTermVectorPayloads(field.storeTermPayloads());
    
    byte[] bytes = new byte[Integer.BYTES];
    NumericUtils.intToSortableBytes(intValue, bytes, 0);         
    return new Field(field.getName(), bytes, newType) {
      @Override public Number numericValue() {
        return NumericUtils.sortableBytesToInt(((BytesRef)fieldsData).bytes, 0);
      }
    };
  }

  @Override
  public List<IndexableField> createFields(SchemaField sf, Object value) {
    if ( ! sf.hasDocValues()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
          getClass().getSimpleName() + " requires docValues=\"true\".");
    }
    final IndexableField field = createField(sf, value);
    final List<IndexableField> fields = new ArrayList<>();
    fields.add(field);
    final long longValue = field.numericValue().longValue();
    if (sf.multiValued()) {
      fields.add(new SortedNumericDocValuesField(sf.getName(), longValue));
    } else {
      fields.add(new NumericDocValuesField(sf.getName(), longValue));
    }
    return fields;
  }

  @Override
  public final ValueSource getSingleValueSource(MultiValueSelector choice, SchemaField field, QParser parser) {
    if ( ! field.multiValued()) {           // trivial base case
      return getValueSource(field, parser); // single value matches any selector
    }
    SortedNumericSelector.Type selectorType = choice.getSortedNumericSelectorType();
    if (null == selectorType) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          choice.toString() + " is not a supported option for picking a single value"
              + " from the multivalued field: " + field.getName() +
              " (type: " + this.getTypeName() + ")");
    }
    return new MultiValuedIntFieldSource(field.getName(), selectorType);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    final SortField result = getNumericSort(field, NumberType.INTEGER, top);
    if (null == result.getMissingValue()) {
      // special case 'enum' default behavior: assume missing values are "below" all enum values
      result.setMissingValue(Integer.MIN_VALUE);
    }
    return result;
  }
}
