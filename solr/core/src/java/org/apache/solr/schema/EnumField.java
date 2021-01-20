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
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.legacy.LegacyFieldType;
import org.apache.solr.legacy.LegacyIntField;
import org.apache.solr.legacy.LegacyNumericRangeQuery;
import org.apache.solr.legacy.LegacyNumericType;
import org.apache.solr.legacy.LegacyNumericUtils;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Field type for support of string values with custom sort order.
 * @deprecated use {@link EnumFieldType} instead.
 */
@Deprecated
public class EnumField extends AbstractEnumField {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected static final int DEFAULT_PRECISION_STEP = Integer.MAX_VALUE;
  
  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return Type.SORTED_SET_INTEGER;
    } else {
      return Type.LEGACY_INTEGER;
    }
  }

  @Override
  protected Query getSpecializedRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive, boolean maxInclusive) {
    Integer minValue = enumMapping.stringValueToIntValue(min);
    Integer maxValue = enumMapping.stringValueToIntValue(max);

    if (field.multiValued() && field.hasDocValues() && !field.indexed()) {
      // for the multi-valued dv-case, the default rangeimpl over toInternal is correct
      return super.getSpecializedRangeQuery(parser, field, minValue.toString(), maxValue.toString(), minInclusive, maxInclusive);
    }
    Query query = null;
    final boolean matchOnly = field.hasDocValues() && !field.indexed();
    if (matchOnly) {
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
      query = new ConstantScoreQuery(NumericDocValuesField.newSlowRangeQuery(field.getName(), lowerValue, upperValue));
    } else {
      query = LegacyNumericRangeQuery.newIntRange(field.getName(), DEFAULT_PRECISION_STEP,
          min == null ? null : minValue,
          max == null ? null : maxValue,
          minInclusive, maxInclusive);
    }

    return query;
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    final String s = val.toString();
    if (s == null)
      return;

    final Integer intValue = enumMapping.stringValueToIntValue(s);
    LegacyNumericUtils.intToPrefixCoded(intValue, 0, result);
  }

  @Override
  public String indexedToReadable(String indexedForm) {
    if (indexedForm == null)
      return null;
    final BytesRef bytesRef = new BytesRef(indexedForm);
    final Integer intValue = LegacyNumericUtils.prefixCodedToInt(bytesRef);
    return enumMapping.intValueToStringValue(intValue);
  }

  @Override
  public CharsRef indexedToReadable(BytesRef input, CharsRefBuilder output) {
    final Integer intValue = LegacyNumericUtils.prefixCodedToInt(input);
    final String stringValue = enumMapping.intValueToStringValue(intValue);
    output.grow(stringValue.length());
    output.setLength(stringValue.length());
    stringValue.getChars(0, output.length(), output.chars(), 0);
    return output.get();
  }

  @Override
  public EnumFieldValue toObject(SchemaField sf, BytesRef term) {
    final Integer intValue = LegacyNumericUtils.prefixCodedToInt(term);
    final String stringValue = enumMapping.intValueToStringValue(intValue);
    return new EnumFieldValue(intValue, stringValue);
  }

  @Override
  public String storedToIndexed(IndexableField f) {
    final Number val = f.numericValue();
    if (val == null)
      return null;
    final BytesRefBuilder bytes = new BytesRefBuilder();
    LegacyNumericUtils.intToPrefixCoded(val.intValue(), 0, bytes);
    return bytes.get().utf8ToString();
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    final boolean indexed = field.indexed();
    final boolean stored = field.stored();
    final boolean docValues = field.hasDocValues();

    if (!indexed && !stored && !docValues) {
      if (log.isTraceEnabled())
        log.trace("Ignoring unindexed/unstored field: {}", field);
      return null;
    }
    final Integer intValue = enumMapping.stringValueToIntValue(value.toString());
    if (intValue == null || intValue.equals(EnumMapping.DEFAULT_VALUE)) {
      String exceptionMessage = String.format(Locale.ENGLISH, "Unknown value for enum field: %s, value: %s",
          field.getName(), value.toString());
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,  exceptionMessage);
    }

    final LegacyFieldType newType = new LegacyFieldType();

    newType.setTokenized(field.isTokenized());
    newType.setStored(field.stored());
    newType.setOmitNorms(field.omitNorms());
    newType.setIndexOptions(field.indexOptions());
    newType.setStoreTermVectors(field.storeTermVector());
    newType.setStoreTermVectorOffsets(field.storeTermOffsets());
    newType.setStoreTermVectorPositions(field.storeTermPositions());
    newType.setStoreTermVectorPayloads(field.storeTermPayloads());
    newType.setNumericType(LegacyNumericType.INT);
    newType.setNumericPrecisionStep(DEFAULT_PRECISION_STEP);

    return new LegacyIntField(field.getName(), intValue.intValue(), newType);
  }

  @Override
  public List<IndexableField> createFields(SchemaField sf, Object value) {
    if (sf.hasDocValues()) {
      List<IndexableField> fields = new ArrayList<>();
      final IndexableField field = createField(sf, value);
      fields.add(field);

      if (sf.multiValued()) {
        BytesRefBuilder bytes = new BytesRefBuilder();
        readableToIndexed(enumMapping.stringValueToIntValue(value.toString()).toString(), bytes);
        fields.add(new SortedSetDocValuesField(sf.getName(), bytes.toBytesRef()));
      } else {
        final long bits = field.numericValue().intValue();
        fields.add(new NumericDocValuesField(sf.getName(), bits));
      }
      return fields;
    } else {
      return Collections.singletonList(createField(sf, value));
    }
  }
}
