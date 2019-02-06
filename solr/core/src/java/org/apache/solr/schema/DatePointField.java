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

import java.time.Instant;
import java.util.Collection;
import java.util.Date;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedLongFieldSource;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.mutable.MutableValueDate;
import org.apache.lucene.util.mutable.MutableValueLong;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader;
import org.apache.solr.update.processor.TimestampUpdateProcessorFactory;
import org.apache.solr.util.DateMathParser;

/**
 * FieldType that can represent any Date/Time with millisecond precision.
 * <p>
 * Date Format for the XML, incoming and outgoing:
 * </p>
 * <blockquote>
 * A date field shall be of the form 1995-12-31T23:59:59Z
 * The trailing "Z" designates UTC time and is mandatory
 * (See below for an explanation of UTC).
 * Optional fractional seconds are allowed, as long as they do not end
 * in a trailing 0 (but any precision beyond milliseconds will be ignored).
 * All other parts are mandatory.
 * </blockquote>
 * <p>
 * This format was derived to be standards compliant (ISO 8601) and is a more
 * restricted form of the
 * <a href="http://www.w3.org/TR/xmlschema-2/#dateTime-canonical-representation">canonical
 * representation of dateTime</a> from XML schema part 2.  Examples...
 * </p>
 * <ul>
 *   <li>1995-12-31T23:59:59Z</li>
 *   <li>1995-12-31T23:59:59.9Z</li>
 *   <li>1995-12-31T23:59:59.99Z</li>
 *   <li>1995-12-31T23:59:59.999Z</li>
 * </ul>
 * <p>
 * Note that <code>DatePointField</code> is lenient with regards to parsing fractional
 * seconds that end in trailing zeros and will ensure that those values
 * are indexed in the correct canonical format.
 * </p>
 * <p>
 * This FieldType also supports incoming "Date Math" strings for computing
 * values by adding/rounding internals of time relative either an explicit
 * datetime (in the format specified above) or the literal string "NOW",
 * ie: "NOW+1YEAR", "NOW/DAY", "1995-12-31T23:59:59.999Z+5MINUTES", etc...
 * -- see {@link DateMathParser} for more examples.
 * </p>
 * <p>
 * <b>NOTE:</b> Although it is possible to configure a <code>DatePointField</code>
 * instance with a default value of "<code>NOW</code>" to compute a timestamp
 * of when the document was indexed, this is not advisable when using SolrCloud
 * since each replica of the document may compute a slightly different value.
 * {@link TimestampUpdateProcessorFactory} is recommended instead.
 * </p>
 *
 * <p>
 * Explanation of "UTC"...
 * </p>
 * <blockquote>
 * "In 1970 the Coordinated Universal Time system was devised by an
 * international advisory group of technical experts within the International
 * Telecommunication Union (ITU).  The ITU felt it was best to designate a
 * single abbreviation for use in all languages in order to minimize
 * confusion.  Since unanimous agreement could not be achieved on using
 * either the English word order, CUT, or the French word order, TUC, the
 * acronym UTC was chosen as a compromise."
 * </blockquote>
 *
 * @see TrieDateField
 * @see PointField
 */
public class DatePointField extends PointField implements DateValueFieldType {

  public DatePointField() {
    type = NumberType.DATE;
  }


  @Override
  public Object toNativeType(Object val) {
    if (val instanceof CharSequence) {
      return DateMathParser.parseMath(null, val.toString());
    }
    return super.toNativeType(val);
  }

  @Override
  public Query getPointRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive, boolean maxInclusive) {
    long actualMin, actualMax;
    if (min == null) {
      actualMin = Long.MIN_VALUE;
    } else {
      actualMin = DateMathParser.parseMath(null, min).getTime();
      if (!minInclusive) {
        if (actualMin == Long.MAX_VALUE) return new MatchNoDocsQuery();
        actualMin++;
      }
    }
    if (max == null) {
      actualMax = Long.MAX_VALUE;
    } else {
      actualMax = DateMathParser.parseMath(null, max).getTime();
      if (!maxInclusive) {
        if (actualMax == Long.MIN_VALUE) return new MatchNoDocsQuery();
        actualMax--;
      }
    }
    return LongPoint.newRangeQuery(field.getName(), actualMin, actualMax);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return new Date(LongPoint.decodeDimension(term.bytes, term.offset));
  }

  @Override
  public Object toObject(IndexableField f) {
    final Number val = f.numericValue();
    if (val != null) {
      return new Date(val.longValue());
    } else {
      throw new AssertionError("Unexpected state. Field: '" + f + "'");
    }
  }

  @Override
  protected Query getExactQuery(SchemaField field, String externalVal) {
    return LongPoint.newExactQuery(field.getName(), DateMathParser.parseMath(null, externalVal).getTime());
  }

  @Override
  public Query getSetQuery(QParser parser, SchemaField field, Collection<String> externalVals) {
    assert externalVals.size() > 0;
    if (!field.indexed()) {
      return super.getSetQuery(parser, field, externalVals);
    }
    long[] values = new long[externalVals.size()];
    int i = 0;
    for (String val:externalVals) {
      values[i] = DateMathParser.parseMath(null, val).getTime();
      i++;
    }
    return LongPoint.newSetQuery(field.getName(), values);
  }

  @Override
  protected String indexedToReadable(BytesRef indexedForm) {
    return Instant.ofEpochMilli(LongPoint.decodeDimension(indexedForm.bytes, indexedForm.offset)).toString();
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    Date date = (Date) toNativeType(val.toString());
    result.grow(Long.BYTES);
    result.setLength(Long.BYTES);
    LongPoint.encodeDimension(date.getTime(), result.bytes(), 0);
  }

  @Override
  public UninvertingReader.Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return null;
    } else {
      return UninvertingReader.Type.LONG_POINT;
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    field.checkFieldCacheSource();
    return new DatePointFieldSource(field.getName());
  }

  @Override
  protected ValueSource getSingleValueSource(SortedNumericSelector.Type choice, SchemaField field) {
    return new MultiValuedLongFieldSource(field.getName(), choice);
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    Date date = (value instanceof Date)
        ? ((Date)value)
        : DateMathParser.parseMath(null, value.toString());
    return new LongPoint(field.getName(), date.getTime());
  }

  @Override
  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), ((Date) this.toNativeType(value)).getTime());
  }
}

class DatePointFieldSource extends LongFieldSource {

  public DatePointFieldSource(String field) {
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
