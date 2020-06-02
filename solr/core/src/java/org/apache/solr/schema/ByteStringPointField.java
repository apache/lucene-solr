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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DocTermsIndexDocValues;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;


/**
 * {@code PointField} implementation for {@code BigInteger} values.
 * Allowable values are limited to those that a PointField can store (128 bits)
 *
 * @see PointField
 * @see org.apache.lucene.document.BinaryPoint
 */
@SuppressWarnings("ALL")
public class ByteStringPointField extends PointField {
  /**
   * The max number of bytes per dimension: 128 bits = 16 bytes
   */
  public static final int MAX_NUM_BYTES = 16;

  private static final int HEX = 16;
  private static final int DEC = 10;
  private static final String HEX_PREFIX = "0x";
  private static final BigInteger ONE = BigInteger.ONE;
  private static final Locale locale = Locale.US;
  private static final String NUM_BYTES_ARG = "numbytes";

  private BigInteger minValue;
  private BigInteger maxValue;

  private FieldType fieldType = null;

  /** returns the number of bytes the current type is configured with */
  public int getFieldLength() {
    return fieldType.pointNumBytes();
  }

  /** returns the minimum value this instance of ByteStringPointField can have */
  public BigInteger getMinValue() {
    return minValue;
  }

  /** returns the maximum value this instance of ByteStringPointField can have*/
  public BigInteger getMaxValue() {
    return maxValue;
  }

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    int fieldLength;
    final String numString = args.get(NUM_BYTES_ARG);
    if (numString != null) {
      args.remove(NUM_BYTES_ARG);
      try {
        fieldLength = Integer.valueOf(numString);
      } catch (Exception ex) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.format(locale,
            "Length option must be a valid number between 1 and %d got %s", MAX_NUM_BYTES, numString ));
      }
      if ((fieldLength > 0) && (fieldLength <= MAX_NUM_BYTES)) {
        fieldType = getFieldType(1, fieldLength);
        minValue = BigInteger.ONE.shiftLeft(fieldLength * 8 - 1).negate();
        maxValue = BigInteger.ONE.shiftLeft(fieldLength * 8 - 1).subtract(BigInteger.ONE);
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.format(locale,
            "Length option must be a valid number between 2 and %d got %s", MAX_NUM_BYTES, numString ));
      }
    }
  }

  /**
   * Checks to make sure this bigint can be stored by a Point Field (within 128 bits)
   *
   * @param number bigint to check
   * @return TRUE if value can be stored in a Point, FALSE otherwise
   */
  private boolean withinRange(final BigInteger number) {
    assert (number != null);
    return ((number.compareTo(maxValue) <= 0) && (number.compareTo(minValue) >= 0));
  }

  /**
   * Utility to parse BigInt numbers of hex or decimal, if hex, removes the 0x prefix from the string
   *
   * @param num String to parse
   * @return BigInteger
   * @throws NumberFormatException if number contains unexpected digits/chars
   */
  private static BigInteger parseBigInt(final String num) {
    int radix = DEC;
    if (num.length() > 2) {
      radix = (num.substring(0, 1).compareToIgnoreCase(HEX_PREFIX) == 0) ? HEX : DEC;
    }
    return new BigInteger(radix == HEX ? num.substring(2) : num, radix);
  }

  private static String renderFieldName(String fieldName) {
    return String.format(Locale.US, "%s", (null == fieldName ? "" : " for field " + fieldName));
  }

  /**
   * Wrapper for {@link BigInteger#BigInteger(String, int)}} that throws a BAD_REQUEST error if the input is not valid
   *
   * @param fieldName used in any exception, may be null
   * @param val       string to parse, NPE if null
   */
  private static BigInteger parseBigIntFromUser(final String fieldName, final String val) {
    if (val == null) {
      throw new NullPointerException("Invalid input" + renderFieldName(fieldName));
    }
    try {
      return parseBigInt(val);
    } catch (NumberFormatException e) {
      final String msg = String.format(locale, "Invalid Number: %s %s", val, renderFieldName(fieldName));
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
    }
  }

  /**
   * Wrapper for {@link BigInteger#BigInteger(String, int)}} that throws a BAD_REQUEST error if the input is not valid
   *
   * @param fieldName used in any exception, may be null
   * @param val       string to parse, null if val was null
   */
  private BigInteger parseNumberFromUser(final String fieldName, final String val) {
    if (val == null) return null;
    final BigInteger number = parseBigIntFromUser(fieldName, val);
    if (withinRange(number)) {
      return number;
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.format(locale,
          "%s is too big or small to be stored in %s", number.toString(), fieldName));
    }
  }

  private BigInteger parseNumberFromUserDefault(final String fieldName, final String val,
                                                       final BigInteger bigintDefault) {
    return (val == null) ? bigintDefault : parseNumberFromUser(fieldName, val);
  }

  private BytesRef getSortOrderedByteRef(final BigInteger bigint) {
    return new BytesRef(getSortOrderedBytes(bigint));
  }

  /**
   * Returns byte array proccesed so that negative values sort natively
   *
   * @param bigint number to extract bytes from
   * @return byte[field num byes]
   */
  private byte[] getSortOrderedBytes(final BigInteger bigint) {
    final byte[] bytes = new byte[getFieldLength()];
    encodeDimension(bigint, bytes, 0);
    return bytes;
  }

  private static FieldType getFieldType(final int numDimensions, final int numBytes) {
    final FieldType type = new FieldType();
    type.setDimensions(numDimensions,numBytes);
    type.freeze();
    return type;
  }

  /**
   * Encode single BigInteger dimension
   */
  private void encodeDimension(BigInteger value, byte[] dest, int offset) {
    NumericUtils.bigIntToSortableBytes(value, getFieldLength(), dest, offset);
  }

  /**
   * Decode single BigInteger dimension
   */
  private BigInteger decodeDimension(byte[] value, int offset) {
    return decodeDimension(value, offset, getFieldLength());
  }

  private static BigInteger decodeDimension(byte[] value, int offset, int fieldLength) {
    return NumericUtils.sortableBytesToBigInt(value, offset, fieldLength);
  }

  @Override
  /*
   * Native type for a ByteStringPointField is BigInteger
   */
  public Object toNativeType(Object val) {
    BigInteger retval = null;
    if (val == null) return null;
    if (val instanceof BigInteger) retval = (BigInteger) val;
    if (val instanceof Number) retval = BigInteger.valueOf(((Number) val).longValue());
    if (val instanceof CharSequence) {
      try {
        retval = parseBigInt(val.toString());
      } catch (NumberFormatException e) {
        // modeled on LongPoint, which attempts to parse a float if long parsing fails
        retval = new BigDecimal(val.toString()).toBigInteger();
      }
    }
    if (val instanceof byte[]) {
      final byte[] sourceBytes = (byte[]) val;
      retval = new BigInteger(sourceBytes);
    }

    if (retval != null) {
      if (withinRange(retval)) {
        // make sure bigint is backed by the configured size of our type
        return retval;
      } else {
        throw new NumberFormatException(String.format(locale, "%s is too big or small to be stored in a %s",
            retval.toString(), getTypeName()));
      }
    }
    return super.toNativeType(val);
  }

  @Override
  public Query getPointRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive,
                                  boolean maxInclusive) {
    BigInteger actualMin = parseNumberFromUserDefault(field.getName(), min, minValue);
    if (!minInclusive) {
      if (actualMin.equals(maxValue)) return new MatchNoDocsQuery();
      actualMin = actualMin.add(ONE);
    }
    final byte[] actualMinBytes = getSortOrderedBytes(actualMin);

    BigInteger actualMax = parseNumberFromUserDefault(field.getName(), max, maxValue);
    if (!maxInclusive) {
      if (actualMax.equals(minValue)) return new MatchNoDocsQuery();
      actualMax = actualMax.subtract(ONE);
    }
    final byte[] actualMaxBytes = getSortOrderedBytes(actualMax);

    return BinaryPoint.newRangeQuery(field.getName(), actualMinBytes, actualMaxBytes);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    if (term == null) {
      throw new AssertionError(String.format(locale, "Unexpected state. Field: '%s'", sf));
    }
    return decodeDimension(term.bytes, term.offset);
  }

  @Override
  public Object toObject(IndexableField f) {
    if ((f == null) || (f.binaryValue() == null)) {
      throw new AssertionError(String.format(locale, "Unexpected state. Field: '%s'", f));
    }
    final BytesRef byteref = f.binaryValue();
    return decodeDimension(byteref.bytes, byteref.offset);
  }

  @Override
  protected Query getExactQuery(SchemaField field, String externalVal) {
    final BigInteger bigint = parseNumberFromUser(field.getName(), externalVal);
    final byte[] bytes = getSortOrderedBytes(bigint);
    return BinaryPoint.newExactQuery(field.getName(), bytes);
  }

  @Override
  public Query getSetQuery(QParser parser, SchemaField field, Collection<String> externalVal) {
    assert externalVal.size() > 0;
    final String fieldName = field.getName();
    if (!field.indexed()) {
      return super.getSetQuery(parser, field, externalVal);
    }

    final byte[][] values = externalVal.stream()
        .map(x -> parseNumberFromUser(fieldName, x))
        .map(x ->getSortOrderedBytes(x))
        .toArray(byte[][]::new);

    return BinaryPoint.newSetQuery(field.getName(), values);
  }

  @Override
  protected String indexedToReadable(BytesRef indexedForm) {
    return decodeDimension(indexedForm.bytes, indexedForm.offset).toString();
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    result.grow(getFieldLength());
    result.setLength(getFieldLength());
    encodeDimension(parseBigIntFromUser(null, val.toString()), result.bytes(),
        0);
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    return sf.multiValued() ? Type.SORTED_SET_BINARY : Type.BINARY;
  }

  @Override
  public List<IndexableField> createFields(SchemaField sf, Object value) {
    if (!isFieldUsed(sf)) {
      return Collections.emptyList();
    }
    final List<IndexableField> fields = new ArrayList<>(3);
    // cache parsed value so we don't have to reparse multiple times
    BytesRef bytesRef = null;

    if (sf.indexed()) {
      final IndexableField indexField = createField(sf, value);
      bytesRef = indexField.binaryValue();
      fields.add(indexField);
    }

    if (sf.hasDocValues()) {
      if (bytesRef == null) {
        bytesRef = getSortOrderedByteRef((BigInteger) toNativeType(value));
      }
      if (!sf.multiValued()) {
        fields.add(new SortedDocValuesField(sf.getName(), bytesRef));
      } else {
        fields.add(new SortedSetDocValuesField(sf.getName(), bytesRef));
      }
    }

    if (sf.stored()) {
      if (bytesRef == null) {
        bytesRef = getSortOrderedByteRef((BigInteger) toNativeType(value));
      }
      fields.add(getStoredField(sf, bytesRef));
    }
    return fields;
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    final byte[] bytes = new byte[getFieldLength()];
    encodeDimension((BigInteger) toNativeType(value), bytes, 0);
    return new BinaryPoint(field.getName(), bytes, fieldType);
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    // is this even worthwhile if the ValueSource is not numeric?
    return new ByteStringFieldSource(field.getName(), fieldType.pointNumBytes());
  }

  @Override
  protected ValueSource getSingleValueSource(SortedNumericSelector.Type choice, SchemaField field) {
    // is this even worthwhile if the ValueSource is not numeric?
    if (!field.multiValued()) {
      // single value matches any selector
      return getValueSource(field, null);
    } else {
      return new MultiValuedByteStringFieldSource(field.getName(), getFieldLength());
    }
  }

  @Override
  protected Query getDocValuesRangeQuery(QParser parser, SchemaField field, String min, String max,
                                         boolean minInclusive, boolean maxInclusive) {
    assert field.hasDocValues() && (field.getType().isPointField() || !field.multiValued());

    final BigInteger minBigInt = parseNumberFromUserDefault(field.getName(), min, minValue);
    final BytesRef minBytes = getSortOrderedByteRef(minBigInt);

    final BigInteger maxBigInt = parseNumberFromUserDefault(field.getName(), max, maxValue);
    final BytesRef maxBytes = getSortOrderedByteRef(maxBigInt);

    if (field.multiValued()) {
      return SortedSetDocValuesField.newSlowRangeQuery(field.getName(),
          minBytes, maxBytes, minInclusive, maxInclusive);
    } else {
      return SortedDocValuesField.newSlowRangeQuery(field.getName(),
          minBytes, maxBytes, minInclusive, maxInclusive);
    }
  }

  @Override
  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), (BytesRef) value);
  }

  @Override
  public SortField getSortField(final SchemaField field, final boolean reverse) {
    SortField sortField = null;
    if (field.multiValued()) {
      MultiValueSelector selector = getDefaultMultiValueSelectorForSort(field, reverse);
      if (null != selector) {
        sortField = getSortedSetSortField(field, selector.getSortedSetSelectorType(),
            reverse, SortField.STRING_FIRST, SortField.STRING_LAST);
      }
    } else {
      sortField = new SortField(field.getName(),
          new FieldComparatorSource() {
            @Override
            public FieldComparator.TermOrdValComparator newComparator
                (final String fieldname, final int numHits, final int sortPos, final boolean reversed) {
              return new FieldComparator.TermOrdValComparator(numHits, fieldname, true);
            }
          },
          reverse);
    }
    return sortField;
  }


  /**
   * An extension to BytesRefFieldSource that does not attempt to cast its binary
   * value to UTF8 - there are plenty of bit patterns that may cause an exception doing this
   */
  private static class ByteStringFieldSource extends BytesRefFieldSource {
    private final int fieldLength;

    public int getFieldLength() {
      return fieldLength;
    }

    ByteStringFieldSource(String field, int fieldLength) {
      super(field);
      this.fieldLength = fieldLength;
    }

    public String description() {
      return String.format(locale, "bytestring('%s')", field);
    }

    @Override
    public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
      return new DocTermsIndexDocValues(this, readerContext, field) {
        @Override
        protected String toTerm(String readableValue) {
          return readableValue;
        }

        @Override
        public Object objectVal(int doc) throws IOException {
          if (getOrdForDoc(doc) == -1) {
            return null;
          }
          final BytesRef term = termsIndex.binaryValue();
          return ByteStringPointField.decodeDimension(term.bytes, term.offset, fieldLength);
        }

        @Override
        public String toString(int doc) throws IOException {
          return String.format(locale, "%s=%s", description(), strVal(doc));
        }

        @Override
        public String strVal(int doc) throws IOException {
          if (getOrdForDoc(doc) == -1) {
            return null;
          }
          final Object obj = objectVal(doc);
          return obj == null ? "" : obj.toString();
        }
      };
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      ByteStringFieldSource that = (ByteStringFieldSource) o;
      return Objects.equals(getField(), that.getField());
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), getField());
    }

    @Override
    public String toString() {
      return String.format(locale, "ByteStringFieldSource{field='%s'}", field);
    }
  }

  /**
   * Obtains bytestring field values and gives a single-valued ValueSource view of a field.
   */
  @SuppressWarnings("SpellCheckingInspection")
  private static class MultiValuedByteStringFieldSource extends ByteStringFieldSource {
    MultiValuedByteStringFieldSource(String field, int fieldLengfth) {
      super(field,fieldLengfth);
      Objects.requireNonNull(field, "Field is required to create a MultiValuedLongFieldSource");
    }

    @Override
    public SortField getSortField(boolean reverse) {
      return new SortedSetSortField(field, reverse);
    }

    @Override
    public String description() {
      return String.format(locale, "bytestring(%s,min)", field);
    }

    @Override
    public String toString() {
      return String.format(locale, "MultiValuedByteStringFieldSource{field='%s', length=%d}",
          field, getFieldLength());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      MultiValuedByteStringFieldSource that = (MultiValuedByteStringFieldSource) o;
      return Objects.equals(getField(), that.getField());
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), getField());
    }
  }

}
