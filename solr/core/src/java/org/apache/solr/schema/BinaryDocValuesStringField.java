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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.FieldCacheSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueStr;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

/**
 * A special UTF8 field type designed to be configured as multiValued=false, indexed=false,
 * docValues=true ("stored" may be configured true or false, but "true" may be superfluous to
 * useDocValuesAsStored=true). Not appropriate to use for faceting or sorting.
 *
 * Particularly useful for export, where docValues are a prerequisite, and useDocValuesAsStored=true.
 * For these use cases, term dictionary indirection of {@link SortedDocValues} is not necessary, and
 * imposes a limitation on term size.
 *
 * This class directly extends {@link PrimitiveFieldType} (i.e., as opposed to extending
 * {@link StrField}) because there are a number of places in the extant codebase where it is
 * assumed that instances of {@link StrField} must have docValues type of either
 * {@link DocValuesType#SORTED} or {@link DocValuesType#SORTED_SET}.
 *
 * A maximum field length threshold may be explicitly set via the {@value #MAX_LENGTH_ARGNAME}
 * configuration option. By default there is no limit.
 */
public class BinaryDocValuesStringField extends PrimitiveFieldType {

  private static final String MAX_LENGTH_ARGNAME = "maxLength";
  private static final int DEFAULT_MAX_LENGTH = Integer.MAX_VALUE;

  protected int maxLength;

  @Override
  protected void init(IndexSchema schema, Map<String,String> args) {
    String tmp = args.remove(MAX_LENGTH_ARGNAME);
    this.maxLength = tmp == null ? DEFAULT_MAX_LENGTH : Integer.parseInt(tmp);
    super.init(schema, args);
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    if (field.indexed() || field.stored() || !field.hasDocValues() || field.multiValued()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "field type "+BinaryDocValuesStringField.class.getName()+
          " must have docValues, be single-valued, and must be neither indexed nor stored");
    } else {
      IndexableField docval;
      final BytesRef bytes;
      try {
        bytes = valueToIndexedBytesRef(value);
      } catch (IOException ex) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "for field "+field.getName(), ex);
      }
      docval = new BinaryDocValuesField(field.getName(), bytes);
      return Collections.singletonList(docval);
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    field.checkFieldCacheSource();
    return new BinaryStringValueSource(field.getName(), field.getType());
  }

  protected static class BinaryStringValueSource extends FieldCacheSource {

    private final FieldType fieldType;

    private BinaryStringValueSource(String field, FieldType fieldType) {
      super(field);
      this.fieldType = fieldType;
    }
    @Override
    public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
      return new BinaryStringFunctionValues(field, fieldType, DocValues.getBinary(readerContext.reader(), field));
    }
  }

  protected static class BinaryStringFunctionValues extends FunctionValues {

    private final String fieldName;
    private final FieldType fieldType;
    private final BinaryDocValues binaryValues;
    private final CharsRefBuilder cref = new CharsRefBuilder();
    private int lastDocID = -1;

    private BinaryStringFunctionValues(String fieldName, FieldType fieldType, BinaryDocValues binaryValues) {
      this.fieldName = fieldName;
      this.fieldType = fieldType;
      this.binaryValues = binaryValues;
    }
    private BytesRef getValueForDoc(int doc) throws IOException {
      if (doc < lastDocID) {
        throw new IllegalArgumentException("docs were sent out-of-order: lastDocID=" + lastDocID + " vs docID=" + doc);
      }
      lastDocID = doc;
      int curDocID = binaryValues.docID();
      if (doc > curDocID) {
        curDocID = binaryValues.advance(doc);
      }
      if (doc == curDocID) {
        return binaryValues.binaryValue();
      } else {
        return null;
      }
    }

    @Override
    public boolean exists(int doc) throws IOException {
      return getValueForDoc(doc) != null;
    }

    @Override
    public boolean bytesVal(int doc, BytesRefBuilder target) throws IOException {
      BytesRef value = getValueForDoc(doc);
      if (value == null || value.length == 0) {
        return false;
      } else {
        target.copyBytes(value);
        return true;
      }
    }

    public String strVal(int doc) throws IOException {
      BytesRef value = getValueForDoc(doc);
      if (value == null || value.length == 0) {
        return null;
      } else {
        cref.clear();
        fieldType.indexedToReadable(value, cref);
        return cref.toString();
      }
    }

    @Override
    public Object objectVal(int doc) throws IOException {
      return strVal(doc);
    }

    @Override
    public String toString(int doc) throws IOException {
      return fieldName + '=' + strVal(doc);
    }

    @Override
    public ValueFiller getValueFiller() {
      return new ValueFiller() {
        private final MutableValueStr mval = new MutableValueStr();

        @Override
        public MutableValue getValue() {
          return mval;
        }

        @Override
        public void fillValue(int doc) throws IOException {
          BytesRef value = getValueForDoc(doc);
          mval.exists = value != null;
          mval.value.clear();
          if (value != null) {
            mval.value.copyBytes(value);
          }
        }
      };
    }
  }

  /**
   * Converts an input index value into the binary representation for storage as docValues
   * in the index. The default implementation assumes input type of {@link ByteArrayUtf8CharSequence}
   * or {@link String}, and returns a UTF8-encoded representation of the input.
   *
   * @param value - input object
   * @return representation for storage as docValues in the index
   * @throws IOException - if output field length would exceed the effective {@link #maxLength}
   */
  protected BytesRef valueToIndexedBytesRef(Object value) throws IOException {
    if (value instanceof ByteArrayUtf8CharSequence) {
      ByteArrayUtf8CharSequence utf8 = (ByteArrayUtf8CharSequence) value;
      return bytesToIndexedBytesRef(utf8.getBuf(), utf8.offset(), utf8.size());
    } else {
      final String origStrValue = value.toString();
      final byte[] utf8 = new byte[UnicodeUtil.maxUTF8Length(origStrValue.length())];
      final int length = UnicodeUtil.UTF16toUTF8(origStrValue, 0, origStrValue.length(), utf8);
      return bytesToIndexedBytesRef(utf8, 0, length);
    }
  }

  /**
   * A hook for subclasses to implement custom binary serialization of input byte
   * sequences. The default implementation simply returns a {@link BytesRef} wrapping
   * the input byte sequence.
   *
   * @param bytes - input byte array
   * @param offset - start offset of byte sequence
   * @param length - length of byte sequence
   * @return - an index-internal binary encoding of the input byte sequence
   * @throws IOException - if output field length would exceed the effective {@link #maxLength}
   */
  protected BytesRef bytesToIndexedBytesRef(byte[] bytes, int offset, int length) throws IOException {
    if (length > maxLength) {
      throw new IOException("length of field (" + length + ") exceeds effective maxLength of " + maxLength);
    }
    return new BytesRef(bytes, offset, length);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return term.utf8ToString();
  }

  @Override
  public boolean isUtf8Field() {
    return true;
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    // does not support uninversion
    return null;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, f.binaryValue().utf8ToString(), true);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new UnsupportedOperationException("sorting is not supported on " + getClass() + " (field \"" + field.name + "\")");
  }

}
