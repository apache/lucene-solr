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
package org.apache.lucene.queries.function.valuesource;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.DocTermsIndexDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueStr;

/**
 * An implementation for retrieving {@link FunctionValues} instances for string based fields.
 */
public class BytesRefFieldSource extends FieldCacheSource {

  public BytesRefFieldSource(String field) {
    super(field);
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FieldInfo fieldInfo = readerContext.reader().getFieldInfos().fieldInfo(field);

    // To be sorted or not to be sorted, that is the question
    // TODO: do it cleaner?
    if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
      final BinaryDocValues binaryValues = DocValues.getBinary(readerContext.reader(), field);
      return new FunctionValues() {
        int lastDocID = -1;

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
          final BytesRefBuilder bytes = new BytesRefBuilder();
          return bytesVal(doc, bytes)
              ? bytes.get().utf8ToString()
              : null;
        }

        @Override
        public Object objectVal(int doc) throws IOException {
          return strVal(doc);
        }

        @Override
        public String toString(int doc) throws IOException {
          return description() + '=' + strVal(doc);
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

      };
    } else {
      return new DocTermsIndexDocValues(this, readerContext, field) {

        @Override
        protected String toTerm(String readableValue) {
          return readableValue;
        }

        @Override
        public Object objectVal(int doc) throws IOException {
          return strVal(doc);
        }

        @Override
        public String toString(int doc) throws IOException {
          return description() + '=' + strVal(doc);
        }
      };
    }
  }
}
