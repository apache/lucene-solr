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
package org.apache.lucene.queries.function.valuesource.vectors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueStr;

/**
 * An implementation for retrieving {@link FunctionValues} instances for string based fields.
 */
public class FloatVectorFieldSource extends BytesRefFieldSource {

  protected Encoding encoding;

  public FloatVectorFieldSource(String field, Encoding encoding) {
    super(field);
    this.encoding = encoding;
  }

  public enum Encoding {
    STRING, // "[1.0,2.23]|[4.56,7]". Separator = "|"
    BASE64, // Base64 Encoded float[]. Separator = "|"
    BFLOAT16 // Compressed float[] to bfloat. Separator = "|"
  }

  public static float[] rawStringToVector(String stringifiedVector) {
    //format: 1.0,2.345

    String[] split = stringifiedVector.split(",");
    float[] output = new float[split.length];

    for(int i=0;  i< split.length; i++) {
      output[i] = Float.parseFloat(split[i]);
    }

    return output;
  }

  public static String vectorToRawString(float[] vector) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < vector.length; i++) {
      if (sb.length() > 0) {
        sb.append(',');
      }
      sb.append(Float.toString(vector[i]));
    }
    return sb.toString();
  }

  public static BytesRef vectorToRawStringBytesRef(float[] vector){
      BytesRefBuilder builder = new BytesRefBuilder();
      byte[] bytes;
      for (int i=0; i<vector.length; i++){
        if (builder.length() > 0){
          builder.append((byte)',');
        }
        bytes = Float.toString(vector[i]).getBytes(StandardCharsets.UTF_8);
        builder.append(bytes, 0, bytes.length);
      }
      return builder.get();
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {

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
            return decode(binaryValues.binaryValue());
          } else {
            return null;
          }
        }


        //TODO: maybe change to float[][] later... ?
        protected BytesRef decode(BytesRef encoded){
          byte[] encodedVectors = encoded.bytes;
          BytesRefBuilder currentEncodedVector = new BytesRefBuilder();
          BytesRefBuilder decodedVectorString = new BytesRefBuilder();  //TODO: float[][] later

          for (int i=encoded.offset; i< encoded.offset + encoded.length; i++) {
            if (encodedVectors[i] == (byte) '|' || i == encoded.offset + encoded.length - 1) {
              float[] currentVector = null;
              switch (encoding) {
                case BFLOAT16:
                  //TODO
                  break;
                case BASE64:
                  currentVector = base64ToVector(currentEncodedVector.toBytesRef().bytes);
                  break;
                case STRING:
                  currentVector = rawStringToVector(currentEncodedVector.get().toString());
                  break;
              }
              if (decodedVectorString.length() > 0) {
                decodedVectorString.append((byte)'|');
              }
              decodedVectorString.append(vectorToRawStringBytesRef(currentVector));
              currentEncodedVector.setLength(0);
            } else {
              currentEncodedVector.append(encodedVectors[i]);
            }
          }
          return decodedVectorString.get();
        }

        protected float[] base64ToVector(byte[] encoded) {
          final byte[] decoded = java.util.Base64.getDecoder().decode(encoded);
          final FloatBuffer buffer = ByteBuffer.wrap(decoded).asFloatBuffer();
          final float[] vector = new float[buffer.capacity()];
          buffer.get(vector);

          return vector;
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
          return strVal(doc); //TODO - should it be bytesVal instead?
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
  }

}