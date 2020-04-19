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
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.vectors.FloatVectorFieldSource;
import org.apache.lucene.queries.function.valuesource.vectors.FloatVectorFieldSource.Encoding;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.lucene.queries.function.valuesource.vectors.FloatVectorFieldSource.rawStringToVector;
import static org.apache.lucene.queries.function.valuesource.vectors.FloatVectorFieldSource.vectorToRawString;

public class DenseVectorField extends FieldType  {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String DIMENSIONS_PARAM = "dimensions";
  public static final int DEFAULT_DIMENSIONS = -1; //Don't enforce
  public static int dimensions; //-1 = don't validate

  public static final String ENCODING = "encoding";
  public static final Encoding DEFAULT_ENCODING = Encoding.STRING; //for now
  public static Encoding encoding;


  public int getDimensions() {
    return dimensions;
  }
  public Encoding getEncoding() {
    return encoding;
  }

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    SolrParams p = new MapSolrParams(args);
    dimensions = p.getInt(DIMENSIONS_PARAM, DEFAULT_DIMENSIONS);
    args.remove(DIMENSIONS_PARAM);

    encoding = Encoding.valueOf(p.get(ENCODING, DEFAULT_ENCODING.toString()).toUpperCase(Locale.ROOT));  //TODO: Error handling on parse
    args.remove(ENCODING);

    super.init(schema, args);

    //TODO: remove these? Not using them.
    properties |= DOC_VALUES;
    properties &= USE_DOCVALUES_AS_STORED;


  }

  /** called to get the default value source (normally, from the
   *  Lucene FieldCache.)
   */
  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    field.checkFieldCacheSource();
    return new FloatVectorFieldSource(field.name, encoding);
  }

  @Override
  protected void checkSupportsDocValues() { // we support DocValues
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, toExternal(f), false);
    //Vector Fields are Not Indexable by default. If overrridden by a field type
    //that enables quantization, then perform indexing logic there.
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new RuntimeException("Cannot sort on a DenseVectorField");
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    // TODO: maybe just return null?
    //if (sf.multiValued()) {
    //  return Type.SORTED_SET_BINARY;
    //} else {
      return Type.BINARY;
    //}
  }

  //@Override
  //public String toExternal(IndexableField f) {
    //Convert from Encoded to Array of String
  //  return indexableFieldToReadableString(f);
  //}

  @Override
  public String toInternal(String val) {
    return super.toInternal(val);
  }




  /*@Override
  public ByteBuffer toObject(IndexableField f) {
    float[] decodedVector;
    switch (encoding) {
      case BFLOAT16:
        //TODO
        break;
      case BASE64:
        decodedVector = base64ToVector(f.stringValue());
        break;
      case STRING:
        decodedVector = parseStringInputToVector(f.stringValue());
        break;
    }

    decodedVector.
    return  ByteBuffer.wrap(bytes.bytes, bytes.offset, bytes.length);
  }
  */

  //@Override
  //public ByteBuffer toObject(IndexableField f) {
  //  String vector = f.stringValue();
  //
  //  BytesRef bytes = f.binaryValue();
  //  return  ByteBuffer.wrap(bytes.bytes, bytes.offset, bytes.length);
  //}


  /*
  protected float[] parseStringInputToVector(String stringifiedVector) {
    //format: 1.0,2.345|6.0,7.89

    String[] split = stringifiedVector.split(",");
    float[] output = new float[split.length];

    for(int i=0;  i< split.length; i++) {
      output[i] = Float.parseFloat(split[i]);
    }

    return output;
  }
*/

  protected static float[] base64ToVector(byte[] encoded) {
    final byte[] decoded = java.util.Base64.getDecoder().decode(encoded);
    final FloatBuffer buffer = ByteBuffer.wrap(decoded).asFloatBuffer();
    final float[] vector = new float[buffer.capacity()];
    buffer.get(vector);

    return vector;
  }

  protected static byte[] vectorToBase64(float[] vector) {
    int size = Float.BYTES * vector.length;
    ByteBuffer buffer = ByteBuffer.allocate(size);
    for (float value : vector) {
      buffer.putFloat(value);
    }
    buffer.rewind();


    //Temporary - testing decoding
    byte[] encoded = java.util.Base64.getEncoder().encode(buffer).array();
    vector = base64ToVector(encoded);
    int size2 = Float.BYTES * vector.length;
    ByteBuffer buffer2 = ByteBuffer.allocate(size2);
    for (float value : vector) {
      buffer2.putFloat(value);
    }
    buffer2.rewind();

    return java.util.Base64.getEncoder().encode(buffer2).array();
  }

  /*
  protected static float[] base64ToVector(String encoded) {
    final byte[] decoded = java.util.Base64.getDecoder().decode(encoded.getBytes());
    final FloatBuffer buffer = ByteBuffer.wrap(decoded).asFloatBuffer();
    final float[] vector = new float[buffer.capacity()];
    buffer.get(vector);

    return vector;
  }
*/
  /*
  //change to float[][] later...
  protected String indexableFieldToReadableString(IndexableField field){
    byte[] encodedVectors = field.binaryValue().bytes;
    BytesRefBuilder currentEncodedVector = new BytesRefBuilder();
    StringBuilder external = new StringBuilder();

      for (int i=0; i< encodedVectors.length; i++) {
        if (encodedVectors[i] == (byte) '|' || i == encodedVectors.length - 1) {
          float[] currentVector = null;
          switch (encoding) {
            case BFLOAT16:
              //TODO
              break;
            case BASE64:
              currentVector = base64ToVector(currentEncodedVector.get().toString());
              break;
            case STRING:
              currentVector = parseStringInputToVector(currentEncodedVector.get().toString());
              break;
          }
          if (external.length() > 0) {
            external.append("|");
          }
          external.append(vectorToString(currentVector));

        } else {
          currentEncodedVector.append(encodedVectors[i]);
        }
      }
    return external.toString();
  }
*/




  protected BytesRef readableStringToEncodedBytes(String val){

    final char[] inputStringVectors = ((String) val).toCharArray();
    StringBuilder currentInputStringVector = new StringBuilder();
    float[] currentInputVector = null;
    BytesRefBuilder encodedVectors = new BytesRefBuilder();

    for (int i = 0; i < inputStringVectors.length; i++) {
      if (inputStringVectors[i] == (byte) '|' || i == inputStringVectors.length - 1) {

        currentInputVector = rawStringToVector(currentInputStringVector.toString());
        currentInputStringVector.setLength(0); //clear StringBuilder

        if (encodedVectors.length() > 0) {
          encodedVectors.append((byte) '|');
        }

        byte[] currentVectorBytes = null;
        switch (encoding) {
          case BFLOAT16:
            //TODO
            break;
          case BASE64:
            currentVectorBytes = vectorToBase64(currentInputVector);
            break;
          case STRING:
            currentVectorBytes = vectorToRawString(currentInputVector).getBytes();
            break;
        }
        encodedVectors.append(currentVectorBytes, 0, currentVectorBytes.length);
      } else {
        currentInputStringVector.append(inputStringVectors[i]);
      }
    }
    return encodedVectors.get();
  }

  public IndexableField createField(SchemaField field, Object val) {
    if (val == null) return null;
    return new org.apache.lucene.document.BinaryDocValuesField(field.getName(), readableStringToEncodedBytes((String)val));
  }

  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), (String)value); //TODO: convert value
  }

  @Override
  public List<IndexableField> createFields(SchemaField sf, Object val) {
    List<IndexableField> fields = new ArrayList<>(2);
    fields.add(createField(sf, val));
    fields.add(getStoredField(sf, val));
    return fields;
  }

  /**Converts any Object to a java Object native to this field type
   */
  @Override
  public Object toNativeType(Object val) {
    if (val instanceof float[]) {
      return val;
    }
    else if (val instanceof String){
      return val;
    }
    return val;
  }

}
