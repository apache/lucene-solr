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
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;


public abstract class FloatVectorFunction extends ValueSource {

  protected float[] queryVector;
  protected ValueSource denseVectorFieldValueSource;
  protected ScoreSelectionStrategy scoreSelectionStrategy;
  //protected Encoding encoding;

  protected FloatVectorFunction(String queryVector, ValueSource denseVectorFieldValueSource, Selector selector) {
    this.queryVector = parseStringInputToVector(queryVector);
    this.denseVectorFieldValueSource = denseVectorFieldValueSource;
    this.scoreSelectionStrategy = createSelectionStrategy(selector);
  }

  private ScoreSelectionStrategy createSelectionStrategy(Selector selector){
    ScoreSelectionStrategy strategy = new MaxScoreSelectionStrategy();
    switch (selector) {
      case FIRST:  strategy = new FirstScoreSelectionStrategy(); break;
      case LAST:  strategy =  new LastScoreSelectionStrategy(); break;
      case MIN:  strategy =  new MinScoreSelectionStrategy(); break;
      case AVERAGE:  strategy =  new AverageScoreSelectionStrategy();
  }
  return strategy;
}

/*
   protected static String vectorToBase64(float[] vector) {
     int size = Float.BYTES * vector.length;
     ByteBuffer buffer = ByteBuffer.allocate(size);
     for (double value : vector) {
       buffer.putFloat((float) value);
     }
     buffer.rewind();

     return new String(Base64.getEncoder().encode(buffer).array());
   }

  protected static float[] vectorFromBase64(String encoded) {
    final byte[] decoded = Base64.getDecoder().decode(encoded.getBytes());
    final FloatBuffer buffer = ByteBuffer.wrap(decoded).asFloatBuffer();
    final float[] vector = new float[buffer.capacity()];
    buffer.get(vector);

    return vector;
  }
*/

  protected float[] parseStringInputToVector(String stringifiedVector) {
     //format: 1.0,2.345|6.0,7.89

        String[] split = stringifiedVector.split(",");
        float[] output = new float[split.length];

        for(int i=0;  i< split.length; i++) {
          output[i] = Float.parseFloat(split[i]);
        }

        return output;
   }

  //implement similarity logic
  protected abstract float func(float[] vectorA, float[] vectorB);

    protected interface ScoreSelectionStrategy{
     float selectScore(float[] vectorA, BytesRef encodedVectors);
   }

   protected class MaxScoreSelectionStrategy implements ScoreSelectionStrategy{

     public float selectScore(float[] vectorA, BytesRef encodedVectorsContent){
       //BytesRef instead of String because we'll switch to BinaryDocValues later
       //and I want to keep the signature consistent
       final char[] encodedVectors = encodedVectorsContent.utf8ToString().toCharArray();

       StringBuilder currentEncodedVector = new StringBuilder();
       float[] currentDecodedVector;
       float newScore;
       float maxScore = 0;

       for (int i=0; i<encodedVectors.length; i++){
         if (encodedVectors[i] == (byte)'|' || i == encodedVectors.length - 1){

           //currentDecodedVector = vectorFromBase64(currentEncodedVector.toString());
           currentDecodedVector = parseStringInputToVector(currentEncodedVector.toString());
           currentEncodedVector.setLength(0); //clear StringBuilder

           newScore = func(vectorA, currentDecodedVector);
           if (newScore > maxScore){
             maxScore = newScore;
           }
         }
         else {
           currentEncodedVector.append(encodedVectors[i]);
         }

       }
       return maxScore;
     }
   }

  protected class FirstScoreSelectionStrategy implements ScoreSelectionStrategy {

    public float selectScore(float[] vectorA, BytesRef encodedVectorsContent) {
      //BytesRef instead of String because we'll switch to BinaryDocValues later
      //and I want to keep the signature consistent
      final char[] encodedVectors = encodedVectorsContent.utf8ToString().toCharArray();


      StringBuilder currentEncodedVector = new StringBuilder();

      for (int i=0; i<encodedVectors.length && encodedVectors[i] != (byte)'|'; i++){
        currentEncodedVector.append(encodedVectors[i]);
      }

      //float[] currentDecodedVector = vectorFromBase64(currentEncodedVector.toString());
      float[] currentDecodedVector = parseStringInputToVector(currentEncodedVector.toString());

      return func(vectorA, currentDecodedVector);
    }

  }


  protected class AverageScoreSelectionStrategy implements ScoreSelectionStrategy{

      public float selectScore(float[] vectorA, BytesRef encodedVectorsContent){
      //BytesRef instead of String because we'll switch to BinaryDocValues later
      //and I want to keep the signature consistent
      final char[] encodedVectors = encodedVectorsContent.utf8ToString().toCharArray();

      //StringBuilder currentEncodedVector = new StringBuilder();
      float[] currentDecodedVector;
      StringBuilder currentEncodedVector = new StringBuilder();
      float cumulativeSum=0;
      int cumulativeItems=0;

      for (int i=0; i<encodedVectors.length; i++){
        if (encodedVectors[i] == (byte)'|' || i == encodedVectors.length - 1){
          //currentDecodedVector = vectorFromBase64(currentEncodedVector.toString());
          currentDecodedVector = parseStringInputToVector(currentEncodedVector.toString());
          cumulativeSum += func(vectorA, currentDecodedVector);
          cumulativeItems +=1;
          currentEncodedVector.setLength(0); //clear StringBuilder
        }
        else {
          currentEncodedVector.append(encodedVectors[i]);
        }

      }
      return cumulativeSum / cumulativeItems;
    }
  }

  protected class MinScoreSelectionStrategy implements ScoreSelectionStrategy{

    public float selectScore(float[] vectorA, BytesRef vectorsFieldContent){
      //final char[] encodedVectors = bytesTochars(vectorsFieldContent);
      //BytesRef instead of String because we'll switch to BinaryDocValues later
      //and I want to keep the signature consistent
      final char[] encodedVectors = vectorsFieldContent.utf8ToString().toCharArray();

      StringBuilder currentEncodedVector = new StringBuilder();
      float[] currentDecodedVector;
      float newScore;
      float minScore=Float.MAX_VALUE; //TODO: return 0 if vector is empty?

      for (int i=0; i<encodedVectors.length; i++){
        if (encodedVectors[i] == (byte)'|' || i == encodedVectors.length - 1){
          //currentDecodedVector = vectorFromBase64(currentEncodedVector.toString());
          currentDecodedVector = parseStringInputToVector(currentEncodedVector.toString());
          currentEncodedVector.setLength(0); //clear StringBuilder

          newScore = func(vectorA, currentDecodedVector);
          if (newScore < minScore){
            minScore = newScore;
          }
        }
        else {
          currentEncodedVector.append(encodedVectors[i]);
        }

      }
      return minScore;
    }
  }

  protected class LastScoreSelectionStrategy implements ScoreSelectionStrategy {

    public float selectScore(float[] vectorA, BytesRef vectorsFieldContent) {
      //BytesRef instead of String because we'll switch to BinaryDocValues later
      //and I want to keep the signature consistent
      final char[] encodedVectors = vectorsFieldContent.utf8ToString().toCharArray();

      //optimize by scanning from end
      StringBuilder currentEncodedVector = new StringBuilder();
      for (int i = encodedVectors.length - 1; i >= 0 && encodedVectors[i] != (byte) '|'; i--) {
        currentEncodedVector.insert(0, encodedVectors[i]);
      }

      float[] currentDecodedVector = parseStringInputToVector(currentEncodedVector.toString());

      return func(vectorA, currentDecodedVector);
    }
  }


    /**
     * Type of selection to perform.
     */
    public enum Selector {
      MIN, //smallest similarity score across all vectors
      MAX, //largest similarity score across all vectors
      AVERAGE, //average similarity score across all vectors
      FIRST, //similarity score of first vector in valuesource (perf optimization)
      LAST //similarity score of last vector in valuesource (perf optimization)
    }

    /*
    public enum Encoding {
      RAW, //slowest, but easiest to debug
      BASE64, //more efficient String Encoding, but harder to work with
      BINARY //fastest - will implement once we have dedicated Vector Field BinaryDocValues
    }
    */

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {

    return new FloatDocValues(this) {
      @Override
      public float floatVal(int doc) throws IOException {
        BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();
        denseVectorFieldValueSource.getValues(context, readerContext).bytesVal(doc, bytesRefBuilder);

        return scoreSelectionStrategy.selectScore(queryVector, bytesRefBuilder.get());

      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (o.getClass() != this.getClass()) {
      return false;
    }
    FloatVectorFunction other = (FloatVectorFunction) o;
    if (!this.queryVector.equals(other.queryVector) ||
        !this.denseVectorFieldValueSource.equals(other.denseVectorFieldValueSource) ||
    !this.scoreSelectionStrategy.equals(other.scoreSelectionStrategy)){
      return false;
    }
    else {
      return true;
    }
  }

  @Override
  public int hashCode() {
    return this.queryVector.hashCode()
        + this.denseVectorFieldValueSource.hashCode()
        + this.scoreSelectionStrategy.getClass().getName().hashCode(); //TBD - fix hashcode/equals for scoreSelectionStrategy
  }

}
