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
import java.util.Arrays;
import java.util.Locale;
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

  protected FloatVectorFunction(String queryVector, ValueSource denseVectorFieldValueSource, Selector selector) {
    this.queryVector = parseStringInputToVector(queryVector);
    this.denseVectorFieldValueSource = denseVectorFieldValueSource;
    this.scoreSelectionStrategy = createSelectionStrategy(selector);
  }

  /**
   * Type of selection to perform.
   */
  public enum Selector {
    MIN, //smallest similarity score across all vectors
    MAX, //largest similarity score across all vectors
    AVG, //average similarity score across all vectors
    FIRST, //similarity score of first vector in valuesource (perf optimization)
    LAST //similarity score of last vector in valuesource (perf optimization)
  }

  private ScoreSelectionStrategy createSelectionStrategy(Selector selector){
    ScoreSelectionStrategy strategy = null;
    switch (selector) {
      case FIRST:  strategy = new FirstScoreSelectionStrategy(); break;
      case LAST:  strategy =  new LastScoreSelectionStrategy(); break;
      case MAX: strategy = new MaxScoreSelectionStrategy(); break;
      case MIN:  strategy =  new MinScoreSelectionStrategy(); break;
      case AVG:  strategy =  new AverageScoreSelectionStrategy();
  }
  return strategy;
}

  protected float[] parseStringInputToVector(String stringifiedVector) {
     //format: 1.0, 2.345,6,7.8
    String[] split = stringifiedVector.split(",");
    float[] output = new float[split.length];
    for(int i=0;  i< split.length; i++) {
      output[i] = Float.parseFloat(split[i]);
    }
    return output;
   }

  //implements similarity scoring logic (cosine, dotproduct, etc.)
  protected abstract float func(float[] vectorA, float[] vectorB);

  protected abstract String getName();

    protected interface ScoreSelectionStrategy{
      Selector getName();
     float selectScore(float[] vectorA, BytesRef encodedVectors);
   }

   protected class MaxScoreSelectionStrategy implements ScoreSelectionStrategy{
      protected Selector name = Selector.MAX;

      public Selector getName(){
        return name;
      }

     public float selectScore(float[] vectorA, BytesRef encodedVectorsContent){
        //TODO: revisit this pattern when we implement bfloat16...
       final char[] encodedVectors = encodedVectorsContent.utf8ToString().toCharArray();
       StringBuilder currentEncodedVector = new StringBuilder();
       float[] currentDecodedVector;
       float newScore;
       float maxScore = 0;
       for (int i=0; i<encodedVectors.length; i++){
         if (encodedVectors[i] == (byte)'|' || i == encodedVectors.length - 1){
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
    protected Selector name = Selector.FIRST;

    public Selector getName(){
      return name;
    }

    public float selectScore(float[] vectorA, BytesRef encodedVectorsContent) {
      //TODO: revisit this pattern when we implement bfloat16...
      final char[] encodedVectors = encodedVectorsContent.utf8ToString().toCharArray();
      StringBuilder currentEncodedVector = new StringBuilder();
      for (int i=0; i<encodedVectors.length && encodedVectors[i] != (byte)'|'; i++){
        currentEncodedVector.append(encodedVectors[i]);
      }
      float[] currentDecodedVector = parseStringInputToVector(currentEncodedVector.toString());
      return func(vectorA, currentDecodedVector);
    }
  }


  protected class AverageScoreSelectionStrategy implements ScoreSelectionStrategy{
    protected Selector name = Selector.AVG;
    public Selector getName(){
      return name;
    }

    public float selectScore(float[] vectorA, BytesRef encodedVectorsContent){
      //TODO: revisit this pattern when we implement bfloat16...
      final char[] encodedVectors = encodedVectorsContent.utf8ToString().toCharArray();
      float[] currentDecodedVector;
      StringBuilder currentEncodedVector = new StringBuilder();
      float cumulativeSum=0;
      int cumulativeItems=0;
      for (int i=0; i<encodedVectors.length; i++){
        if (encodedVectors[i] == (byte)'|' || i == encodedVectors.length - 1){
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
    protected Selector name = Selector.MIN;

    public Selector getName(){
      return name;
    }

    public float selectScore(float[] vectorA, BytesRef vectorsFieldContent){
      //TODO: revisit this pattern when we implement bfloat16...
      final char[] encodedVectors = vectorsFieldContent.utf8ToString().toCharArray();
      StringBuilder currentEncodedVector = new StringBuilder();
      float[] currentDecodedVector;
      float newScore;
      float minScore=Float.MAX_VALUE; //TODO: return 0 if vector is empty?
      for (int i=0; i<encodedVectors.length; i++){
        if (encodedVectors[i] == (byte)'|' || i == encodedVectors.length - 1){
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
    protected Selector name = Selector.LAST;

    public Selector getName(){
      return name;
    }

    public float selectScore(float[] vectorA, BytesRef vectorsFieldContent) {
      //TODO: revisit this pattern when we implement bfloat16...
      final char[] encodedVectors = vectorsFieldContent.utf8ToString().toCharArray();
      StringBuilder currentEncodedVector = new StringBuilder();
      //optimize by scanning from end
      for (int i = encodedVectors.length - 1; i >= 0 && encodedVectors[i] != (byte) '|'; i--) {
        currentEncodedVector.insert(0, encodedVectors[i]);
      }
      float[] currentDecodedVector = parseStringInputToVector(currentEncodedVector.toString());
      return func(vectorA, currentDecodedVector);
    }
  }

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
  public String description() {
    return this.getName() + "(\"" + FloatVectorFieldSource.vectorToRawString(queryVector) + "\", " + denseVectorFieldValueSource + ", " + this.scoreSelectionStrategy.getName().toString().toLowerCase(Locale.ROOT) + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (o.getClass() != this.getClass()) {
      return false;
    }
    FloatVectorFunction other = (FloatVectorFunction) o;
    if (!Arrays.equals(this.queryVector, other.queryVector) ||
        !this.denseVectorFieldValueSource.equals(other.denseVectorFieldValueSource) ||
    !this.scoreSelectionStrategy.getName().equals(other.scoreSelectionStrategy.getName())){
      return false;
    }
    else {
      return true;
    }
  }

  @Override
  public int hashCode() {
    return this.getClass().hashCode()
        + Arrays.hashCode(this.queryVector)
        + this.denseVectorFieldValueSource.hashCode()
        + this.scoreSelectionStrategy.getName().hashCode();
  }

}