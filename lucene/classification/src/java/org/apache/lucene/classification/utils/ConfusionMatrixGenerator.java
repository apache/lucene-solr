package org.apache.lucene.classification.utils;

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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.classification.ClassificationResult;
import org.apache.lucene.classification.Classifier;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.util.BytesRef;

/**
 * Utility class to generate the confusion matrix of a {@link Classifier}
 */
public class ConfusionMatrixGenerator {

  private ConfusionMatrixGenerator() {

  }

  /**
   * get the {@link org.apache.lucene.classification.utils.ConfusionMatrixGenerator.ConfusionMatrix} of a given {@link Classifier},
   * generated on the given {@link LeafReader}, class and text fields.
   *
   * @param reader         the {@link LeafReader} containing the index used for creating the {@link Classifier}
   * @param classifier     the {@link Classifier} whose confusion matrix has to be generated
   * @param classFieldName the name of the Lucene field used as the classifier's output
   * @param textFieldName  the nome the Lucene field used as the classifier's input
   * @param <T>            the return type of the {@link ClassificationResult} returned by the given {@link Classifier}
   * @return a {@link org.apache.lucene.classification.utils.ConfusionMatrixGenerator.ConfusionMatrix}
   * @throws IOException if problems occurr while reading the index or using the classifier
   */
  public static <T> ConfusionMatrix getConfusionMatrix(LeafReader reader, Classifier<T> classifier, String classFieldName, String textFieldName) throws IOException {

    Map<String, Map<String, Long>> counts = new HashMap<>();

    for (int i = 0; i < reader.maxDoc(); i++) {
      StoredDocument doc = reader.document(i);
      String correctAnswer = doc.get(classFieldName);

      if (correctAnswer != null && correctAnswer.length() > 0) {

        ClassificationResult<T> result = classifier.assignClass(doc.get(textFieldName));
        T assignedClass = result.getAssignedClass();
        String classified = assignedClass instanceof BytesRef ? ((BytesRef) assignedClass).utf8ToString() : assignedClass.toString();

        Map<String, Long> stringLongMap = counts.get(correctAnswer);
        if (stringLongMap != null) {
          Long aLong = stringLongMap.get(classified);
          if (aLong != null) {
            stringLongMap.put(classified, aLong + 1);
          } else {
            stringLongMap.put(classified, 1l);
          }
        } else {
          stringLongMap = new HashMap<>();
          stringLongMap.put(classified, 1l);
          counts.put(correctAnswer, stringLongMap);
        }

      }
    }
    return new ConfusionMatrix(counts);
  }

  /**
   * a confusion matrix, backed by a {@link Map} representing the linearized matrix
   */
  public static class ConfusionMatrix {

    private final Map<String, Map<String, Long>> linearizedMatrix;

    private ConfusionMatrix(Map<String, Map<String, Long>> linearizedMatrix) {
      this.linearizedMatrix = linearizedMatrix;
    }

    /**
     * get the linearized confusion matrix as a {@link Map}
     * @return a {@link Map} whose keys are the correct answers and whose values are the actual answers' counts
     */
    public Map<String, Map<String, Long>> getLinearizedMatrix() {
      return Collections.unmodifiableMap(linearizedMatrix);
    }

    @Override
    public String toString() {
      return "ConfusionMatrix{" +
              "linearizedMatrix=" + linearizedMatrix +
              '}';
    }
  }
}
