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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.lucene.classification.ClassificationResult;
import org.apache.lucene.classification.Classifier;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
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
  public static <T> ConfusionMatrix getConfusionMatrix(LeafReader reader, Classifier<T> classifier, String classFieldName,
                                                       String textFieldName) throws IOException {

    ExecutorService executorService = Executors.newFixedThreadPool(1);

    try {

      Map<String, Map<String, Long>> counts = new HashMap<>();
      IndexSearcher indexSearcher = new IndexSearcher(reader);
      TopDocs topDocs = indexSearcher.search(new WildcardQuery(new Term(classFieldName, "*")), Integer.MAX_VALUE);
      double time = 0d;

      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        StoredDocument doc = reader.document(scoreDoc.doc);
        String correctAnswer = doc.get(classFieldName);

        if (correctAnswer != null && correctAnswer.length() > 0) {
          ClassificationResult<T> result;
          String text = doc.get(textFieldName);
          if (text != null) {
            try {
              // fail if classification takes more than 5s
              long start = System.currentTimeMillis();
              result = executorService.submit(() -> classifier.assignClass(text)).get(5, TimeUnit.SECONDS);
              long end = System.currentTimeMillis();
              time += end - start;

              if (result != null) {
                T assignedClass = result.getAssignedClass();
                if (assignedClass != null) {
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
            } catch (TimeoutException timeoutException) {
              // add timeout
              time += 5000;
            } catch (ExecutionException | InterruptedException executionException) {
              throw new RuntimeException(executionException);
            }

          }
        }
      }
      return new ConfusionMatrix(counts, time / topDocs.totalHits, topDocs.totalHits);
    } finally {
      executorService.shutdown();
    }
  }

  /**
   * a confusion matrix, backed by a {@link Map} representing the linearized matrix
   */
  public static class ConfusionMatrix {

    private final Map<String, Map<String, Long>> linearizedMatrix;
    private final double avgClassificationTime;
    private final int numberOfEvaluatedDocs;

    private ConfusionMatrix(Map<String, Map<String, Long>> linearizedMatrix, double avgClassificationTime, int numberOfEvaluatedDocs) {
      this.linearizedMatrix = linearizedMatrix;
      this.avgClassificationTime = avgClassificationTime;
      this.numberOfEvaluatedDocs = numberOfEvaluatedDocs;
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
          ", avgClassificationTime=" + avgClassificationTime +
          ", numberOfEvaluatedDocs=" + numberOfEvaluatedDocs +
          '}';
    }

    /**
     * get the average classification time in milliseconds
     * @return the avg classification time
     */
    public double getAvgClassificationTime() {
      return avgClassificationTime;
    }

    /**
     * get the no. of documents evaluated while generating this confusion matrix
     * @return the no. of documents evaluated
     */
    public int getNumberOfEvaluatedDocs() {
      return numberOfEvaluatedDocs;
    }
  }
}
