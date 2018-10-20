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
package org.apache.lucene.classification.utils;


import java.io.IOException;
import java.util.Arrays;
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
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NamedThreadFactory;

/**
 * Utility class to generate the confusion matrix of a {@link Classifier}
 */
public class ConfusionMatrixGenerator {

  private ConfusionMatrixGenerator() {

  }

  /**
   * get the {@link org.apache.lucene.classification.utils.ConfusionMatrixGenerator.ConfusionMatrix} of a given {@link Classifier},
   * generated on the given {@link IndexReader}, class and text fields.
   *
   * @param reader              the {@link IndexReader} containing the index used for creating the {@link Classifier}
   * @param classifier          the {@link Classifier} whose confusion matrix has to be generated
   * @param classFieldName      the name of the Lucene field used as the classifier's output
   * @param textFieldName       the nome the Lucene field used as the classifier's input
   * @param timeoutMilliseconds timeout to wait before stopping creating the confusion matrix
   * @param <T>                 the return type of the {@link ClassificationResult} returned by the given {@link Classifier}
   * @return a {@link org.apache.lucene.classification.utils.ConfusionMatrixGenerator.ConfusionMatrix}
   * @throws IOException if problems occurr while reading the index or using the classifier
   */
  public static <T> ConfusionMatrix getConfusionMatrix(IndexReader reader, Classifier<T> classifier, String classFieldName,
                                                       String textFieldName, long timeoutMilliseconds) throws IOException {

    ExecutorService executorService = Executors.newFixedThreadPool(1, new NamedThreadFactory("confusion-matrix-gen-"));

    try {

      Map<String, Map<String, Long>> counts = new HashMap<>();
      IndexSearcher indexSearcher = new IndexSearcher(reader);
      TopDocs topDocs = indexSearcher.search(new TermRangeQuery(classFieldName, null, null, true, true), Integer.MAX_VALUE);
      double time = 0d;

      int counter = 0;
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {

        if (timeoutMilliseconds > 0 && time >= timeoutMilliseconds) {
          break;
        }

        Document doc = reader.document(scoreDoc.doc);
        String[] correctAnswers = doc.getValues(classFieldName);

        if (correctAnswers != null && correctAnswers.length > 0) {
          Arrays.sort(correctAnswers);
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
                  counter++;
                  String classified = assignedClass instanceof BytesRef ? ((BytesRef) assignedClass).utf8ToString() : assignedClass.toString();

                  String correctAnswer;
                  if (Arrays.binarySearch(correctAnswers, classified) >= 0) {
                    correctAnswer = classified;
                  } else {
                    correctAnswer = correctAnswers[0];
                  }

                  Map<String, Long> stringLongMap = counts.get(correctAnswer);
                  if (stringLongMap != null) {
                    Long aLong = stringLongMap.get(classified);
                    if (aLong != null) {
                      stringLongMap.put(classified, aLong + 1);
                    } else {
                      stringLongMap.put(classified, 1L);
                    }
                  } else {
                    stringLongMap = new HashMap<>();
                    stringLongMap.put(classified, 1L);
                    counts.put(correctAnswer, stringLongMap);
                  }

                }
              }
            } catch (TimeoutException timeoutException) {
              // add classification timeout
              time += 5000;
            } catch (ExecutionException | InterruptedException executionException) {
              throw new RuntimeException(executionException);
            }

          }
        }
      }
      return new ConfusionMatrix(counts, time / counter, counter);
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
    private double accuracy = -1d;

    private ConfusionMatrix(Map<String, Map<String, Long>> linearizedMatrix, double avgClassificationTime, int numberOfEvaluatedDocs) {
      this.linearizedMatrix = linearizedMatrix;
      this.avgClassificationTime = avgClassificationTime;
      this.numberOfEvaluatedDocs = numberOfEvaluatedDocs;
    }

    /**
     * get the linearized confusion matrix as a {@link Map}
     *
     * @return a {@link Map} whose keys are the correct classification answers and whose values are the actual answers'
     * counts
     */
    public Map<String, Map<String, Long>> getLinearizedMatrix() {
      return Collections.unmodifiableMap(linearizedMatrix);
    }

    /**
     * calculate precision on the given class
     *
     * @param klass the class to calculate the precision for
     * @return the precision for the given class
     */
    public double getPrecision(String klass) {
      Map<String, Long> classifications = linearizedMatrix.get(klass);
      double tp = 0;
      double den = 0; // tp + fp
      if (classifications != null) {
        for (Map.Entry<String, Long> entry : classifications.entrySet()) {
          if (klass.equals(entry.getKey())) {
            tp += entry.getValue();
          }
        }
        for (Map<String, Long> values : linearizedMatrix.values()) {
          if (values.containsKey(klass)) {
            den += values.get(klass);
          }
        }
      }
      return tp > 0 ? tp / den : 0;
    }

    /**
     * calculate recall on the given class
     *
     * @param klass the class to calculate the recall for
     * @return the recall for the given class
     */
    public double getRecall(String klass) {
      Map<String, Long> classifications = linearizedMatrix.get(klass);
      double tp = 0;
      double fn = 0;
      if (classifications != null) {
        for (Map.Entry<String, Long> entry : classifications.entrySet()) {
          if (klass.equals(entry.getKey())) {
            tp += entry.getValue();
          } else {
            fn += entry.getValue();
          }
        }
      }
      return tp + fn > 0 ? tp / (tp + fn) : 0;
    }

    /**
     * get the F-1 measure of the given class
     *
     * @param klass the class to calculate the F-1 measure for
     * @return the F-1 measure for the given class
     */
    public double getF1Measure(String klass) {
      double recall = getRecall(klass);
      double precision = getPrecision(klass);
      return precision > 0 && recall > 0 ? 2 * precision * recall / (precision + recall) : 0;
    }

    /**
     * get the F-1 measure on this confusion matrix
     *
     * @return the F-1 measure
     */
    public double getF1Measure() {
      double recall = getRecall();
      double precision = getPrecision();
      return precision > 0 && recall > 0 ? 2 * precision * recall / (precision + recall) : 0;
    }

    /**
     * Calculate accuracy on this confusion matrix using the formula:
     * {@literal accuracy = correctly-classified / (correctly-classified + wrongly-classified)}
     *
     * @return the accuracy
     */
    public double getAccuracy() {
      if (this.accuracy == -1) {
        double tp = 0d;
        double tn = 0d;
        double tfp = 0d; // tp + fp
        double fn = 0d;
        for (Map.Entry<String, Map<String, Long>> classification : linearizedMatrix.entrySet()) {
          String klass = classification.getKey();
          for (Map.Entry<String, Long> entry : classification.getValue().entrySet()) {
            if (klass.equals(entry.getKey())) {
              tp += entry.getValue();
            } else {
              fn += entry.getValue();
            }
          }
          for (Map<String, Long> values : linearizedMatrix.values()) {
            if (values.containsKey(klass)) {
              tfp += values.get(klass);
            } else {
              tn++;
            }
          }

        }
        this.accuracy = (tp + tn) / (tfp + fn + tn);
      }
      return this.accuracy;
    }

    /**
     * get the macro averaged precision (see {@link #getPrecision(String)}) over all the classes.
     *
     * @return the macro averaged precision as computed from the confusion matrix
     */
    public double getPrecision() {
      double p = 0;
      for (Map.Entry<String, Map<String, Long>> classification : linearizedMatrix.entrySet()) {
        String klass = classification.getKey();
        p += getPrecision(klass);
      }

      return p / linearizedMatrix.size();
    }

    /**
     * get the macro averaged recall (see {@link #getRecall(String)}) over all the classes
     *
     * @return the recall as computed from the confusion matrix
     */
    public double getRecall() {
      double r = 0;
      for (Map.Entry<String, Map<String, Long>> classification : linearizedMatrix.entrySet()) {
        String klass = classification.getKey();
        r += getRecall(klass);
      }

      return r / linearizedMatrix.size();
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
     *
     * @return the avg classification time
     */
    public double getAvgClassificationTime() {
      return avgClassificationTime;
    }

    /**
     * get the no. of documents evaluated while generating this confusion matrix
     *
     * @return the no. of documents evaluated
     */
    public int getNumberOfEvaluatedDocs() {
      return numberOfEvaluatedDocs;
    }
  }
}
