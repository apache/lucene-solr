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
package org.apache.lucene.classification;

/**
 * The result of a call to {@link Classifier#assignClass(String)} holding an assigned class and a score.
 * @lucene.experimental
 */
public class ClassificationResult {

  private String assignedClass;
  private double score;

  /**
   * Constructor
   * @param assignedClass the class <code>String</code> assigned by a {@link Classifier}
   * @param score the score for the assignedClass as a <code>double</code>
   */
  public ClassificationResult(String assignedClass, double score) {
    this.assignedClass = assignedClass;
    this.score = score;
  }

  /**
   * retrieve the result class
   * @return a <code>String</code> representing an assigned class
   */
  public String getAssignedClass() {
    return assignedClass;
  }

  /**
   * retrieve the result score
   * @return a <code>double</code> representing a result score
   */
  public double getScore() {
    return score;
  }
}
