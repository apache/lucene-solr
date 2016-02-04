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
package org.apache.lucene.classification.document;


import java.io.IOException;
import java.util.List;

import org.apache.lucene.classification.ClassificationResult;
import org.apache.lucene.document.Document;

/**
 * A classifier, see <code>http://en.wikipedia.org/wiki/Classifier_(mathematics)</code>, which assign classes of type
 * <code>T</code> to a {@link org.apache.lucene.document.Document}s
 *
 * @lucene.experimental
 */
public interface DocumentClassifier<T> {
  /**
   * Assign a class (with score) to the given {@link org.apache.lucene.document.Document}
   *
   * @param document a {@link org.apache.lucene.document.Document}  to be classified. Fields are considered features for the classification.
   * @return a {@link org.apache.lucene.classification.ClassificationResult} holding assigned class of type <code>T</code> and score
   * @throws java.io.IOException If there is a low-level I/O error.
   */
  ClassificationResult<T> assignClass(Document document) throws IOException;

  /**
   * Get all the classes (sorted by score, descending) assigned to the given {@link org.apache.lucene.document.Document}.
   *
   * @param document a {@link org.apache.lucene.document.Document}  to be classified. Fields are considered features for the classification.
   * @return the whole list of {@link org.apache.lucene.classification.ClassificationResult}, the classes and scores. Returns <code>null</code> if the classifier can't make lists.
   * @throws java.io.IOException If there is a low-level I/O error.
   */
  List<ClassificationResult<T>> getClasses(Document document) throws IOException;

  /**
   * Get the first <code>max</code> classes (sorted by score, descending) assigned to the given text String.
   *
   * @param document a {@link org.apache.lucene.document.Document}  to be classified. Fields are considered features for the classification.
   * @param max      the number of return list elements
   * @return the whole list of {@link org.apache.lucene.classification.ClassificationResult}, the classes and scores. Cut for "max" number of elements. Returns <code>null</code> if the classifier can't make lists.
   * @throws java.io.IOException If there is a low-level I/O error.
   */
  List<ClassificationResult<T>> getClasses(Document document, int max) throws IOException;

}