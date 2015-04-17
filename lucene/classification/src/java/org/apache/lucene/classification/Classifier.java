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

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

/**
 * A classifier, see <code>http://en.wikipedia.org/wiki/Classifier_(mathematics)</code>, which assign classes of type
 * <code>T</code>
 *
 * @lucene.experimental
 */
public interface Classifier<T> {

  /**
   * Assign a class (with score) to the given text String
   *
   * @param text a String containing text to be classified
   * @return a {@link ClassificationResult} holding assigned class of type <code>T</code> and score
   * @throws IOException If there is a low-level I/O error.
   */
  public ClassificationResult<T> assignClass(String text) throws IOException;

  /**
   * Get all the classes (sorted by score, descending) assigned to the given text String.
   *
   * @param text a String containing text to be classified
   * @return the whole list of {@link ClassificationResult}, the classes and scores. Returns <code>null</code> if the classifier can't make lists.
   * @throws IOException If there is a low-level I/O error.
   */
  public List<ClassificationResult<T>> getClasses(String text) throws IOException;

  /**
   * Get the first <code>max</code> classes (sorted by score, descending) assigned to the given text String.
   *
   * @param text a String containing text to be classified
   * @param max  the number of return list elements
   * @return the whole list of {@link ClassificationResult}, the classes and scores. Cut for "max" number of elements. Returns <code>null</code> if the classifier can't make lists.
   * @throws IOException If there is a low-level I/O error.
   */
  public List<ClassificationResult<T>> getClasses(String text, int max) throws IOException;

  /**
   * Train the classifier using the underlying Lucene index
   *
   * @param leafReader   the reader to use to access the Lucene index
   * @param textFieldName  the name of the field used to compare documents
   * @param classFieldName the name of the field containing the class assigned to documents
   * @param analyzer       the analyzer used to tokenize / filter the unseen text
   * @throws IOException If there is a low-level I/O error.
   */
  public void train(LeafReader leafReader, String textFieldName, String classFieldName, Analyzer analyzer)
      throws IOException;

  /**
   * Train the classifier using the underlying Lucene index
   *
   * @param leafReader   the reader to use to access the Lucene index
   * @param textFieldName  the name of the field used to compare documents
   * @param classFieldName the name of the field containing the class assigned to documents
   * @param analyzer       the analyzer used to tokenize / filter the unseen text
   * @param query          the query to filter which documents use for training
   * @throws IOException If there is a low-level I/O error.
   */
  public void train(LeafReader leafReader, String textFieldName, String classFieldName, Analyzer analyzer, Query query)
      throws IOException;

  /**
   * Train the classifier using the underlying Lucene index
   *
   * @param leafReader   the reader to use to access the Lucene index
   * @param textFieldNames the names of the fields to be used to compare documents
   * @param classFieldName the name of the field containing the class assigned to documents
   * @param analyzer       the analyzer used to tokenize / filter the unseen text
   * @param query          the query to filter which documents use for training
   * @throws IOException If there is a low-level I/O error.
   */
  public void train(LeafReader leafReader, String[] textFieldNames, String classFieldName, Analyzer analyzer, Query query)
      throws IOException;

}
