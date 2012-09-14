package org.apache.lucene.classification;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.AtomicReader;

import java.io.IOException;

/**
 * A classifier, see <code>http://en.wikipedia.org/wiki/Classifier_(mathematics)</code>
 * @lucene.experimental
 */
public interface Classifier {

  /**
   * Assign a class to the given text String
   * @param text a String containing text to be classified
   * @return a String representing a class
   * @throws IOException
   */
  public String assignClass(String text) throws IOException;

  /**
   * Train the classifier using the underlying Lucene index
   * @param atomicReader the reader to use to access the Lucene index
   * @param textFieldName the name of the field used to compare documents
   * @param classFieldName the name of the field containing the class assigned to documents
   * @param analyzer the analyzer used to tokenize / filter the unseen text
   * @throws IOException
   */
  public void train(AtomicReader atomicReader, String textFieldName, String classFieldName, Analyzer analyzer)
      throws IOException;

}
