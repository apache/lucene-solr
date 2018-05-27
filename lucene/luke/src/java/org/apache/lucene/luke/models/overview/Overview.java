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

package org.apache.lucene.luke.models.overview;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A dedicated interface for Luke's Overview tab.
 */
public interface Overview {

  /**
   * Returns the currently opened index directory path,
   * or the root directory path if multiple index directories are opened.
   */
  String getIndexPath();

  /**
   * Returns the number of fields in this index.
   */
  int getNumFields();

  /**
   * Returns the number of documents in this index.
   */
  int getNumDocuments();

  /**
   * Returns the total number of terms in this index.
   *
   * @throws org.apache.lucene.luke.models.LukeException - if an internal error occurs when accessing index
   */
  long getNumTerms();

  /**
   * Returns true if this index includes deleted documents.
   */
  boolean hasDeletions();

  /**
   * Returns the number of deleted documents in this index.
   */
  int getNumDeletedDocs();

  /**
   * Returns true if the index is optimized.
   * Empty Optional instance is returned if multiple indexes are opened.
   */
  Optional<Boolean> isOptimized();

  /**
   * Returns the version number when this index was opened.
   * Empty Optional instance is returned if multiple indexes are opened.
   */
  Optional<Long> getIndexVersion();

  /**
   * Returns the string representation for the Lucene segment version when the index was created.
   * Empty Optional instance is returned if multiple indexes are opened.
   *
   * @throws org.apache.lucene.luke.models.LukeException - if an internal error occurs when accessing index
   */
  Optional<String> getIndexFormat();

  /**
   * Returns the currently opened {@link org.apache.lucene.store.Directory} implementation class name.
   * Empty Optional instance is returned if multiple indexes are opened.
   */
  Optional<String> getDirImpl();

  /**
   * Returns the information of the commit point that reader has opened.
   *
   * Empty Optional instance is returned if multiple indexes are opened.
   */
  Optional<String> getCommitDescription();

  /**
   * Returns the user provided data for the commit point.
   * Empty Optional instance is returned if multiple indexes are opened.
   *
   * @throws org.apache.lucene.luke.models.LukeException - if an internal error occurs when accessing index
   */
  Optional<String> getCommitUserData();

  /**
   * Returns all fields with the number of terms for each field sorted by {@link TermCountsOrder}
   *
   * @param order - the sort order
   * @return the ordered map of terms and their frequencies
   * @throws org.apache.lucene.luke.models.LukeException - if an internal error occurs when accessing index
   */
  Map<String, Long> getSortedTermCounts(TermCountsOrder order);

  /**
   * Returns the top indexed terms with their statistics for the specified field.
   *
   * @param field - the field name
   * @param numTerms - the max number of terms to be returned
   * @return the list of top terms and their document frequencies
   * @throws org.apache.lucene.luke.models.LukeException - if an internal error occurs when accessing index
   */
  List<TermStats> getTopTerms(String field, int numTerms);
}
