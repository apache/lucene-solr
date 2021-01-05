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

package org.apache.lucene.luke.models.tools;

import java.io.PrintStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.search.Query;

/** A dedicated interface for Luke's various index manipulations. */
public interface IndexTools {

  /**
   * Execute force merges.
   *
   * <p>Merges are executed until there are <i>maxNumSegments</i> segments. <br>
   * When <i>expunge</i> is true, <i>maxNumSegments</i> parameter is ignored.
   *
   * @param expunge - if true, only segments having deleted documents are merged
   * @param maxNumSegments - max number of segments
   * @param ps - information stream
   * @throws LukeException - if an internal error occurs when accessing index
   */
  void optimize(boolean expunge, int maxNumSegments, PrintStream ps);

  /**
   * Check the current index status.
   *
   * @param ps information stream
   * @return index status
   * @throws LukeException - if an internal error occurs when accessing index
   */
  CheckIndex.Status checkIndex(PrintStream ps);

  /**
   * Try to repair the corrupted index using previously returned index status.
   *
   * <p>This method must be called with the return value from {@link
   * IndexTools#checkIndex(PrintStream)}.
   *
   * @param st - index status
   * @param ps - information stream
   * @throws LukeException - if an internal error occurs when accessing index
   */
  void repairIndex(CheckIndex.Status st, PrintStream ps);

  /**
   * Add new document to this index.
   *
   * @param doc - document to be added
   * @param analyzer - analyzer for parsing to document
   * @throws LukeException - if an internal error occurs when accessing index
   */
  void addDocument(Document doc, Analyzer analyzer);

  /**
   * Delete documents from this index by the specified query.
   *
   * @param query - query for deleting
   * @throws LukeException - if an internal error occurs when accessing index
   */
  void deleteDocuments(Query query);

  /**
   * Create a new index.
   *
   * @throws LukeException - if an internal error occurs when accessing index
   */
  void createNewIndex();

  /**
   * Create a new index with sample documents.
   *
   * @param dataDir - the directory path which contains sample documents (20 Newsgroups).
   */
  void createNewIndex(String dataDir);

  /**
   * Export terms from given field into a new file on the destination directory
   *
   * @param destDir - destination directory
   * @param field - field name
   * @param delimiter - delimiter to separate terms and their frequency
   * @return The file containing the export
   */
  String exportTerms(String destDir, String field, String delimiter);
}
