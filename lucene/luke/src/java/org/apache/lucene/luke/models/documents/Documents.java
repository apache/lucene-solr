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

package org.apache.lucene.luke.models.documents;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.luke.models.LukeException;

/** A dedicated interface for Luke's Documents tab. */
public interface Documents {

  /** Returns one greater than the largest possible document number. */
  int getMaxDoc();

  /** Returns field names in this index. */
  Collection<String> getFieldNames();

  /**
   * Returns true if the document with the specified <code>docid</code> is not deleted, otherwise
   * false.
   *
   * @param docid - document id
   */
  boolean isLive(int docid);

  /**
   * Returns the list of field information and field data for the specified document.
   *
   * @param docid - document id
   * @throws LukeException - if an internal error occurs when accessing index
   */
  List<DocumentField> getDocumentFields(int docid);

  /** Returns the current target field name. */
  String getCurrentField();

  /**
   * Returns the first indexed term in the specified field. Empty Optional instance is returned if
   * no terms are available for the field.
   *
   * @param field - field name
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Optional<Term> firstTerm(String field);

  /**
   * Increments the terms iterator and returns the next indexed term for the target field. Empty
   * Optional instance is returned if the terms iterator has not been positioned yet, or has been
   * exhausted.
   *
   * @return next term, if exists, or empty
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Optional<Term> nextTerm();

  /**
   * Seeks to the specified term, if it exists, or to the next (ceiling) term. Returns the term that
   * was found. Empty Optional instance is returned if the terms iterator has not been positioned
   * yet, or has been exhausted.
   *
   * @param termText - term to seek
   * @return found term, if exists, or empty
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Optional<Term> seekTerm(String termText);

  /**
   * Returns the first document id (posting) associated with the current term. Empty Optional
   * instance is returned if the terms iterator has not been positioned yet, or the postings
   * iterator has been exhausted.
   *
   * @return document id, if exists, or empty
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Optional<Integer> firstTermDoc();

  /**
   * Increments the postings iterator and returns the next document id (posting) for the current
   * term. Empty Optional instance is returned if the terms iterator has not been positioned yet, or
   * the postings iterator has been exhausted.
   *
   * @return document id, if exists, or empty
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Optional<Integer> nextTermDoc();

  /**
   * Returns the list of the position information for the current posting.
   *
   * @throws LukeException - if an internal error occurs when accessing index
   */
  List<TermPosting> getTermPositions();

  /**
   * Returns the document frequency for the current term (the number of documents containing the
   * current term.) Empty Optional instance is returned if the terms iterator has not been
   * positioned yet.
   *
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Optional<Integer> getDocFreq();

  /**
   * Returns the term vectors for the specified field in the specified document. If no term vector
   * is available for the field, empty list is returned.
   *
   * @param docid - document id
   * @param field - field name
   * @return list of term vector elements
   * @throws LukeException - if an internal error occurs when accessing index
   */
  List<TermVectorEntry> getTermVectors(int docid, String field);

  /**
   * Returns the doc values for the specified field in the specified document. Empty Optional
   * instance is returned if no doc values is available for the field.
   *
   * @param docid - document id
   * @param field - field name
   * @return doc values, if exists, or empty
   * @throws LukeException - if an internal error occurs when accessing index
   */
  Optional<DocValues> getDocValues(int docid, String field);
}
