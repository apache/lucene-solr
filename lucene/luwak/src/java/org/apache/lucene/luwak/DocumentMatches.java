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

package org.apache.lucene.luwak;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * Records matches from a match run for a specific {@link InputDocument} within a {@link DocumentBatch}
 *
 * @param <T> the type of {@link QueryMatch} recorded
 */
public class DocumentMatches<T extends QueryMatch> implements Iterable<T> {

  private final String docId;

  private final Collection<T> matches;

  /**
   * Create a DocumentMatches object recording no matches for a given document
   *
   * @param docId Lucene document id for the document
   * @return a DocumentMatches object recording no matches for a given document
   */
  public static <T extends QueryMatch> DocumentMatches<T> noMatches(String docId) {
    return new DocumentMatches<>(docId, Collections.<T>emptyList());
  }

  /**
   * Create a DocumentMatches object for a specific document
   *
   * @param docId   the document id
   * @param matches a collection of QueryMatch objects
   */
  public DocumentMatches(String docId, Collection<T> matches) {
    this.docId = docId;
    this.matches = matches;
  }

  @Override
  public Iterator<T> iterator() {
    return matches.iterator();
  }

  /**
   * Return the docid for this object
   *
   * @return the docid for this object
   */
  public String getDocId() {
    return docId;
  }

  /**
   * Return the matches for this object
   *
   * @return the matches for this object
   */
  public Collection<T> getMatches() {
    return matches;
  }
}
