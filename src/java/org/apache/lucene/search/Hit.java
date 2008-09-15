/**
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

package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;

/**
 * Wrapper used by {@link HitIterator} to provide a lazily loaded hit
 * from {@link Hits}.
 *
 * @deprecated Hits will be removed in Lucene 3.0. Use {@link TopDocCollector} and {@link TopDocs} instead.
 */
public class Hit implements java.io.Serializable {

  private Document doc = null;

  private boolean resolved = false;

  private Hits hits = null;
  private int hitNumber;

  /**
   * Constructed from {@link HitIterator}
   * @param hits Hits returned from a search
   * @param hitNumber Hit index in Hits
   */
  Hit(Hits hits, int hitNumber) {
    this.hits = hits;
    this.hitNumber = hitNumber;
  }

  /**
   * Returns document for this hit.
   *
   * @see Hits#doc(int)
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public Document getDocument() throws CorruptIndexException, IOException {
    if (!resolved) fetchTheHit();
    return doc;
  }

  /**
   * Returns score for this hit.
   *
   * @see Hits#score(int)
   */
  public float getScore() throws IOException {
    return hits.score(hitNumber);
  }

  /**
   * Returns id for this hit.
   *
   * @see Hits#id(int)
   */
  public int getId() throws IOException {
    return hits.id(hitNumber);
  }

  private void fetchTheHit() throws CorruptIndexException, IOException {
    doc = hits.doc(hitNumber);
    resolved = true;
  }

  // provide some of the Document style interface (the simple stuff)

  /**
   * Returns the boost factor for this hit on any field of the underlying document.
   *
   * @see Document#getBoost()
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public float getBoost() throws CorruptIndexException, IOException {
    return getDocument().getBoost();
  }

  /**
   * Returns the string value of the field with the given name if any exist in
   * this document, or null.  If multiple fields exist with this name, this
   * method returns the first value added. If only binary fields with this name
   * exist, returns null.
   *
   * @see Document#get(String)
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public String get(String name) throws CorruptIndexException, IOException {
    return getDocument().get(name);
  }

  /**
   * Prints the parameters to be used to discover the promised result.
   */
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Hit<");
    buffer.append(hits.toString());
    buffer.append(" [");
    buffer.append(hitNumber);
    buffer.append("] ");
    if (resolved) {
        buffer.append("resolved");
    } else {
        buffer.append("unresolved");
    }
    buffer.append(">");
    return buffer.toString();
  }


}
