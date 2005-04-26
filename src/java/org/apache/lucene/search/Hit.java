/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

/**
 * a lazy future for a hit, useful for iterators over instances of Hits
 *
 * @author Jeremy Rayner
 */
public class Hit implements java.io.Serializable {

  private float score;
  private int id;
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
   * @see {@link Hits#doc(int)}
   */
  public Document getDocument() throws IOException {
    if (!resolved) fetchTheHit();
    return doc;
  }

  /**
   * Returns score for this hit.
   *
   * @see {@link Hits#score(int)}
   */
  public float getScore() throws IOException {
    if (!resolved) fetchTheHit();
    return score;
  }

  /**
   * Returns id for this hit.
   *
   * @see {@link Hits#id(int)}
   */
  public int getId() throws IOException {
    if (!resolved) fetchTheHit();
    return id;
  }

  private void fetchTheHit() throws IOException {
    doc = hits.doc(hitNumber);
    score = hits.score(hitNumber);
    id = hits.id(hitNumber);
    resolved = true;
  }

  // provide some of the Document style interface (the simple stuff)

  /**
   * Returns the boost factor for this hit on any field of the underlying document.
   *
   * @see {@link Document#getBoost()}
   */
  public float getBoost() throws IOException {
    return getDocument().getBoost();
  }

  /**
   * Returns the string value of the field with the given name if any exist in
   * this document, or null.  If multiple fields exist with this name, this
   * method returns the first value added. If only binary fields with this name
   * exist, returns null.
   *
   * @see {@link Document#get(String)}
   */
  public String get(String name) throws IOException {
    return getDocument().get(name);
  }

  /**
   * Prints the fields of the underlying document for human consumption.
   * <p/>
   * If an IOException occurs whilst getting the document, returns null
   *
   * @see {@link Document#toString()} 
   */
  public String toString() {
    try {
      return getDocument().toString();
    } catch (IOException e) {
      return null;
    }
  }


}
