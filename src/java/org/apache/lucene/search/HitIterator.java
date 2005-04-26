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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator over {@link Hits} that provides lazy fetching of each document.
 * {@link Hits#iterator()} returns an instance of this class.  Calls to {@link #next()}
 * return a {@link Hit} instance.
 *
 * @author Jeremy Rayner
 */
public class HitIterator implements Iterator {
  private Hits hits;
  private int hitNumber = 0;

  /**
   * Constructed from {@link Hits#iterator()}.
   */
  HitIterator(Hits hits) {
    this.hits = hits;
  }

  /**
   * @return true if current hit is less than the total number of {@link Hits}.
   */
  public boolean hasNext() {
    return hitNumber < hits.length();
  }

  /**
   * Returns a {@link Hit} instance representing the next hit in {@link Hits}.
   *
   * @return Next {@link Hit}.
   */
  public Object next() {
    try {
      Object next = new Hit(hits, hitNumber);
      hitNumber++;
      return next;
    } catch (IndexOutOfBoundsException e) {
      throw new NoSuchElementException();
    }
  }

  /**
   * Unsupported operation.
   *
   * @throws UnsupportedOperationException
   */
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the total number of hits.
   */
  public int length() {
    return hits.length();
  }
}

