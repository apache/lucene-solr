package org.apache.lucene.search;

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

import java.io.IOException;

/**
 * This abstract class defines methods to iterate over a set of non-decreasing
 * doc ids. Note that this class assumes it iterates on doc Ids, and therefore
 * {@link #NO_MORE_DOCS} is set to {@value #NO_MORE_DOCS} in order to be used as
 * a sentinel object. Implementations of this class are expected to consider
 * {@link Integer#MAX_VALUE} as an invalid value.
 */
public abstract class DocIdSetIterator {
  
  // TODO (3.0): review the javadocs and remove any references to '3.0'.
  private int doc = -1;
  
  /**
   * When returned by {@link #nextDoc()}, {@link #advance(int)} and
   * {@link #doc()} it means there are no more docs in the iterator.
   */
  public static final int NO_MORE_DOCS = Integer.MAX_VALUE;

  /**
   * Unsupported anymore. Call {@link #docID()} instead. This method throws
   * {@link UnsupportedOperationException} if called.
   * 
   * @deprecated use {@link #docID()} instead.
   */
  public int doc() {
    throw new UnsupportedOperationException("Call docID() instead.");
  }

  /**
   * Returns the following:
   * <ul>
   * <li>-1 or {@link #NO_MORE_DOCS} if {@link #nextDoc()} or
   * {@link #advance(int)} were not called yet.
   * <li>{@link #NO_MORE_DOCS} if the iterator has exhausted.
   * <li>Otherwise it should return the doc ID it is currently on.
   * </ul>
   * <p>
   * <b>NOTE:</b> in 3.0, this method will become abstract.
   * 
   * @since 2.9
   */
  public int docID() {
    return doc;
  }

  /**
   * Unsupported anymore. Call {@link #nextDoc()} instead. This method throws
   * {@link UnsupportedOperationException} if called.
   * 
   * @deprecated use {@link #nextDoc()} instead. This will be removed in 3.0
   */
  public boolean next() throws IOException {
    throw new UnsupportedOperationException("Call nextDoc() instead.");
  }

  /**
   * Unsupported anymore. Call {@link #advance(int)} instead. This method throws
   * {@link UnsupportedOperationException} if called.
   * 
   * @deprecated use {@link #advance(int)} instead. This will be removed in 3.0
   */
  public boolean skipTo(int target) throws IOException {
    throw new UnsupportedOperationException("Call advance() instead.");
  }

  /**
   * Advances to the next document in the set and returns the doc it is
   * currently on, or {@link #NO_MORE_DOCS} if there are no more docs in the
   * set.<br>
   * 
   * <b>NOTE:</b> in 3.0 this method will become abstract, following the removal
   * of {@link #next()}. For backward compatibility it is implemented as:
   * 
   * <pre>
   * public int nextDoc() throws IOException {
   *   return next() ? doc() : NO_MORE_DOCS;
   * }
   * </pre>
   * 
   * <b>NOTE:</b> after the iterator has exhausted you should not call this
   * method, as it may result in unpredicted behavior.
   * 
   * @since 2.9
   */
  public int nextDoc() throws IOException {
    return doc = next() ? doc() : NO_MORE_DOCS;
  }

  /**
   * Advances to the first beyond the current whose document number is greater
   * than or equal to <i>target</i>. Returns the current document number or
   * {@link #NO_MORE_DOCS} if there are no more docs in the set.
   * <p>
   * Behaves as if written:
   * 
   * <pre>
   * int advance(int target) {
   *   int doc;
   *   while ((doc = nextDoc()) &lt; target) {
   *   }
   *   return doc;
   * }
   * </pre>
   * 
   * Some implementations are considerably more efficient than that.
   * <p>
   * <b>NOTE:</b> certain implementations may return a different value (each
   * time) if called several times in a row with the same target.
   * <p>
   * <b>NOTE:</b> this method may be called with {@value #NO_MORE_DOCS} for
   * efficiency by some Scorers. If your implementation cannot efficiently
   * determine that it should exhaust, it is recommended that you check for that
   * value in each call to this method.
   * <p>
   * <b>NOTE:</b> after the iterator has exhausted you should not call this
   * method, as it may result in unpredicted behavior.
   * <p>
   * <b>NOTE:</b> in 3.0 this method will become abstract, following the removal
   * of {@link #skipTo(int)}.
   * 
   * @since 2.9
   */
  public int advance(int target) throws IOException {
    if (target == NO_MORE_DOCS) {
      return doc = NO_MORE_DOCS;
    }
    return doc = skipTo(target) ? doc() : NO_MORE_DOCS;
  }

}
