package org.apache.lucene.facet.search;

import java.io.IOException;

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

/**
 * An interface for iterating over a "category list", i.e., the list of
 * categories per document.
 * <p>
 * <b>NOTE:</b>
 * <ul>
 * <li>This class operates as a key to a Map. Appropriate implementation of
 * <code>hashCode()</code> and <code>equals()</code> must be provided.
 * <li>{@link #init()} must be called before you consume any categories, or call
 * {@link #skipTo(int)}.
 * <li>{@link #skipTo(int)} must be called before any calls to
 * {@link #nextCategory()}.
 * <li>{@link #nextCategory()} returns values &lt; {@link Integer#MAX_VALUE}, so
 * you can use it as a stop condition.
 * </ul>
 * 
 * @lucene.experimental
 */
public interface CategoryListIterator {

  /**
   * Initializes the iterator. This method must be called before any calls to
   * {@link #skipTo(int)}, and its return value indicates whether there are
   * any relevant documents for this iterator. If it returns false, any call
   * to {@link #skipTo(int)} will return false as well.<br>
   * <b>NOTE:</b> calling this method twice may result in skipping over
   * documents for some implementations. Also, calling it again after all
   * documents were consumed may yield unexpected behavior.
   */
  public boolean init() throws IOException;

  /**
   * Skips forward to document docId. Returns true iff this document exists
   * and has any categories. This method must be called before calling
   * {@link #nextCategory()} for a particular document.<br>
   * <b>NOTE:</b> Users should call this method with increasing docIds, and
   * implementations can assume that this is the case.
   */
  public boolean skipTo(int docId) throws IOException;

  /**
   * Returns the next category for the current document that is set through
   * {@link #skipTo(int)}, or a number higher than {@link Integer#MAX_VALUE}.
   * No assumptions can be made on the order of the categories.
   */
  public long nextCategory() throws IOException;

}
