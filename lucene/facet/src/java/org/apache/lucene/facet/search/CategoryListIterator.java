package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.util.IntsRef;

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
 * An interface for obtaining the category ordinals of documents.
 * <p>
 * <b>NOTE:</b> this class operates as a key to a map, and therefore you should
 * implement {@code equals()} and {@code hashCode()} for proper behavior.
 * 
 * @lucene.experimental
 */
public interface CategoryListIterator {

  /**
   * Initializes the iterator. This method must be called before any calls to
   * {@link #getOrdinals(int, IntsRef)}, and its return value indicates whether there are
   * any relevant documents for this iterator.
   */
  public boolean init() throws IOException;

  /**
   * Stores the category ordinals of the given document ID in the given
   * {@link IntsRef}, starting at position 0 upto {@link IntsRef#length}. Grows
   * the {@link IntsRef} if it is not large enough.
   * 
   * <p>
   * <b>NOTE:</b> if the requested document does not category ordinals
   * associated with it, {@link IntsRef#length} is set to zero.
   */
  public void getOrdinals(int docID, IntsRef ints) throws IOException;
  
}
