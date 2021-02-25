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
package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * Source of {@link Impacts}.
 *
 * @lucene.internal
 */
public interface ImpactsSource {

  /**
   * Shallow-advance to {@code target}. This is cheaper than calling {@link
   * DocIdSetIterator#advance(int)} and allows further calls to {@link #getImpacts()} to ignore doc
   * IDs that are less than {@code target} in order to get more precise information about impacts.
   * This method may not be called on targets that are less than the current {@link
   * DocIdSetIterator#docID()}. After this method has been called, {@link
   * DocIdSetIterator#nextDoc()} may not be called if the current doc ID is less than {@code target
   * - 1} and {@link DocIdSetIterator#advance(int)} may not be called on targets that are less than
   * {@code target}.
   */
  void advanceShallow(int target) throws IOException;

  /**
   * Get information about upcoming impacts for doc ids that are greater than or equal to the
   * maximum of {@link DocIdSetIterator#docID()} and the last target that was passed to {@link
   * #advanceShallow(int)}. This method may not be called on an unpositioned iterator on which
   * {@link #advanceShallow(int)} has never been called. NOTE: advancing this iterator may
   * invalidate the returned impacts, so they should not be used after the iterator has been
   * advanced.
   */
  Impacts getImpacts() throws IOException;
}
