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
package org.apache.lucene.search;

import java.io.IOException;

/**
 * Decorates a wrapped LeafFieldComparator to add a functionality to skip over non-competitive docs.
 * FilteringLeafFieldComparator provides two additional functions to a LeafFieldComparator:
 *  {@code competitiveIterator()} and {@code setCanUpdateIterator()}.
 */
public interface FilteringLeafFieldComparator extends LeafFieldComparator {
  /**
   * Returns a competitive iterator
   * @return an iterator over competitive docs that are stronger than already collected docs
   * or {@code null} if such an iterator is not available for the current segment.
   */
  DocIdSetIterator competitiveIterator() throws IOException;

  /**
   * Informs this leaf comparator that it is allowed to start updating its competitive iterator.
   * This method is called from a collector when queue becomes full and threshold is reached.
   */
  void setCanUpdateIterator() throws IOException;
}
