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
 * Extension of {@link PostingsEnum} which also provides information about the
 * produced scores.
 * @lucene.experimental
 */
public abstract class ImpactsEnum extends PostingsEnum {

  /** Sole constructor. */
  protected ImpactsEnum() {}

  /**
   * Advance to the block of documents that contains {@code target} in order to
   * get scoring information about this block. This method is implicitly called
   * by {@link DocIdSetIterator#advance(int)} and
   * {@link DocIdSetIterator#nextDoc()}. Calling this method doesn't modify the
   * current {@link DocIdSetIterator#docID()}.
   * It returns a number that is greater than or equal to all documents
   * contained in the current block, but less than any doc IDS of the next block.
   * {@code target} must be &gt;= {@link #docID()} as well as all targets that
   * have been passed to {@link #advanceShallow(int)} so far.
   */
  public abstract int advanceShallow(int target) throws IOException;

  /**
   * Return the maximum score that documents between the last {@code target}
   * that this iterator was {@link #advanceShallow(int) shallow-advanced} to
   * included and {@code upTo} included.
   */
  public abstract float getMaxScore(int upTo) throws IOException;

}
