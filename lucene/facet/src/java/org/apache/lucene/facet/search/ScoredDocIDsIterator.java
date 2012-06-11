package org.apache.lucene.facet.search;

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
 * Iterator over document IDs and their scores. Each {@link #next()} retrieves
 * the next docID and its score which can be later be retrieved by
 * {@link #getDocID()} and {@link #getScore()}. <b>NOTE:</b> you must call
 * {@link #next()} before {@link #getDocID()} and/or {@link #getScore()}, or
 * otherwise the returned values are unexpected.
 * 
 * @lucene.experimental
 */
public interface ScoredDocIDsIterator {

  /** Default score used in case scoring is disabled. */
  public static final float DEFAULT_SCORE = 1.0f;

  /** Iterate to the next document/score pair. Returns true iff there is such a pair. */
  public abstract boolean next();

  /** Returns the ID of the current document. */
  public abstract int getDocID();

  /** Returns the score of the current document. */
  public abstract float getScore();

}