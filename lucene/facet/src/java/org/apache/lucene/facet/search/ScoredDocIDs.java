package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;

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
 * Document IDs with scores for each, driving facets accumulation. Document
 * scores are optionally used in the process of facets scoring.
 * 
 * @see StandardFacetsAccumulator#accumulate(ScoredDocIDs)
 * @lucene.experimental
 */
public interface ScoredDocIDs {

  /** Returns an iterator over the document IDs and their scores. */
  public ScoredDocIDsIterator iterator() throws IOException;

  /** Returns the set of doc IDs. */
  public DocIdSet getDocIDs();

  /** Returns the number of scored documents. */
  public int size();

}
