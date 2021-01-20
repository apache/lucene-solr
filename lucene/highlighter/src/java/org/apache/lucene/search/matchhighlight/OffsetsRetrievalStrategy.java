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
package org.apache.lucene.search.matchhighlight;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.search.MatchesIterator;

/**
 * Determines how match offset regions are computed from {@link MatchesIterator}. Several
 * possibilities exist, ranging from retrieving offsets directly from a match instance to
 * re-evaluating the document's field and recomputing offsets from there.
 */
public interface OffsetsRetrievalStrategy {
  /** Return value offsets (match ranges) acquired from the given {@link MatchesIterator}. */
  List<OffsetRange> get(
      MatchesIterator matchesIterator, MatchRegionRetriever.FieldValueProvider doc)
      throws IOException;

  /** Whether this strategy requires document field access. */
  default boolean requiresDocument() {
    return false;
  }
}
