package org.apache.lucene.search;
/**
 * Copyright 2007 The Apache Software Foundation
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

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/** Abstract base class providing a mechanism to restrict searches to a subset
 of an index and also maintains and returns position information.

 This is useful if you want to compare the positions from a SpanQuery with the positions of items in
 a filter.  For instance, if you had a SpanFilter that marked all the occurrences of the word "foo" in documents,
 and then you entered a new SpanQuery containing bar, you could not only filter by the word foo, but you could
 then compare position information for post processing.
 */
public abstract class SpanFilter extends Filter{
  /** Returns a SpanFilterResult with true for documents which should be permitted in
    search results, and false for those that should not and Spans for where the true docs match.
   * @param context The {@link AtomicReaderContext} to load position and DocIdSet information from
   * @return A {@link SpanFilterResult}
   * @throws java.io.IOException if there was an issue accessing the necessary information
   * */
  public abstract SpanFilterResult bitSpans(AtomicReaderContext context, Bits acceptDocs) throws IOException;
}
