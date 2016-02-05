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
package org.apache.lucene.search.spans;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.search.IndexSearcher;

/**
 * 
 * A wrapper to perform span operations on a non-leaf reader context
 * <p>
 * NOTE: This should be used for testing purposes only
 * @lucene.internal
 */
public class MultiSpansWrapper {

  public static Spans wrap(IndexReader reader, SpanQuery spanQuery) throws IOException {
    return wrap(reader, spanQuery, SpanWeight.Postings.POSITIONS);
  }

  public static Spans wrap(IndexReader reader, SpanQuery spanQuery, SpanWeight.Postings requiredPostings) throws IOException {

    LeafReader lr = SlowCompositeReaderWrapper.wrap(reader); // slow, but ok for testing
    LeafReaderContext lrContext = lr.getContext();
    IndexSearcher searcher = new IndexSearcher(lr);
    searcher.setQueryCache(null);

    SpanWeight w = spanQuery.createWeight(searcher, false);

    return w.getSpans(lrContext, requiredPostings);
  }
}
