package org.apache.lucene.search.spans;

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

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;

import java.io.IOException;

/**
 * An interface defining the collection of postings information from the leaves
 * of a {@link org.apache.lucene.search.spans.Spans}
 *
 * Typical use would be as follows:
 * <pre>
 *   while (spans.nextStartPosition() != NO_MORE_POSITIONS) {
 *     spanCollector.reset();
 *     spans.collect(spanCollector);
 *     doSomethingWith(spanCollector);
 *   }
 * </pre>
 *
 * @lucene.experimental
 */
public interface SpanCollector {

  /**
   * Called to indicate that the driving {@link org.apache.lucene.search.spans.Spans} has
   * been moved to a new position
   */
  public void reset();

  /**
   * Returns an integer indicating what postings information should be retrieved
   *
   * See {@link org.apache.lucene.index.TermsEnum#postings(org.apache.lucene.util.Bits, org.apache.lucene.index.PostingsEnum, int)}
   *
   * @return the postings flag
   */
  public int requiredPostings();

  /**
   * Collect information from postings
   * @param postings a {@link PostingsEnum}
   * @param term     the {@link Term} for this postings list
   * @throws IOException on error
   */
  public void collectLeaf(PostingsEnum postings, Term term) throws IOException;

  /**
   * Return a {@link BufferedSpanCollector} for use by eager spans implementations, such
   * as {@link NearSpansOrdered}.
   *
   * @return a BufferedSpanCollector
   */
  public BufferedSpanCollector buffer();

  /**
   * @return the SpanCollector used by the {@link org.apache.lucene.search.spans.BufferedSpanCollector}
   *          returned from {@link #buffer()}.
   */
  public SpanCollector bufferedCollector();

  /**
   * A default No-op implementation of SpanCollector
   */
  public static final SpanCollector NO_OP = new SpanCollector() {

    @Override
    public void reset() {

    }

    @Override
    public int requiredPostings() {
      return PostingsEnum.POSITIONS;
    }

    @Override
    public void collectLeaf(PostingsEnum postings, Term term) {

    }

    @Override
    public BufferedSpanCollector buffer() {
      return BufferedSpanCollector.NO_OP;
    }

    @Override
    public SpanCollector bufferedCollector() {
      return this;
    }
  };

}
