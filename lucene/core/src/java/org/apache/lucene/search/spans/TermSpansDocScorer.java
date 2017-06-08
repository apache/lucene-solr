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

import org.apache.lucene.index.PostingsEnum;


/**
 * For {@link SpansTreeQuery}. Public for extension.
 *
 * @lucene.experimental
 */
public class TermSpansDocScorer extends AsSingleTermSpansDocScorer<TermSpans> {

  protected final PostingsEnum postings;

  /**
   * @param termSpans Provides matching term occurrences.
   * @param nonMatchWeight The non negative weight to be used for the non matching term occurrences.
   */
  public TermSpansDocScorer(TermSpans termSpans, double nonMatchWeight) {
    super(termSpans.simScorer, nonMatchWeight);
    this.postings = termSpans.getPostings();
  }

  @Override
  public int termFreqInDoc() throws IOException {
    return postings.freq();
  }
}
