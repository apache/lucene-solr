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
import java.util.ArrayList;

/**
 * For {@link SpansTreeQuery}. Public for extension.
 *
 * @lucene.experimental
 */
public class SynonymSpansDocScorer
      extends AsSingleTermSpansDocScorer<SynonymSpans> {

  protected final ArrayList<Spans> subSpansAtDoc;
  protected final SynonymSpans synSpans;

  /**
   * @param synSpans       Provides matching synonym occurrences.
   *                       This should only contain TermSpans.
   * @param nonMatchWeight The non negative weight to be used for the non matching term occurrences.
   */
  public SynonymSpansDocScorer(SynonymSpans synSpans, double nonMatchWeight) {
    super(synSpans.simScorer, nonMatchWeight);
    this.synSpans = synSpans;
    this.subSpansAtDoc = new ArrayList<>(synSpans.subSpans().size());
  }

  @Override
  public int termFreqInDoc() throws IOException {
    int freq = 0;
    for (Spans subSpans : subSpansAtDoc) {
      freq += ((TermSpans)subSpans).getPostings().freq();
    }
    return freq;
  }

  @Override
  public void beginDoc(int doc) throws IOException {
    subSpansAtDoc.clear();
    synSpans.extractSubSpansAtCurrentDoc(subSpansAtDoc);
    assert subSpansAtDoc.size() > 0 : "empty subSpansAtDoc docID=" + docID();
    assert subSpansAtDoc.get(0).docID() == doc;
    super.beginDoc(doc); // calls termFreqInDoc.
  }

}
