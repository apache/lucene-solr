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

package org.apache.lucene.queries.mlt.terms;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.mlt.MoreLikeThisParameters;
import org.apache.lucene.queries.mlt.terms.scorer.ScoredTerm;
import org.apache.lucene.util.PriorityQueue;

/**
 * This class has the responsiblity of extracting interesting terms from a lucene document in input.
 * Each term will have a score assigned, indicating how much important is in the field.
 *
 * This class is currently used in :
 * - CloudMLTQParser
 */
public class LuceneDocumentTermsRetriever extends InterestingTermsRetriever{

  public LuceneDocumentTermsRetriever(IndexReader ir, MoreLikeThisParameters params) {
    this.ir = ir;
    this.parameters =params;
  }

  public LuceneDocumentTermsRetriever(IndexReader ir) {
    this.ir = ir;
  }

  public PriorityQueue<ScoredTerm> retrieveTermsFromDocument(Document luceneDocument) throws
      IOException {
    DocumentTermFrequencies perFieldTermFrequencies = new DocumentTermFrequencies();
    Map<String, Float> fieldToNorm = new HashMap<>();
    for (String fieldName : parameters.getFieldNames()) {
      for (IndexableField field : luceneDocument.getFields(fieldName)) {
        updateTermFrequenciesCount(field, perFieldTermFrequencies);
        float indexTimeBoost=1.0f; // at the moment we will stand with this simplification
        float norm = getNorm(perFieldTermFrequencies, fieldName,indexTimeBoost);
        fieldToNorm.put(fieldName,norm);
      }
    }
    super.interestingTermsScorer.setField2norm(fieldToNorm);

    return retrieveInterestingTerms(perFieldTermFrequencies);
  }



}
