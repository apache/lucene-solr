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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.mlt.MoreLikeThisParameters;
import org.apache.lucene.queries.mlt.terms.scorer.ScoredTerm;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.PriorityQueue;

/**
 * This class has the responsiblity of extracting interesting terms from a document already indexed.
 * Each term will have a score assigned, indicating how much important is in the field.
 *
 * This class is currently used in :
 * - MoreLikeThis Request Handler
 * - Simple More Like This query parser
 */
public class LocalDocumentTermsRetriever extends InterestingTermsRetriever{

  public LocalDocumentTermsRetriever(IndexReader ir) {
    this.ir = ir;
  }

  public LocalDocumentTermsRetriever(IndexReader ir, MoreLikeThisParameters params) {
    this.ir = ir;
    this.parameters =params;
  }

  /**
   * Find words for a more-like-this query former.
   *
   * @param docNum the id of the lucene document from which to find terms
   */
  public PriorityQueue<ScoredTerm> retrieveTermsFromLocalDocument(int docNum) throws IOException {
    DocumentTermFrequencies perFieldTermFrequencies =new DocumentTermFrequencies();
    Map<String, NumericDocValues> fieldToNorms = new HashMap<>();

    for (String fieldName : parameters.getFieldNames()) {
      fieldToNorms.put(fieldName,MultiDocValues.getNormValues(ir,fieldName));
      final Fields vectors = ir.getTermVectors(docNum);
      final Terms vector;

      if (vectors != null) {
        vector = vectors.terms(fieldName);
      } else {
        vector = null;
      }
      // field does not store term vector info
      if (vector == null) {
        Document localDocument = ir.document(docNum);
        IndexableField[] fields = localDocument.getFields(fieldName);
        for (IndexableField field : fields) {
          updateTermFrequenciesCount(field,perFieldTermFrequencies);
        }
      } else {
        updateTermFrequenciesCount(perFieldTermFrequencies, vector, fieldName);
      }
    }
    super.interestingTermsScorer.setField2normsFromIndex(fieldToNorms);
    super.interestingTermsScorer.setDocId(docNum);

    return retrieveInterestingTerms(perFieldTermFrequencies);
  }

  /**
   * Adds terms and frequencies found in vector into the Map termFreqMap
   *
   * @param perFieldTermFrequencies a Map of terms and their frequencies per field
   * @param vector List of terms and their frequencies for a doc/field
   */
  protected void updateTermFrequenciesCount(DocumentTermFrequencies perFieldTermFrequencies, Terms vector, String fieldName) throws IOException {
    final TermsEnum termsEnum = vector.iterator();
    final CharsRefBuilder spare = new CharsRefBuilder();
    BytesRef text;
    while((text = termsEnum.next()) != null) {
      spare.copyUTF8Bytes(text);
      final String term = spare.toString();
      if (isNoiseWord(term)) {
        continue;
      }
      final int freq = (int) termsEnum.totalTermFreq();
      perFieldTermFrequencies.increment(fieldName,term,freq);
    }
  }

  public String[] retrieveInterestingTerms(int docNum) throws IOException {
    final int maxQueryTerms = parameters.getMaxQueryTerms();

    ArrayList<Object> al = new ArrayList<>(maxQueryTerms);
    PriorityQueue<ScoredTerm> pq = retrieveTermsFromLocalDocument(docNum);
    ScoredTerm scoredTerm;
    int lim = maxQueryTerms; // have to be careful, retrieveTerms returns all words but that's probably not useful to our caller...
    // we just want to return the top words
    while (((scoredTerm = pq.pop()) != null) && lim-- > 0) {
      al.add(scoredTerm.term); // the 1st entry is the interesting term
    }
    String[] res = new String[al.size()];
    return al.toArray(res);
  }

}
