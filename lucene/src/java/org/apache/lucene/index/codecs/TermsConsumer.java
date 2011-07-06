package org.apache.lucene.index.codecs;

/**
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

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.MultiDocsEnum;
import org.apache.lucene.index.MultiDocsAndPositionsEnum;

import org.apache.lucene.util.BytesRef;

/**
 * @lucene.experimental
 */

public abstract class TermsConsumer {

  /** Starts a new term in this field; this may be called
   *  with no corresponding call to finish if the term had
   *  no docs. */
  public abstract PostingsConsumer startTerm(BytesRef text) throws IOException;

  /** Finishes the current term; numDocs must be > 0. */
  public abstract void finishTerm(BytesRef text, TermStats stats) throws IOException;

  /** Called when we are done adding terms to this field */
  public abstract void finish(long sumTotalTermFreq) throws IOException;

  /** Return the BytesRef Comparator used to sort terms
   *  before feeding to this API. */
  public abstract Comparator<BytesRef> getComparator() throws IOException;

  /** Default merge impl */
  private MappingMultiDocsEnum docsEnum = null;
  private MappingMultiDocsAndPositionsEnum postingsEnum = null;

  public void merge(MergeState mergeState, TermsEnum termsEnum) throws IOException {

    BytesRef term;
    assert termsEnum != null;
    long sumTotalTermFreq = 0;
    long sumDF = 0;

    if (mergeState.fieldInfo.omitTermFreqAndPositions) {
      if (docsEnum == null) {
        docsEnum = new MappingMultiDocsEnum();
      }
      docsEnum.setMergeState(mergeState);

      MultiDocsEnum docsEnumIn = null;

      while((term = termsEnum.next()) != null) {
        docsEnumIn = (MultiDocsEnum) termsEnum.docs(mergeState.multiLiveDocs, docsEnumIn);
        if (docsEnumIn != null) {
          docsEnum.reset(docsEnumIn);
          final PostingsConsumer postingsConsumer = startTerm(term);
          final TermStats stats = postingsConsumer.merge(mergeState, docsEnum);
          if (stats.docFreq > 0) {
            finishTerm(term, stats);
            sumDF += stats.docFreq;
            if (sumDF > 60000) {
              mergeState.checkAbort.work(sumDF/5.0);
              sumDF = 0;
            }
          }
        }
      }
    } else {
      if (postingsEnum == null) {
        postingsEnum = new MappingMultiDocsAndPositionsEnum();
      }
      postingsEnum.setMergeState(mergeState);
      MultiDocsAndPositionsEnum postingsEnumIn = null;
      while((term = termsEnum.next()) != null) {
        postingsEnumIn = (MultiDocsAndPositionsEnum) termsEnum.docsAndPositions(mergeState.multiLiveDocs, postingsEnumIn);
        if (postingsEnumIn != null) {
          postingsEnum.reset(postingsEnumIn);
          // set PayloadProcessor
          if (mergeState.hasPayloadProcessorProvider) {
            for (int i = 0; i < mergeState.readerCount; i++) {
              if (mergeState.dirPayloadProcessor[i] != null) {
                mergeState.currentPayloadProcessor[i] = mergeState.dirPayloadProcessor[i].getProcessor(mergeState.fieldInfo.name, term);
              }
            }
          }
          final PostingsConsumer postingsConsumer = startTerm(term);
          final TermStats stats = postingsConsumer.merge(mergeState, postingsEnum);
          if (stats.docFreq > 0) {
            finishTerm(term, stats);
            sumTotalTermFreq += stats.totalTermFreq;
            sumDF += stats.docFreq;
            if (sumDF > 60000) {
              mergeState.checkAbort.work(sumDF/5.0);
              sumDF = 0;
            }
          }
        }
      }
    }

    finish(sumTotalTermFreq);
  }
}
