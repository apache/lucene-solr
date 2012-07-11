package org.apache.lucene.codecs;

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

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.index.FieldInfo; // javadocs
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.MultiDocsEnum;
import org.apache.lucene.index.MultiDocsAndPositionsEnum;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * Abstract API that consumes terms for an individual field.
 * <p>
 * The lifecycle is:
 * <ol>
 *   <li>TermsConsumer is returned for each field 
 *       by {@link FieldsConsumer#addField(FieldInfo)}.
 *   <li>TermsConsumer returns a {@link PostingsConsumer} for
 *       each term in {@link #startTerm(BytesRef)}.
 *   <li>When the producer (e.g. IndexWriter)
 *       is done adding documents for the term, it calls 
 *       {@link #finishTerm(BytesRef, TermStats)}, passing in
 *       the accumulated term statistics.
 *   <li>Producer calls {@link #finish(long, long, int)} with
 *       the accumulated collection statistics when it is finished
 *       adding terms to the field.
 * </ol>
 * 
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
  public abstract void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException;

  /** Return the BytesRef Comparator used to sort terms
   *  before feeding to this API. */
  public abstract Comparator<BytesRef> getComparator() throws IOException;

  private MappingMultiDocsEnum docsEnum;
  private MappingMultiDocsEnum docsAndFreqsEnum;
  private MappingMultiDocsAndPositionsEnum postingsEnum;

  /** Default merge impl */
  public void merge(MergeState mergeState, TermsEnum termsEnum) throws IOException {

    BytesRef term;
    assert termsEnum != null;
    long sumTotalTermFreq = 0;
    long sumDocFreq = 0;
    long sumDFsinceLastAbortCheck = 0;
    FixedBitSet visitedDocs = new FixedBitSet(mergeState.segmentInfo.getDocCount());

    IndexOptions indexOptions = mergeState.fieldInfo.getIndexOptions();
    if (indexOptions == IndexOptions.DOCS_ONLY) {
      if (docsEnum == null) {
        docsEnum = new MappingMultiDocsEnum();
      }
      docsEnum.setMergeState(mergeState);

      MultiDocsEnum docsEnumIn = null;

      while((term = termsEnum.next()) != null) {
        // We can pass null for liveDocs, because the
        // mapping enum will skip the non-live docs:
        docsEnumIn = (MultiDocsEnum) termsEnum.docs(null, docsEnumIn, false);
        if (docsEnumIn != null) {
          docsEnum.reset(docsEnumIn);
          final PostingsConsumer postingsConsumer = startTerm(term);
          final TermStats stats = postingsConsumer.merge(mergeState, docsEnum, visitedDocs);
          if (stats.docFreq > 0) {
            finishTerm(term, stats);
            sumTotalTermFreq += stats.docFreq;
            sumDFsinceLastAbortCheck += stats.docFreq;
            sumDocFreq += stats.docFreq;
            if (sumDFsinceLastAbortCheck > 60000) {
              mergeState.checkAbort.work(sumDFsinceLastAbortCheck/5.0);
              sumDFsinceLastAbortCheck = 0;
            }
          }
        }
      }
    } else if (indexOptions == IndexOptions.DOCS_AND_FREQS) {
      if (docsAndFreqsEnum == null) {
        docsAndFreqsEnum = new MappingMultiDocsEnum();
      }
      docsAndFreqsEnum.setMergeState(mergeState);

      MultiDocsEnum docsAndFreqsEnumIn = null;

      while((term = termsEnum.next()) != null) {
        // We can pass null for liveDocs, because the
        // mapping enum will skip the non-live docs:
        docsAndFreqsEnumIn = (MultiDocsEnum) termsEnum.docs(null, docsAndFreqsEnumIn, true);
        assert docsAndFreqsEnumIn != null;
        docsAndFreqsEnum.reset(docsAndFreqsEnumIn);
        final PostingsConsumer postingsConsumer = startTerm(term);
        final TermStats stats = postingsConsumer.merge(mergeState, docsAndFreqsEnum, visitedDocs);
        if (stats.docFreq > 0) {
          finishTerm(term, stats);
          sumTotalTermFreq += stats.totalTermFreq;
          sumDFsinceLastAbortCheck += stats.docFreq;
          sumDocFreq += stats.docFreq;
          if (sumDFsinceLastAbortCheck > 60000) {
            mergeState.checkAbort.work(sumDFsinceLastAbortCheck/5.0);
            sumDFsinceLastAbortCheck = 0;
          }
        }
      }
    } else if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
      if (postingsEnum == null) {
        postingsEnum = new MappingMultiDocsAndPositionsEnum();
      }
      postingsEnum.setMergeState(mergeState);
      MultiDocsAndPositionsEnum postingsEnumIn = null;
      while((term = termsEnum.next()) != null) {
        // We can pass null for liveDocs, because the
        // mapping enum will skip the non-live docs:
        postingsEnumIn = (MultiDocsAndPositionsEnum) termsEnum.docsAndPositions(null, postingsEnumIn, false);
        assert postingsEnumIn != null;
        postingsEnum.reset(postingsEnumIn);
        // set PayloadProcessor
        if (mergeState.payloadProcessorProvider != null) {
          for (int i = 0; i < mergeState.readers.size(); i++) {
            if (mergeState.readerPayloadProcessor[i] != null) {
              mergeState.currentPayloadProcessor[i] = mergeState.readerPayloadProcessor[i].getProcessor(mergeState.fieldInfo.name, term);
            }
          }
        }
        final PostingsConsumer postingsConsumer = startTerm(term);
        final TermStats stats = postingsConsumer.merge(mergeState, postingsEnum, visitedDocs);
        if (stats.docFreq > 0) {
          finishTerm(term, stats);
          sumTotalTermFreq += stats.totalTermFreq;
          sumDFsinceLastAbortCheck += stats.docFreq;
          sumDocFreq += stats.docFreq;
          if (sumDFsinceLastAbortCheck > 60000) {
            mergeState.checkAbort.work(sumDFsinceLastAbortCheck/5.0);
            sumDFsinceLastAbortCheck = 0;
          }
        }
      }
    } else {
      assert indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
      if (postingsEnum == null) {
        postingsEnum = new MappingMultiDocsAndPositionsEnum();
      }
      postingsEnum.setMergeState(mergeState);
      MultiDocsAndPositionsEnum postingsEnumIn = null;
      while((term = termsEnum.next()) != null) {
        // We can pass null for liveDocs, because the
        // mapping enum will skip the non-live docs:
        postingsEnumIn = (MultiDocsAndPositionsEnum) termsEnum.docsAndPositions(null, postingsEnumIn, true);
        assert postingsEnumIn != null;
        postingsEnum.reset(postingsEnumIn);
        // set PayloadProcessor
        if (mergeState.payloadProcessorProvider != null) {
          for (int i = 0; i < mergeState.readers.size(); i++) {
            if (mergeState.readerPayloadProcessor[i] != null) {
              mergeState.currentPayloadProcessor[i] = mergeState.readerPayloadProcessor[i].getProcessor(mergeState.fieldInfo.name, term);
            }
          }
        }
        final PostingsConsumer postingsConsumer = startTerm(term);
        final TermStats stats = postingsConsumer.merge(mergeState, postingsEnum, visitedDocs);
        if (stats.docFreq > 0) {
          finishTerm(term, stats);
          sumTotalTermFreq += stats.totalTermFreq;
          sumDFsinceLastAbortCheck += stats.docFreq;
          sumDocFreq += stats.docFreq;
          if (sumDFsinceLastAbortCheck > 60000) {
            mergeState.checkAbort.work(sumDFsinceLastAbortCheck/5.0);
            sumDFsinceLastAbortCheck = 0;
          }
        }
      }
    }
    finish(sumTotalTermFreq, sumDocFreq, visitedDocs.cardinality());
  }
}
