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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;

/** Translates the "pull" API from {@link FieldsConsumer}
 *  into a "push" API that pushes fields, terms, postings to
 *  the consumer.
 *
 *  <p>
 *  The lifecycle is:
 *  <ol>
 *    <li>PushFieldsConsumer is created by 
 *        {@link PostingsFormat#fieldsConsumer(SegmentWriteState)}.
 *    <li>For each field, {@link #addField(FieldInfo)} is called,
 *        returning a {@link TermsConsumer} for the field.
 *    <li>After all fields are added, the consumer is {@link #close}d.
 *  </ol>
 *
 * @lucene.experimental
 */
public abstract class PushFieldsConsumer extends FieldsConsumer implements Closeable {

  final SegmentWriteState writeState;

  /** Sole constructor */
  protected PushFieldsConsumer(SegmentWriteState writeState) {
    this.writeState = writeState;
  }

  /** Add a new field */
  public abstract TermsConsumer addField(FieldInfo field) throws IOException;

  /** Called when we are done adding everything. */
  @Override
  public abstract void close() throws IOException;

  @Override
  public final void write(Fields fields) throws IOException {

    boolean success = false;
    try {
      for(String field : fields) { // for all fields
        FieldInfo fieldInfo = writeState.fieldInfos.fieldInfo(field);
        IndexOptions indexOptions = fieldInfo.getIndexOptions();
        TermsConsumer termsConsumer = addField(fieldInfo);

        Terms terms = fields.terms(field);
        if (terms != null) {

          // Holds all docs that have this field:
          FixedBitSet visitedDocs = new FixedBitSet(writeState.segmentInfo.getDocCount());

          boolean hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
          boolean hasPositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
          assert hasPositions == terms.hasPositions();
          boolean hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
          assert hasOffsets == terms.hasOffsets();
          boolean hasPayloads = fieldInfo.hasPayloads();

          long sumTotalTermFreq = 0;
          long sumDocFreq = 0;

          int flags = 0;
          if (hasPositions == false) {
            if (hasFreq) {
              flags = flags | DocsEnum.FLAG_FREQS;
            }
          } else {
            if (hasPayloads) {
              flags = flags | DocsAndPositionsEnum.FLAG_PAYLOADS;
            }
            if (hasOffsets) {
              flags = flags | DocsAndPositionsEnum.FLAG_OFFSETS;
            }
          }

          DocsEnum docsEnum = null;
          DocsAndPositionsEnum docsAndPositionsEnum = null;
          TermsEnum termsEnum = terms.iterator(null);

          while (true) { // for all terms in this field
            BytesRef term = termsEnum.next();
            if (term == null) {
              break;
            }
            if (hasPositions) {
              docsAndPositionsEnum = termsEnum.docsAndPositions(null, docsAndPositionsEnum, flags);
              docsEnum = docsAndPositionsEnum;
            } else {
              docsEnum = termsEnum.docs(null, docsEnum, flags);
              docsAndPositionsEnum = null;
            }
            assert docsEnum != null;

            PostingsConsumer postingsConsumer = termsConsumer.startTerm(term);

            // How many documents have this term:
            int docFreq = 0;

            // How many times this term occurs:
            long totalTermFreq = 0;

            while(true) { // for all docs in this field+term
              int doc = docsEnum.nextDoc();
              if (doc == DocsEnum.NO_MORE_DOCS) {
                break;
              }
              docFreq++;
              visitedDocs.set(doc);
              if (hasFreq) {
                int freq = docsEnum.freq();
                postingsConsumer.startDoc(doc, freq);
                totalTermFreq += freq;

                if (hasPositions) {
                  for(int i=0;i<freq;i++) { // for all positions in this field+term + doc
                    int pos = docsAndPositionsEnum.nextPosition();
                    BytesRef payload = docsAndPositionsEnum.getPayload();
                    if (hasOffsets) {
                      postingsConsumer.addPosition(pos, payload, docsAndPositionsEnum.startOffset(), docsAndPositionsEnum.endOffset());
                    } else {
                      postingsConsumer.addPosition(pos, payload, -1, -1);
                    }
                  }
                }
              } else {
                postingsConsumer.startDoc(doc, -1);
              }
              postingsConsumer.finishDoc();
            }

            if (docFreq > 0) {
              termsConsumer.finishTerm(term, new TermStats(docFreq, hasFreq ? totalTermFreq : -1));
              sumTotalTermFreq += totalTermFreq;
              sumDocFreq += docFreq;
            }
          }

          termsConsumer.finish(hasFreq ? sumTotalTermFreq : -1, sumDocFreq, visitedDocs.cardinality());
        }
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(this);
      } else {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }
}
