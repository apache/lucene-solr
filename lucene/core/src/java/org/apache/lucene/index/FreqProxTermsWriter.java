package org.apache.lucene.index;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;

final class FreqProxTermsWriter extends TermsHashConsumer {

  @Override
  void abort() {}

  private void applyDeletes(SegmentWriteState state, Fields fields) throws IOException {
    // Process any pending Term deletes for this newly
    // flushed segment:
    if (state.segDeletes != null && state.segDeletes.terms.size() > 0) {
      Map<Term,Integer> segDeletes = state.segDeletes.terms;
      List<Term> deleteTerms = new ArrayList<Term>(segDeletes.keySet());
      Collections.sort(deleteTerms);
      String lastField = null;
      TermsEnum termsEnum = null;
      DocsEnum docsEnum = null;
      for(Term deleteTerm : deleteTerms) {
        if (deleteTerm.field().equals(lastField) == false) {
          lastField = deleteTerm.field();
          Terms terms = fields.terms(lastField);
          if (terms != null) {
            termsEnum = terms.iterator(termsEnum);
          }
        }

        if (termsEnum != null && termsEnum.seekExact(deleteTerm.bytes())) {
          docsEnum = termsEnum.docs(null, docsEnum, 0);
          int delDocLimit = segDeletes.get(deleteTerm);
          while (true) {
            int doc = docsEnum.nextDoc();
            if (doc == DocsEnum.NO_MORE_DOCS) {
              break;
            }
            if (doc < delDocLimit) {
              if (state.liveDocs == null) {
                state.liveDocs = state.segmentInfo.getCodec().liveDocsFormat().newLiveDocs(state.segmentInfo.getDocCount());
              }
              if (state.liveDocs.get(doc)) {
                state.delCountOnFlush++;
                state.liveDocs.clear(doc);
              }
            } else {
              break;
            }
          }
        }
      }
    }
  }

  // TODO: would be nice to factor out more of this, eg the
  // FreqProxFieldMergeState, and code to visit all Fields
  // under the same FieldInfo together, up into TermsHash*.
  // Other writers would presumably share alot of this...

  @Override
  public void flush(Map<String,TermsHashConsumerPerField> fieldsToFlush, final SegmentWriteState state) throws IOException {

    // Gather all FieldData's that have postings, across all
    // ThreadStates
    List<FreqProxTermsWriterPerField> allFields = new ArrayList<FreqProxTermsWriterPerField>();

    for (TermsHashConsumerPerField f : fieldsToFlush.values()) {
      final FreqProxTermsWriterPerField perField = (FreqProxTermsWriterPerField) f;
      if (perField.termsHashPerField.bytesHash.size() > 0) {
        perField.sortPostings();
        assert perField.fieldInfo.isIndexed();
        allFields.add(perField);
      }
    }

    // Sort by field name
    CollectionUtil.introSort(allFields);

    Fields fields = new FreqProxFields(allFields);

    applyDeletes(state, fields);

    state.segmentInfo.getCodec().postingsFormat().fieldsConsumer(state).write(fields);
  }

  BytesRef payload;

  @Override
  public TermsHashConsumerPerField addField(TermsHashPerField termsHashPerField, FieldInfo fieldInfo) {
    return new FreqProxTermsWriterPerField(termsHashPerField, this, fieldInfo);
  }

  @Override
  void finishDocument(TermsHash termsHash) {
  }

  @Override
  void startDocument() {
  }
}
