package org.apache.lucene.index;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;

final class FreqProxTermsWriter extends TermsHashConsumer {

  @Override
  void abort() {}

  // TODO: would be nice to factor out more of this, eg the
  // FreqProxFieldMergeState, and code to visit all Fields
  // under the same FieldInfo together, up into TermsHash*.
  // Other writers would presumably share alot of this...

  @Override
  public void flush(Map<FieldInfo, TermsHashConsumerPerField> fieldsToFlush, final SegmentWriteState state) throws IOException {

    // Gather all FieldData's that have postings, across all
    // ThreadStates
    List<FreqProxTermsWriterPerField> allFields = new ArrayList<FreqProxTermsWriterPerField>();

    for (TermsHashConsumerPerField f : fieldsToFlush.values()) {
        final FreqProxTermsWriterPerField perField = (FreqProxTermsWriterPerField) f;
        if (perField.termsHashPerField.bytesHash.size() > 0) {
          allFields.add(perField);
        }
    }

    final int numAllFields = allFields.size();

    // Sort by field name
    CollectionUtil.quickSort(allFields);

    final FieldsConsumer consumer = state.codec.postingsFormat().fieldsConsumer(state);

    boolean success = false;

    try {
      TermsHash termsHash = null;
      
      /*
    Current writer chain:
      FieldsConsumer
        -> IMPL: FormatPostingsTermsDictWriter
          -> TermsConsumer
            -> IMPL: FormatPostingsTermsDictWriter.TermsWriter
              -> DocsConsumer
                -> IMPL: FormatPostingsDocsWriter
                  -> PositionsConsumer
                    -> IMPL: FormatPostingsPositionsWriter
       */
      
      for (int fieldNumber = 0; fieldNumber < numAllFields; fieldNumber++) {
        final FieldInfo fieldInfo = allFields.get(fieldNumber).fieldInfo;
        
        final FreqProxTermsWriterPerField fieldWriter = allFields.get(fieldNumber);
        
        // Aggregate the storePayload as seen by the same
        // field across multiple threads
        if (fieldInfo.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
          fieldInfo.storePayloads |= fieldWriter.hasPayloads;
        }
        
        // If this field has postings then add them to the
        // segment
        fieldWriter.flush(fieldInfo.name, consumer, state);
        
        TermsHashPerField perField = fieldWriter.termsHashPerField;
        assert termsHash == null || termsHash == perField.termsHash;
        termsHash = perField.termsHash;
        int numPostings = perField.bytesHash.size();
        perField.reset();
        perField.shrinkHash(numPostings);
        fieldWriter.reset();
      }
      
      if (termsHash != null) {
        termsHash.reset();
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(consumer);
      } else {
        IOUtils.closeWhileHandlingException(consumer);
      }
    }
  }

  BytesRef payload;

  @Override
  public TermsHashConsumerPerField addField(TermsHashPerField termsHashPerField, FieldInfo fieldInfo) {
    return new FreqProxTermsWriterPerField(termsHashPerField, this, fieldInfo);
  }

  @Override
  void finishDocument(TermsHash termsHash) throws IOException {
  }

  @Override
  void startDocument() throws IOException {
  }
}
