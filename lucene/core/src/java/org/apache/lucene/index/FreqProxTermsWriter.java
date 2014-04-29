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
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;

final class FreqProxTermsWriter extends TermsHash {

  public FreqProxTermsWriter(DocumentsWriterPerThread docWriter, TermsHash termVectors) {
    super(docWriter, true, termVectors);
  }

  @Override
  public void flush(Map<String,TermsHashPerField> fieldsToFlush, final SegmentWriteState state) throws IOException {
    super.flush(fieldsToFlush, state);

    // Gather all fields that saw any postings:
    List<FreqProxTermsWriterPerField> allFields = new ArrayList<>();

    for (TermsHashPerField f : fieldsToFlush.values()) {
      final FreqProxTermsWriterPerField perField = (FreqProxTermsWriterPerField) f;
      if (perField.bytesHash.size() > 0) {
        allFields.add(perField);
      }
    }

    final int numAllFields = allFields.size();

    // Sort by field name
    CollectionUtil.introSort(allFields);

    final FieldsConsumer consumer = state.segmentInfo.getCodec().postingsFormat().fieldsConsumer(state);

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

        // If this field has postings then add them to the
        // segment
        fieldWriter.flush(fieldInfo.name, consumer, state);
        
        TermsHashPerField perField = fieldWriter;
        assert termsHash == null || termsHash == perField.termsHash;
        termsHash = perField.termsHash;
        int numPostings = perField.bytesHash.size();
        perField.reset();
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

  @Override
  public TermsHashPerField addField(FieldInvertState invertState, FieldInfo fieldInfo) {
    return new FreqProxTermsWriterPerField(invertState, this, fieldInfo, nextTermsHash.addField(invertState, fieldInfo));
  }
}
