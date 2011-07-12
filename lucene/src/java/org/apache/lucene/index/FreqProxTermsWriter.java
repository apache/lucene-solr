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

import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.util.UnicodeUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.util.BitVector;
import org.apache.lucene.util.CollectionUtil;

final class FreqProxTermsWriter extends TermsHashConsumer {

  @Override
  public TermsHashConsumerPerThread addThread(TermsHashPerThread perThread) {
    return new FreqProxTermsWriterPerThread(perThread);
  }

  private static int compareText(final char[] text1, int pos1, final char[] text2, int pos2) {
    while(true) {
      final char c1 = text1[pos1++];
      final char c2 = text2[pos2++];
      if (c1 != c2) {
        if (0xffff == c2)
          return 1;
        else if (0xffff == c1)
          return -1;
        else
          return c1-c2;
      } else if (0xffff == c1)
        return 0;
    }
  }

  @Override
  void abort() {}

  // TODO: would be nice to factor out more of this, eg the
  // FreqProxFieldMergeState, and code to visit all Fields
  // under the same FieldInfo together, up into TermsHash*.
  // Other writers would presumably share alot of this...

  @Override
  public void flush(Map<TermsHashConsumerPerThread,Collection<TermsHashConsumerPerField>> threadsAndFields, final SegmentWriteState state) throws IOException {

    // Gather all FieldData's that have postings, across all
    // ThreadStates
    List<FreqProxTermsWriterPerField> allFields = new ArrayList<FreqProxTermsWriterPerField>();

    for (Map.Entry<TermsHashConsumerPerThread,Collection<TermsHashConsumerPerField>> entry : threadsAndFields.entrySet()) {

      Collection<TermsHashConsumerPerField> fields = entry.getValue();

      for (final TermsHashConsumerPerField i : fields) {
        final FreqProxTermsWriterPerField perField = (FreqProxTermsWriterPerField) i;
        if (perField.termsHashPerField.numPostings > 0)
          allFields.add(perField);
      }
    }

    // Sort by field name
    CollectionUtil.quickSort(allFields);
    final int numAllFields = allFields.size();

    // TODO: allow Lucene user to customize this consumer:
    final FormatPostingsFieldsConsumer consumer = new FormatPostingsFieldsWriter(state, fieldInfos);
    /*
    Current writer chain:
      FormatPostingsFieldsConsumer
        -> IMPL: FormatPostingsFieldsWriter
          -> FormatPostingsTermsConsumer
            -> IMPL: FormatPostingsTermsWriter
              -> FormatPostingsDocConsumer
                -> IMPL: FormatPostingsDocWriter
                  -> FormatPostingsPositionsConsumer
                    -> IMPL: FormatPostingsPositionsWriter
    */
    try {
      int start = 0;
      while(start < numAllFields) {
        final FieldInfo fieldInfo = allFields.get(start).fieldInfo;
        final String fieldName = fieldInfo.name;
        
        int end = start+1;
        while(end < numAllFields && allFields.get(end).fieldInfo.name.equals(fieldName))
          end++;
        
        FreqProxTermsWriterPerField[] fields = new FreqProxTermsWriterPerField[end-start];
        for(int i=start;i<end;i++) {
          fields[i-start] = allFields.get(i);
          
          // Aggregate the storePayload as seen by the same
          // field across multiple threads
          if (fieldInfo.indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
            fieldInfo.storePayloads |= fields[i-start].hasPayloads;
          }
        }
        
        // If this field has postings then add them to the
        // segment
        appendPostings(fieldName, state, fields, consumer);
        
        for(int i=0;i<fields.length;i++) {
          TermsHashPerField perField = fields[i].termsHashPerField;
          int numPostings = perField.numPostings;
          perField.reset();
          perField.shrinkHash(numPostings);
          fields[i].reset();
        }
        
        start = end;
      }
      
      for (Map.Entry<TermsHashConsumerPerThread,Collection<TermsHashConsumerPerField>> entry : threadsAndFields.entrySet()) {
        FreqProxTermsWriterPerThread perThread = (FreqProxTermsWriterPerThread) entry.getKey();
        perThread.termsHashPerThread.reset(true);
      }
    } finally {
      consumer.finish();
    }
  }

  private byte[] payloadBuffer;

  /* Walk through all unique text tokens (Posting
   * instances) found in this field and serialize them
   * into a single RAM segment. */
  void appendPostings(String fieldName, SegmentWriteState state,
                      FreqProxTermsWriterPerField[] fields,
                      FormatPostingsFieldsConsumer consumer)
    throws CorruptIndexException, IOException {

    int numFields = fields.length;

    final FreqProxFieldMergeState[] mergeStates = new FreqProxFieldMergeState[numFields];

    for(int i=0;i<numFields;i++) {
      FreqProxFieldMergeState fms = mergeStates[i] = new FreqProxFieldMergeState(fields[i]);

      assert fms.field.fieldInfo == fields[0].fieldInfo;

      // Should always be true
      boolean result = fms.nextTerm();
      assert result;
    }

    final FormatPostingsTermsConsumer termsConsumer = consumer.addField(fields[0].fieldInfo);
    final Term protoTerm = new Term(fieldName);

    FreqProxFieldMergeState[] termStates = new FreqProxFieldMergeState[numFields];

    final IndexOptions currentFieldIndexOptions = fields[0].fieldInfo.indexOptions;

    final Map<Term,Integer> segDeletes;
    if (state.segDeletes != null && state.segDeletes.terms.size() > 0) {
      segDeletes = state.segDeletes.terms;
    } else {
      segDeletes = null;
    }

    try {
      // TODO: really TermsHashPerField should take over most
      // of this loop, including merge sort of terms from
      // multiple threads and interacting with the
      // TermsConsumer, only calling out to us (passing us the
      // DocsConsumer) to handle delivery of docs/positions
      while(numFields > 0) {

        // Get the next term to merge
        termStates[0] = mergeStates[0];
        int numToMerge = 1;

        // TODO: pqueue
        for(int i=1;i<numFields;i++) {
          final char[] text = mergeStates[i].text;
          final int textOffset = mergeStates[i].textOffset;
          final int cmp = compareText(text, textOffset, termStates[0].text, termStates[0].textOffset);

          if (cmp < 0) {
            termStates[0] = mergeStates[i];
            numToMerge = 1;
          } else if (cmp == 0)
            termStates[numToMerge++] = mergeStates[i];
        }

        final FormatPostingsDocsConsumer docConsumer = termsConsumer.addTerm(termStates[0].text, termStates[0].textOffset);

        final int delDocLimit;
        if (segDeletes != null) {
          final Integer docIDUpto = segDeletes.get(protoTerm.createTerm(termStates[0].termText()));
          if (docIDUpto != null) {
            delDocLimit = docIDUpto;
          } else {
            delDocLimit = 0;
          }
        } else {
          delDocLimit = 0;
        }

        try {
          // Now termStates has numToMerge FieldMergeStates
          // which all share the same term.  Now we must
          // interleave the docID streams.
          while(numToMerge > 0) {
            
            FreqProxFieldMergeState minState = termStates[0];
            for(int i=1;i<numToMerge;i++)
              if (termStates[i].docID < minState.docID)
                minState = termStates[i];

            final int termDocFreq = minState.termFreq;

            final FormatPostingsPositionsConsumer posConsumer = docConsumer.addDoc(minState.docID, termDocFreq);

            // NOTE: we could check here if the docID was
            // deleted, and skip it.  However, this is somewhat
            // dangerous because it can yield non-deterministic
            // behavior since we may see the docID before we see
            // the term that caused it to be deleted.  This
            // would mean some (but not all) of its postings may
            // make it into the index, which'd alter the docFreq
            // for those terms.  We could fix this by doing two
            // passes, ie first sweep marks all del docs, and
            // 2nd sweep does the real flush, but I suspect
            // that'd add too much time to flush.

            if (minState.docID < delDocLimit) {
              // Mark it deleted.  TODO: we could also skip
              // writing its postings; this would be
              // deterministic (just for this Term's docs).
              if (state.deletedDocs == null) {
                state.deletedDocs = new BitVector(state.numDocs);
              }
              state.deletedDocs.set(minState.docID);
            }

            final ByteSliceReader prox = minState.prox;

            // Carefully copy over the prox + payload info,
            // changing the format to match Lucene's segment
            // format.
            if (currentFieldIndexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
              // omitTermFreqAndPositions == false so we do write positions &
              // payload  
              try {
                int position = 0;
                for(int j=0;j<termDocFreq;j++) {
                  final int code = prox.readVInt();
                  position += code >> 1;
                
                final int payloadLength;
                if ((code & 1) != 0) {
                  // This position has a payload
                  payloadLength = prox.readVInt();
                  
                  if (payloadBuffer == null || payloadBuffer.length < payloadLength)
                    payloadBuffer = new byte[payloadLength];
                  
                  prox.readBytes(payloadBuffer, 0, payloadLength);
                  
                } else
                  payloadLength = 0;
                
                posConsumer.addPosition(position, payloadBuffer, 0, payloadLength);
                } //End for
              } finally {
                posConsumer.finish();
              }
            }

            if (!minState.nextDoc()) {

              // Remove from termStates
              int upto = 0;
              for(int i=0;i<numToMerge;i++)
                if (termStates[i] != minState)
                  termStates[upto++] = termStates[i];
              numToMerge--;
              assert upto == numToMerge;

              // Advance this state to the next term

              if (!minState.nextTerm()) {
                // OK, no more terms, so remove from mergeStates
                // as well
                upto = 0;
                for(int i=0;i<numFields;i++)
                  if (mergeStates[i] != minState)
                    mergeStates[upto++] = mergeStates[i];
                numFields--;
                assert upto == numFields;
              }
            }
          }
        } finally {
          docConsumer.finish();
        }
      }
    } finally {
      termsConsumer.finish();
    }
  }

  final UnicodeUtil.UTF8Result termsUTF8 = new UnicodeUtil.UTF8Result();
}
