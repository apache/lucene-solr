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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Comparator;

import org.apache.lucene.index.codecs.PostingsConsumer;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.TermsConsumer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;

final class FreqProxTermsWriter extends TermsHashConsumer {

  @Override
  public TermsHashConsumerPerThread addThread(TermsHashPerThread perThread) {
    return new FreqProxTermsWriterPerThread(perThread);
  }

  @Override
  void closeDocStore(SegmentWriteState state) {}

  @Override
  void abort() {}

  private int flushedDocCount;

  // TODO: would be nice to factor out more of this, eg the
  // FreqProxFieldMergeState, and code to visit all Fields
  // under the same FieldInfo together, up into TermsHash*.
  // Other writers would presumably share alot of this...

  @Override
  public void flush(Map<TermsHashConsumerPerThread,Collection<TermsHashConsumerPerField>> threadsAndFields, final SegmentWriteState state) throws IOException {

    // Gather all FieldData's that have postings, across all
    // ThreadStates
    List<FreqProxTermsWriterPerField> allFields = new ArrayList<FreqProxTermsWriterPerField>();
    
    flushedDocCount = state.numDocs;

    for (Map.Entry<TermsHashConsumerPerThread,Collection<TermsHashConsumerPerField>> entry : threadsAndFields.entrySet()) {

      Collection<TermsHashConsumerPerField> fields = entry.getValue();


      for (final TermsHashConsumerPerField i : fields) {
        final FreqProxTermsWriterPerField perField = (FreqProxTermsWriterPerField) i;
        if (perField.termsHashPerField.bytesHash.size() > 0)
          allFields.add(perField);
      }
    }

    final int numAllFields = allFields.size();

    // Sort by field name
    CollectionUtil.quickSort(allFields);

    // TODO: allow Lucene user to customize this codec:
    final FieldsConsumer consumer = state.codec.fieldsConsumer(state);

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
        fieldInfo.storePayloads |= fields[i-start].hasPayloads;
      }

      // If this field has postings then add them to the
      // segment
      appendPostings(fields, consumer);

      for(int i=0;i<fields.length;i++) {
        TermsHashPerField perField = fields[i].termsHashPerField;
        int numPostings = perField.bytesHash.size();
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
    consumer.close();
  }

  BytesRef payload;

  /* Walk through all unique text tokens (Posting
   * instances) found in this field and serialize them
   * into a single RAM segment. */
  void appendPostings(FreqProxTermsWriterPerField[] fields,
                      FieldsConsumer consumer)
    throws CorruptIndexException, IOException {

    int numFields = fields.length;

    final BytesRef text = new BytesRef();

    final FreqProxFieldMergeState[] mergeStates = new FreqProxFieldMergeState[numFields];

    final TermsConsumer termsConsumer = consumer.addField(fields[0].fieldInfo);
    final Comparator<BytesRef> termComp = termsConsumer.getComparator();

    for(int i=0;i<numFields;i++) {
      FreqProxFieldMergeState fms = mergeStates[i] = new FreqProxFieldMergeState(fields[i], termComp);

      assert fms.field.fieldInfo == fields[0].fieldInfo;

      // Should always be true
      boolean result = fms.nextTerm();
      assert result;
    }

    FreqProxFieldMergeState[] termStates = new FreqProxFieldMergeState[numFields];

    final boolean currentFieldOmitTermFreqAndPositions = fields[0].fieldInfo.omitTermFreqAndPositions;
    //System.out.println("flush terms field=" + fields[0].fieldInfo.name);

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
        final int cmp = termComp.compare(mergeStates[i].text, termStates[0].text);
        if (cmp < 0) {
          termStates[0] = mergeStates[i];
          numToMerge = 1;
        } else if (cmp == 0) {
          termStates[numToMerge++] = mergeStates[i];
        }
      }

      // Need shallow copy here because termStates[0].text
      // changes by the time we call finishTerm
      text.bytes = termStates[0].text.bytes;
      text.offset = termStates[0].text.offset;
      text.length = termStates[0].text.length;  

      //System.out.println("  term=" + text.toUnicodeString());
      //System.out.println("  term=" + text.toString());

      final PostingsConsumer postingsConsumer = termsConsumer.startTerm(text);

      // Now termStates has numToMerge FieldMergeStates
      // which all share the same term.  Now we must
      // interleave the docID streams.
      int numDocs = 0;
      while(numToMerge > 0) {
        
        FreqProxFieldMergeState minState = termStates[0];
        for(int i=1;i<numToMerge;i++) {
          if (termStates[i].docID < minState.docID) {
            minState = termStates[i];
          }
        }

        final int termDocFreq = minState.termFreq;
        numDocs++;

        assert minState.docID < flushedDocCount: "doc=" + minState.docID + " maxDoc=" + flushedDocCount;

        postingsConsumer.startDoc(minState.docID, termDocFreq);

        final ByteSliceReader prox = minState.prox;

        // Carefully copy over the prox + payload info,
        // changing the format to match Lucene's segment
        // format.
        if (!currentFieldOmitTermFreqAndPositions) {
          // omitTermFreqAndPositions == false so we do write positions &
          // payload          
          int position = 0;
          for(int j=0;j<termDocFreq;j++) {
            final int code = prox.readVInt();
            position += code >> 1;
            //System.out.println("    pos=" + position);

            final int payloadLength;
            final BytesRef thisPayload;

            if ((code & 1) != 0) {
              // This position has a payload
              payloadLength = prox.readVInt();  
              
              if (payload == null) {
                payload = new BytesRef();
                payload.bytes = new byte[payloadLength];
              } else if (payload.bytes.length < payloadLength) {
                payload.grow(payloadLength);
              }

              prox.readBytes(payload.bytes, 0, payloadLength);
              payload.length = payloadLength;
              thisPayload = payload;

            } else {
              payloadLength = 0;
              thisPayload = null;
            }

            postingsConsumer.addPosition(position, thisPayload);
          } //End for

          postingsConsumer.finishDoc();
        }

        if (!minState.nextDoc()) {

          // Remove from termStates
          int upto = 0;
          // TODO: inefficient O(N) where N = number of
          // threads that had seen this term:
          for(int i=0;i<numToMerge;i++) {
            if (termStates[i] != minState) {
              termStates[upto++] = termStates[i];
            }
          }
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

      assert numDocs > 0;
      termsConsumer.finishTerm(text, numDocs);
    }

    termsConsumer.finish();
  }
}
