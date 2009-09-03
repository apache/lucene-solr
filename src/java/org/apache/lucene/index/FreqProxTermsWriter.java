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

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.UnicodeUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

final class FreqProxTermsWriter extends TermsHashConsumer {

  public TermsHashConsumerPerThread addThread(TermsHashPerThread perThread) {
    return new FreqProxTermsWriterPerThread(perThread);
  }

  void createPostings(RawPostingList[] postings, int start, int count) {
    final int end = start + count;
    for(int i=start;i<end;i++)
      postings[i] = new PostingList();
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

  void closeDocStore(SegmentWriteState state) {}
  void abort() {}


  // TODO: would be nice to factor out more of this, eg the
  // FreqProxFieldMergeState, and code to visit all Fields
  // under the same FieldInfo together, up into TermsHash*.
  // Other writers would presumably share alot of this...

  public void flush(Map threadsAndFields, final SegmentWriteState state) throws IOException {

    // Gather all FieldData's that have postings, across all
    // ThreadStates
    List allFields = new ArrayList();

    Iterator it = threadsAndFields.entrySet().iterator();
    while(it.hasNext()) {

      Map.Entry entry = (Map.Entry) it.next();

      Collection fields = (Collection) entry.getValue();

      Iterator fieldsIt = fields.iterator();

      while(fieldsIt.hasNext()) {
        FreqProxTermsWriterPerField perField = (FreqProxTermsWriterPerField) fieldsIt.next();
        if (perField.termsHashPerField.numPostings > 0)
          allFields.add(perField);
      }
    }

    // Sort by field name
    Collections.sort(allFields);
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

    int start = 0;
    while(start < numAllFields) {
      final FieldInfo fieldInfo = ((FreqProxTermsWriterPerField) allFields.get(start)).fieldInfo;
      final String fieldName = fieldInfo.name;

      int end = start+1;
      while(end < numAllFields && ((FreqProxTermsWriterPerField) allFields.get(end)).fieldInfo.name.equals(fieldName))
        end++;
      
      FreqProxTermsWriterPerField[] fields = new FreqProxTermsWriterPerField[end-start];
      for(int i=start;i<end;i++) {
        fields[i-start] = (FreqProxTermsWriterPerField) allFields.get(i);

        // Aggregate the storePayload as seen by the same
        // field across multiple threads
        fieldInfo.storePayloads |= fields[i-start].hasPayloads;
      }

      // If this field has postings then add them to the
      // segment
      appendPostings(fields, consumer);

      for(int i=0;i<fields.length;i++) {
        TermsHashPerField perField = fields[i].termsHashPerField;
        int numPostings = perField.numPostings;
        perField.reset();
        perField.shrinkHash(numPostings);
        fields[i].reset();
      }

      start = end;
    }

    it = threadsAndFields.entrySet().iterator();
    while(it.hasNext()) {
      Map.Entry entry = (Map.Entry) it.next();
      FreqProxTermsWriterPerThread perThread = (FreqProxTermsWriterPerThread) entry.getKey();
      perThread.termsHashPerThread.reset(true);
    }

    consumer.finish();
  }

  private byte[] payloadBuffer;

  /* Walk through all unique text tokens (Posting
   * instances) found in this field and serialize them
   * into a single RAM segment. */
  void appendPostings(FreqProxTermsWriterPerField[] fields,
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

    FreqProxFieldMergeState[] termStates = new FreqProxFieldMergeState[numFields];

    final boolean currentFieldOmitTermFreqAndPositions = fields[0].fieldInfo.omitTermFreqAndPositions;

    while(numFields > 0) {

      // Get the next term to merge
      termStates[0] = mergeStates[0];
      int numToMerge = 1;

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

          posConsumer.finish();
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

      docConsumer.finish();
    }

    termsConsumer.finish();
  }

  private final TermInfo termInfo = new TermInfo(); // minimize consing

  final UnicodeUtil.UTF8Result termsUTF8 = new UnicodeUtil.UTF8Result();

  void files(Collection files) {}

  static final class PostingList extends RawPostingList {
    int docFreq;                                    // # times this term occurs in the current doc
    int lastDocID;                                  // Last docID where this term occurred
    int lastDocCode;                                // Code for prior doc
    int lastPosition;                               // Last position where this term occurred
  }

  int bytesPerPosting() {
    return RawPostingList.BYTES_SIZE + 4 * DocumentsWriter.INT_NUM_BYTE;
  }
}
