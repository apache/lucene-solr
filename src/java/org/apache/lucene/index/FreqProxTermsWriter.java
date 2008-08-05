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

  void closeDocStore(DocumentsWriter.FlushState state) {}
  void abort() {}


  // TODO: would be nice to factor out morme of this, eg the
  // FreqProxFieldMergeState, and code to visit all Fields
  // under the same FieldInfo together, up into TermsHash*.
  // Other writers would presumably share alot of this...

  public void flush(Map threadsAndFields, final DocumentsWriter.FlushState state) throws IOException {

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

    final TermInfosWriter termsOut = new TermInfosWriter(state.directory,
                                                         state.segmentName,
                                                         fieldInfos,
                                                         state.docWriter.writer.getTermIndexInterval());

    final IndexOutput freqOut = state.directory.createOutput(state.segmentFileName(IndexFileNames.FREQ_EXTENSION));
    final IndexOutput proxOut;

    if (fieldInfos.hasProx())
      proxOut = state.directory.createOutput(state.segmentFileName(IndexFileNames.PROX_EXTENSION));
    else
      proxOut = null;

    final DefaultSkipListWriter skipListWriter = new DefaultSkipListWriter(termsOut.skipInterval,
                                                                           termsOut.maxSkipLevels,
                                                                           state.numDocsInRAM, freqOut, proxOut);

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
      appendPostings(state, fields, termsOut, freqOut, proxOut, skipListWriter);

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

    freqOut.close();
    if (proxOut != null) {
      state.flushedFiles.add(state.segmentFileName(IndexFileNames.PROX_EXTENSION));
      proxOut.close();
    }
    termsOut.close();
    
    // Record all files we have flushed
    state.flushedFiles.add(state.segmentFileName(IndexFileNames.FIELD_INFOS_EXTENSION));
    state.flushedFiles.add(state.segmentFileName(IndexFileNames.FREQ_EXTENSION));
    state.flushedFiles.add(state.segmentFileName(IndexFileNames.TERMS_EXTENSION));
    state.flushedFiles.add(state.segmentFileName(IndexFileNames.TERMS_INDEX_EXTENSION));
  }

  final byte[] copyByteBuffer = new byte[4096];

  /** Copy numBytes from srcIn to destIn */
  void copyBytes(IndexInput srcIn, IndexOutput destIn, long numBytes) throws IOException {
    // TODO: we could do this more efficiently (save a copy)
    // because it's always from a ByteSliceReader ->
    // IndexOutput
    while(numBytes > 0) {
      final int chunk;
      if (numBytes > 4096)
        chunk = 4096;
      else
        chunk = (int) numBytes;
      srcIn.readBytes(copyByteBuffer, 0, chunk);
      destIn.writeBytes(copyByteBuffer, 0, chunk);
      numBytes -= chunk;
    }
  }

  /* Walk through all unique text tokens (Posting
   * instances) found in this field and serialize them
   * into a single RAM segment. */
  void appendPostings(final DocumentsWriter.FlushState flushState,
                      FreqProxTermsWriterPerField[] fields,
                      TermInfosWriter termsOut,
                      IndexOutput freqOut,
                      IndexOutput proxOut,
                      DefaultSkipListWriter skipListWriter)
    throws CorruptIndexException, IOException {

    final int fieldNumber = fields[0].fieldInfo.number;
    int numFields = fields.length;

    final FreqProxFieldMergeState[] mergeStates = new FreqProxFieldMergeState[numFields];

    for(int i=0;i<numFields;i++) {
      FreqProxFieldMergeState fms = mergeStates[i] = new FreqProxFieldMergeState(fields[i]);

      assert fms.field.fieldInfo == fields[0].fieldInfo;

      // Should always be true
      boolean result = fms.nextTerm();
      assert result;
    }

    final int skipInterval = termsOut.skipInterval;
    final boolean currentFieldOmitTf = fields[0].fieldInfo.omitTf;

    // If current field omits tf then it cannot store
    // payloads.  We silently drop the payloads in this case:
    final boolean currentFieldStorePayloads = currentFieldOmitTf ? false : fields[0].fieldInfo.storePayloads;
  
    FreqProxFieldMergeState[] termStates = new FreqProxFieldMergeState[numFields];

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

      int df = 0;
      int lastPayloadLength = -1;

      int lastDoc = 0;

      final char[] text = termStates[0].text;
      final int start = termStates[0].textOffset;

      final long freqPointer = freqOut.getFilePointer();
      final long proxPointer;
      if (proxOut != null)
        proxPointer = proxOut.getFilePointer();
      else
        proxPointer = 0;

      skipListWriter.resetSkip();

      // Now termStates has numToMerge FieldMergeStates
      // which all share the same term.  Now we must
      // interleave the docID streams.
      while(numToMerge > 0) {
        
        if ((++df % skipInterval) == 0) {
          skipListWriter.setSkipData(lastDoc, currentFieldStorePayloads, lastPayloadLength);
          skipListWriter.bufferSkip(df);
        }

        FreqProxFieldMergeState minState = termStates[0];
        for(int i=1;i<numToMerge;i++)
          if (termStates[i].docID < minState.docID)
            minState = termStates[i];

        final int doc = minState.docID;
        final int termDocFreq = minState.termFreq;

        assert doc < flushState.numDocsInRAM;
        assert doc > lastDoc || df == 1;

        final ByteSliceReader prox = minState.prox;

        // Carefully copy over the prox + payload info,
        // changing the format to match Lucene's segment
        // format.
        if (!currentFieldOmitTf) {
          // omitTf == false so we do write positions & payload          
          assert proxOut != null;
          for(int j=0;j<termDocFreq;j++) {
            final int code = prox.readVInt();
            if (currentFieldStorePayloads) {
              final int payloadLength;
              if ((code & 1) != 0) {
                // This position has a payload
                payloadLength = prox.readVInt();
              } else
                payloadLength = 0;
              if (payloadLength != lastPayloadLength) {
                proxOut.writeVInt(code|1);
                proxOut.writeVInt(payloadLength);
                lastPayloadLength = payloadLength;
              } else
                proxOut.writeVInt(code & (~1));
              if (payloadLength > 0)
                copyBytes(prox, proxOut, payloadLength);
            } else {
              assert 0 == (code & 1);
              proxOut.writeVInt(code>>1);
            }
          } //End for
          
          final int newDocCode = (doc-lastDoc)<<1;

          if (1 == termDocFreq) {
            freqOut.writeVInt(newDocCode|1);
           } else {
            freqOut.writeVInt(newDocCode);
            freqOut.writeVInt(termDocFreq);
          }
        } else {
          // omitTf==true: we store only the docs, without
          // term freq, positions, payloads
          freqOut.writeVInt(doc-lastDoc);
        }

        lastDoc = doc;

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

      assert df > 0;

      // Done merging this term

      long skipPointer = skipListWriter.writeSkip(freqOut);

      // Write term
      termInfo.set(df, freqPointer, proxPointer, (int) (skipPointer - freqPointer));

      // TODO: we could do this incrementally
      UnicodeUtil.UTF16toUTF8(text, start, termsUTF8);

      // TODO: we could save O(n) re-scan of the term by
      // computing the shared prefix with the last term
      // while during the UTF8 encoding
      termsOut.add(fieldNumber,
                   termsUTF8.result,
                   termsUTF8.length,
                   termInfo);
    }
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
