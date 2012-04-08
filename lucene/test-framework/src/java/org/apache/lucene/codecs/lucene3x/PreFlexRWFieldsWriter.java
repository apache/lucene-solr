package org.apache.lucene.codecs.lucene3x;

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

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.codecs.lucene40.Lucene40SkipListWriter;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

class PreFlexRWFieldsWriter extends FieldsConsumer {

  private final TermInfosWriter termsOut;
  private final IndexOutput freqOut;
  private final IndexOutput proxOut;
  private final Lucene40SkipListWriter skipListWriter;
  private final int totalNumDocs;

  public PreFlexRWFieldsWriter(SegmentWriteState state) throws IOException {
    termsOut = new TermInfosWriter(state.directory,
                                   state.segmentName,
                                   state.fieldInfos,
                                   state.termIndexInterval);

    boolean success = false;
    try {
      final String freqFile = IndexFileNames.segmentFileName(state.segmentName, "", Lucene3xPostingsFormat.FREQ_EXTENSION);
      freqOut = state.directory.createOutput(freqFile, state.context);
      totalNumDocs = state.numDocs;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(termsOut);
      }
    }

    success = false;
    try {
      if (state.fieldInfos.hasProx()) {
        final String proxFile = IndexFileNames.segmentFileName(state.segmentName, "", Lucene3xPostingsFormat.PROX_EXTENSION);
        proxOut = state.directory.createOutput(proxFile, state.context);
      } else {
        proxOut = null;
      }
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(termsOut, freqOut);
      }
    }

    skipListWriter = new Lucene40SkipListWriter(termsOut.skipInterval,
                                               termsOut.maxSkipLevels,
                                               totalNumDocs,
                                               freqOut,
                                               proxOut);
    //System.out.println("\nw start seg=" + segment);
  }

  @Override
  public TermsConsumer addField(FieldInfo field) throws IOException {
    assert field.number != -1;
    if (field.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
      throw new UnsupportedOperationException("this codec cannot index offsets");
    }
    //System.out.println("w field=" + field.name + " storePayload=" + field.storePayloads + " number=" + field.number);
    return new PreFlexTermsWriter(field);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(termsOut, freqOut, proxOut);
  }

  private class PreFlexTermsWriter extends TermsConsumer {
    private final FieldInfo fieldInfo;
    private final boolean omitTF;
    private final boolean storePayloads;
    
    private final TermInfo termInfo = new TermInfo();
    private final PostingsWriter postingsWriter = new PostingsWriter();

    public PreFlexTermsWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      omitTF = fieldInfo.indexOptions == IndexOptions.DOCS_ONLY;
      storePayloads = fieldInfo.storePayloads;
    }

    private class PostingsWriter extends PostingsConsumer {
      private int lastDocID;
      private int lastPayloadLength = -1;
      private int lastPosition;
      private int df;

      public PostingsWriter reset() {
        df = 0;
        lastDocID = 0;
        lastPayloadLength = -1;
        return this;
      }

      @Override
      public void startDoc(int docID, int termDocFreq) throws IOException {
        //System.out.println("    w doc=" + docID);

        final int delta = docID - lastDocID;
        if (docID < 0 || (df > 0 && delta <= 0)) {
          throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )");
        }

        if ((++df % termsOut.skipInterval) == 0) {
          skipListWriter.setSkipData(lastDocID, storePayloads, lastPayloadLength, false, 0);
          skipListWriter.bufferSkip(df);
        }

        lastDocID = docID;

        assert docID < totalNumDocs: "docID=" + docID + " totalNumDocs=" + totalNumDocs;

        if (omitTF) {
          freqOut.writeVInt(delta);
        } else {
          final int code = delta << 1;
          if (termDocFreq == 1) {
            freqOut.writeVInt(code|1);
          } else {
            freqOut.writeVInt(code);
            freqOut.writeVInt(termDocFreq);
          }
        }
        lastPosition = 0;
      }

      @Override
      public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
        assert proxOut != null;
        assert startOffset == -1;
        assert endOffset == -1;
        //System.out.println("      w pos=" + position + " payl=" + payload);
        final int delta = position - lastPosition;
        lastPosition = position;

        if (storePayloads) {
          final int payloadLength = payload == null ? 0 : payload.length;
          if (payloadLength != lastPayloadLength) {
            //System.out.println("        write payload len=" + payloadLength);
            lastPayloadLength = payloadLength;
            proxOut.writeVInt((delta<<1)|1);
            proxOut.writeVInt(payloadLength);
          } else {
            proxOut.writeVInt(delta << 1);
          }
          if (payloadLength > 0) {
            proxOut.writeBytes(payload.bytes, payload.offset, payload.length);
          }
        } else {
          proxOut.writeVInt(delta);
        }
      }

      @Override
      public void finishDoc() throws IOException {
      }
    }

    @Override
    public PostingsConsumer startTerm(BytesRef text) throws IOException {
      //System.out.println("  w term=" + text.utf8ToString());
      skipListWriter.resetSkip();
      termInfo.freqPointer = freqOut.getFilePointer();
      if (proxOut != null) {
        termInfo.proxPointer = proxOut.getFilePointer();
      }
      return postingsWriter.reset();
    }

    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {
      if (stats.docFreq > 0) {
        long skipPointer = skipListWriter.writeSkip(freqOut);
        termInfo.docFreq = stats.docFreq;
        termInfo.skipOffset = (int) (skipPointer - termInfo.freqPointer);
        //System.out.println("  w finish term=" + text.utf8ToString() + " fnum=" + fieldInfo.number);
        termsOut.add(fieldInfo.number,
                     text,
                     termInfo);
      }
    }

    @Override
    public void finish(long sumTotalTermCount, long sumDocFreq, int docCount) throws IOException {
    }

    @Override
    public Comparator<BytesRef> getComparator() throws IOException {
      return BytesRef.getUTF8SortedAsUTF16Comparator();
    }
  }
}