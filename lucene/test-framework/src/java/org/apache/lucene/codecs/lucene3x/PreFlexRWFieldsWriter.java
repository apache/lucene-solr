package org.apache.lucene.codecs.lucene3x;

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

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;

class PreFlexRWFieldsWriter extends FieldsConsumer {

  private final TermInfosWriter termsOut;
  private final IndexOutput freqOut;
  private final IndexOutput proxOut;
  private final PreFlexRWSkipListWriter skipListWriter;
  private final int totalNumDocs;
  private final SegmentWriteState writeState;

  public PreFlexRWFieldsWriter(SegmentWriteState state) throws IOException {
    this.writeState = state;
    termsOut = new TermInfosWriter(state.directory,
                                   state.segmentInfo.name,
                                   state.fieldInfos,
                                   state.termIndexInterval);

    boolean success = false;
    try {
      final String freqFile = IndexFileNames.segmentFileName(state.segmentInfo.name, "", Lucene3xPostingsFormat.FREQ_EXTENSION);
      freqOut = state.directory.createOutput(freqFile, state.context);
      totalNumDocs = state.segmentInfo.getDocCount();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(termsOut);
      }
    }

    success = false;
    try {
      if (state.fieldInfos.hasProx()) {
        final String proxFile = IndexFileNames.segmentFileName(state.segmentInfo.name, "", Lucene3xPostingsFormat.PROX_EXTENSION);
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

    skipListWriter = new PreFlexRWSkipListWriter(termsOut.skipInterval,
                                               termsOut.maxSkipLevels,
                                               totalNumDocs,
                                               freqOut,
                                               proxOut);
    //System.out.println("\nw start seg=" + segment);
  }

  public PreFlexTermsWriter addField(FieldInfo field) throws IOException {
    assert field.number != -1;
    if (field.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
      throw new UnsupportedOperationException("this codec cannot index offsets");
    }
    //System.out.println("w field=" + field.name + " storePayload=" + field.storePayloads + " number=" + field.number);
    return new PreFlexTermsWriter(field);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(termsOut, freqOut, proxOut);
  }

  private class PreFlexTermsWriter {
    private final FieldInfo fieldInfo;
    private final boolean omitTF;
    private final boolean storePayloads;
    
    private final TermInfo termInfo = new TermInfo();
    private final PostingsWriter postingsWriter = new PostingsWriter();

    public PreFlexTermsWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      omitTF = fieldInfo.getIndexOptions() == IndexOptions.DOCS_ONLY;
      storePayloads = fieldInfo.hasPayloads();
    }

    private class PostingsWriter {
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

      public void startDoc(int docID, int termDocFreq) throws IOException {
        //System.out.println("    w doc=" + docID);

        final int delta = docID - lastDocID;
        if (docID < 0 || (df > 0 && delta <= 0)) {
          throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )");
        }

        if ((++df % termsOut.skipInterval) == 0) {
          skipListWriter.setSkipData(lastDocID, storePayloads, lastPayloadLength);
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

      public void finishDoc() throws IOException {
      }
    }

    public PostingsWriter startTerm(BytesRef text) throws IOException {
      //System.out.println("  w term=" + text.utf8ToString());
      skipListWriter.resetSkip();
      termInfo.freqPointer = freqOut.getFilePointer();
      if (proxOut != null) {
        termInfo.proxPointer = proxOut.getFilePointer();
      }
      return postingsWriter.reset();
    }

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

    public void finish(long sumTotalTermCount, long sumDocFreq, int docCount) throws IOException {
    }
  }
  
  @Override
  public Comparator<BytesRef> getComparator() {
    return BytesRef.getUTF8SortedAsUTF16Comparator();
  }

  @Override
  public final void write(Fields fields) throws IOException {
    
    boolean success = false;
    try {
      for(String field : fields) { // for all fields
        FieldInfo fieldInfo = writeState.fieldInfos.fieldInfo(field);
        IndexOptions indexOptions = fieldInfo.getIndexOptions();
        PreFlexTermsWriter termsConsumer = addField(fieldInfo);
        
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
            
            PreFlexTermsWriter.PostingsWriter postingsConsumer = termsConsumer.startTerm(term);
            
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