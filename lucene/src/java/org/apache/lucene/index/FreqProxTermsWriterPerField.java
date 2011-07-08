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
import java.util.Comparator;
import java.util.Map;

import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.PostingsConsumer;
import org.apache.lucene.index.codecs.TermStats;
import org.apache.lucene.index.codecs.TermsConsumer;
import org.apache.lucene.util.BitVector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

// TODO: break into separate freq and prox writers as
// codecs; make separate container (tii/tis/skip/*) that can
// be configured as any number of files 1..N
final class FreqProxTermsWriterPerField extends TermsHashConsumerPerField implements Comparable<FreqProxTermsWriterPerField> {

  final FreqProxTermsWriter parent;
  final TermsHashPerField termsHashPerField;
  final FieldInfo fieldInfo;
  final DocumentsWriterPerThread.DocState docState;
  final FieldInvertState fieldState;
  boolean omitTermFreqAndPositions;
  PayloadAttribute payloadAttribute;

  public FreqProxTermsWriterPerField(TermsHashPerField termsHashPerField, FreqProxTermsWriter parent, FieldInfo fieldInfo) {
    this.termsHashPerField = termsHashPerField;
    this.parent = parent;
    this.fieldInfo = fieldInfo;
    docState = termsHashPerField.docState;
    fieldState = termsHashPerField.fieldState;
    omitTermFreqAndPositions = fieldInfo.omitTermFreqAndPositions;
  }

  @Override
  int getStreamCount() {
    if (fieldInfo.omitTermFreqAndPositions)
      return 1;
    else
      return 2;
  }

  @Override
  void finish() {}

  boolean hasPayloads;

  @Override
  void skippingLongTerm() throws IOException {}

  public int compareTo(FreqProxTermsWriterPerField other) {
    return fieldInfo.name.compareTo(other.fieldInfo.name);
  }

  void reset() {
    // Record, up front, whether our in-RAM format will be
    // with or without term freqs:
    omitTermFreqAndPositions = fieldInfo.omitTermFreqAndPositions;
    payloadAttribute = null;
  }

  @Override
  boolean start(Fieldable[] fields, int count) {
    for(int i=0;i<count;i++)
      if (fields[i].isIndexed())
        return true;
    return false;
  }

  @Override
  void start(Fieldable f) {
    if (fieldState.attributeSource.hasAttribute(PayloadAttribute.class)) {
      payloadAttribute = fieldState.attributeSource.getAttribute(PayloadAttribute.class);
    } else {
      payloadAttribute = null;
    }
  }

  void writeProx(final int termID, int proxCode) {
    final Payload payload;
    if (payloadAttribute == null) {
      payload = null;
    } else {
      payload = payloadAttribute.getPayload();
    }

    if (payload != null && payload.length > 0) {
      termsHashPerField.writeVInt(1, (proxCode<<1)|1);
      termsHashPerField.writeVInt(1, payload.length);
      termsHashPerField.writeBytes(1, payload.data, payload.offset, payload.length);
      hasPayloads = true;
    } else
      termsHashPerField.writeVInt(1, proxCode<<1);

    FreqProxPostingsArray postings = (FreqProxPostingsArray) termsHashPerField.postingsArray;
    postings.lastPositions[termID] = fieldState.position;

  }

  @Override
  void newTerm(final int termID) {
    // First time we're seeing this term since the last
    // flush
    assert docState.testPoint("FreqProxTermsWriterPerField.newTerm start");

    FreqProxPostingsArray postings = (FreqProxPostingsArray) termsHashPerField.postingsArray;
    postings.lastDocIDs[termID] = docState.docID;
    if (omitTermFreqAndPositions) {
      postings.lastDocCodes[termID] = docState.docID;
    } else {
      postings.lastDocCodes[termID] = docState.docID << 1;
      postings.docFreqs[termID] = 1;
      writeProx(termID, fieldState.position);
    }
    fieldState.maxTermFrequency = Math.max(1, fieldState.maxTermFrequency);
    fieldState.uniqueTermCount++;
  }

  @Override
  void addTerm(final int termID) {

    assert docState.testPoint("FreqProxTermsWriterPerField.addTerm start");

    FreqProxPostingsArray postings = (FreqProxPostingsArray) termsHashPerField.postingsArray;

    assert omitTermFreqAndPositions || postings.docFreqs[termID] > 0;

    if (omitTermFreqAndPositions) {
      if (docState.docID != postings.lastDocIDs[termID]) {
        assert docState.docID > postings.lastDocIDs[termID];
        termsHashPerField.writeVInt(0, postings.lastDocCodes[termID]);
        postings.lastDocCodes[termID] = docState.docID - postings.lastDocIDs[termID];
        postings.lastDocIDs[termID] = docState.docID;
        fieldState.uniqueTermCount++;
      }
    } else {
      if (docState.docID != postings.lastDocIDs[termID]) {
        assert docState.docID > postings.lastDocIDs[termID]:"id: "+docState.docID + " postings ID: "+ postings.lastDocIDs[termID] + " termID: "+termID;
        // Term not yet seen in the current doc but previously
        // seen in other doc(s) since the last flush

        // Now that we know doc freq for previous doc,
        // write it & lastDocCode
        if (1 == postings.docFreqs[termID])
          termsHashPerField.writeVInt(0, postings.lastDocCodes[termID]|1);
        else {
          termsHashPerField.writeVInt(0, postings.lastDocCodes[termID]);
          termsHashPerField.writeVInt(0, postings.docFreqs[termID]);
        }
        postings.docFreqs[termID] = 1;
        fieldState.maxTermFrequency = Math.max(1, fieldState.maxTermFrequency);
        postings.lastDocCodes[termID] = (docState.docID - postings.lastDocIDs[termID]) << 1;
        postings.lastDocIDs[termID] = docState.docID;
        writeProx(termID, fieldState.position);
        fieldState.uniqueTermCount++;
      } else {
        fieldState.maxTermFrequency = Math.max(fieldState.maxTermFrequency, ++postings.docFreqs[termID]);
        writeProx(termID, fieldState.position-postings.lastPositions[termID]);
      }
    }
  }

  @Override
  ParallelPostingsArray createPostingsArray(int size) {
    return new FreqProxPostingsArray(size);
  }

  static final class FreqProxPostingsArray extends ParallelPostingsArray {
    public FreqProxPostingsArray(int size) {
      super(size);
      docFreqs = new int[size];
      lastDocIDs = new int[size];
      lastDocCodes = new int[size];
      lastPositions = new int[size];
    }

    int docFreqs[];                                    // # times this term occurs in the current doc
    int lastDocIDs[];                                  // Last docID where this term occurred
    int lastDocCodes[];                                // Code for prior doc
    int lastPositions[];                               // Last position where this term occurred

    @Override
    ParallelPostingsArray newInstance(int size) {
      return new FreqProxPostingsArray(size);
    }

    @Override
    void copyTo(ParallelPostingsArray toArray, int numToCopy) {
      assert toArray instanceof FreqProxPostingsArray;
      FreqProxPostingsArray to = (FreqProxPostingsArray) toArray;

      super.copyTo(toArray, numToCopy);

      System.arraycopy(docFreqs, 0, to.docFreqs, 0, numToCopy);
      System.arraycopy(lastDocIDs, 0, to.lastDocIDs, 0, numToCopy);
      System.arraycopy(lastDocCodes, 0, to.lastDocCodes, 0, numToCopy);
      System.arraycopy(lastPositions, 0, to.lastPositions, 0, numToCopy);
    }

    @Override
    int bytesPerPosting() {
      return ParallelPostingsArray.BYTES_PER_POSTING + 4 * RamUsageEstimator.NUM_BYTES_INT;
    }
  }

  public void abort() {}

  BytesRef payload;

  /* Walk through all unique text tokens (Posting
   * instances) found in this field and serialize them
   * into a single RAM segment. */
  void flush(String fieldName, FieldsConsumer consumer,  final SegmentWriteState state)
    throws CorruptIndexException, IOException {

    final TermsConsumer termsConsumer = consumer.addField(fieldInfo);
    final Comparator<BytesRef> termComp = termsConsumer.getComparator();

    final boolean currentFieldOmitTermFreqAndPositions = fieldInfo.omitTermFreqAndPositions;

    final Map<Term,Integer> segDeletes;
    if (state.segDeletes != null && state.segDeletes.terms.size() > 0) {
      segDeletes = state.segDeletes.terms;
    } else {
      segDeletes = null;
    }

    final int[] termIDs = termsHashPerField.sortPostings(termComp);
    final int numTerms = termsHashPerField.bytesHash.size();
    final BytesRef text = new BytesRef();
    final FreqProxPostingsArray postings = (FreqProxPostingsArray) termsHashPerField.postingsArray;
    final ByteSliceReader freq = new ByteSliceReader();
    final ByteSliceReader prox = new ByteSliceReader();

    long sumTotalTermFreq = 0;
    long sumDocFreq = 0;

    for (int i = 0; i < numTerms; i++) {
      final int termID = termIDs[i];
      // Get BytesRef
      final int textStart = postings.textStarts[termID];
      termsHashPerField.bytePool.setBytesRef(text, textStart);

      termsHashPerField.initReader(freq, termID, 0);
      if (!fieldInfo.omitTermFreqAndPositions) {
        termsHashPerField.initReader(prox, termID, 1);
      }

      // TODO: really TermsHashPerField should take over most
      // of this loop, including merge sort of terms from
      // multiple threads and interacting with the
      // TermsConsumer, only calling out to us (passing us the
      // DocsConsumer) to handle delivery of docs/positions

      final PostingsConsumer postingsConsumer = termsConsumer.startTerm(text);

      final int delDocLimit;
      if (segDeletes != null) {
        final Integer docIDUpto = segDeletes.get(new Term(fieldName, text));
        if (docIDUpto != null) {
          delDocLimit = docIDUpto;
        } else {
          delDocLimit = 0;
        }
      } else {
        delDocLimit = 0;
      }

      // Now termStates has numToMerge FieldMergeStates
      // which all share the same term.  Now we must
      // interleave the docID streams.
      int numDocs = 0;
      long totTF = 0;
      int docID = 0;
      int termFreq = 0;

      while(true) {
        if (freq.eof()) {
          if (postings.lastDocCodes[termID] != -1) {
            // Return last doc
            docID = postings.lastDocIDs[termID];
            if (!omitTermFreqAndPositions) {
              termFreq = postings.docFreqs[termID];
            }
            postings.lastDocCodes[termID] = -1;
          } else {
            // EOF
            break;
          }
        } else {
          final int code = freq.readVInt();
          if (omitTermFreqAndPositions) {
            docID += code;
          } else {
            docID += code >>> 1;
            if ((code & 1) != 0) {
              termFreq = 1;
            } else {
              termFreq = freq.readVInt();
            }
          }

          assert docID != postings.lastDocIDs[termID];
        }

        numDocs++;
        assert docID < state.numDocs: "doc=" + docID + " maxDoc=" + state.numDocs;
        final int termDocFreq = termFreq;

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
        postingsConsumer.startDoc(docID, termDocFreq);
        if (docID < delDocLimit) {
          // Mark it deleted.  TODO: we could also skip
          // writing its postings; this would be
          // deterministic (just for this Term's docs).
          if (state.liveDocs == null) {
            state.liveDocs = new BitVector(state.numDocs);
            state.liveDocs.invertAll();
          }
          state.liveDocs.clear(docID);
        }

        // Carefully copy over the prox + payload info,
        // changing the format to match Lucene's segment
        // format.
        if (!currentFieldOmitTermFreqAndPositions) {
          // omitTermFreqAndPositions == false so we do write positions &
          // payload
          int position = 0;
          totTF += termDocFreq;
          for(int j=0;j<termDocFreq;j++) {
            final int code = prox.readVInt();
            position += code >> 1;

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
          }

          postingsConsumer.finishDoc();
        }
      }
      termsConsumer.finishTerm(text, new TermStats(numDocs, totTF));
      sumTotalTermFreq += totTF;
      sumDocFreq += numDocs;
    }

    termsConsumer.finish(sumTotalTermFreq, sumDocFreq);
  }

}

