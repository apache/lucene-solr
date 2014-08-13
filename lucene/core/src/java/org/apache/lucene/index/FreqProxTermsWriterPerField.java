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
import java.util.Comparator;
import java.util.Map;

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

// TODO: break into separate freq and prox writers as
// codecs; make separate container (tii/tis/skip/*) that can
// be configured as any number of files 1..N
final class FreqProxTermsWriterPerField extends TermsHashPerField {

  private FreqProxPostingsArray freqProxPostingsArray;

  final boolean hasFreq;
  final boolean hasProx;
  final boolean hasOffsets;
  PayloadAttribute payloadAttribute;
  OffsetAttribute offsetAttribute;

  /** Set to true if any token had a payload in the current
   *  segment. */
  boolean sawPayloads;

  public FreqProxTermsWriterPerField(FieldInvertState invertState, TermsHash termsHash, FieldInfo fieldInfo, TermsHashPerField nextPerField) {
    super(fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 ? 2 : 1, invertState, termsHash, nextPerField, fieldInfo);
    IndexOptions indexOptions = fieldInfo.getIndexOptions();
    assert indexOptions != null;
    hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
  }

  @Override
  void finish() throws IOException {
    super.finish();
    if (sawPayloads) {
      fieldInfo.setStorePayloads();
    }
  }

  @Override
  boolean start(IndexableField f, boolean first) {
    super.start(f, first);
    payloadAttribute = fieldState.payloadAttribute;
    offsetAttribute = fieldState.offsetAttribute;
    return true;
  }

  void writeProx(int termID, int proxCode) {
    if (payloadAttribute == null) {
      writeVInt(1, proxCode<<1);
    } else {
      BytesRef payload = payloadAttribute.getPayload();
      if (payload != null && payload.length > 0) {
        writeVInt(1, (proxCode<<1)|1);
        writeVInt(1, payload.length);
        writeBytes(1, payload.bytes, payload.offset, payload.length);
        sawPayloads = true;
      } else {
        writeVInt(1, proxCode<<1);
      }
    }

    assert postingsArray == freqProxPostingsArray;
    freqProxPostingsArray.lastPositions[termID] = fieldState.position;
  }

  void writeOffsets(int termID, int offsetAccum) {
    final int startOffset = offsetAccum + offsetAttribute.startOffset();
    final int endOffset = offsetAccum + offsetAttribute.endOffset();
    assert startOffset - freqProxPostingsArray.lastOffsets[termID] >= 0;
    writeVInt(1, startOffset - freqProxPostingsArray.lastOffsets[termID]);
    writeVInt(1, endOffset - startOffset);
    freqProxPostingsArray.lastOffsets[termID] = startOffset;
  }

  @Override
  void newTerm(final int termID) {
    // First time we're seeing this term since the last
    // flush
    assert docState.testPoint("FreqProxTermsWriterPerField.newTerm start");

    final FreqProxPostingsArray postings = freqProxPostingsArray;

    postings.lastDocIDs[termID] = docState.docID;
    if (!hasFreq) {
      assert postings.termFreqs == null;
      postings.lastDocCodes[termID] = docState.docID;
    } else {
      postings.lastDocCodes[termID] = docState.docID << 1;
      postings.termFreqs[termID] = 1;
      if (hasProx) {
        writeProx(termID, fieldState.position);
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset);
        }
      } else {
        assert !hasOffsets;
      }
    }
    fieldState.maxTermFrequency = Math.max(1, fieldState.maxTermFrequency);
    fieldState.uniqueTermCount++;
  }

  @Override
  void addTerm(final int termID) {

    assert docState.testPoint("FreqProxTermsWriterPerField.addTerm start");

    final FreqProxPostingsArray postings = freqProxPostingsArray;

    assert !hasFreq || postings.termFreqs[termID] > 0;

    if (!hasFreq) {
      assert postings.termFreqs == null;
      if (docState.docID != postings.lastDocIDs[termID]) {
        // New document; now encode docCode for previous doc:
        assert docState.docID > postings.lastDocIDs[termID];
        writeVInt(0, postings.lastDocCodes[termID]);
        postings.lastDocCodes[termID] = docState.docID - postings.lastDocIDs[termID];
        postings.lastDocIDs[termID] = docState.docID;
        fieldState.uniqueTermCount++;
      }
    } else if (docState.docID != postings.lastDocIDs[termID]) {
      assert docState.docID > postings.lastDocIDs[termID]:"id: "+docState.docID + " postings ID: "+ postings.lastDocIDs[termID] + " termID: "+termID;
      // Term not yet seen in the current doc but previously
      // seen in other doc(s) since the last flush

      // Now that we know doc freq for previous doc,
      // write it & lastDocCode
      if (1 == postings.termFreqs[termID]) {
        writeVInt(0, postings.lastDocCodes[termID]|1);
      } else {
        writeVInt(0, postings.lastDocCodes[termID]);
        writeVInt(0, postings.termFreqs[termID]);
      }

      // Init freq for the current document
      postings.termFreqs[termID] = 1;
      fieldState.maxTermFrequency = Math.max(1, fieldState.maxTermFrequency);
      postings.lastDocCodes[termID] = (docState.docID - postings.lastDocIDs[termID]) << 1;
      postings.lastDocIDs[termID] = docState.docID;
      if (hasProx) {
        writeProx(termID, fieldState.position);
        if (hasOffsets) {
          postings.lastOffsets[termID] = 0;
          writeOffsets(termID, fieldState.offset);
        }
      } else {
        assert !hasOffsets;
      }
      fieldState.uniqueTermCount++;
    } else {
      fieldState.maxTermFrequency = Math.max(fieldState.maxTermFrequency, ++postings.termFreqs[termID]);
      if (hasProx) {
        writeProx(termID, fieldState.position-postings.lastPositions[termID]);
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset);
        }
      }
    }
  }

  @Override
  public void newPostingsArray() {
    freqProxPostingsArray = (FreqProxPostingsArray) postingsArray;
  }

  @Override
  ParallelPostingsArray createPostingsArray(int size) {
    IndexOptions indexOptions = fieldInfo.getIndexOptions();
    assert indexOptions != null;
    boolean hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    boolean hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    boolean hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    return new FreqProxPostingsArray(size, hasFreq, hasProx, hasOffsets);
  }

  static final class FreqProxPostingsArray extends ParallelPostingsArray {
    public FreqProxPostingsArray(int size, boolean writeFreqs, boolean writeProx, boolean writeOffsets) {
      super(size);
      if (writeFreqs) {
        termFreqs = new int[size];
      }
      lastDocIDs = new int[size];
      lastDocCodes = new int[size];
      if (writeProx) {
        lastPositions = new int[size];
        if (writeOffsets) {
          lastOffsets = new int[size];
        }
      } else {
        assert !writeOffsets;
      }
      //System.out.println("PA init freqs=" + writeFreqs + " pos=" + writeProx + " offs=" + writeOffsets);
    }

    int termFreqs[];                                   // # times this term occurs in the current doc
    int lastDocIDs[];                                  // Last docID where this term occurred
    int lastDocCodes[];                                // Code for prior doc
    int lastPositions[];                               // Last position where this term occurred
    int lastOffsets[];                                 // Last endOffset where this term occurred

    @Override
    ParallelPostingsArray newInstance(int size) {
      return new FreqProxPostingsArray(size, termFreqs != null, lastPositions != null, lastOffsets != null);
    }

    @Override
    void copyTo(ParallelPostingsArray toArray, int numToCopy) {
      assert toArray instanceof FreqProxPostingsArray;
      FreqProxPostingsArray to = (FreqProxPostingsArray) toArray;

      super.copyTo(toArray, numToCopy);

      System.arraycopy(lastDocIDs, 0, to.lastDocIDs, 0, numToCopy);
      System.arraycopy(lastDocCodes, 0, to.lastDocCodes, 0, numToCopy);
      if (lastPositions != null) {
        assert to.lastPositions != null;
        System.arraycopy(lastPositions, 0, to.lastPositions, 0, numToCopy);
      }
      if (lastOffsets != null) {
        assert to.lastOffsets != null;
        System.arraycopy(lastOffsets, 0, to.lastOffsets, 0, numToCopy);
      }
      if (termFreqs != null) {
        assert to.termFreqs != null;
        System.arraycopy(termFreqs, 0, to.termFreqs, 0, numToCopy);
      }
    }

    @Override
    int bytesPerPosting() {
      int bytes = ParallelPostingsArray.BYTES_PER_POSTING + 2 * RamUsageEstimator.NUM_BYTES_INT;
      if (lastPositions != null) {
        bytes += RamUsageEstimator.NUM_BYTES_INT;
      }
      if (lastOffsets != null) {
        bytes += RamUsageEstimator.NUM_BYTES_INT;
      }
      if (termFreqs != null) {
        bytes += RamUsageEstimator.NUM_BYTES_INT;
      }

      return bytes;
    }
  }

  /* Walk through all unique text tokens (Posting
   * instances) found in this field and serialize them
   * into a single RAM segment. */
  void flush(String fieldName, FieldsConsumer consumer,  final SegmentWriteState state)
    throws IOException {

    BytesRefBuilder payload = null;

    if (!fieldInfo.isIndexed()) {
      return; // nothing to flush, don't bother the codec with the unindexed field
    }
    
    final TermsConsumer termsConsumer = consumer.addField(fieldInfo);
    final Comparator<BytesRef> termComp = termsConsumer.getComparator();

    // CONFUSING: this.indexOptions holds the index options
    // that were current when we first saw this field.  But
    // it's possible this has changed, eg when other
    // documents are indexed that cause a "downgrade" of the
    // IndexOptions.  So we must decode the in-RAM buffer
    // according to this.indexOptions, but then write the
    // new segment to the directory according to
    // currentFieldIndexOptions:
    final IndexOptions currentFieldIndexOptions = fieldInfo.getIndexOptions();
    assert currentFieldIndexOptions != null;

    final boolean writeTermFreq = currentFieldIndexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    final boolean writePositions = currentFieldIndexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    final boolean writeOffsets = currentFieldIndexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;

    final boolean readTermFreq = this.hasFreq;
    final boolean readPositions = this.hasProx;
    final boolean readOffsets = this.hasOffsets;

    //System.out.println("flush readTF=" + readTermFreq + " readPos=" + readPositions + " readOffs=" + readOffsets);

    // Make sure FieldInfo.update is working correctly!:
    assert !writeTermFreq || readTermFreq;
    assert !writePositions || readPositions;
    assert !writeOffsets || readOffsets;

    assert !writeOffsets || writePositions;

    final Map<Term,Integer> segDeletes;
    if (state.segUpdates != null && state.segUpdates.terms.size() > 0) {
      segDeletes = state.segUpdates.terms;
    } else {
      segDeletes = null;
    }

    final int[] termIDs = sortPostings(termComp);
    final int numTerms = bytesHash.size();
    final BytesRef text = new BytesRef();
    final FreqProxPostingsArray postings = freqProxPostingsArray;
    final ByteSliceReader freq = new ByteSliceReader();
    final ByteSliceReader prox = new ByteSliceReader();

    FixedBitSet visitedDocs = new FixedBitSet(state.segmentInfo.getDocCount());
    long sumTotalTermFreq = 0;
    long sumDocFreq = 0;

    Term protoTerm = new Term(fieldName);
    for (int i = 0; i < numTerms; i++) {
      final int termID = termIDs[i];
      //System.out.println("term=" + termID);
      // Get BytesRef
      final int textStart = postings.textStarts[termID];
      bytePool.setBytesRef(text, textStart);

      initReader(freq, termID, 0);
      if (readPositions || readOffsets) {
        initReader(prox, termID, 1);
      }

      // TODO: really TermsHashPerField should take over most
      // of this loop, including merge sort of terms from
      // multiple threads and interacting with the
      // TermsConsumer, only calling out to us (passing us the
      // DocsConsumer) to handle delivery of docs/positions

      final PostingsConsumer postingsConsumer = termsConsumer.startTerm(text);

      final int delDocLimit;
      if (segDeletes != null) {
        protoTerm.bytes = text;
        final Integer docIDUpto = segDeletes.get(protoTerm);
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
      int docFreq = 0;
      long totalTermFreq = 0;
      int docID = 0;

      while(true) {
        //System.out.println("  cycle");
        final int termFreq;
        if (freq.eof()) {
          if (postings.lastDocCodes[termID] != -1) {
            // Return last doc
            docID = postings.lastDocIDs[termID];
            if (readTermFreq) {
              termFreq = postings.termFreqs[termID];
            } else {
              termFreq = -1;
            }
            postings.lastDocCodes[termID] = -1;
          } else {
            // EOF
            break;
          }
        } else {
          final int code = freq.readVInt();
          if (!readTermFreq) {
            docID += code;
            termFreq = -1;
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

        docFreq++;
        assert docID < state.segmentInfo.getDocCount(): "doc=" + docID + " maxDoc=" + state.segmentInfo.getDocCount();

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
        visitedDocs.set(docID);
        postingsConsumer.startDoc(docID, writeTermFreq ? termFreq : -1);
        if (docID < delDocLimit) {
          // Mark it deleted.  TODO: we could also skip
          // writing its postings; this would be
          // deterministic (just for this Term's docs).
          
          // TODO: can we do this reach-around in a cleaner way????
          if (state.liveDocs == null) {
            state.liveDocs = docState.docWriter.codec.liveDocsFormat().newLiveDocs(state.segmentInfo.getDocCount());
          }
          if (state.liveDocs.get(docID)) {
            state.delCountOnFlush++;
            state.liveDocs.clear(docID);
          }
        }

        totalTermFreq += termFreq;
        
        // Carefully copy over the prox + payload info,
        // changing the format to match Lucene's segment
        // format.

        if (readPositions || readOffsets) {
          // we did record positions (& maybe payload) and/or offsets
          int position = 0;
          int offset = 0;
          for(int j=0;j<termFreq;j++) {
            final BytesRef thisPayload;

            if (readPositions) {
              final int code = prox.readVInt();
              position += code >>> 1;

              if ((code & 1) != 0) {

                // This position has a payload
                final int payloadLength = prox.readVInt();

                if (payload == null) {
                  payload = new BytesRefBuilder();
                }
                payload.grow(payloadLength);

                prox.readBytes(payload.bytes(), 0, payloadLength);
                payload.setLength(payloadLength);
                thisPayload = payload.get();

              } else {
                thisPayload = null;
              }

              if (readOffsets) {
                final int startOffset = offset + prox.readVInt();
                final int endOffset = startOffset + prox.readVInt();
                if (writePositions) {
                  if (writeOffsets) {
                    assert startOffset >=0 && endOffset >= startOffset : "startOffset=" + startOffset + ",endOffset=" + endOffset + ",offset=" + offset;
                    postingsConsumer.addPosition(position, thisPayload, startOffset, endOffset);
                  } else {
                    postingsConsumer.addPosition(position, thisPayload, -1, -1);
                  }
                }
                offset = startOffset;
              } else if (writePositions) {
                postingsConsumer.addPosition(position, thisPayload, -1, -1);
              }
            }
          }
        }
        postingsConsumer.finishDoc();
      }
      termsConsumer.finishTerm(text, new TermStats(docFreq, writeTermFreq ? totalTermFreq : -1));
      sumTotalTermFreq += totalTermFreq;
      sumDocFreq += docFreq;
    }

    termsConsumer.finish(writeTermFreq ? sumTotalTermFreq : -1, sumDocFreq, visitedDocs.cardinality());
  }
}
