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

import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.BytesRefHash.BytesStartArray;
import org.apache.lucene.util.BytesRefHash.MaxBytesLengthExceededException;

final class TermsHashPerField extends InvertedDocConsumerPerField {
  private static final int HASH_INIT_SIZE = 4;

  final TermsHashConsumerPerField consumer;

  final TermsHash termsHash;

  final TermsHashPerField nextPerField;
  final DocumentsWriterPerThread.DocState docState;
  final FieldInvertState fieldState;
  TermToBytesRefAttribute termAtt;
  BytesRef termBytesRef;

  // Copied from our perThread
  final IntBlockPool intPool;
  final ByteBlockPool bytePool;
  final ByteBlockPool termBytePool;

  final int streamCount;
  final int numPostingInt;

  final FieldInfo fieldInfo;

  final BytesRefHash bytesHash;

  ParallelPostingsArray postingsArray;
  private final Counter bytesUsed;

  public TermsHashPerField(DocInverterPerField docInverterPerField, final TermsHash termsHash, final TermsHash nextTermsHash, final FieldInfo fieldInfo) {
    intPool = termsHash.intPool;
    bytePool = termsHash.bytePool;
    termBytePool = termsHash.termBytePool;
    docState = termsHash.docState;
    this.termsHash = termsHash;
    bytesUsed = termsHash.bytesUsed;
    fieldState = docInverterPerField.fieldState;
    this.consumer = termsHash.consumer.addField(this, fieldInfo);
    PostingsBytesStartArray byteStarts = new PostingsBytesStartArray(this, bytesUsed);
    bytesHash = new BytesRefHash(termBytePool, HASH_INIT_SIZE, byteStarts);
    streamCount = consumer.getStreamCount();
    numPostingInt = 2*streamCount;
    this.fieldInfo = fieldInfo;
    if (nextTermsHash != null)
      nextPerField = (TermsHashPerField) nextTermsHash.addField(docInverterPerField, fieldInfo);
    else
      nextPerField = null;
  }

  void shrinkHash(int targetSize) {
    // Fully free the bytesHash on each flush but keep the pool untouched
    // bytesHash.clear will clear the ByteStartArray and in turn the ParallelPostingsArray too
    bytesHash.clear(false);
  }

  public void reset() {
    bytesHash.clear(false);
    if (nextPerField != null)
      nextPerField.reset();
  }

  @Override
  public void abort() {
    reset();
    if (nextPerField != null)
      nextPerField.abort();
  }

  public void initReader(ByteSliceReader reader, int termID, int stream) {
    assert stream < streamCount;
    int intStart = postingsArray.intStarts[termID];
    final int[] ints = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
    final int upto = intStart & IntBlockPool.INT_BLOCK_MASK;
    reader.init(bytePool,
                postingsArray.byteStarts[termID]+stream*ByteBlockPool.FIRST_LEVEL_SIZE,
                ints[upto+stream]);
  }

  /** Collapse the hash table & sort in-place. */
  public int[] sortPostings(Comparator<BytesRef> termComp) {
   return bytesHash.sort(termComp);
  }

  private boolean doCall;
  private boolean doNextCall;

  @Override
  void start(IndexableField f) {
    termAtt = fieldState.attributeSource.getAttribute(TermToBytesRefAttribute.class);
    termBytesRef = termAtt.getBytesRef();
    consumer.start(f);
    if (nextPerField != null) {
      nextPerField.start(f);
    }
  }

  @Override
  boolean start(IndexableField[] fields, int count) throws IOException {
    doCall = consumer.start(fields, count);
    bytesHash.reinit();
    if (nextPerField != null) {
      doNextCall = nextPerField.start(fields, count);
    }
    return doCall || doNextCall;
  }

  // Secondary entry point (for 2nd & subsequent TermsHash),
  // because token text has already been "interned" into
  // textStart, so we hash by textStart
  public void add(int textStart) throws IOException {
    int termID = bytesHash.addByPoolOffset(textStart);
    if (termID >= 0) {      // New posting
      // First time we are seeing this token since we last
      // flushed the hash.
      // Init stream slices
      if (numPostingInt + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE)
        intPool.nextBuffer();

      if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt*ByteBlockPool.FIRST_LEVEL_SIZE) {
        bytePool.nextBuffer();
      }

      intUptos = intPool.buffer;
      intUptoStart = intPool.intUpto;
      intPool.intUpto += streamCount;

      postingsArray.intStarts[termID] = intUptoStart + intPool.intOffset;

      for(int i=0;i<streamCount;i++) {
        final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
        intUptos[intUptoStart+i] = upto + bytePool.byteOffset;
      }
      postingsArray.byteStarts[termID] = intUptos[intUptoStart];

      consumer.newTerm(termID);

    } else {
      termID = (-termID)-1;
      int intStart = postingsArray.intStarts[termID];
      intUptos = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
      intUptoStart = intStart & IntBlockPool.INT_BLOCK_MASK;
      consumer.addTerm(termID);
    }
  }

  // Primary entry point (for first TermsHash)
  @Override
  void add() throws IOException {

    // We are first in the chain so we must "intern" the
    // term text into textStart address
    // Get the text & hash of this term.
    int termID;
    try {
      termID = bytesHash.add(termBytesRef, termAtt.fillBytesRef());
    } catch (MaxBytesLengthExceededException e) {
      // Not enough room in current block
      // Just skip this term, to remain as robust as
      // possible during indexing.  A TokenFilter
      // can be inserted into the analyzer chain if
      // other behavior is wanted (pruning the term
      // to a prefix, throwing an exception, etc).
      if (docState.maxTermPrefix == null) {
        final int saved = termBytesRef.length;
        try {
          termBytesRef.length = Math.min(30, DocumentsWriterPerThread.MAX_TERM_LENGTH_UTF8);
          docState.maxTermPrefix = termBytesRef.toString();
        } finally {
          termBytesRef.length = saved;
        }
      }
      consumer.skippingLongTerm();
      return;
    }
    if (termID >= 0) {// New posting
      bytesHash.byteStart(termID);
      // Init stream slices
      if (numPostingInt + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE) {
        intPool.nextBuffer();
      }

      if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt*ByteBlockPool.FIRST_LEVEL_SIZE) {
        bytePool.nextBuffer();
      }

      intUptos = intPool.buffer;
      intUptoStart = intPool.intUpto;
      intPool.intUpto += streamCount;

      postingsArray.intStarts[termID] = intUptoStart + intPool.intOffset;

      for(int i=0;i<streamCount;i++) {
        final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
        intUptos[intUptoStart+i] = upto + bytePool.byteOffset;
      }
      postingsArray.byteStarts[termID] = intUptos[intUptoStart];

      consumer.newTerm(termID);

    } else {
      termID = (-termID)-1;
      final int intStart = postingsArray.intStarts[termID];
      intUptos = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
      intUptoStart = intStart & IntBlockPool.INT_BLOCK_MASK;
      consumer.addTerm(termID);
    }

    if (doNextCall)
      nextPerField.add(postingsArray.textStarts[termID]);
  }

  int[] intUptos;
  int intUptoStart;

  void writeByte(int stream, byte b) {
    int upto = intUptos[intUptoStart+stream];
    byte[] bytes = bytePool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
    assert bytes != null;
    int offset = upto & ByteBlockPool.BYTE_BLOCK_MASK;
    if (bytes[offset] != 0) {
      // End of slice; allocate a new one
      offset = bytePool.allocSlice(bytes, offset);
      bytes = bytePool.buffer;
      intUptos[intUptoStart+stream] = offset + bytePool.byteOffset;
    }
    bytes[offset] = b;
    (intUptos[intUptoStart+stream])++;
  }

  public void writeBytes(int stream, byte[] b, int offset, int len) {
    // TODO: optimize
    final int end = offset + len;
    for(int i=offset;i<end;i++)
      writeByte(stream, b[i]);
  }

  void writeVInt(int stream, int i) {
    assert stream < streamCount;
    while ((i & ~0x7F) != 0) {
      writeByte(stream, (byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByte(stream, (byte) i);
  }

  @Override
  void finish() throws IOException {
    consumer.finish();
    if (nextPerField != null)
      nextPerField.finish();
  }

  private static final class PostingsBytesStartArray extends BytesStartArray {

    private final TermsHashPerField perField;
    private final Counter bytesUsed;

    private PostingsBytesStartArray(
        TermsHashPerField perField, Counter bytesUsed) {
      this.perField = perField;
      this.bytesUsed = bytesUsed;
    }

    @Override
    public int[] init() {
      if (perField.postingsArray == null) {
        perField.postingsArray = perField.consumer.createPostingsArray(2);
        bytesUsed.addAndGet(perField.postingsArray.size * perField.postingsArray.bytesPerPosting());
      }
      return perField.postingsArray.textStarts;
    }

    @Override
    public int[] grow() {
      ParallelPostingsArray postingsArray = perField.postingsArray;
      final int oldSize = perField.postingsArray.size;
      postingsArray = perField.postingsArray = postingsArray.grow();
      bytesUsed.addAndGet((postingsArray.bytesPerPosting() * (postingsArray.size - oldSize)));
      return postingsArray.textStarts;
    }

    @Override
    public int[] clear() {
      if(perField.postingsArray != null) {
        bytesUsed.addAndGet(-(perField.postingsArray.size * perField.postingsArray.bytesPerPosting()));
        perField.postingsArray = null;
      }
      return null;
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }

  }

}
