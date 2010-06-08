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
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

final class TermsHashPerField extends InvertedDocConsumerPerField {

  final TermsHashConsumerPerField consumer;

  final TermsHashPerField nextPerField;
  final TermsHashPerThread perThread;
  final DocumentsWriter.DocState docState;
  final FieldInvertState fieldState;
  TermToBytesRefAttribute termAtt;

  // Copied from our perThread
  final IntBlockPool intPool;
  final ByteBlockPool bytePool;
  final ByteBlockPool termBytePool;

  final int streamCount;
  final int numPostingInt;

  final FieldInfo fieldInfo;

  boolean postingsCompacted;
  int numPostings;
  private int postingsHashSize = 4;
  private int postingsHashHalfSize = postingsHashSize/2;
  private int postingsHashMask = postingsHashSize-1;
  private int[] postingsHash;
 
  ParallelPostingsArray postingsArray;
  private final BytesRef utf8;
  private Comparator<BytesRef> termComp;

  public TermsHashPerField(DocInverterPerField docInverterPerField, final TermsHashPerThread perThread, final TermsHashPerThread nextPerThread, final FieldInfo fieldInfo) {
    this.perThread = perThread;
    intPool = perThread.intPool;
    bytePool = perThread.bytePool;
    termBytePool = perThread.termBytePool;
    docState = perThread.docState;

    postingsHash = new int[postingsHashSize];
    Arrays.fill(postingsHash, -1);
    bytesUsed(postingsHashSize * RamUsageEstimator.NUM_BYTES_INT);

    fieldState = docInverterPerField.fieldState;
    this.consumer = perThread.consumer.addField(this, fieldInfo);
    initPostingsArray();

    streamCount = consumer.getStreamCount();
    numPostingInt = 2*streamCount;
    utf8 = perThread.utf8;
    this.fieldInfo = fieldInfo;
    if (nextPerThread != null)
      nextPerField = (TermsHashPerField) nextPerThread.addField(docInverterPerField, fieldInfo);
    else
      nextPerField = null;
  }

  private void initPostingsArray() {
    postingsArray = consumer.createPostingsArray(2);
    bytesUsed(postingsArray.size * postingsArray.bytesPerPosting());
  }

  // sugar: just forwards to DW
  private void bytesUsed(long size) {
    if (perThread.termsHash.trackAllocations) {
      perThread.termsHash.docWriter.bytesUsed(size);
    }
  }
  
  void shrinkHash(int targetSize) {
    assert postingsCompacted || numPostings == 0;

    final int newSize = 4;
    if (newSize != postingsHash.length) {
      final long previousSize = postingsHash.length;
      postingsHash = new int[newSize];
      bytesUsed((newSize-previousSize)*RamUsageEstimator.NUM_BYTES_INT);
      Arrays.fill(postingsHash, -1);
      postingsHashSize = newSize;
      postingsHashHalfSize = newSize/2;
      postingsHashMask = newSize-1;
    }

    // Fully free the postings array on each flush:
    if (postingsArray != null) {
      bytesUsed(-postingsArray.bytesPerPosting() * postingsArray.size);
      postingsArray = null;
    }
  }

  public void reset() {
    if (!postingsCompacted)
      compactPostings();
    assert numPostings <= postingsHash.length;
    if (numPostings > 0) {
      Arrays.fill(postingsHash, 0, numPostings, -1);
      numPostings = 0;
    }
    postingsCompacted = false;
    if (nextPerField != null)
      nextPerField.reset();
  }

  @Override
  synchronized public void abort() {
    reset();
    if (nextPerField != null)
      nextPerField.abort();
  }
  
  private final void growParallelPostingsArray() {
    int oldSize = postingsArray.size;
    this.postingsArray = this.postingsArray.grow();
    bytesUsed(postingsArray.bytesPerPosting() * (postingsArray.size - oldSize));
  }

  public void initReader(ByteSliceReader reader, int termID, int stream) {
    assert stream < streamCount;
    int intStart = postingsArray.intStarts[termID];
    final int[] ints = intPool.buffers[intStart >> DocumentsWriter.INT_BLOCK_SHIFT];
    final int upto = intStart & DocumentsWriter.INT_BLOCK_MASK;
    reader.init(bytePool,
                postingsArray.byteStarts[termID]+stream*ByteBlockPool.FIRST_LEVEL_SIZE,
                ints[upto+stream]);
  }

  private synchronized void compactPostings() {
    int upto = 0;
    for(int i=0;i<postingsHashSize;i++) {
      if (postingsHash[i] != -1) {
        if (upto < i) {
          postingsHash[upto] = postingsHash[i];
          postingsHash[i] = -1;
        }
        upto++;
      }
    }

    assert upto == numPostings;
    postingsCompacted = true;
  }

  /** Collapse the hash table & sort in-place. */
  public int[] sortPostings(Comparator<BytesRef> termComp) {
    this.termComp = termComp;
    compactPostings();
    quickSort(postingsHash, 0, numPostings-1);
    return postingsHash;
  }

  void quickSort(int[] termIDs, int lo, int hi) {
    if (lo >= hi)
      return;
    else if (hi == 1+lo) {
      if (comparePostings(termIDs[lo], termIDs[hi]) > 0) {
        final int tmp = termIDs[lo];
        termIDs[lo] = termIDs[hi];
        termIDs[hi] = tmp;
      }
      return;
    }

    int mid = (lo + hi) >>> 1;

    if (comparePostings(termIDs[lo], termIDs[mid]) > 0) {
      int tmp = termIDs[lo];
      termIDs[lo] = termIDs[mid];
      termIDs[mid] = tmp;
    }

    if (comparePostings(termIDs[mid], termIDs[hi]) > 0) {
      int tmp = termIDs[mid];
      termIDs[mid] = termIDs[hi];
      termIDs[hi] = tmp;

      if (comparePostings(termIDs[lo], termIDs[mid]) > 0) {
        int tmp2 = termIDs[lo];
        termIDs[lo] = termIDs[mid];
        termIDs[mid] = tmp2;
      }
    }

    int left = lo + 1;
    int right = hi - 1;

    if (left >= right)
      return;

    int partition = termIDs[mid];

    for (; ;) {
      while (comparePostings(termIDs[right], partition) > 0)
        --right;

      while (left < right && comparePostings(termIDs[left], partition) <= 0)
        ++left;

      if (left < right) {
        int tmp = termIDs[left];
        termIDs[left] = termIDs[right];
        termIDs[right] = tmp;
        --right;
      } else {
        break;
      }
    }

    quickSort(termIDs, lo, left);
    quickSort(termIDs, left + 1, hi);
  }

  /** Compares term text for two Posting instance and
   *  returns -1 if p1 < p2; 1 if p1 > p2; else 0. */
  int comparePostings(int term1, int term2) {

    if (term1 == term2) {
      // Our quicksort does this, eg during partition
      return 0;
    }

    termBytePool.setBytesRef(perThread.tr1, postingsArray.textStarts[term1]);
    termBytePool.setBytesRef(perThread.tr2, postingsArray.textStarts[term2]);

    return termComp.compare(perThread.tr1, perThread.tr2);
  }

  /** Test whether the text for current RawPostingList p equals
   *  current tokenText in utf8. */
  private boolean postingEquals(final int termID) {
    final int textStart = postingsArray.textStarts[termID];
    final byte[] text = termBytePool.buffers[textStart >> DocumentsWriter.BYTE_BLOCK_SHIFT];
    assert text != null;

    int pos = textStart & DocumentsWriter.BYTE_BLOCK_MASK;
    
    final int len;
    if ((text[pos] & 0x80) == 0) {
      // length is 1 byte
      len = text[pos];
      pos += 1;
    } else {
      // length is 2 bytes
      len = (text[pos]&0x7f) + ((text[pos+1]&0xff)<<7);
      pos += 2;
    }

    if (len == utf8.length) {
      final byte[] utf8Bytes = utf8.bytes;
      for(int tokenPos=0;tokenPos<utf8.length;pos++,tokenPos++) {
        if (utf8Bytes[tokenPos] != text[pos]) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }
  
  private boolean doCall;
  private boolean doNextCall;

  @Override
  void start(Fieldable f) {
    termAtt = fieldState.attributeSource.getAttribute(TermToBytesRefAttribute.class);
    consumer.start(f);
    if (nextPerField != null) {
      nextPerField.start(f);
    }
  }
  
  @Override
  boolean start(Fieldable[] fields, int count) throws IOException {
    doCall = consumer.start(fields, count);
    if (postingsArray == null) {
      initPostingsArray();
    }

    if (nextPerField != null)
      doNextCall = nextPerField.start(fields, count);
    return doCall || doNextCall;
  }

  // Secondary entry point (for 2nd & subsequent TermsHash),
  // because token text has already been "interned" into
  // textStart, so we hash by textStart
  public void add(int textStart) throws IOException {
    int code = textStart;

    int hashPos = code & postingsHashMask;

    assert !postingsCompacted;

    // Locate RawPostingList in hash
    int termID = postingsHash[hashPos];

    if (termID != -1 && postingsArray.textStarts[termID] != textStart) {
      // Conflict: keep searching different locations in
      // the hash table.
      final int inc = ((code>>8)+code)|1;
      do {
        code += inc;
        hashPos = code & postingsHashMask;
        termID = postingsHash[hashPos];
      } while (termID != -1 && postingsArray.textStarts[termID] != textStart);
    }

    if (termID == -1) {

      // First time we are seeing this token since we last
      // flushed the hash.

      // New posting
      termID = numPostings++;
      if (termID >= postingsArray.size) {
        growParallelPostingsArray();
      }

      assert termID >= 0;

      postingsArray.textStarts[termID] = textStart;
          
      assert postingsHash[hashPos] == -1;
      postingsHash[hashPos] = termID;

      if (numPostings == postingsHashHalfSize)
        rehashPostings(2*postingsHashSize);

      // Init stream slices
      if (numPostingInt + intPool.intUpto > DocumentsWriter.INT_BLOCK_SIZE)
        intPool.nextBuffer();

      if (DocumentsWriter.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt*ByteBlockPool.FIRST_LEVEL_SIZE)
        bytePool.nextBuffer();

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
      int intStart = postingsArray.intStarts[termID];
      intUptos = intPool.buffers[intStart >> DocumentsWriter.INT_BLOCK_SHIFT];
      intUptoStart = intStart & DocumentsWriter.INT_BLOCK_MASK;
      consumer.addTerm(termID);
    }
  }

  // Primary entry point (for first TermsHash)
  @Override
  void add() throws IOException {

    assert !postingsCompacted;

    // We are first in the chain so we must "intern" the
    // term text into textStart address

    // Get the text & hash of this term.
    int code = termAtt.toBytesRef(utf8);

    int hashPos = code & postingsHashMask;

    // Locate RawPostingList in hash
    int termID = postingsHash[hashPos];

    if (termID != -1 && !postingEquals(termID)) {
      // Conflict: keep searching different locations in
      // the hash table.
      final int inc = ((code>>8)+code)|1;
      do {
        code += inc;
        hashPos = code & postingsHashMask;
        termID = postingsHash[hashPos];
      } while (termID != -1 && !postingEquals(termID));
    }

    if (termID == -1) {

      // First time we are seeing this token since we last
      // flushed the hash.
      final int textLen2 = 2+utf8.length;
      if (textLen2 + bytePool.byteUpto > DocumentsWriter.BYTE_BLOCK_SIZE) {
        // Not enough room in current block

        if (utf8.length > DocumentsWriter.MAX_TERM_LENGTH_UTF8) {
          // Just skip this term, to remain as robust as
          // possible during indexing.  A TokenFilter
          // can be inserted into the analyzer chain if
          // other behavior is wanted (pruning the term
          // to a prefix, throwing an exception, etc).
          if (docState.maxTermPrefix == null) {
            final int saved = utf8.length;
            try {
              utf8.length = Math.min(30, DocumentsWriter.MAX_TERM_LENGTH_UTF8);
              docState.maxTermPrefix = utf8.toString();
            } finally {
              utf8.length = saved;
            }
          }

          consumer.skippingLongTerm();
          return;
        }
        bytePool.nextBuffer();
      }

      // New posting
      termID = numPostings++;
      if (termID >= postingsArray.size) {
        growParallelPostingsArray();
      }

      assert termID != -1;
      assert postingsHash[hashPos] == -1;

      postingsHash[hashPos] = termID;

      final byte[] text = bytePool.buffer;
      final int textUpto = bytePool.byteUpto;
      postingsArray.textStarts[termID] = textUpto + bytePool.byteOffset;

      // We first encode the length, followed by the UTF8
      // bytes.  Length is encoded as vInt, but will consume
      // 1 or 2 bytes at most (we reject too-long terms,
      // above).

      // encode length @ start of bytes
      if (utf8.length < 128) {
        // 1 byte to store length
        text[textUpto] = (byte) utf8.length;
        bytePool.byteUpto += utf8.length + 1;
        System.arraycopy(utf8.bytes, 0, text, textUpto+1, utf8.length);
      } else {
        // 2 byte to store length
        text[textUpto] = (byte) (0x80 | (utf8.length & 0x7f));
        text[textUpto+1] = (byte) ((utf8.length>>7) & 0xff);
        bytePool.byteUpto += utf8.length + 2;
        System.arraycopy(utf8.bytes, 0, text, textUpto+2, utf8.length);
      }

      if (numPostings == postingsHashHalfSize) {
        rehashPostings(2*postingsHashSize);
        bytesUsed(2*numPostings * RamUsageEstimator.NUM_BYTES_INT);
      }

      // Init stream slices
      if (numPostingInt + intPool.intUpto > DocumentsWriter.INT_BLOCK_SIZE) {
        intPool.nextBuffer();
      }

      if (DocumentsWriter.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt*ByteBlockPool.FIRST_LEVEL_SIZE) {
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
      final int intStart = postingsArray.intStarts[termID];
      intUptos = intPool.buffers[intStart >> DocumentsWriter.INT_BLOCK_SHIFT];
      intUptoStart = intStart & DocumentsWriter.INT_BLOCK_MASK;
      consumer.addTerm(termID);
    }

    if (doNextCall)
      nextPerField.add(postingsArray.textStarts[termID]);
  }

  int[] intUptos;
  int intUptoStart;

  void writeByte(int stream, byte b) {
    int upto = intUptos[intUptoStart+stream];
    byte[] bytes = bytePool.buffers[upto >> DocumentsWriter.BYTE_BLOCK_SHIFT];
    assert bytes != null;
    int offset = upto & DocumentsWriter.BYTE_BLOCK_MASK;
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

  /** Called when postings hash is too small (> 50%
   *  occupied) or too large (< 20% occupied). */
  void rehashPostings(final int newSize) {

    final int newMask = newSize-1;

    int[] newHash = new int[newSize];
    Arrays.fill(newHash, -1);
    for(int i=0;i<postingsHashSize;i++) {
      int termID = postingsHash[i];
      if (termID != -1) {
        int code;
        if (perThread.primary) {
          final int textStart = postingsArray.textStarts[termID];
          final int start = textStart & DocumentsWriter.BYTE_BLOCK_MASK;
          final byte[] text = bytePool.buffers[textStart >> DocumentsWriter.BYTE_BLOCK_SHIFT];
          code = 0;

          final int len;
          int pos;
          if ((text[start] & 0x80) == 0) {
            // length is 1 byte
            len = text[start];
            pos = start+1;
          } else {
            len = (text[start]&0x7f) + ((text[start+1]&0xff)<<7);
            pos = start+2;
          }

          final int endPos = pos+len;
          while(pos < endPos) {
            code = (code*31) + text[pos++];
          }
        } else {
          code = postingsArray.textStarts[termID];
        }

        int hashPos = code & newMask;
        assert hashPos >= 0;
        if (newHash[hashPos] != -1) {
          final int inc = ((code>>8)+code)|1;
          do {
            code += inc;
            hashPos = code & newMask;
          } while (newHash[hashPos] != -1);
        }
        newHash[hashPos] = termID;
      }
    }

    postingsHashMask = newMask;
    postingsHash = newHash;

    postingsHashSize = newSize;
    postingsHashHalfSize = newSize >> 1;
  }
}
