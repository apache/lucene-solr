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

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.util.UnicodeUtil;

final class TermsHashPerField extends InvertedDocConsumerPerField {

  final TermsHashConsumerPerField consumer;
  final TermsHashPerField nextPerField;
  final TermsHashPerThread perThread;
  final DocumentsWriter.DocState docState;
  final DocInverter.FieldInvertState fieldState;

  // Copied from our perThread
  final CharBlockPool charPool;
  final IntBlockPool intPool;
  final ByteBlockPool bytePool;

  final int streamCount;
  final int numPostingInt;

  final FieldInfo fieldInfo;

  boolean postingsCompacted;
  int numPostings;
  private int postingsHashSize = 4;
  private int postingsHashHalfSize = postingsHashSize/2;
  private int postingsHashMask = postingsHashSize-1;
  private RawPostingList[] postingsHash = new RawPostingList[postingsHashSize];
  private RawPostingList p;

  public TermsHashPerField(DocInverterPerField docInverterPerField, final TermsHashPerThread perThread, final TermsHashPerThread nextPerThread, final FieldInfo fieldInfo) {
    this.perThread = perThread;
    intPool = perThread.intPool;
    charPool = perThread.charPool;
    bytePool = perThread.bytePool;
    docState = perThread.docState;
    fieldState = docInverterPerField.fieldState;
    this.consumer = perThread.consumer.addField(this, fieldInfo);
    streamCount = consumer.getStreamCount();
    numPostingInt = 2*streamCount;
    this.fieldInfo = fieldInfo;
    if (nextPerThread != null)
      nextPerField = (TermsHashPerField) nextPerThread.addField(docInverterPerField, fieldInfo);
    else
      nextPerField = null;
  }

  void shrinkHash(int targetSize) {
    assert postingsCompacted || numPostings == 0;

    // Cannot use ArrayUtil.shrink because we require power
    // of 2:
    int newSize = postingsHash.length;
    while(newSize >= 8 && newSize/4 > targetSize) {
      newSize /= 2;
    }

    if (newSize != postingsHash.length) {
      postingsHash = new RawPostingList[newSize];
      postingsHashSize = newSize;
      postingsHashHalfSize = newSize/2;
      postingsHashMask = newSize-1;
    }
  }

  public void reset() {
    if (!postingsCompacted)
      compactPostings();
    assert numPostings <= postingsHash.length;
    if (numPostings > 0) {
      perThread.termsHash.recyclePostings(postingsHash, numPostings);
      Arrays.fill(postingsHash, 0, numPostings, null);
      numPostings = 0;
    }
    postingsCompacted = false;
    if (nextPerField != null)
      nextPerField.reset();
  }

  synchronized public void abort() {
    reset();
    if (nextPerField != null)
      nextPerField.abort();
  }

  public void initReader(ByteSliceReader reader, RawPostingList p, int stream) {
    assert stream < streamCount;
    final int[] ints = intPool.buffers[p.intStart >> DocumentsWriter.INT_BLOCK_SHIFT];
    final int upto = p.intStart & DocumentsWriter.INT_BLOCK_MASK;
    reader.init(bytePool,
                p.byteStart+stream*ByteBlockPool.FIRST_LEVEL_SIZE,
                ints[upto+stream]);
  }

  private synchronized void compactPostings() {
    int upto = 0;
    for(int i=0;i<postingsHashSize;i++) {
      if (postingsHash[i] != null) {
        if (upto < i) {
          postingsHash[upto] = postingsHash[i];
          postingsHash[i] = null;
        }
        upto++;
      }
    }

    assert upto == numPostings;
    postingsCompacted = true;
  }

  /** Collapse the hash table & sort in-place. */
  public RawPostingList[] sortPostings() {
    compactPostings();
    quickSort(postingsHash, 0, numPostings-1);
    return postingsHash;
  }

  void quickSort(RawPostingList[] postings, int lo, int hi) {
    if (lo >= hi)
      return;
    else if (hi == 1+lo) {
      if (comparePostings(postings[lo], postings[hi]) > 0) {
        final RawPostingList tmp = postings[lo];
        postings[lo] = postings[hi];
        postings[hi] = tmp;
      }
      return;
    }

    int mid = (lo + hi) >>> 1;

    if (comparePostings(postings[lo], postings[mid]) > 0) {
      RawPostingList tmp = postings[lo];
      postings[lo] = postings[mid];
      postings[mid] = tmp;
    }

    if (comparePostings(postings[mid], postings[hi]) > 0) {
      RawPostingList tmp = postings[mid];
      postings[mid] = postings[hi];
      postings[hi] = tmp;

      if (comparePostings(postings[lo], postings[mid]) > 0) {
        RawPostingList tmp2 = postings[lo];
        postings[lo] = postings[mid];
        postings[mid] = tmp2;
      }
    }

    int left = lo + 1;
    int right = hi - 1;

    if (left >= right)
      return;

    RawPostingList partition = postings[mid];

    for (; ;) {
      while (comparePostings(postings[right], partition) > 0)
        --right;

      while (left < right && comparePostings(postings[left], partition) <= 0)
        ++left;

      if (left < right) {
        RawPostingList tmp = postings[left];
        postings[left] = postings[right];
        postings[right] = tmp;
        --right;
      } else {
        break;
      }
    }

    quickSort(postings, lo, left);
    quickSort(postings, left + 1, hi);
  }

  /** Compares term text for two Posting instance and
   *  returns -1 if p1 < p2; 1 if p1 > p2; else 0. */
  int comparePostings(RawPostingList p1, RawPostingList p2) {

    if (p1 == p2)
      return 0;

    final char[] text1 = charPool.buffers[p1.textStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];
    int pos1 = p1.textStart & DocumentsWriter.CHAR_BLOCK_MASK;
    final char[] text2 = charPool.buffers[p2.textStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];
    int pos2 = p2.textStart & DocumentsWriter.CHAR_BLOCK_MASK;

    assert text1 != text2 || pos1 != pos2;

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
      } else
        // This method should never compare equal postings
        // unless p1==p2
        assert c1 != 0xffff;
    }
  }

  /** Test whether the text for current RawPostingList p equals
   *  current tokenText. */
  private boolean postingEquals(final char[] tokenText, final int tokenTextLen) {

    final char[] text = perThread.charPool.buffers[p.textStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];
    assert text != null;
    int pos = p.textStart & DocumentsWriter.CHAR_BLOCK_MASK;

    int tokenPos = 0;
    for(;tokenPos<tokenTextLen;pos++,tokenPos++)
      if (tokenText[tokenPos] != text[pos])
        return false;
    return 0xffff == text[pos];
  }
  
  private boolean doCall;
  private boolean doNextCall;

  boolean start(Fieldable[] fields, int count) throws IOException {
    doCall = consumer.start(fields, count);
    if (nextPerField != null)
      doNextCall = nextPerField.start(fields, count);
    return doCall || doNextCall;
  }

  // Secondary entry point (for 2nd & subsequent TermsHash),
  // because token text has already been "interned" into
  // textStart, so we hash by textStart
  public void add(Token token, int textStart) throws IOException {

    int code = textStart;

    int hashPos = code & postingsHashMask;

    assert !postingsCompacted;

    // Locate RawPostingList in hash
    p = postingsHash[hashPos];

    if (p != null && p.textStart != textStart) {
      // Conflict: keep searching different locations in
      // the hash table.
      final int inc = ((code>>8)+code)|1;
      do {
        code += inc;
        hashPos = code & postingsHashMask;
        p = postingsHash[hashPos];
      } while (p != null && p.textStart != textStart);
    }

    if (p == null) {

      // First time we are seeing this token since we last
      // flushed the hash.

      // Refill?
      if (0 == perThread.freePostingsCount)
        perThread.morePostings();

      // Pull next free RawPostingList from free list
      p = perThread.freePostings[--perThread.freePostingsCount];
      assert p != null;

      p.textStart = textStart;
          
      assert postingsHash[hashPos] == null;
      postingsHash[hashPos] = p;
      numPostings++;

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

      p.intStart = intUptoStart + intPool.intOffset;

      for(int i=0;i<streamCount;i++) {
        final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
        intUptos[intUptoStart+i] = upto + bytePool.byteOffset;
      }
      p.byteStart = intUptos[intUptoStart];

      consumer.newTerm(token, p);

    } else {
      intUptos = intPool.buffers[p.intStart >> DocumentsWriter.INT_BLOCK_SHIFT];
      intUptoStart = p.intStart & DocumentsWriter.INT_BLOCK_MASK;
      consumer.addTerm(token, p);
    }
  }

  // Primary entry point (for first TermsHash)
  void add(Token token) throws IOException {

    assert !postingsCompacted;

    // We are first in the chain so we must "intern" the
    // term text into textStart address

    // Get the text of this term.
    final char[] tokenText = token.termBuffer();
    final int tokenTextLen = token.termLength();

    // Compute hashcode & replace any invalid UTF16 sequences
    int downto = tokenTextLen;
    int code = 0;
    while (downto > 0) {
      char ch = tokenText[--downto];

      if (ch >= UnicodeUtil.UNI_SUR_LOW_START && ch <= UnicodeUtil.UNI_SUR_LOW_END) {
        if (0 == downto) {
          // Unpaired
          ch = tokenText[downto] = UnicodeUtil.UNI_REPLACEMENT_CHAR;
        } else {
          final char ch2 = tokenText[downto-1];
          if (ch2 >= UnicodeUtil.UNI_SUR_HIGH_START && ch2 <= UnicodeUtil.UNI_SUR_HIGH_END) {
            // OK: high followed by low.  This is a valid
            // surrogate pair.
            code = ((code*31) + ch)*31+ch2;
            downto--;
            continue;
          } else {
            // Unpaired
            ch = tokenText[downto] = UnicodeUtil.UNI_REPLACEMENT_CHAR;
          }            
        }
      } else if (ch >= UnicodeUtil.UNI_SUR_HIGH_START && ch <= UnicodeUtil.UNI_SUR_HIGH_END)
        // Unpaired
        ch = tokenText[downto] = UnicodeUtil.UNI_REPLACEMENT_CHAR;

      code = (code*31) + ch;
    }

    int hashPos = code & postingsHashMask;

    // Locate RawPostingList in hash
    p = postingsHash[hashPos];

    if (p != null && !postingEquals(tokenText, tokenTextLen)) {
      // Conflict: keep searching different locations in
      // the hash table.
      final int inc = ((code>>8)+code)|1;
      do {
        code += inc;
        hashPos = code & postingsHashMask;
        p = postingsHash[hashPos];
      } while (p != null && !postingEquals(tokenText, tokenTextLen));
    }

    if (p == null) {

      // First time we are seeing this token since we last
      // flushed the hash.
      final int textLen1 = 1+tokenTextLen;
      if (textLen1 + charPool.charUpto > DocumentsWriter.CHAR_BLOCK_SIZE) {
        if (textLen1 > DocumentsWriter.CHAR_BLOCK_SIZE) {
          // Just skip this term, to remain as robust as
          // possible during indexing.  A TokenFilter
          // can be inserted into the analyzer chain if
          // other behavior is wanted (pruning the term
          // to a prefix, throwing an exception, etc).

          if (docState.maxTermPrefix == null)
            docState.maxTermPrefix = new String(tokenText, 0, 30);

          consumer.skippingLongTerm(token);
          return;
        }
        charPool.nextBuffer();
      }

      // Refill?
      if (0 == perThread.freePostingsCount)
        perThread.morePostings();

      // Pull next free RawPostingList from free list
      p = perThread.freePostings[--perThread.freePostingsCount];
      assert p != null;

      final char[] text = charPool.buffer;
      final int textUpto = charPool.charUpto;
      p.textStart = textUpto + charPool.charOffset;
      charPool.charUpto += textLen1;
      System.arraycopy(tokenText, 0, text, textUpto, tokenTextLen);
      text[textUpto+tokenTextLen] = 0xffff;
          
      assert postingsHash[hashPos] == null;
      postingsHash[hashPos] = p;
      numPostings++;

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

      p.intStart = intUptoStart + intPool.intOffset;

      for(int i=0;i<streamCount;i++) {
        final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
        intUptos[intUptoStart+i] = upto + bytePool.byteOffset;
      }
      p.byteStart = intUptos[intUptoStart];

      consumer.newTerm(token, p);

    } else {
      intUptos = intPool.buffers[p.intStart >> DocumentsWriter.INT_BLOCK_SHIFT];
      intUptoStart = p.intStart & DocumentsWriter.INT_BLOCK_MASK;
      consumer.addTerm(token, p);
    }

    if (doNextCall)
      nextPerField.add(token, p.textStart);
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

  void finish() throws IOException {
    consumer.finish();
    if (nextPerField != null)
      nextPerField.finish();
  }

  /** Called when postings hash is too small (> 50%
   *  occupied) or too large (< 20% occupied). */
  void rehashPostings(final int newSize) {

    final int newMask = newSize-1;

    RawPostingList[] newHash = new RawPostingList[newSize];
    for(int i=0;i<postingsHashSize;i++) {
      RawPostingList p0 = postingsHash[i];
      if (p0 != null) {
        int code;
        if (perThread.primary) {
          final int start = p0.textStart & DocumentsWriter.CHAR_BLOCK_MASK;
          final char[] text = charPool.buffers[p0.textStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];
          int pos = start;
          while(text[pos] != 0xffff)
            pos++;
          code = 0;
          while (pos > start)
            code = (code*31) + text[--pos];
        } else
          code = p0.textStart;

        int hashPos = code & newMask;
        assert hashPos >= 0;
        if (newHash[hashPos] != null) {
          final int inc = ((code>>8)+code)|1;
          do {
            code += inc;
            hashPos = code & newMask;
          } while (newHash[hashPos] != null);
        }
        newHash[hashPos] = p0;
      }
    }

    postingsHashMask = newMask;
    postingsHash = newHash;
    postingsHashSize = newSize;
    postingsHashHalfSize = newSize >> 1;
  }
}
