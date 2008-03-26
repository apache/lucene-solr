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

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.UnicodeUtil;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

/** Used by DocumentsWriter to hold data associated with a
 *  single field in a single ThreadState, including the
 *  Postings hash.  A document may have many occurrences for
 *  a given field name; we gather all such occurrences here
 *  (in docFields) so that we can process the entire field
 *  at once. */

final class DocumentsWriterFieldData implements Comparable {

  final DocumentsWriterThreadState threadState;
  FieldInfo fieldInfo;

  int fieldCount;
  Fieldable[] docFields = new Fieldable[1];

  int lastGen = -1;
  DocumentsWriterFieldData next;

  boolean doNorms;
  boolean doVectors;
  boolean doVectorPositions;
  boolean doVectorOffsets;
  boolean postingsCompacted;

  int numPostings;
      
  Posting[] postingsHash;
  int postingsHashSize;
  int postingsHashHalfSize;
  int postingsHashMask;

  int position;
  int length;
  int offset;
  float boost;
  int postingsVectorsUpto;

  public DocumentsWriterFieldData(DocumentsWriterThreadState threadState, FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    this.threadState = threadState;
  }

  void resetPostingArrays() {
    if (!postingsCompacted)
      compactPostings();
    threadState.docWriter.recyclePostings(this.postingsHash, numPostings);
    Arrays.fill(postingsHash, 0, postingsHash.length, null);
    postingsCompacted = false;
    numPostings = 0;
  }

  void initPostingArrays() {
    // Target hash fill factor of <= 50%
    // NOTE: must be a power of two for hash collision
    // strategy to work correctly
    postingsHashSize = 4;
    postingsHashHalfSize = 2;
    postingsHashMask = postingsHashSize-1;
    postingsHash = new Posting[postingsHashSize];
  }

  public int compareTo(Object o) {
    return fieldInfo.name.compareTo(((DocumentsWriterFieldData) o).fieldInfo.name);
  }

  private void compactPostings() {
    int upto = 0;
    for(int i=0;i<postingsHashSize;i++)
      if (postingsHash[i] != null)
        postingsHash[upto++] = postingsHash[i];

    assert upto == numPostings;
    postingsCompacted = true;
  }

  /** Collapse the hash table & sort in-place. */
  public Posting[] sortPostings() {
    compactPostings();
    threadState.doPostingSort(postingsHash, numPostings);
    return postingsHash;
  }

  /** Process all occurrences of one field in the document. */
  public void processField(Analyzer analyzer) throws IOException, AbortException {
    length = 0;
    position = 0;
    offset = 0;
    boost = threadState.docBoost;

    final int maxFieldLength = threadState.docWriter.writer.getMaxFieldLength();

    final int limit = fieldCount;
    final Fieldable[] docFieldsFinal = docFields;

    boolean doWriteVectors = true;

    // Walk through all occurrences in this doc for this
    // field:
    try {
      for(int j=0;j<limit;j++) {
        Fieldable field = docFieldsFinal[j];

        if (field.isIndexed())
          invertField(field, analyzer, maxFieldLength);

        if (field.isStored()) {
          threadState.numStoredFields++;
          boolean success = false;
          try {
            threadState.localFieldsWriter.writeField(fieldInfo, field);
            success = true;
          } finally {
            // If we hit an exception inside
            // localFieldsWriter.writeField, the
            // contents of fdtLocal can be corrupt, so
            // we must discard all stored fields for
            // this document:
            if (!success)
              threadState.fdtLocal.reset();
          }
        }

        docFieldsFinal[j] = null;
      }
    } catch (AbortException ae) {
      doWriteVectors = false;
      throw ae;
    } finally {
      if (postingsVectorsUpto > 0) {
        try {
          if (doWriteVectors) {
            // Add term vectors for this field
            boolean success = false;
            try {
              writeVectors(fieldInfo);
              success = true;
            } finally {
              if (!success) {
                // If we hit an exception inside
                // writeVectors, the contents of tvfLocal
                // can be corrupt, so we must discard all
                // term vectors for this document:
                threadState.numVectorFields = 0;
                threadState.tvfLocal.reset();
              }
            }
          }
        } finally {
          if (postingsVectorsUpto > threadState.maxPostingsVectors)
            threadState.maxPostingsVectors = postingsVectorsUpto;
          postingsVectorsUpto = 0;
          threadState.vectorsPool.reset();
        }
      }
    }
  }

  int offsetEnd;
  Token localToken = new Token();

  /* Invert one occurrence of one field in the document */
  public void invertField(Fieldable field, Analyzer analyzer, final int maxFieldLength) throws IOException, AbortException {

    if (length>0)
      position += analyzer.getPositionIncrementGap(fieldInfo.name);

    if (!field.isTokenized()) {		  // un-tokenized field
      String stringValue = field.stringValue();
      final int valueLength = stringValue.length();
      Token token = localToken;
      token.clear();
      char[] termBuffer = token.termBuffer();
      if (termBuffer.length < valueLength)
        termBuffer = token.resizeTermBuffer(valueLength);
      stringValue.getChars(0, valueLength, termBuffer, 0);
      token.setTermLength(valueLength);
      token.setStartOffset(offset);
      token.setEndOffset(offset + stringValue.length());
      addPosition(token);
      offset += stringValue.length();
      length++;
    } else {                                  // tokenized field
      final TokenStream stream;
      final TokenStream streamValue = field.tokenStreamValue();

      if (streamValue != null) 
        stream = streamValue;
      else {
        // the field does not have a TokenStream,
        // so we have to obtain one from the analyzer
        final Reader reader;			  // find or make Reader
        final Reader readerValue = field.readerValue();

        if (readerValue != null)
          reader = readerValue;
        else {
          String stringValue = field.stringValue();
          if (stringValue == null)
            throw new IllegalArgumentException("field must have either TokenStream, String or Reader value");
          threadState.stringReader.init(stringValue);
          reader = threadState.stringReader;
        }
          
        // Tokenize field and add to postingTable
        stream = analyzer.reusableTokenStream(fieldInfo.name, reader);
      }

      // reset the TokenStream to the first token
      stream.reset();

      try {
        offsetEnd = offset-1;
        Token token;
        for(;;) {
          token = stream.next(localToken);
          if (token == null) break;
          position += (token.getPositionIncrement() - 1);
          addPosition(token);
          if (++length >= maxFieldLength) {
            if (threadState.docWriter.infoStream != null)
              threadState.docWriter.infoStream.println("maxFieldLength " +maxFieldLength+ " reached for field " + fieldInfo.name + ", ignoring following tokens");
            break;
          }
        }
        offset = offsetEnd+1;
      } finally {
        stream.close();
      }
    }

    boost *= field.getBoost();
  }

  /** Only called when term vectors are enabled.  This
   *  is called the first time we see a given term for
   *  each document, to allocate a PostingVector
   *  instance that is used to record data needed to
   *  write the posting vectors. */
  private PostingVector addNewVector() {

    if (postingsVectorsUpto == threadState.postingsVectors.length) {
      final int newSize;
      if (threadState.postingsVectors.length < 2)
        newSize = 2;
      else
        newSize = (int) (1.5*threadState.postingsVectors.length);
      PostingVector[] newArray = new PostingVector[newSize];
      System.arraycopy(threadState.postingsVectors, 0, newArray, 0, threadState.postingsVectors.length);
      threadState.postingsVectors = newArray;
    }
        
    p.vector = threadState.postingsVectors[postingsVectorsUpto];
    if (p.vector == null)
      p.vector = threadState.postingsVectors[postingsVectorsUpto] = new PostingVector();

    postingsVectorsUpto++;

    final PostingVector v = p.vector;
    v.p = p;

    if (doVectorPositions) {
      final int upto = threadState.vectorsPool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
      v.posStart = v.posUpto = threadState.vectorsPool.byteOffset + upto;
    }

    if (doVectorOffsets) {
      final int upto = threadState.vectorsPool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
      v.offsetStart = v.offsetUpto = threadState.vectorsPool.byteOffset + upto;
    }

    return v;
  }

  int offsetStartCode;
  int offsetStart;

  // Current posting we are working on
  Posting p;
  PostingVector vector;

  /** Test whether the text for current Posting p equals
   *  current tokenText. */
  boolean postingEquals(final char[] tokenText, final int tokenTextLen) {

    final char[] text = threadState.charPool.buffers[p.textStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];
    assert text != null;
    int pos = p.textStart & DocumentsWriter.CHAR_BLOCK_MASK;

    int tokenPos = 0;
    for(;tokenPos<tokenTextLen;pos++,tokenPos++)
      if (tokenText[tokenPos] != text[pos])
        return false;
    return 0xffff == text[pos];
  }

  /** This is the hotspot of indexing: it's called once
   *  for every term of every document.  Its job is to *
   *  update the postings byte stream (Postings hash) *
   *  based on the occurence of a single term. */
  private void addPosition(Token token) throws AbortException {

    final Payload payload = token.getPayload();

    // Get the text of this term.  Term can either
    // provide a String token or offset into a char[]
    // array
    final char[] tokenText = token.termBuffer();
    final int tokenTextLen = token.termLength();

    int code = 0;

    // Compute hashcode & replace any invalid UTF16 sequences
    int downto = tokenTextLen;
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

    // System.out.println("  addPosition: field=" + fieldInfo.name + " buffer=" + new String(tokenText, 0, tokenTextLen) + " pos=" + position + " offsetStart=" + (offset+token.startOffset()) + " offsetEnd=" + (offset + token.endOffset()) + " docID=" + docID + " doPos=" + doVectorPositions + " doOffset=" + doVectorOffsets);

    int hashPos = code & postingsHashMask;

    assert !postingsCompacted;

    // Locate Posting in hash
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

    final int proxCode;

    // If we hit an exception below, it's possible the
    // posting list or term vectors data will be
    // partially written and thus inconsistent if
    // flushed, so we have to abort all documents
    // since the last flush:

    try {

      if (p != null) {       // term seen since last flush

        if (threadState.docID != p.lastDocID) { // term not yet seen in this doc
            
          // System.out.println("    seen before (new docID=" + docID + ") freqUpto=" + p.freqUpto +" proxUpto=" + p.proxUpto);

          assert p.docFreq > 0;

          // Now that we know doc freq for previous doc,
          // write it & lastDocCode
          freqUpto = p.freqUpto & DocumentsWriter.BYTE_BLOCK_MASK;
          freq = threadState.postingsPool.buffers[p.freqUpto >> DocumentsWriter.BYTE_BLOCK_SHIFT];
          if (1 == p.docFreq)
            writeFreqVInt(p.lastDocCode|1);
          else {
            writeFreqVInt(p.lastDocCode);
            writeFreqVInt(p.docFreq);
          }
          p.freqUpto = freqUpto + (p.freqUpto & DocumentsWriter.BYTE_BLOCK_NOT_MASK);

          if (doVectors) {
            vector = addNewVector();
            if (doVectorOffsets) {
              offsetStartCode = offsetStart = offset + token.startOffset();
              offsetEnd = offset + token.endOffset();
            }
          }

          proxCode = position;

          p.docFreq = 1;

          // Store code so we can write this after we're
          // done with this new doc
          p.lastDocCode = (threadState.docID-p.lastDocID) << 1;
          p.lastDocID = threadState.docID;

        } else {                                // term already seen in this doc
          // System.out.println("    seen before (same docID=" + docID + ") proxUpto=" + p.proxUpto);
          p.docFreq++;

          proxCode = position-p.lastPosition;

          if (doVectors) {
            vector = p.vector;
            if (vector == null)
              vector = addNewVector();
            if (doVectorOffsets) {
              offsetStart = offset + token.startOffset();
              offsetEnd = offset + token.endOffset();
              offsetStartCode = offsetStart-vector.lastOffset;
            }
          }
        }
      } else {					  // term not seen before
        // System.out.println("    never seen docID=" + docID);

        // Refill?
        if (0 == threadState.postingsFreeCount) {
          threadState.docWriter.getPostings(threadState.postingsFreeList);
          threadState.postingsFreeCount = threadState.postingsFreeList.length;
        }

        final int textLen1 = 1+tokenTextLen;
        if (textLen1 + threadState.charPool.byteUpto > DocumentsWriter.CHAR_BLOCK_SIZE) {
          if (textLen1 > DocumentsWriter.CHAR_BLOCK_SIZE) {
            // Just skip this term, to remain as robust as
            // possible during indexing.  A TokenFilter
            // can be inserted into the analyzer chain if
            // other behavior is wanted (pruning the term
            // to a prefix, throwing an exception, etc).
            if (threadState.maxTermPrefix == null)
              threadState.maxTermPrefix = new String(tokenText, 0, 30);

            // Still increment position:
            position++;
            return;
          }
          threadState.charPool.nextBuffer();
        }

        final char[] text = threadState.charPool.buffer;
        final int textUpto = threadState.charPool.byteUpto;

        // Pull next free Posting from free list
        p = threadState.postingsFreeList[--threadState.postingsFreeCount];

        p.textStart = textUpto + threadState.charPool.byteOffset;
        threadState.charPool.byteUpto += textLen1;

        System.arraycopy(tokenText, 0, text, textUpto, tokenTextLen);

        text[textUpto+tokenTextLen] = 0xffff;
          
        assert postingsHash[hashPos] == null;

        postingsHash[hashPos] = p;
        numPostings++;

        if (numPostings == postingsHashHalfSize)
          rehashPostings(2*postingsHashSize);

        // Init first slice for freq & prox streams
        final int upto1 = threadState.postingsPool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
        p.freqStart = p.freqUpto = threadState.postingsPool.byteOffset + upto1;

        final int upto2 = threadState.postingsPool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
        p.proxStart = p.proxUpto = threadState.postingsPool.byteOffset + upto2;

        p.lastDocCode = threadState.docID << 1;
        p.lastDocID = threadState.docID;
        p.docFreq = 1;

        if (doVectors) {
          vector = addNewVector();
          if (doVectorOffsets) {
            offsetStart = offsetStartCode = offset + token.startOffset();
            offsetEnd = offset + token.endOffset();
          }
        }

        proxCode = position;
      }

      proxUpto = p.proxUpto & DocumentsWriter.BYTE_BLOCK_MASK;
      prox = threadState.postingsPool.buffers[p.proxUpto >> DocumentsWriter.BYTE_BLOCK_SHIFT];
      assert prox != null;

      if (payload != null && payload.length > 0) {
        writeProxVInt((proxCode<<1)|1);
        writeProxVInt(payload.length);
        writeProxBytes(payload.data, payload.offset, payload.length);
        fieldInfo.storePayloads = true;
      } else
        writeProxVInt(proxCode<<1);

      p.proxUpto = proxUpto + (p.proxUpto & DocumentsWriter.BYTE_BLOCK_NOT_MASK);

      p.lastPosition = position++;

      if (doVectorPositions) {
        posUpto = vector.posUpto & DocumentsWriter.BYTE_BLOCK_MASK;
        pos = threadState.vectorsPool.buffers[vector.posUpto >> DocumentsWriter.BYTE_BLOCK_SHIFT];
        writePosVInt(proxCode);
        vector.posUpto = posUpto + (vector.posUpto & DocumentsWriter.BYTE_BLOCK_NOT_MASK);
      }

      if (doVectorOffsets) {
        offsetUpto = vector.offsetUpto & DocumentsWriter.BYTE_BLOCK_MASK;
        offsets = threadState.vectorsPool.buffers[vector.offsetUpto >> DocumentsWriter.BYTE_BLOCK_SHIFT];
        writeOffsetVInt(offsetStartCode);
        writeOffsetVInt(offsetEnd-offsetStart);
        vector.lastOffset = offsetEnd;
        vector.offsetUpto = offsetUpto + (vector.offsetUpto & DocumentsWriter.BYTE_BLOCK_NOT_MASK);
      }
    } catch (Throwable t) {
      throw new AbortException(t, threadState.docWriter);
    }
  }

  /** Write vInt into freq stream of current Posting */
  public void writeFreqVInt(int i) {
    while ((i & ~0x7F) != 0) {
      writeFreqByte((byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeFreqByte((byte) i);
  }

  /** Write vInt into prox stream of current Posting */
  public void writeProxVInt(int i) {
    while ((i & ~0x7F) != 0) {
      writeProxByte((byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeProxByte((byte) i);
  }

  /** Write byte into freq stream of current Posting */
  byte[] freq;
  int freqUpto;
  public void writeFreqByte(byte b) {
    assert freq != null;
    if (freq[freqUpto] != 0) {
      freqUpto = threadState.postingsPool.allocSlice(freq, freqUpto);
      freq = threadState.postingsPool.buffer;
      p.freqUpto = threadState.postingsPool.byteOffset;
    }
    freq[freqUpto++] = b;
  }

  /** Write byte into prox stream of current Posting */
  byte[] prox;
  int proxUpto;
  public void writeProxByte(byte b) {
    assert prox != null;
    if (prox[proxUpto] != 0) {
      proxUpto = threadState.postingsPool.allocSlice(prox, proxUpto);
      prox = threadState.postingsPool.buffer;
      p.proxUpto = threadState.postingsPool.byteOffset;
      assert prox != null;
    }
    prox[proxUpto++] = b;
    assert proxUpto != prox.length;
  }

  /** Currently only used to copy a payload into the prox
   *  stream. */
  public void writeProxBytes(byte[] b, int offset, int len) {
    final int offsetEnd = offset + len;
    while(offset < offsetEnd) {
      if (prox[proxUpto] != 0) {
        // End marker
        proxUpto = threadState.postingsPool.allocSlice(prox, proxUpto);
        prox = threadState.postingsPool.buffer;
        p.proxUpto = threadState.postingsPool.byteOffset;
      }

      prox[proxUpto++] = b[offset++];
      assert proxUpto != prox.length;
    }
  }

  /** Write vInt into offsets stream of current
   *  PostingVector */
  public void writeOffsetVInt(int i) {
    while ((i & ~0x7F) != 0) {
      writeOffsetByte((byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeOffsetByte((byte) i);
  }

  byte[] offsets;
  int offsetUpto;

  /** Write byte into offsets stream of current
   *  PostingVector */
  public void writeOffsetByte(byte b) {
    assert offsets != null;
    if (offsets[offsetUpto] != 0) {
      offsetUpto = threadState.vectorsPool.allocSlice(offsets, offsetUpto);
      offsets = threadState.vectorsPool.buffer;
      vector.offsetUpto = threadState.vectorsPool.byteOffset;
    }
    offsets[offsetUpto++] = b;
  }

  /** Write vInt into pos stream of current
   *  PostingVector */
  public void writePosVInt(int i) {
    while ((i & ~0x7F) != 0) {
      writePosByte((byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writePosByte((byte) i);
  }

  byte[] pos;
  int posUpto;

  /** Write byte into pos stream of current
   *  PostingVector */
  public void writePosByte(byte b) {
    assert pos != null;
    if (pos[posUpto] != 0) {
      posUpto = threadState.vectorsPool.allocSlice(pos, posUpto);
      pos = threadState.vectorsPool.buffer;
      vector.posUpto = threadState.vectorsPool.byteOffset;
    }
    pos[posUpto++] = b;
  }

  /** Called when postings hash is too small (> 50%
   *  occupied) or too large (< 20% occupied). */
  void rehashPostings(final int newSize) {

    final int newMask = newSize-1;

    Posting[] newHash = new Posting[newSize];
    for(int i=0;i<postingsHashSize;i++) {
      Posting p0 = postingsHash[i];
      if (p0 != null) {
        final int start = p0.textStart & DocumentsWriter.CHAR_BLOCK_MASK;
        final char[] text = threadState.charPool.buffers[p0.textStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];
        int pos = start;
        while(text[pos] != 0xffff)
          pos++;
        int code = 0;
        while (pos > start)
          code = (code*31) + text[--pos];

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

    postingsHashMask =  newMask;
    postingsHash = newHash;
    postingsHashSize = newSize;
    postingsHashHalfSize = newSize >> 1;
  }
      
  final ByteSliceReader vectorSliceReader = new ByteSliceReader();

  /** Called once per field per document if term vectors
   *  are enabled, to write the vectors to *
   *  RAMOutputStream, which is then quickly flushed to
   *  * the real term vectors files in the Directory. */
  void writeVectors(FieldInfo fieldInfo) throws IOException {

    assert fieldInfo.storeTermVector;
    assert threadState.vectorFieldsInOrder(fieldInfo);

    threadState.vectorFieldNumbers[threadState.numVectorFields] = fieldInfo.number;
    threadState.vectorFieldPointers[threadState.numVectorFields] = threadState.tvfLocal.getFilePointer();
    threadState.numVectorFields++;

    final int numPostingsVectors = postingsVectorsUpto;
    final PostingVector[] postingsVectors = threadState.postingsVectors;

    final IndexOutput tvfLocal = threadState.tvfLocal;

    threadState.tvfLocal.writeVInt(numPostingsVectors);
    byte bits = 0x0;
    if (doVectorPositions)
      bits |= TermVectorsReader.STORE_POSITIONS_WITH_TERMVECTOR;
    if (doVectorOffsets) 
      bits |= TermVectorsReader.STORE_OFFSET_WITH_TERMVECTOR;
    threadState.tvfLocal.writeByte(bits);

    threadState.doVectorSort(postingsVectors, numPostingsVectors);

    int encoderUpto = 0;
    int lastTermBytesCount = 0;

    final ByteSliceReader reader = vectorSliceReader;
    final char[][] charBuffers = threadState.charPool.buffers;

    for(int j=0;j<numPostingsVectors;j++) {
      final PostingVector vector = postingsVectors[j];
      Posting posting = vector.p;
      final int freq = posting.docFreq;
          
      final char[] text2 = charBuffers[posting.textStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];
      final int start2 = posting.textStart & DocumentsWriter.CHAR_BLOCK_MASK;

      // We swap between two encoders to save copying
      // last Term's byte array
      final UnicodeUtil.UTF8Result utf8Result = threadState.utf8Results[encoderUpto];

      // TODO: we could do this incrementally
      UnicodeUtil.UTF16toUTF8(text2, start2, utf8Result);
      final int termBytesCount = utf8Result.length;

      // TODO: UTF16toUTF8 could tell us this prefix
      // Compute common prefix between last term and
      // this term
      int prefix = 0;
      if (j > 0) {
        final byte[] lastTermBytes = threadState.utf8Results[1-encoderUpto].result;
        final byte[] termBytes = threadState.utf8Results[encoderUpto].result;
        while(prefix < lastTermBytesCount && prefix < termBytesCount) {
          if (lastTermBytes[prefix] != termBytes[prefix])
            break;
          prefix++;
        }
      }
      encoderUpto = 1-encoderUpto;
      lastTermBytesCount = termBytesCount;

      final int suffix = termBytesCount - prefix;
      tvfLocal.writeVInt(prefix);
      tvfLocal.writeVInt(suffix);
      tvfLocal.writeBytes(utf8Result.result, prefix, suffix);
      tvfLocal.writeVInt(freq);

      if (doVectorPositions) {
        reader.init(threadState.vectorsPool, vector.posStart, vector.posUpto);
        reader.writeTo(tvfLocal);
      }

      if (doVectorOffsets) {
        reader.init(threadState.vectorsPool, vector.offsetStart, vector.offsetUpto);
        reader.writeTo(tvfLocal);
      }
    }
  }
}
