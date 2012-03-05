package org.apache.lucene.codecs;

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
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

/**
 * Selects every Nth term as and index term, and hold term
 * bytes fully expanded in memory.  This terms index
 * supports seeking by ord.  See {@link
 * VariableGapTermsIndexWriter} for a more memory efficient
 * terms index that does not support seeking by ord.
 *
 * @lucene.experimental */
public class FixedGapTermsIndexWriter extends TermsIndexWriterBase {
  protected final IndexOutput out;

  /** Extension of terms index file */
  static final String TERMS_INDEX_EXTENSION = "tii";

  final static String CODEC_NAME = "SIMPLE_STANDARD_TERMS_INDEX";
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  final private int termIndexInterval;

  private final List<SimpleFieldWriter> fields = new ArrayList<SimpleFieldWriter>();
  
  @SuppressWarnings("unused") private final FieldInfos fieldInfos; // unread

  public FixedGapTermsIndexWriter(SegmentWriteState state) throws IOException {
    final String indexFileName = IndexFileNames.segmentFileName(state.segmentName, state.segmentSuffix, TERMS_INDEX_EXTENSION);
    termIndexInterval = state.termIndexInterval;
    out = state.directory.createOutput(indexFileName, state.context);
    boolean success = false;
    try {
      fieldInfos = state.fieldInfos;
      writeHeader(out);
      out.writeInt(termIndexInterval);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }
  
  protected void writeHeader(IndexOutput out) throws IOException {
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
    // Placeholder for dir offset
    out.writeLong(0);
  }

  @Override
  public FieldWriter addField(FieldInfo field, long termsFilePointer) {
    //System.out.println("FGW: addFfield=" + field.name);
    SimpleFieldWriter writer = new SimpleFieldWriter(field, termsFilePointer);
    fields.add(writer);
    return writer;
  }

  /** NOTE: if your codec does not sort in unicode code
   *  point order, you must override this method, to simply
   *  return indexedTerm.length. */
  protected int indexedTermPrefixLength(final BytesRef priorTerm, final BytesRef indexedTerm) {
    // As long as codec sorts terms in unicode codepoint
    // order, we can safely strip off the non-distinguishing
    // suffix to save RAM in the loaded terms index.
    final int idxTermOffset = indexedTerm.offset;
    final int priorTermOffset = priorTerm.offset;
    final int limit = Math.min(priorTerm.length, indexedTerm.length);
    for(int byteIdx=0;byteIdx<limit;byteIdx++) {
      if (priorTerm.bytes[priorTermOffset+byteIdx] != indexedTerm.bytes[idxTermOffset+byteIdx]) {
        return byteIdx+1;
      }
    }
    return Math.min(1+priorTerm.length, indexedTerm.length);
  }

  private class SimpleFieldWriter extends FieldWriter {
    final FieldInfo fieldInfo;
    int numIndexTerms;
    final long indexStart;
    final long termsStart;
    long packedIndexStart;
    long packedOffsetsStart;
    private long numTerms;

    // TODO: we could conceivably make a PackedInts wrapper
    // that auto-grows... then we wouldn't force 6 bytes RAM
    // per index term:
    private short[] termLengths;
    private int[] termsPointerDeltas;
    private long lastTermsPointer;
    private long totTermLength;

    private final BytesRef lastTerm = new BytesRef();

    SimpleFieldWriter(FieldInfo fieldInfo, long termsFilePointer) {
      this.fieldInfo = fieldInfo;
      indexStart = out.getFilePointer();
      termsStart = lastTermsPointer = termsFilePointer;
      termLengths = new short[0];
      termsPointerDeltas = new int[0];
    }

    @Override
    public boolean checkIndexTerm(BytesRef text, TermStats stats) throws IOException {
      // First term is first indexed term:
      //System.out.println("FGW: checkIndexTerm text=" + text.utf8ToString());
      if (0 == (numTerms++ % termIndexInterval)) {
        return true;
      } else {
        if (0 == numTerms % termIndexInterval) {
          // save last term just before next index term so we
          // can compute wasted suffix
          lastTerm.copyBytes(text);
        }
        return false;
      }
    }

    @Override
    public void add(BytesRef text, TermStats stats, long termsFilePointer) throws IOException {
      final int indexedTermLength = indexedTermPrefixLength(lastTerm, text);
      //System.out.println("FGW: add text=" + text.utf8ToString() + " " + text + " fp=" + termsFilePointer);

      // write only the min prefix that shows the diff
      // against prior term
      out.writeBytes(text.bytes, text.offset, indexedTermLength);

      if (termLengths.length == numIndexTerms) {
        termLengths = ArrayUtil.grow(termLengths);
      }
      if (termsPointerDeltas.length == numIndexTerms) {
        termsPointerDeltas = ArrayUtil.grow(termsPointerDeltas);
      }

      // save delta terms pointer
      termsPointerDeltas[numIndexTerms] = (int) (termsFilePointer - lastTermsPointer);
      lastTermsPointer = termsFilePointer;

      // save term length (in bytes)
      assert indexedTermLength <= Short.MAX_VALUE;
      termLengths[numIndexTerms] = (short) indexedTermLength;
      totTermLength += indexedTermLength;

      lastTerm.copyBytes(text);
      numIndexTerms++;
    }

    @Override
    public void finish(long termsFilePointer) throws IOException {

      // write primary terms dict offsets
      packedIndexStart = out.getFilePointer();

      PackedInts.Writer w = PackedInts.getWriter(out, numIndexTerms, PackedInts.bitsRequired(termsFilePointer));

      // relative to our indexStart
      long upto = 0;
      for(int i=0;i<numIndexTerms;i++) {
        upto += termsPointerDeltas[i];
        w.add(upto);
      }
      w.finish();

      packedOffsetsStart = out.getFilePointer();

      // write offsets into the byte[] terms
      w = PackedInts.getWriter(out, 1+numIndexTerms, PackedInts.bitsRequired(totTermLength));
      upto = 0;
      for(int i=0;i<numIndexTerms;i++) {
        w.add(upto);
        upto += termLengths[i];
      }
      w.add(upto);
      w.finish();

      // our referrer holds onto us, while other fields are
      // being written, so don't tie up this RAM:
      termLengths = null;
      termsPointerDeltas = null;
    }
  }

  public void close() throws IOException {
    boolean success = false;
    try {
      final long dirStart = out.getFilePointer();
      final int fieldCount = fields.size();
      
      int nonNullFieldCount = 0;
      for(int i=0;i<fieldCount;i++) {
        SimpleFieldWriter field = fields.get(i);
        if (field.numIndexTerms > 0) {
          nonNullFieldCount++;
        }
      }
      
      out.writeVInt(nonNullFieldCount);
      for(int i=0;i<fieldCount;i++) {
        SimpleFieldWriter field = fields.get(i);
        if (field.numIndexTerms > 0) {
          out.writeVInt(field.fieldInfo.number);
          out.writeVInt(field.numIndexTerms);
          out.writeVLong(field.termsStart);
          out.writeVLong(field.indexStart);
          out.writeVLong(field.packedIndexStart);
          out.writeVLong(field.packedOffsetsStart);
        }
      }
      writeTrailer(dirStart);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(out);
      } else {
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }

  protected void writeTrailer(long dirStart) throws IOException {
    out.seek(CodecUtil.headerLength(CODEC_NAME));
    out.writeLong(dirStart);
  }
}
