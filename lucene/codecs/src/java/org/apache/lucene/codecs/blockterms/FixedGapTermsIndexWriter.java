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
package org.apache.lucene.codecs.blockterms;


import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

/**
 * Selects every Nth term as and index term, and hold term
 * bytes (mostly) fully expanded in memory.  This terms index
 * supports seeking by ord.  See {@link
 * VariableGapTermsIndexWriter} for a more memory efficient
 * terms index that does not support seeking by ord.
 *
 * @lucene.experimental */
public class FixedGapTermsIndexWriter extends TermsIndexWriterBase {
  protected IndexOutput out;

  /** Extension of terms index file */
  static final String TERMS_INDEX_EXTENSION = "tii";

  final static String CODEC_NAME = "FixedGapTermsIndex";
  final static int VERSION_START = 4;
  final static int VERSION_CURRENT = VERSION_START;

  final static int BLOCKSIZE = 4096;
  final private int termIndexInterval;
  public static final int DEFAULT_TERM_INDEX_INTERVAL = 32;

  private final List<SimpleFieldWriter> fields = new ArrayList<>();
  
  public FixedGapTermsIndexWriter(SegmentWriteState state) throws IOException {
    this(state, DEFAULT_TERM_INDEX_INTERVAL);
  }
  
  public FixedGapTermsIndexWriter(SegmentWriteState state, int termIndexInterval) throws IOException {
    if (termIndexInterval <= 0) {
      throw new IllegalArgumentException("invalid termIndexInterval: " + termIndexInterval);
    }
    this.termIndexInterval = termIndexInterval;
    final String indexFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TERMS_INDEX_EXTENSION);
    out = state.directory.createOutput(indexFileName, state.context);
    boolean success = false;
    try {
      CodecUtil.writeIndexHeader(out, CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      out.writeVInt(termIndexInterval);
      out.writeVInt(PackedInts.VERSION_CURRENT);
      out.writeVInt(BLOCKSIZE);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out);
      }
    }
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
    return StringHelper.sortKeyLength(priorTerm, indexedTerm);
  }

  private class SimpleFieldWriter extends FieldWriter {
    final FieldInfo fieldInfo;
    int numIndexTerms;
    final long indexStart;
    final long termsStart;
    long packedIndexStart;
    long packedOffsetsStart;
    private long numTerms;

    private RAMOutputStream offsetsBuffer = new RAMOutputStream();
    private MonotonicBlockPackedWriter termOffsets = new MonotonicBlockPackedWriter(offsetsBuffer, BLOCKSIZE);
    private long currentOffset;

    private RAMOutputStream addressBuffer = new RAMOutputStream();
    private MonotonicBlockPackedWriter termAddresses = new MonotonicBlockPackedWriter(addressBuffer, BLOCKSIZE);

    private final BytesRefBuilder lastTerm = new BytesRefBuilder();

    SimpleFieldWriter(FieldInfo fieldInfo, long termsFilePointer) {
      this.fieldInfo = fieldInfo;
      indexStart = out.getFilePointer();
      termsStart = termsFilePointer;
      // we write terms+1 offsets, term n's length is n+1 - n
      try {
        termOffsets.add(0L);
      } catch (IOException bogus) {
        throw new RuntimeException(bogus);
      }
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
      final int indexedTermLength;
      if (numIndexTerms == 0) {
        // no previous term: no bytes to write
        indexedTermLength = 0;
      } else {
        indexedTermLength = indexedTermPrefixLength(lastTerm.get(), text);
      }
      //System.out.println("FGW: add text=" + text.utf8ToString() + " " + text + " fp=" + termsFilePointer);

      // write only the min prefix that shows the diff
      // against prior term
      out.writeBytes(text.bytes, text.offset, indexedTermLength);

      // save delta terms pointer
      termAddresses.add(termsFilePointer - termsStart);

      // save term length (in bytes)
      assert indexedTermLength <= Short.MAX_VALUE;
      currentOffset += indexedTermLength;
      termOffsets.add(currentOffset);

      lastTerm.copyBytes(text);
      numIndexTerms++;
    }

    @Override
    public void finish(long termsFilePointer) throws IOException {

      // write primary terms dict offsets
      packedIndexStart = out.getFilePointer();

      // relative to our indexStart
      termAddresses.finish();
      addressBuffer.writeTo(out);

      packedOffsetsStart = out.getFilePointer();

      // write offsets into the byte[] terms
      termOffsets.finish();
      offsetsBuffer.writeTo(out);

      // our referrer holds onto us, while other fields are
      // being written, so don't tie up this RAM:
      termOffsets = termAddresses = null;
      addressBuffer = offsetsBuffer = null;
    }
  }

  @Override
  public void close() throws IOException {
    if (out != null) {
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
        CodecUtil.writeFooter(out);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(out);
        } else {
          IOUtils.closeWhileHandlingException(out);
        }
        out = null;
      }
    }
  }

  private void writeTrailer(long dirStart) throws IOException {
    out.writeLong(dirStart);
  }
}
