package org.apache.lucene.index.codecs;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.CodecUtil;

/**
 * Writes terms dict and interacts with docs/positions
 * consumers to write the postings files.
 *
 * The [new] terms dict format is field-centric: each field
 * has its own section in the file.  Fields are written in
 * UTF16 string comparison order.  Within each field, each
 * term's text is written in UTF16 string comparison order.
 * @lucene.experimental
 */

public class PrefixCodedTermsWriter extends FieldsConsumer {

  final static String CODEC_NAME = "STANDARD_TERMS_DICT";

  // Initial format
  public static final int VERSION_START = 0;

  public static final int VERSION_CURRENT = VERSION_START;

  /** Extension of terms file */
  static final String TERMS_EXTENSION = "tis";

  private final DeltaBytesWriter termWriter;

  protected final IndexOutput out;
  final PostingsWriterBase postingsWriter;
  final FieldInfos fieldInfos;
  FieldInfo currentField;
  private final TermsIndexWriterBase termsIndexWriter;
  private final List<TermsWriter> fields = new ArrayList<TermsWriter>();
  private final Comparator<BytesRef> termComp;

  public PrefixCodedTermsWriter(
      TermsIndexWriterBase termsIndexWriter,
      SegmentWriteState state,
      PostingsWriterBase postingsWriter,
      Comparator<BytesRef> termComp) throws IOException
  {
    final String termsFileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, TERMS_EXTENSION);
    this.termsIndexWriter = termsIndexWriter;
    this.termComp = termComp;
    out = state.directory.createOutput(termsFileName);
    termsIndexWriter.setTermsOutput(out);

    fieldInfos = state.fieldInfos;
    writeHeader(out);
    termWriter = new DeltaBytesWriter(out);
    currentField = null;
    this.postingsWriter = postingsWriter;

    postingsWriter.start(out);                          // have consumer write its format/header
  }
  
  protected void writeHeader(IndexOutput out) throws IOException {
    // Count indexed fields up front
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT); 

    out.writeLong(0);                             // leave space for end index pointer    
  }

  @Override
  public TermsConsumer addField(FieldInfo field) throws IOException {
    assert currentField == null || currentField.name.compareTo(field.name) < 0;
    currentField = field;
    TermsIndexWriterBase.FieldWriter fieldIndexWriter = termsIndexWriter.addField(field);
    final TermsWriter terms = new TermsWriter(fieldIndexWriter, field, postingsWriter);
    fields.add(terms);
    return terms;
  }
  
  @Override
  public void close() throws IOException {

    try {
      
      int nonZeroCount = 0;
      for(TermsWriter field : fields) {
        if (field.numTerms > 0) {
          nonZeroCount++;
        }
      }

      final long dirStart = out.getFilePointer();

      out.writeVInt(nonZeroCount);
      for(TermsWriter field : fields) {
        if (field.numTerms > 0) {
          out.writeVInt(field.fieldInfo.number);
          out.writeVLong(field.numTerms);
          out.writeVLong(field.termsStartPointer);
          if (!field.fieldInfo.omitTermFreqAndPositions) {
            out.writeVLong(field.sumTotalTermFreq);
          }
        }
      }
      writeTrailer(dirStart);
    } finally {
      try {
        out.close();
      } finally {
        try {
          postingsWriter.close();
        } finally {
          termsIndexWriter.close();
        }
      }
    }
  }

  protected void writeTrailer(long dirStart) throws IOException {
    // TODO Auto-generated method stub
    out.seek(CodecUtil.headerLength(CODEC_NAME));
    out.writeLong(dirStart);    
  }
  
  class TermsWriter extends TermsConsumer {
    private final FieldInfo fieldInfo;
    private final PostingsWriterBase postingsWriter;
    private final long termsStartPointer;
    private long numTerms;
    private final TermsIndexWriterBase.FieldWriter fieldIndexWriter;
    long sumTotalTermFreq;

    TermsWriter(
        TermsIndexWriterBase.FieldWriter fieldIndexWriter,
        FieldInfo fieldInfo,
        PostingsWriterBase postingsWriter) 
    {
      this.fieldInfo = fieldInfo;
      this.fieldIndexWriter = fieldIndexWriter;

      termWriter.reset();
      termsStartPointer = out.getFilePointer();
      postingsWriter.setField(fieldInfo);
      this.postingsWriter = postingsWriter;
    }
    
    @Override
    public Comparator<BytesRef> getComparator() {
      return termComp;
    }

    @Override
    public PostingsConsumer startTerm(BytesRef text) throws IOException {
      postingsWriter.startTerm();
      return postingsWriter;
    }

    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {

      assert stats.docFreq > 0;
      //System.out.println("finishTerm term=" + fieldInfo.name + ":" + text.utf8ToString() + " fp="  + out.getFilePointer());

      final boolean isIndexTerm = fieldIndexWriter.checkIndexTerm(text, stats);

      termWriter.write(text);
      final int highBit = isIndexTerm ? 0x80 : 0;
      //System.out.println("  isIndex=" + isIndexTerm);

      // This is a vInt, except, we steal top bit to record
      // whether this was an indexed term:
      if ((stats.docFreq & ~0x3F) == 0) {
        // Fast case -- docFreq fits in 6 bits
        out.writeByte((byte) (highBit | stats.docFreq));
      } else {
        // Write bottom 6 bits of docFreq, then write the
        // remainder as vInt:
        out.writeByte((byte) (highBit | 0x40 | (stats.docFreq & 0x3F)));
        out.writeVInt(stats.docFreq >>> 6);
      }
      if (!fieldInfo.omitTermFreqAndPositions) {
        assert stats.totalTermFreq >= stats.docFreq;
        out.writeVLong(stats.totalTermFreq - stats.docFreq);
      }
      postingsWriter.finishTerm(stats, isIndexTerm);
      numTerms++;
    }

    // Finishes all terms in this field
    @Override
    public void finish(long sumTotalTermFreq) throws IOException {
      // EOF marker:
      this.sumTotalTermFreq = sumTotalTermFreq;
      out.writeVInt(DeltaBytesWriter.TERM_EOF);
      fieldIndexWriter.finish();
    }
  }
}
