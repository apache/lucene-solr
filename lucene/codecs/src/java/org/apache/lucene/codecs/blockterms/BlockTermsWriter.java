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


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;

// TODO: currently we encode all terms between two indexed
// terms as a block; but, we could decouple the two, ie
// allow several blocks in between two indexed terms

/**
 * Writes terms dict, block-encoding (column stride) each
 * term's metadata for each set of terms between two
 * index terms.
 *
 * @lucene.experimental
 */

public class BlockTermsWriter extends FieldsConsumer implements Closeable {

  final static String CODEC_NAME = "BlockTermsWriter";

  // Initial format
  public static final int VERSION_START = 4;
  public static final int VERSION_CURRENT = VERSION_START;

  /** Extension of terms file */
  static final String TERMS_EXTENSION = "tib";

  protected IndexOutput out;
  final PostingsWriterBase postingsWriter;
  final FieldInfos fieldInfos;
  FieldInfo currentField;
  private final TermsIndexWriterBase termsIndexWriter;
  private final int maxDoc;

  private static class FieldMetaData {
    public final FieldInfo fieldInfo;
    public final long numTerms;
    public final long termsStartPointer;
    public final long sumTotalTermFreq;
    public final long sumDocFreq;
    public final int docCount;

    public FieldMetaData(FieldInfo fieldInfo, long numTerms, long termsStartPointer, long sumTotalTermFreq, long sumDocFreq, int docCount) {
      assert numTerms > 0;
      this.fieldInfo = fieldInfo;
      this.termsStartPointer = termsStartPointer;
      this.numTerms = numTerms;
      this.sumTotalTermFreq = sumTotalTermFreq;
      this.sumDocFreq = sumDocFreq;
      this.docCount = docCount;
    }
  }

  private final List<FieldMetaData> fields = new ArrayList<>();

  // private final String segment;

  public BlockTermsWriter(TermsIndexWriterBase termsIndexWriter,
      SegmentWriteState state, PostingsWriterBase postingsWriter)
      throws IOException {
    final String termsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TERMS_EXTENSION);
    this.termsIndexWriter = termsIndexWriter;
    maxDoc = state.segmentInfo.maxDoc();
    out = state.directory.createOutput(termsFileName, state.context);
    boolean success = false;
    try {
      fieldInfos = state.fieldInfos;
      CodecUtil.writeIndexHeader(out, CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      currentField = null;
      this.postingsWriter = postingsWriter;
      // segment = state.segmentName;
      
      //System.out.println("BTW.init seg=" + state.segmentName);
      
      postingsWriter.init(out, state); // have consumer write its format/header
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }

  @Override
  public void write(Fields fields, NormsProducer norms) throws IOException {

    for(String field : fields) {

      Terms terms = fields.terms(field);
      if (terms == null) {
        continue;
      }

      TermsEnum termsEnum = terms.iterator();

      TermsWriter termsWriter = addField(fieldInfos.fieldInfo(field));

      while (true) {
        BytesRef term = termsEnum.next();
        if (term == null) {
          break;
        }

        termsWriter.write(term, termsEnum, norms);
      }

      termsWriter.finish();
    }
  }

  private TermsWriter addField(FieldInfo field) throws IOException {
    //System.out.println("\nBTW.addField seg=" + segment + " field=" + field.name);
    assert currentField == null || currentField.name.compareTo(field.name) < 0;
    currentField = field;
    TermsIndexWriterBase.FieldWriter fieldIndexWriter = termsIndexWriter.addField(field, out.getFilePointer());
    return new TermsWriter(fieldIndexWriter, field, postingsWriter);
  }

  @Override
  public void close() throws IOException {
    if (out != null) {
      try {
        final long dirStart = out.getFilePointer();
        
        out.writeVInt(fields.size());
        for(FieldMetaData field : fields) {
          out.writeVInt(field.fieldInfo.number);
          out.writeVLong(field.numTerms);
          out.writeVLong(field.termsStartPointer);
          if (field.fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
            out.writeVLong(field.sumTotalTermFreq);
          }
          out.writeVLong(field.sumDocFreq);
          out.writeVInt(field.docCount);
        }
        writeTrailer(dirStart);
        CodecUtil.writeFooter(out);
      } finally {
        IOUtils.close(out, postingsWriter, termsIndexWriter);
        out = null;
      }
    }
  }

  private void writeTrailer(long dirStart) throws IOException {
    out.writeLong(dirStart);    
  }
  
  private static class TermEntry {
    public final BytesRefBuilder term = new BytesRefBuilder();
    public BlockTermState state;
  }

  class TermsWriter {
    private final FieldInfo fieldInfo;
    private final PostingsWriterBase postingsWriter;
    private final long termsStartPointer;
    private long numTerms;
    private final TermsIndexWriterBase.FieldWriter fieldIndexWriter;
    private final FixedBitSet docsSeen;
    long sumTotalTermFreq;
    long sumDocFreq;
    int docCount;

    private TermEntry[] pendingTerms;

    private int pendingCount;

    TermsWriter(
        TermsIndexWriterBase.FieldWriter fieldIndexWriter,
        FieldInfo fieldInfo,
        PostingsWriterBase postingsWriter) 
    {
      this.fieldInfo = fieldInfo;
      this.fieldIndexWriter = fieldIndexWriter;
      this.docsSeen = new FixedBitSet(maxDoc);
      pendingTerms = new TermEntry[32];
      for(int i=0;i<pendingTerms.length;i++) {
        pendingTerms[i] = new TermEntry();
      }
      termsStartPointer = out.getFilePointer();
      this.postingsWriter = postingsWriter;
      postingsWriter.setField(fieldInfo);
    }
    
    private final BytesRefBuilder lastPrevTerm = new BytesRefBuilder();

    void write(BytesRef text, TermsEnum termsEnum, NormsProducer norms) throws IOException {

      BlockTermState state = postingsWriter.writeTerm(text, termsEnum, docsSeen, norms);
      if (state == null) {
        // No docs for this term:
        return;
      }
      sumDocFreq += state.docFreq;
      sumTotalTermFreq += state.totalTermFreq;

      assert state.docFreq > 0;
      //System.out.println("BTW: finishTerm term=" + fieldInfo.name + ":" + text.utf8ToString() + " " + text + " seg=" + segment + " df=" + stats.docFreq);

      TermStats stats = new TermStats(state.docFreq, state.totalTermFreq);
      final boolean isIndexTerm = fieldIndexWriter.checkIndexTerm(text, stats);

      if (isIndexTerm) {
        if (pendingCount > 0) {
          // Instead of writing each term, live, we gather terms
          // in RAM in a pending buffer, and then write the
          // entire block in between index terms:
          flushBlock();
        }
        fieldIndexWriter.add(text, stats, out.getFilePointer());
        //System.out.println("  index term!");
      }

      pendingTerms = ArrayUtil.grow(pendingTerms, pendingCount + 1);
      for (int i = pendingCount; i < pendingTerms.length; i++) {
        pendingTerms[i] = new TermEntry();
      }
      final TermEntry te = pendingTerms[pendingCount];
      te.term.copyBytes(text);
      te.state = state;

      pendingCount++;
      numTerms++;
    }

    // Finishes all terms in this field
    void finish() throws IOException {
      if (pendingCount > 0) {
        flushBlock();
      }
      // EOF marker:
      out.writeVInt(0);

      fieldIndexWriter.finish(out.getFilePointer());
      if (numTerms > 0) {
        fields.add(new FieldMetaData(fieldInfo,
                                     numTerms,
                                     termsStartPointer,
                                     fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0 ? sumTotalTermFreq : -1,
                                     sumDocFreq,
                                     docsSeen.cardinality()));
      }
    }

    private int sharedPrefix(BytesRef term1, BytesRef term2) {
      assert term1.offset == 0;
      assert term2.offset == 0;
      int pos1 = 0;
      int pos1End = pos1 + Math.min(term1.length, term2.length);
      int pos2 = 0;
      while(pos1 < pos1End) {
        if (term1.bytes[pos1] != term2.bytes[pos2]) {
          return pos1;
        }
        pos1++;
        pos2++;
      }
      return pos1;
    }

    private final RAMOutputStream bytesWriter = new RAMOutputStream();

    private void flushBlock() throws IOException {
      //System.out.println("BTW.flushBlock seg=" + segment + " pendingCount=" + pendingCount + " fp=" + out.getFilePointer());

      // First pass: compute common prefix for all terms
      // in the block, against term before first term in
      // this block:
      int commonPrefix = sharedPrefix(lastPrevTerm.get(), pendingTerms[0].term.get());
      for(int termCount=1;termCount<pendingCount;termCount++) {
        commonPrefix = Math.min(commonPrefix,
                                sharedPrefix(lastPrevTerm.get(),
                                             pendingTerms[termCount].term.get()));
      }        

      out.writeVInt(pendingCount);
      out.writeVInt(commonPrefix);

      // 2nd pass: write suffixes, as separate byte[] blob
      for(int termCount=0;termCount<pendingCount;termCount++) {
        final int suffix = pendingTerms[termCount].term.length() - commonPrefix;
        // TODO: cutover to better intblock codec, instead
        // of interleaving here:
        bytesWriter.writeVInt(suffix);
        bytesWriter.writeBytes(pendingTerms[termCount].term.bytes(), commonPrefix, suffix);
      }
      out.writeVInt((int) bytesWriter.getFilePointer());
      bytesWriter.writeTo(out);
      bytesWriter.reset();

      // 3rd pass: write the freqs as byte[] blob
      // TODO: cutover to better intblock codec.  simple64?
      // write prefix, suffix first:
      for(int termCount=0;termCount<pendingCount;termCount++) {
        final BlockTermState state = pendingTerms[termCount].state;
        assert state != null;
        bytesWriter.writeVInt(state.docFreq);
        if (fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
          bytesWriter.writeVLong(state.totalTermFreq-state.docFreq);
        }
      }
      out.writeVInt((int) bytesWriter.getFilePointer());
      bytesWriter.writeTo(out);
      bytesWriter.reset();

      // 4th pass: write the metadata 
      boolean absolute = true;
      for(int termCount=0;termCount<pendingCount;termCount++) {
        final BlockTermState state = pendingTerms[termCount].state;
        postingsWriter.encodeTerm(bytesWriter, fieldInfo, state, absolute);
        absolute = false;
      }
      out.writeVInt((int) bytesWriter.getFilePointer());
      bytesWriter.writeTo(out);
      bytesWriter.reset();

      lastPrevTerm.copyBytes(pendingTerms[pendingCount-1].term);
      pendingCount = 0;
    }
  }
}
