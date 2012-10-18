package org.apache.lucene.codecs.blockterms;

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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

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

public class BlockTermsWriter extends FieldsConsumer {

  final static String CODEC_NAME = "BLOCK_TERMS_DICT";

  // Initial format
  public static final int VERSION_START = 0;
  public static final int VERSION_APPEND_ONLY = 1;
  public static final int VERSION_CURRENT = VERSION_APPEND_ONLY;

  /** Extension of terms file */
  static final String TERMS_EXTENSION = "tib";

  protected final IndexOutput out;
  final PostingsWriterBase postingsWriter;
  final FieldInfos fieldInfos;
  FieldInfo currentField;
  private final TermsIndexWriterBase termsIndexWriter;

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

  private final List<FieldMetaData> fields = new ArrayList<FieldMetaData>();

  // private final String segment;

  public BlockTermsWriter(TermsIndexWriterBase termsIndexWriter,
      SegmentWriteState state, PostingsWriterBase postingsWriter)
      throws IOException {
    final String termsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TERMS_EXTENSION);
    this.termsIndexWriter = termsIndexWriter;
    out = state.directory.createOutput(termsFileName, state.context);
    boolean success = false;
    try {
      fieldInfos = state.fieldInfos;
      writeHeader(out);
      currentField = null;
      this.postingsWriter = postingsWriter;
      // segment = state.segmentName;
      
      //System.out.println("BTW.init seg=" + state.segmentName);
      
      postingsWriter.start(out); // have consumer write its format/header
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }
  
  private void writeHeader(IndexOutput out) throws IOException {
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);     
  }

  @Override
  public TermsConsumer addField(FieldInfo field) throws IOException {
    //System.out.println("\nBTW.addField seg=" + segment + " field=" + field.name);
    assert currentField == null || currentField.name.compareTo(field.name) < 0;
    currentField = field;
    TermsIndexWriterBase.FieldWriter fieldIndexWriter = termsIndexWriter.addField(field, out.getFilePointer());
    return new TermsWriter(fieldIndexWriter, field, postingsWriter);
  }

  @Override
  public void close() throws IOException {

    try {
      
      final long dirStart = out.getFilePointer();

      out.writeVInt(fields.size());
      for(FieldMetaData field : fields) {
        out.writeVInt(field.fieldInfo.number);
        out.writeVLong(field.numTerms);
        out.writeVLong(field.termsStartPointer);
        if (field.fieldInfo.getIndexOptions() != IndexOptions.DOCS_ONLY) {
          out.writeVLong(field.sumTotalTermFreq);
        }
        out.writeVLong(field.sumDocFreq);
        out.writeVInt(field.docCount);
      }
      writeTrailer(dirStart);
    } finally {
      IOUtils.close(out, postingsWriter, termsIndexWriter);
    }
  }

  private void writeTrailer(long dirStart) throws IOException {
    out.writeLong(dirStart);    
  }
  
  private static class TermEntry {
    public final BytesRef term = new BytesRef();
    public TermStats stats;
  }

  class TermsWriter extends TermsConsumer {
    private final FieldInfo fieldInfo;
    private final PostingsWriterBase postingsWriter;
    private final long termsStartPointer;
    private long numTerms;
    private final TermsIndexWriterBase.FieldWriter fieldIndexWriter;
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
      pendingTerms = new TermEntry[32];
      for(int i=0;i<pendingTerms.length;i++) {
        pendingTerms[i] = new TermEntry();
      }
      termsStartPointer = out.getFilePointer();
      postingsWriter.setField(fieldInfo);
      this.postingsWriter = postingsWriter;
    }
    
    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    @Override
    public PostingsConsumer startTerm(BytesRef text) throws IOException {
      //System.out.println("BTW: startTerm term=" + fieldInfo.name + ":" + text.utf8ToString() + " " + text + " seg=" + segment);
      postingsWriter.startTerm();
      return postingsWriter;
    }

    private final BytesRef lastPrevTerm = new BytesRef();

    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {

      assert stats.docFreq > 0;
      //System.out.println("BTW: finishTerm term=" + fieldInfo.name + ":" + text.utf8ToString() + " " + text + " seg=" + segment + " df=" + stats.docFreq);

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

      if (pendingTerms.length == pendingCount) {
        final TermEntry[] newArray = new TermEntry[ArrayUtil.oversize(pendingCount+1, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(pendingTerms, 0, newArray, 0, pendingCount);
        for(int i=pendingCount;i<newArray.length;i++) {
          newArray[i] = new TermEntry();
        }
        pendingTerms = newArray;
      }
      final TermEntry te = pendingTerms[pendingCount];
      te.term.copyBytes(text);
      te.stats = stats;

      pendingCount++;

      postingsWriter.finishTerm(stats);
      numTerms++;
    }

    // Finishes all terms in this field
    @Override
    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
      if (pendingCount > 0) {
        flushBlock();
      }
      // EOF marker:
      out.writeVInt(0);

      this.sumTotalTermFreq = sumTotalTermFreq;
      this.sumDocFreq = sumDocFreq;
      this.docCount = docCount;
      fieldIndexWriter.finish(out.getFilePointer());
      if (numTerms > 0) {
        fields.add(new FieldMetaData(fieldInfo,
                                     numTerms,
                                     termsStartPointer,
                                     sumTotalTermFreq,
                                     sumDocFreq,
                                     docCount));
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
      int commonPrefix = sharedPrefix(lastPrevTerm, pendingTerms[0].term);
      for(int termCount=1;termCount<pendingCount;termCount++) {
        commonPrefix = Math.min(commonPrefix,
                                sharedPrefix(lastPrevTerm,
                                             pendingTerms[termCount].term));
      }        

      out.writeVInt(pendingCount);
      out.writeVInt(commonPrefix);

      // 2nd pass: write suffixes, as separate byte[] blob
      for(int termCount=0;termCount<pendingCount;termCount++) {
        final int suffix = pendingTerms[termCount].term.length - commonPrefix;
        // TODO: cutover to better intblock codec, instead
        // of interleaving here:
        bytesWriter.writeVInt(suffix);
        bytesWriter.writeBytes(pendingTerms[termCount].term.bytes, commonPrefix, suffix);
      }
      out.writeVInt((int) bytesWriter.getFilePointer());
      bytesWriter.writeTo(out);
      bytesWriter.reset();

      // 3rd pass: write the freqs as byte[] blob
      // TODO: cutover to better intblock codec.  simple64?
      // write prefix, suffix first:
      for(int termCount=0;termCount<pendingCount;termCount++) {
        final TermStats stats = pendingTerms[termCount].stats;
        assert stats != null;
        bytesWriter.writeVInt(stats.docFreq);
        if (fieldInfo.getIndexOptions() != IndexOptions.DOCS_ONLY) {
          bytesWriter.writeVLong(stats.totalTermFreq-stats.docFreq);
        }
      }

      out.writeVInt((int) bytesWriter.getFilePointer());
      bytesWriter.writeTo(out);
      bytesWriter.reset();

      postingsWriter.flushTermsBlock(pendingCount, pendingCount);
      lastPrevTerm.copyBytes(pendingTerms[pendingCount-1].term);
      pendingCount = 0;
    }
  }
}
