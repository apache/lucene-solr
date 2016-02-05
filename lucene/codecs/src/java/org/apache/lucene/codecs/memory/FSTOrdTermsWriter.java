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
package org.apache.lucene.codecs.memory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

/** 
 * FST-based term dict, using ord as FST output.
 *
 * The FST holds the mapping between &lt;term, ord&gt;, and 
 * term's metadata is delta encoded into a single byte block.
 *
 * Typically the byte block consists of four parts:
 * 1. term statistics: docFreq, totalTermFreq;
 * 2. monotonic long[], e.g. the pointer to the postings list for that term;
 * 3. generic byte[], e.g. other information customized by postings base.
 * 4. single-level skip list to speed up metadata decoding by ord.
 *
 * <p>
 * Files:
 * <ul>
 *  <li><tt>.tix</tt>: <a href="#Termindex">Term Index</a></li>
 *  <li><tt>.tbk</tt>: <a href="#Termblock">Term Block</a></li>
 * </ul>
 *
 * <a name="Termindex"></a>
 * <h3>Term Index</h3>
 * <p>
 *  The .tix contains a list of FSTs, one for each field.
 *  The FST maps a term to its corresponding order in current field.
 * </p>
 * 
 * <ul>
 *  <li>TermIndex(.tix) --&gt; Header, TermFST<sup>NumFields</sup>, Footer</li>
 *  <li>TermFST --&gt; {@link FST FST&lt;long&gt;}</li>
 *  <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 *  <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 *
 * <p>Notes:</p>
 * <ul>
 *  <li>
 *  Since terms are already sorted before writing to <a href="#Termblock">Term Block</a>, 
 *  their ords can directly used to seek term metadata from term block.
 *  </li>
 * </ul>
 *
 * <a name="Termblock"></a>
 * <h3>Term Block</h3>
 * <p>
 *  The .tbk contains all the statistics and metadata for terms, along with field summary (e.g. 
 *  per-field data like number of documents in current field). For each field, there are four blocks:
 *  <ul>
 *   <li>statistics bytes block: contains term statistics; </li>
 *   <li>metadata longs block: delta-encodes monotonic part of metadata; </li>
 *   <li>metadata bytes block: encodes other parts of metadata; </li>
 *   <li>skip block: contains skip data, to speed up metadata seeking and decoding</li>
 *  </ul>
 *
 * <p>File Format:</p>
 * <ul>
 *  <li>TermBlock(.tbk) --&gt; Header, <i>PostingsHeader</i>, FieldSummary, DirOffset</li>
 *  <li>FieldSummary --&gt; NumFields, &lt;FieldNumber, NumTerms, SumTotalTermFreq?, SumDocFreq,
 *                                         DocCount, LongsSize, DataBlock &gt; <sup>NumFields</sup>, Footer</li>
 *
 *  <li>DataBlock --&gt; StatsBlockLength, MetaLongsBlockLength, MetaBytesBlockLength, 
 *                       SkipBlock, StatsBlock, MetaLongsBlock, MetaBytesBlock </li>
 *  <li>SkipBlock --&gt; &lt; StatsFPDelta, MetaLongsSkipFPDelta, MetaBytesSkipFPDelta, 
 *                            MetaLongsSkipDelta<sup>LongsSize</sup> &gt;<sup>NumTerms</sup>
 *  <li>StatsBlock --&gt; &lt; DocFreq[Same?], (TotalTermFreq-DocFreq) ? &gt; <sup>NumTerms</sup>
 *  <li>MetaLongsBlock --&gt; &lt; LongDelta<sup>LongsSize</sup>, BytesSize &gt; <sup>NumTerms</sup>
 *  <li>MetaBytesBlock --&gt; Byte <sup>MetaBytesBlockLength</sup>
 *  <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 *  <li>DirOffset --&gt; {@link DataOutput#writeLong Uint64}</li>
 *  <li>NumFields, FieldNumber, DocCount, DocFreq, LongsSize, 
 *        FieldNumber, DocCount --&gt; {@link DataOutput#writeVInt VInt}</li>
 *  <li>NumTerms, SumTotalTermFreq, SumDocFreq, StatsBlockLength, MetaLongsBlockLength, MetaBytesBlockLength,
 *        StatsFPDelta, MetaLongsSkipFPDelta, MetaBytesSkipFPDelta, MetaLongsSkipStart, TotalTermFreq, 
 *        LongDelta,--&gt; {@link DataOutput#writeVLong VLong}</li>
 *  <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes: </p>
 * <ul>
 *  <li>
 *   The format of PostingsHeader and MetaBytes are customized by the specific postings implementation:
 *   they contain arbitrary per-file data (such as parameters or versioning information), and per-term data 
 *   (non-monotonic ones like pulsed postings data).
 *  </li>
 *  <li>
 *   During initialization the reader will load all the blocks into memory. SkipBlock will be decoded, so that during seek
 *   term dict can lookup file pointers directly. StatsFPDelta, MetaLongsSkipFPDelta, etc. are file offset
 *   for every SkipInterval's term. MetaLongsSkipDelta is the difference from previous one, which indicates
 *   the value of preceding metadata longs for every SkipInterval's term.
 *  </li>
 *  <li>
 *   DocFreq is the count of documents which contain the term. TotalTermFreq is the total number of occurrences of the term. 
 *   Usually these two values are the same for long tail terms, therefore one bit is stole from DocFreq to check this case,
 *   so that encoding of TotalTermFreq may be omitted.
 *  </li>
 * </ul>
 *
 * @lucene.experimental 
 */

public class FSTOrdTermsWriter extends FieldsConsumer {
  static final String TERMS_INDEX_EXTENSION = "tix";
  static final String TERMS_BLOCK_EXTENSION = "tbk";
  static final String TERMS_CODEC_NAME = "FSTOrdTerms";
  static final String TERMS_INDEX_CODEC_NAME = "FSTOrdIndex";

  public static final int VERSION_START = 2;
  public static final int VERSION_CURRENT = VERSION_START;
  public static final int SKIP_INTERVAL = 8;
  
  final PostingsWriterBase postingsWriter;
  final FieldInfos fieldInfos;
  final int maxDoc;
  final List<FieldMetaData> fields = new ArrayList<>();
  IndexOutput blockOut = null;
  IndexOutput indexOut = null;

  public FSTOrdTermsWriter(SegmentWriteState state, PostingsWriterBase postingsWriter) throws IOException {
    final String termsIndexFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TERMS_INDEX_EXTENSION);
    final String termsBlockFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TERMS_BLOCK_EXTENSION);

    this.postingsWriter = postingsWriter;
    this.fieldInfos = state.fieldInfos;
    this.maxDoc = state.segmentInfo.maxDoc();

    boolean success = false;
    try {
      this.indexOut = state.directory.createOutput(termsIndexFileName, state.context);
      this.blockOut = state.directory.createOutput(termsBlockFileName, state.context);
      CodecUtil.writeIndexHeader(indexOut, TERMS_INDEX_CODEC_NAME, VERSION_CURRENT, 
                                             state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.writeIndexHeader(blockOut, TERMS_CODEC_NAME, VERSION_CURRENT, 
                                             state.segmentInfo.getId(), state.segmentSuffix);
      this.postingsWriter.init(blockOut, state); 
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(indexOut, blockOut);
      }
    }
  }

  @Override
  public void write(Fields fields) throws IOException {
    for(String field : fields) {
      Terms terms = fields.terms(field);
      if (terms == null) {
        continue;
      }
      FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
      boolean hasFreq = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
      TermsEnum termsEnum = terms.iterator();
      TermsWriter termsWriter = new TermsWriter(fieldInfo);

      long sumTotalTermFreq = 0;
      long sumDocFreq = 0;
      FixedBitSet docsSeen = new FixedBitSet(maxDoc);
      while (true) {
        BytesRef term = termsEnum.next();
        if (term == null) {
          break;
        }
        BlockTermState termState = postingsWriter.writeTerm(term, termsEnum, docsSeen);
        if (termState != null) {
          termsWriter.finishTerm(term, termState);
          sumTotalTermFreq += termState.totalTermFreq;
          sumDocFreq += termState.docFreq;
        }
      }

      termsWriter.finish(hasFreq ? sumTotalTermFreq : -1, sumDocFreq, docsSeen.cardinality());
    }
  }

  @Override
  public void close() throws IOException {
    if (blockOut != null) {
      boolean success = false;
      try {
        final long blockDirStart = blockOut.getFilePointer();
        
        // write field summary
        blockOut.writeVInt(fields.size());
        for (FieldMetaData field : fields) {
          blockOut.writeVInt(field.fieldInfo.number);
          blockOut.writeVLong(field.numTerms);
          if (field.fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
            blockOut.writeVLong(field.sumTotalTermFreq);
          }
          blockOut.writeVLong(field.sumDocFreq);
          blockOut.writeVInt(field.docCount);
          blockOut.writeVInt(field.longsSize);
          blockOut.writeVLong(field.statsOut.getFilePointer());
          blockOut.writeVLong(field.metaLongsOut.getFilePointer());
          blockOut.writeVLong(field.metaBytesOut.getFilePointer());
          
          field.skipOut.writeTo(blockOut);
          field.statsOut.writeTo(blockOut);
          field.metaLongsOut.writeTo(blockOut);
          field.metaBytesOut.writeTo(blockOut);
          field.dict.save(indexOut);
        }
        writeTrailer(blockOut, blockDirStart);
        CodecUtil.writeFooter(indexOut);
        CodecUtil.writeFooter(blockOut);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(blockOut, indexOut, postingsWriter);
        } else {
          IOUtils.closeWhileHandlingException(blockOut, indexOut, postingsWriter);
        }
        blockOut = null;
      }
    }
  }

  private void writeTrailer(IndexOutput out, long dirStart) throws IOException {
    out.writeLong(dirStart);
  }

  private static class FieldMetaData {
    public FieldInfo fieldInfo;
    public long numTerms;
    public long sumTotalTermFreq;
    public long sumDocFreq;
    public int docCount;
    public int longsSize;
    public FST<Long> dict;

    // TODO: block encode each part 

    // vint encode next skip point (fully decoded when reading)
    public RAMOutputStream skipOut;
    // vint encode df, (ttf-df)
    public RAMOutputStream statsOut;
    // vint encode monotonic long[] and length for corresponding byte[]
    public RAMOutputStream metaLongsOut;
    // generic byte[]
    public RAMOutputStream metaBytesOut;
  }

  final class TermsWriter {
    private final Builder<Long> builder;
    private final PositiveIntOutputs outputs;
    private final FieldInfo fieldInfo;
    private final int longsSize;
    private long numTerms;

    private final IntsRefBuilder scratchTerm = new IntsRefBuilder();
    private final RAMOutputStream statsOut = new RAMOutputStream();
    private final RAMOutputStream metaLongsOut = new RAMOutputStream();
    private final RAMOutputStream metaBytesOut = new RAMOutputStream();

    private final RAMOutputStream skipOut = new RAMOutputStream();
    private long lastBlockStatsFP;
    private long lastBlockMetaLongsFP;
    private long lastBlockMetaBytesFP;
    private long[] lastBlockLongs;

    private long[] lastLongs;
    private long lastMetaBytesFP;

    TermsWriter(FieldInfo fieldInfo) {
      this.numTerms = 0;
      this.fieldInfo = fieldInfo;
      this.longsSize = postingsWriter.setField(fieldInfo);
      this.outputs = PositiveIntOutputs.getSingleton();
      this.builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);

      this.lastBlockStatsFP = 0;
      this.lastBlockMetaLongsFP = 0;
      this.lastBlockMetaBytesFP = 0;
      this.lastBlockLongs = new long[longsSize];

      this.lastLongs = new long[longsSize];
      this.lastMetaBytesFP = 0;
    }

    public void finishTerm(BytesRef text, BlockTermState state) throws IOException {
      if (numTerms > 0 && numTerms % SKIP_INTERVAL == 0) {
        bufferSkip();
      }
      // write term meta data into fst
      final long longs[] = new long[longsSize];
      final long delta = state.totalTermFreq - state.docFreq;
      if (state.totalTermFreq > 0) {
        if (delta == 0) {
          statsOut.writeVInt(state.docFreq<<1|1);
        } else {
          statsOut.writeVInt(state.docFreq<<1);
          statsOut.writeVLong(state.totalTermFreq-state.docFreq);
        }
      } else {
        statsOut.writeVInt(state.docFreq);
      }
      postingsWriter.encodeTerm(longs, metaBytesOut, fieldInfo, state, true);
      for (int i = 0; i < longsSize; i++) {
        metaLongsOut.writeVLong(longs[i] - lastLongs[i]);
        lastLongs[i] = longs[i];
      }
      metaLongsOut.writeVLong(metaBytesOut.getFilePointer() - lastMetaBytesFP);

      builder.add(Util.toIntsRef(text, scratchTerm), numTerms);
      numTerms++;

      lastMetaBytesFP = metaBytesOut.getFilePointer();
    }

    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
      if (numTerms > 0) {
        final FieldMetaData metadata = new FieldMetaData();
        metadata.fieldInfo = fieldInfo;
        metadata.numTerms = numTerms;
        metadata.sumTotalTermFreq = sumTotalTermFreq;
        metadata.sumDocFreq = sumDocFreq;
        metadata.docCount = docCount;
        metadata.longsSize = longsSize;
        metadata.skipOut = skipOut;
        metadata.statsOut = statsOut;
        metadata.metaLongsOut = metaLongsOut;
        metadata.metaBytesOut = metaBytesOut;
        metadata.dict = builder.finish();
        fields.add(metadata);
      }
    }

    private void bufferSkip() throws IOException {
      skipOut.writeVLong(statsOut.getFilePointer() - lastBlockStatsFP);
      skipOut.writeVLong(metaLongsOut.getFilePointer() - lastBlockMetaLongsFP);
      skipOut.writeVLong(metaBytesOut.getFilePointer() - lastBlockMetaBytesFP);
      for (int i = 0; i < longsSize; i++) {
        skipOut.writeVLong(lastLongs[i] - lastBlockLongs[i]);
      }
      lastBlockStatsFP = statsOut.getFilePointer();
      lastBlockMetaLongsFP = metaLongsOut.getFilePointer();
      lastBlockMetaBytesFP = metaBytesOut.getFilePointer();
      System.arraycopy(lastLongs, 0, lastBlockLongs, 0, longsSize);
    }
  }
}
