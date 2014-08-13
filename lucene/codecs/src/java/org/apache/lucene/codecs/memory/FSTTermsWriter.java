package org.apache.lucene.codecs.memory;

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
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.CodecUtil;

/**
 * FST-based term dict, using metadata as FST output.
 *
 * The FST directly holds the mapping between &lt;term, metadata&gt;.
 *
 * Term metadata consists of three parts:
 * 1. term statistics: docFreq, totalTermFreq;
 * 2. monotonic long[], e.g. the pointer to the postings list for that term;
 * 3. generic byte[], e.g. other information need by postings reader.
 *
 * <p>
 * File:
 * <ul>
 *   <li><tt>.tst</tt>: <a href="#Termdictionary">Term Dictionary</a></li>
 * </ul>
 * <p>
 *
 * <a name="Termdictionary" id="Termdictionary"></a>
 * <h3>Term Dictionary</h3>
 * <p>
 *  The .tst contains a list of FSTs, one for each field.
 *  The FST maps a term to its corresponding statistics (e.g. docfreq) 
 *  and metadata (e.g. information for postings list reader like file pointer
 *  to postings list).
 * </p>
 * <p>
 *  Typically the metadata is separated into two parts:
 *  <ul>
 *   <li>
 *    Monotonical long array: Some metadata will always be ascending in order
 *    with the corresponding term. This part is used by FST to share outputs between arcs.
 *   </li>
 *   <li>
 *    Generic byte array: Used to store non-monotonic metadata.
 *   </li>
 *  </ul>
 * </p>
 *
 * File format:
 * <ul>
 *  <li>TermsDict(.tst) --&gt; Header, <i>PostingsHeader</i>, FieldSummary, DirOffset</li>
 *  <li>FieldSummary --&gt; NumFields, &lt;FieldNumber, NumTerms, SumTotalTermFreq?, 
 *                                      SumDocFreq, DocCount, LongsSize, TermFST &gt;<sup>NumFields</sup></li>
 *  <li>TermFST --&gt; {@link FST FST&lt;TermData&gt;}</li>
 *  <li>TermData --&gt; Flag, BytesSize?, LongDelta<sup>LongsSize</sup>?, Byte<sup>BytesSize</sup>?, 
 *                      &lt; DocFreq[Same?], (TotalTermFreq-DocFreq) &gt; ? </li>
 *  <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *  <li>DirOffset --&gt; {@link DataOutput#writeLong Uint64}</li>
 *  <li>DocFreq, LongsSize, BytesSize, NumFields,
 *        FieldNumber, DocCount --&gt; {@link DataOutput#writeVInt VInt}</li>
 *  <li>TotalTermFreq, NumTerms, SumTotalTermFreq, SumDocFreq, LongDelta --&gt; 
 *        {@link DataOutput#writeVLong VLong}</li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 *  <li>
 *   The format of PostingsHeader and generic meta bytes are customized by the specific postings implementation:
 *   they contain arbitrary per-file data (such as parameters or versioning information), and per-term data
 *   (non-monotonic ones like pulsed postings data).
 *  </li>
 *  <li>
 *   The format of TermData is determined by FST, typically monotonic metadata will be dense around shallow arcs,
 *   while in deeper arcs only generic bytes and term statistics exist.
 *  </li>
 *  <li>
 *   The byte Flag is used to indicate which part of metadata exists on current arc. Specially the monotonic part
 *   is omitted when it is an array of 0s.
 *  </li>
 *  <li>
 *   Since LongsSize is per-field fixed, it is only written once in field summary.
 *  </li>
 * </ul>
 *
 * @lucene.experimental
 */

public class FSTTermsWriter extends FieldsConsumer {
  static final String TERMS_EXTENSION = "tmp";
  static final String TERMS_CODEC_NAME = "FST_TERMS_DICT";
  public static final int TERMS_VERSION_START = 0;
  public static final int TERMS_VERSION_CHECKSUM = 1;
  public static final int TERMS_VERSION_CURRENT = TERMS_VERSION_CHECKSUM;
  
  final PostingsWriterBase postingsWriter;
  final FieldInfos fieldInfos;
  IndexOutput out;
  final List<FieldMetaData> fields = new ArrayList<>();

  public FSTTermsWriter(SegmentWriteState state, PostingsWriterBase postingsWriter) throws IOException {
    final String termsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TERMS_EXTENSION);

    this.postingsWriter = postingsWriter;
    this.fieldInfos = state.fieldInfos;
    this.out = state.directory.createOutput(termsFileName, state.context);

    boolean success = false;
    try {
      writeHeader(out);
      this.postingsWriter.init(out); 
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }
  private void writeHeader(IndexOutput out) throws IOException {
    CodecUtil.writeHeader(out, TERMS_CODEC_NAME, TERMS_VERSION_CURRENT);   
  }
  private void writeTrailer(IndexOutput out, long dirStart) throws IOException {
    out.writeLong(dirStart);
  }

  @Override
  public TermsConsumer addField(FieldInfo field) throws IOException {
    return new TermsWriter(field);
  }

  @Override
  public void close() throws IOException {
    if (out != null) {
      boolean success = false;
      try {
        // write field summary
        final long dirStart = out.getFilePointer();
        
        out.writeVInt(fields.size());
        for (FieldMetaData field : fields) {
          out.writeVInt(field.fieldInfo.number);
          out.writeVLong(field.numTerms);
          if (field.fieldInfo.getIndexOptions() != IndexOptions.DOCS_ONLY) {
            out.writeVLong(field.sumTotalTermFreq);
          }
          out.writeVLong(field.sumDocFreq);
          out.writeVInt(field.docCount);
          out.writeVInt(field.longsSize);
          field.dict.save(out);
        }
        writeTrailer(out, dirStart);
        CodecUtil.writeFooter(out);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(out, postingsWriter);
        } else {
          IOUtils.closeWhileHandlingException(out, postingsWriter);
        }
        out = null;
      }
    }
  }

  private static class FieldMetaData {
    public final FieldInfo fieldInfo;
    public final long numTerms;
    public final long sumTotalTermFreq;
    public final long sumDocFreq;
    public final int docCount;
    public final int longsSize;
    public final FST<FSTTermOutputs.TermData> dict;

    public FieldMetaData(FieldInfo fieldInfo, long numTerms, long sumTotalTermFreq, long sumDocFreq, int docCount, int longsSize, FST<FSTTermOutputs.TermData> fst) {
      this.fieldInfo = fieldInfo;
      this.numTerms = numTerms;
      this.sumTotalTermFreq = sumTotalTermFreq;
      this.sumDocFreq = sumDocFreq;
      this.docCount = docCount;
      this.longsSize = longsSize;
      this.dict = fst;
    }
  }

  final class TermsWriter extends TermsConsumer {
    private final Builder<FSTTermOutputs.TermData> builder;
    private final FSTTermOutputs outputs;
    private final FieldInfo fieldInfo;
    private final int longsSize;
    private long numTerms;

    private final IntsRefBuilder scratchTerm = new IntsRefBuilder();
    private final RAMOutputStream statsWriter = new RAMOutputStream();
    private final RAMOutputStream metaWriter = new RAMOutputStream();

    TermsWriter(FieldInfo fieldInfo) {
      this.numTerms = 0;
      this.fieldInfo = fieldInfo;
      this.longsSize = postingsWriter.setField(fieldInfo);
      this.outputs = new FSTTermOutputs(fieldInfo, longsSize);
      this.builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    @Override
    public PostingsConsumer startTerm(BytesRef text) throws IOException {
      postingsWriter.startTerm();
      return postingsWriter;
    }

    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {
      // write term meta data into fst
      final BlockTermState state = postingsWriter.newTermState();
      final FSTTermOutputs.TermData meta = new FSTTermOutputs.TermData();
      meta.longs = new long[longsSize];
      meta.bytes = null;
      meta.docFreq = state.docFreq = stats.docFreq;
      meta.totalTermFreq = state.totalTermFreq = stats.totalTermFreq;
      postingsWriter.finishTerm(state);
      postingsWriter.encodeTerm(meta.longs, metaWriter, fieldInfo, state, true);
      final int bytesSize = (int)metaWriter.getFilePointer();
      if (bytesSize > 0) {
        meta.bytes = new byte[bytesSize];
        metaWriter.writeTo(meta.bytes, 0);
        metaWriter.reset();
      }
      builder.add(Util.toIntsRef(text, scratchTerm), meta);
      numTerms++;
    }

    @Override
    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
      // save FST dict
      if (numTerms > 0) {
        final FST<FSTTermOutputs.TermData> fst = builder.finish();
        fields.add(new FieldMetaData(fieldInfo, numTerms, sumTotalTermFreq, sumDocFreq, docCount, longsSize, fst));
      }
    }
  }
}
