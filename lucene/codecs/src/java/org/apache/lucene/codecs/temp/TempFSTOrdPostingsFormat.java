package org.apache.lucene.codecs.temp;


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

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsWriter;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsReader;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.codecs.CodecUtil;  // javadocs
import org.apache.lucene.store.DataOutput;  // javadocs
import org.apache.lucene.util.fst.FST;  // javadocs

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
 * </p>
 *
 * <a name="Termindex" id="Termindex"></a>
 * <h3>Term Index</h3>
 * <p>
 *  The .tix contains a list of FSTs, one for each field.
 *  The FST maps a term to its corresponding order in current field.
 * </p>
 * 
 * <ul>
 *  <li>TermIndex(.tix) --&gt; Header, TermFST<sup>NumFields</sup></li>
 *  <li>TermFST --&gt; {@link FST FST&lt;long&gt;}</li>
 *  <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
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
 * <a name="Termblock" id="Termblock"></a>
 * <h3>Term Block</h3>
 * <p>
 *  The .tbk contains all the statistics and metadata for terms, along with field summary (e.g. 
 *  per-field data like number of documents in current field). For each field, there are four blocks:
 *  <ul>
 *   <li>statistics bytes block: contains term statistics; </li>
 *   <li>metadata longs block: delta-encodes monotonical part of metadata; </li>
 *   <li>metadata bytes block: encodes other parts of metadata; </li>
 *   <li>skip block: contains skip data, to speed up metadata seeking and decoding</li>
 *  </ul>
 * </p>
 *
 * <p>File Format:</p>
 * <ul>
 *  <li>TermBlock(.tbk) --&gt; Header, <i>PostingsHeader</i>, FieldSummary, DirOffset</li>
 *  <li>FieldSummary --&gt; NumFields, &lt;FieldNumber, NumTerms, SumTotalTermFreq?, SumDocFreq,
 *                                         DocCount, LongsSize, DataBlock &gt; <sup>NumFields</sup></li>
 *
 *  <li>DataBlock --&gt; StatsBlockLength, MetaLongsBlockLength, MetaBytesBlockLength, 
 *                       SkipBlock, StatsBlock, MetaLongsBlock, MetaBytesBlock </li>
 *  <li>SkipBlock --&gt; &lt; StatsFPDelta, MetaLongsSkipFPDelta, MetaBytesSkipFPDelta, 
 *                            MetaLongsSkipDelta<sup>LongsSize</sup> &gt;<sup>NumTerms</sup>
 *  <li>StatsBlock --&gt; &lt; DocFreq[Same?], (TotalTermFreq-DocFreq) ? &gt; <sup>NumTerms</sup>
 *  <li>MetaLongsBlock --&gt; &lt; LongDelta<sup>LongsSize</sup>, BytesSize &gt; <sup>NumTerms</sup>
 *  <li>MetaBytesBlock --&gt; Byte <sup>MetaBytesBlockLength</sup>
 *  <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *  <li>DirOffset --&gt; {@link DataOutput#writeLong Uint64}</li>
 *  <li>NumFields, FieldNumber, DocCount, DocFreq, LongsSize, 
 *        FieldNumber, DocCount --&gt; {@link DataOutput#writeVInt VInt}</li>
 *  <li>NumTerms, SumTotalTermFreq, SumDocFreq, StatsBlockLength, MetaLongsBlockLength, MetaBytesBlockLength,
 *        StatsFPDelta, MetaLongsSkipFPDelta, MetaBytesSkipFPDelta, MetaLongsSkipStart, TotalTermFreq, 
 *        LongDelta,--&gt; {@link DataOutput#writeVLong VLong}</li>
 * </ul>
 * <p>Notes: </p>
 * <ul>
 *  <li>
 *   The format of PostingsHeader and MetaBytes are customized by the specific postings implementation:
 *   they contain arbitrary per-file data (such as parameters or versioning information), and per-term data 
 *   (non-monotonical ones like pulsed postings data).
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

public final class TempFSTOrdPostingsFormat extends PostingsFormat {
  public TempFSTOrdPostingsFormat() {
    super("TempFSTOrd");
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new Lucene41PostingsWriter(state);

    boolean success = false;
    try {
      FieldsConsumer ret = new TempFSTOrdTermsWriter(state, postingsWriter);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsWriter);
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    PostingsReaderBase postingsReader = new Lucene41PostingsReader(state.directory,
                                                                state.fieldInfos,
                                                                state.segmentInfo,
                                                                state.context,
                                                                state.segmentSuffix);
    boolean success = false;
    try {
      FieldsProducer ret = new TempFSTOrdTermsReader(state, postingsReader);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsReader);
      }
    }
  }
}
