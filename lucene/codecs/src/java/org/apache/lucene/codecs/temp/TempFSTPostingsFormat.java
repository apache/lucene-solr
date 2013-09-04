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
 *    Generic byte array: Used to store non-monotonical metadata.
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
 *   (non-monotonical ones like pulsed postings data).
 *  </li>
 *  <li>
 *   The format of TermData is determined by FST, typically monotonical metadata will be dense around shallow arcs,
 *   while in deeper arcs only generic bytes and term statistics exist.
 *  </li>
 *  <li>
 *   The byte Flag is used to indicate which part of metadata exists on current arc. Specially the monotonical part
 *   is omitted when it is an array of 0s.
 *  </li>
 *  <li>
 *   Since LongsSize is per-field fixed, it is only written once in field summary.
 *  </li>
 * </ul>
 *
 * @lucene.experimental
 */

public final class TempFSTPostingsFormat extends PostingsFormat {
  public TempFSTPostingsFormat() {
    super("TempFST");
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
      FieldsConsumer ret = new TempFSTTermsWriter(state, postingsWriter);
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
      FieldsProducer ret = new TempFSTTermsReader(state, postingsReader);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsReader);
      }
    }
  }
}
