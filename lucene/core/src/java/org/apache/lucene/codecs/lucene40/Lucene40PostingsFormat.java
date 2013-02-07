package org.apache.lucene.codecs.lucene40;

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

import org.apache.lucene.codecs.BlockTreeTermsReader;
import org.apache.lucene.codecs.BlockTreeTermsWriter;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.DocsEnum; // javadocs
import org.apache.lucene.index.FieldInfo.IndexOptions; // javadocs
import org.apache.lucene.index.FieldInfos; // javadocs
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput; // javadocs
import org.apache.lucene.util.fst.FST; // javadocs

/** 
 * Lucene 4.0 Postings format.
 * <p>
 * Files:
 * <ul>
 *   <li><tt>.tim</tt>: <a href="#Termdictionary">Term Dictionary</a></li>
 *   <li><tt>.tip</tt>: <a href="#Termindex">Term Index</a></li>
 *   <li><tt>.frq</tt>: <a href="#Frequencies">Frequencies</a></li>
 *   <li><tt>.prx</tt>: <a href="#Positions">Positions</a></li>
 * </ul>
 * </p>
 * <p>
 * <a name="Termdictionary" id="Termdictionary"></a>
 * <h3>Term Dictionary</h3>
 *
 * <p>The .tim file contains the list of terms in each
 * field along with per-term statistics (such as docfreq)
 * and pointers to the frequencies, positions and
 * skip data in the .frq and .prx files.
 * See {@link BlockTreeTermsWriter} for more details on the format.
 * </p>
 *
 * <p>NOTE: The term dictionary can plug into different postings implementations:
 * the postings writer/reader are actually responsible for encoding 
 * and decoding the Postings Metadata and Term Metadata sections described here:</p>
 * <ul>
 *    <li>Postings Metadata --&gt; Header, SkipInterval, MaxSkipLevels, SkipMinimum</li>
 *    <li>Term Metadata --&gt; FreqDelta, SkipDelta?, ProxDelta?
 *    <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *    <li>SkipInterval,MaxSkipLevels,SkipMinimum --&gt; {@link DataOutput#writeInt Uint32}</li>
 *    <li>SkipDelta,FreqDelta,ProxDelta --&gt; {@link DataOutput#writeVLong VLong}</li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 *    <li>Header is a {@link CodecUtil#writeHeader CodecHeader} storing the version information
 *        for the postings.</li>
 *    <li>SkipInterval is the fraction of TermDocs stored in skip tables. It is used to accelerate 
 *        {@link DocsEnum#advance(int)}. Larger values result in smaller indexes, greater 
 *        acceleration, but fewer accelerable cases, while smaller values result in bigger indexes, 
 *        less acceleration (in case of a small value for MaxSkipLevels) and more accelerable cases.
 *        </li>
 *    <li>MaxSkipLevels is the max. number of skip levels stored for each term in the .frq file. A 
 *        low value results in smaller indexes but less acceleration, a larger value results in 
 *        slightly larger indexes but greater acceleration. See format of .frq file for more 
 *        information about skip levels.</li>
 *    <li>SkipMinimum is the minimum document frequency a term must have in order to write any 
 *        skip data at all.</li>
 *    <li>FreqDelta determines the position of this term's TermFreqs within the .frq
 *        file. In particular, it is the difference between the position of this term's
 *        data in that file and the position of the previous term's data (or zero, for
 *        the first term in the block).</li>
 *    <li>ProxDelta determines the position of this term's TermPositions within the
 *        .prx file. In particular, it is the difference between the position of this
 *        term's data in that file and the position of the previous term's data (or zero,
 *        for the first term in the block. For fields that omit position data, this will
 *        be 0 since prox information is not stored.</li>
 *    <li>SkipDelta determines the position of this term's SkipData within the .frq
 *        file. In particular, it is the number of bytes after TermFreqs that the
 *        SkipData starts. In other words, it is the length of the TermFreq data.
 *        SkipDelta is only stored if DocFreq is not smaller than SkipMinimum.</li>
 * </ul>
 * <a name="Termindex" id="Termindex"></a>
 * <h3>Term Index</h3>
 * <p>The .tip file contains an index into the term dictionary, so that it can be 
 * accessed randomly.  See {@link BlockTreeTermsWriter} for more details on the format.</p>
 * <a name="Frequencies" id="Frequencies"></a>
 * <h3>Frequencies</h3>
 * <p>The .frq file contains the lists of documents which contain each term, along
 * with the frequency of the term in that document (except when frequencies are
 * omitted: {@link IndexOptions#DOCS_ONLY}).</p>
 * <ul>
 *   <li>FreqFile (.frq) --&gt; Header, &lt;TermFreqs, SkipData?&gt; <sup>TermCount</sup></li>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <li>TermFreqs --&gt; &lt;TermFreq&gt; <sup>DocFreq</sup></li>
 *   <li>TermFreq --&gt; DocDelta[, Freq?]</li>
 *   <li>SkipData --&gt; &lt;&lt;SkipLevelLength, SkipLevel&gt;
 *       <sup>NumSkipLevels-1</sup>, SkipLevel&gt; &lt;SkipDatum&gt;</li>
 *   <li>SkipLevel --&gt; &lt;SkipDatum&gt; <sup>DocFreq/(SkipInterval^(Level +
 *       1))</sup></li>
 *   <li>SkipDatum --&gt;
 *       DocSkip,PayloadLength?,OffsetLength?,FreqSkip,ProxSkip,SkipChildLevelPointer?</li>
 *   <li>DocDelta,Freq,DocSkip,PayloadLength,OffsetLength,FreqSkip,ProxSkip --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>SkipChildLevelPointer --&gt; {@link DataOutput#writeVLong VLong}</li>
 * </ul>
 * <p>TermFreqs are ordered by term (the term is implicit, from the term dictionary).</p>
 * <p>TermFreq entries are ordered by increasing document number.</p>
 * <p>DocDelta: if frequencies are indexed, this determines both the document
 * number and the frequency. In particular, DocDelta/2 is the difference between
 * this document number and the previous document number (or zero when this is the
 * first document in a TermFreqs). When DocDelta is odd, the frequency is one.
 * When DocDelta is even, the frequency is read as another VInt. If frequencies
 * are omitted, DocDelta contains the gap (not multiplied by 2) between document
 * numbers and no frequency information is stored.</p>
 * <p>For example, the TermFreqs for a term which occurs once in document seven
 * and three times in document eleven, with frequencies indexed, would be the
 * following sequence of VInts:</p>
 * <p>15, 8, 3</p>
 * <p>If frequencies were omitted ({@link IndexOptions#DOCS_ONLY}) it would be this
 * sequence of VInts instead:</p>
 * <p>7,4</p>
 * <p>DocSkip records the document number before every SkipInterval <sup>th</sup>
 * document in TermFreqs. If payloads and offsets are disabled for the term's field, then
 * DocSkip represents the difference from the previous value in the sequence. If
 * payloads and/or offsets are enabled for the term's field, then DocSkip/2 represents the
 * difference from the previous value in the sequence. In this case when
 * DocSkip is odd, then PayloadLength and/or OffsetLength are stored indicating the length of 
 * the last payload/offset before the SkipInterval<sup>th</sup> document in TermPositions.</p>
 * <p>PayloadLength indicates the length of the last payload.</p>
 * <p>OffsetLength indicates the length of the last offset (endOffset-startOffset).</p>
 * <p>
 * FreqSkip and ProxSkip record the position of every SkipInterval <sup>th</sup>
 * entry in FreqFile and ProxFile, respectively. File positions are relative to
 * the start of TermFreqs and Positions, to the previous SkipDatum in the
 * sequence.</p>
 * <p>For example, if DocFreq=35 and SkipInterval=16, then there are two SkipData
 * entries, containing the 15 <sup>th</sup> and 31 <sup>st</sup> document numbers
 * in TermFreqs. The first FreqSkip names the number of bytes after the beginning
 * of TermFreqs that the 16 <sup>th</sup> SkipDatum starts, and the second the
 * number of bytes after that that the 32 <sup>nd</sup> starts. The first ProxSkip
 * names the number of bytes after the beginning of Positions that the 16
 * <sup>th</sup> SkipDatum starts, and the second the number of bytes after that
 * that the 32 <sup>nd</sup> starts.</p>
 * <p>Each term can have multiple skip levels. The amount of skip levels for a
 * term is NumSkipLevels = Min(MaxSkipLevels,
 * floor(log(DocFreq/log(SkipInterval)))). The number of SkipData entries for a
 * skip level is DocFreq/(SkipInterval^(Level + 1)), whereas the lowest skip level
 * is Level=0.<br>
 * Example: SkipInterval = 4, MaxSkipLevels = 2, DocFreq = 35. Then skip level 0
 * has 8 SkipData entries, containing the 3<sup>rd</sup>, 7<sup>th</sup>,
 * 11<sup>th</sup>, 15<sup>th</sup>, 19<sup>th</sup>, 23<sup>rd</sup>,
 * 27<sup>th</sup>, and 31<sup>st</sup> document numbers in TermFreqs. Skip level
 * 1 has 2 SkipData entries, containing the 15<sup>th</sup> and 31<sup>st</sup>
 * document numbers in TermFreqs.<br>
 * The SkipData entries on all upper levels &gt; 0 contain a SkipChildLevelPointer
 * referencing the corresponding SkipData entry in level-1. In the example has
 * entry 15 on level 1 a pointer to entry 15 on level 0 and entry 31 on level 1 a
 * pointer to entry 31 on level 0.
 * </p>
 * <a name="Positions" id="Positions"></a>
 * <h3>Positions</h3>
 * <p>The .prx file contains the lists of positions that each term occurs at
 * within documents. Note that fields omitting positional data do not store
 * anything into this file, and if all fields in the index omit positional data
 * then the .prx file will not exist.</p>
 * <ul>
 *   <li>ProxFile (.prx) --&gt; Header, &lt;TermPositions&gt; <sup>TermCount</sup></li>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <li>TermPositions --&gt; &lt;Positions&gt; <sup>DocFreq</sup></li>
 *   <li>Positions --&gt; &lt;PositionDelta,PayloadLength?,OffsetDelta?,OffsetLength?,PayloadData?&gt; <sup>Freq</sup></li>
 *   <li>PositionDelta,OffsetDelta,OffsetLength,PayloadLength --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>PayloadData --&gt; {@link DataOutput#writeByte byte}<sup>PayloadLength</sup></li>
 * </ul>
 * <p>TermPositions are ordered by term (the term is implicit, from the term dictionary).</p>
 * <p>Positions entries are ordered by increasing document number (the document
 * number is implicit from the .frq file).</p>
 * <p>PositionDelta is, if payloads are disabled for the term's field, the
 * difference between the position of the current occurrence in the document and
 * the previous occurrence (or zero, if this is the first occurrence in this
 * document). If payloads are enabled for the term's field, then PositionDelta/2
 * is the difference between the current and the previous position. If payloads
 * are enabled and PositionDelta is odd, then PayloadLength is stored, indicating
 * the length of the payload at the current term position.</p>
 * <p>For example, the TermPositions for a term which occurs as the fourth term in
 * one document, and as the fifth and ninth term in a subsequent document, would
 * be the following sequence of VInts (payloads disabled):</p>
 * <p>4, 5, 4</p>
 * <p>PayloadData is metadata associated with the current term position. If
 * PayloadLength is stored at the current position, then it indicates the length
 * of this payload. If PayloadLength is not stored, then this payload has the same
 * length as the payload at the previous position.</p>
 * <p>OffsetDelta/2 is the difference between this position's startOffset from the
 * previous occurrence (or zero, if this is the first occurrence in this document).
 * If OffsetDelta is odd, then the length (endOffset-startOffset) differs from the
 * previous occurrence and an OffsetLength follows. Offset data is only written for
 * {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}.</p>
 * 
 *  @deprecated Only for reading old 4.0 segments */

// TODO: this class could be created by wrapping
// BlockTreeTermsDict around Lucene40PostingsBaseFormat; ie
// we should not duplicate the code from that class here:
@Deprecated
public class Lucene40PostingsFormat extends PostingsFormat {

  /** minimum items (terms or sub-blocks) per block for BlockTree */
  protected final int minBlockSize;
  /** maximum items (terms or sub-blocks) per block for BlockTree */
  protected final int maxBlockSize;

  /** Creates {@code Lucene40PostingsFormat} with default
   *  settings. */
  public Lucene40PostingsFormat() {
    this(BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  /** Creates {@code Lucene40PostingsFormat} with custom
   *  values for {@code minBlockSize} and {@code
   *  maxBlockSize} passed to block terms dictionary.
   *  @see BlockTreeTermsWriter#BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int) */
  private Lucene40PostingsFormat(int minBlockSize, int maxBlockSize) {
    super("Lucene40");
    this.minBlockSize = minBlockSize;
    assert minBlockSize > 1;
    this.maxBlockSize = maxBlockSize;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    PostingsReaderBase postings = new Lucene40PostingsReader(state.directory, state.fieldInfos, state.segmentInfo, state.context, state.segmentSuffix);

    boolean success = false;
    try {
      FieldsProducer ret = new BlockTreeTermsReader(
                                                    state.directory,
                                                    state.fieldInfos,
                                                    state.segmentInfo,
                                                    postings,
                                                    state.context,
                                                    state.segmentSuffix,
                                                    state.termsIndexDivisor);
      success = true;
      return ret;
    } finally {
      if (!success) {
        postings.close();
      }
    }
  }

  /** Extension of freq postings file */
  static final String FREQ_EXTENSION = "frq";

  /** Extension of prox postings file */
  static final String PROX_EXTENSION = "prx";

  @Override
  public String toString() {
    return getName() + "(minBlockSize=" + minBlockSize + " maxBlockSize=" + maxBlockSize + ")";
  }
}
