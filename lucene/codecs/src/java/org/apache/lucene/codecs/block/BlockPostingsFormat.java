package org.apache.lucene.codecs.block;


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
import org.apache.lucene.codecs.MultiLevelSkipListWriter;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Block postings format, which encodes postings in packed int blocks 
 * for faster decode.
 *
 * <p><b>NOTE</b>: this format is still experimental and
 * subject to change without backwards compatibility.
 *
 * <p>
 * Basic idea:
 * <ul>
 *   <li>
 *   <b>Packed Block and VInt Block</b>: 
 *   <p>In packed block, integers are encoded with the same bit width ({@link PackedInts packed format}), 
 *      the block size (i.e. number of integers inside block) is fixed. </p>
 *   <p>In VInt block, integers are encoded as {@link DataOutput#writeVInt VInt}, 
 *      the block size is variable.</p>
 *   </li>
 *
 *   <li> 
 *   <b>Block structure</b>: 
 *   <p>When the postings is long enough, BlockPostingsFormat will try to encode most integer data 
 *      as packed block.</p> 
 *   <p>Take a term with 259 documents as example, the first 256 document ids are encoded as two packed 
 *      blocks, while the remaining 3 as one VInt block. </p>
 *   <p>Different kinds of data are always encoded separately into different packed blocks, but may 
 *      possible be encoded into a same VInt block. </p>
 *   <p>This strategy is applied to pairs: 
 *      &lt;document number, frequency&gt;,
 *      &lt;position, payload length&gt;, 
 *      &lt;position, offset start, offset length&gt;, and
 *      &lt;position, payload length, offsetstart, offset length&gt;.</p>
 *   </li>
 *
 *   <li>
 *   <b>Skipper setting</b>: 
 *   <p>The structure of skip table is quite similar to Lucene40PostingsFormat. Skip interval is the 
 *      same as block size, and each skip entry points to the beginning of each block. However, for 
 *      the first block, skip data is omitted.</p>
 *   </li>
 *
 *   <li>
 *   <b>Positions, Payloads, and Offsets</b>: 
 *   <p>A position is an integer indicating where the term occurs at within one document. 
 *      A payload is a blob of metadata associated with current position. 
 *      An offset is a pair of integers indicating the tokenized start/end offsets for given term 
 *      in current position. </p>
 *   <p>When payloads and offsets are not omitted, numPositions==numPayloads==numOffsets (assuming a 
 *      null payload contributes one count). As mentioned in block structure, it is possible to encode 
 *      these three either combined or separately. 
 *   <p>For all the cases, payloads and offsets are stored together. When encoded as packed block, 
 *      position data is separated out as .pos, while payloads and offsets are encoded in .pay (payload 
 *      metadata will also be stored directly in .pay). When encoded as VInt block, all these three are 
 *      stored in .pos (so as payload metadata).</p>
 *   <p>With this strategy, the majority of payload and offset data will be outside .pos file. 
 *      So for queries that require only position data, running on a full index with payloads and offsets, 
 *      this reduces disk pre-fetches.</p>
 *   </li>
 * </ul>
 * </p>
 *
 * <p>
 * Files and detailed format:
 * <ul>
 *   <li><tt>.tim</tt>: <a href="#Termdictionary">Term Dictionary</a></li>
 *   <li><tt>.tip</tt>: <a href="#Termindex">Term Index</a></li>
 *   <li><tt>.doc</tt>: <a href="#Frequencies">Frequencies and Skip Data</a></li>
 *   <li><tt>.pos</tt>: <a href="#Positions">Positions</a></li>
 *   <li><tt>.pay</tt>: <a href="#Payloads">Payloads and Offsets</a></li>
 * </ul>
 * </p>
 *
 * <a name="Termdictionary" id="Termdictionary"></a>
 * <dl>
 * <dd>
 * <b>Term Dictionary</b>
 *
 * <p>The .tim file format is quite similar to Lucene40PostingsFormat, 
 *  with minor difference in MetadataBlock</p>
 *
 * <ul>
 * <!-- TODO: expand on this, its not really correct and doesnt explain sub-blocks etc -->
 *   <li>TermDictionary(.tim) --&gt; Header, DirOffset, PostingsHeader, PackedBlockSize, 
 *                                   &lt;Block&gt;<sup>NumBlocks</sup>, FieldSummary</li>
 *   <li>Block --&gt; SuffixBlock, StatsBlock, MetadataBlock</li>
 *   <li>SuffixBlock --&gt; EntryCount, SuffixLength, {@link DataOutput#writeByte byte}<sup>SuffixLength</sup></li>
 *   <li>StatsBlock --&gt; StatsLength, &lt;DocFreq, TotalTermFreq&gt;<sup>EntryCount</sup></li>
 *   <li>MetadataBlock --&gt; MetaLength, &lt;DocFPDelta, 
 *                            &lt;PosFPDelta, PosVIntBlockFPDelta?, PayFPDelta?&gt;?, 
 *                            SkipFPDelta?&gt;<sup>EntryCount</sup></li>
 *   <li>FieldSummary --&gt; NumFields, &lt;FieldNumber, NumTerms, RootCodeLength, 
 *                           {@link DataOutput#writeByte byte}<sup>RootCodeLength</sup>, SumDocFreq, DocCount&gt;
 *                           <sup>NumFields</sup></li>
 *   <li>Header, PostingsHeader --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <li>DirOffset --&gt; {@link DataOutput#writeLong Uint64}</li>
 *   <li>PackedBlockSize, EntryCount, SuffixLength, StatsLength, DocFreq, MetaLength, 
 *       PosVIntBlockFPDelta, SkipFPDelta, NumFields, FieldNumber, RootCodeLength, DocCount --&gt; 
 *       {@link DataOutput#writeVInt VInt}</li>
 *   <li>TotalTermFreq, DocFPDelta, PosFPDelta, PayFPDelta, NumTerms, SumTotalTermFreq, SumDocFreq --&gt; 
 *       {@link DataOutput#writeVLong VLong}</li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 *    <li>Here explains MetadataBlock only, other fields are mentioned in 
 *   <a href="../lucene40/Lucene40PostingsFormat.html#Termdictionary">Lucene40PostingsFormat:TermDictionary</a>
 *    </li>
 *    <li>PackedBlockSize is the fixed block size for packed blocks. In packed block, bit width is 
 *        determined by the largest integer. Smaller block size result in smaller variance among width 
 *        of integers hence smaller indexes. Larger block size result in more efficient bulk i/o hence
 *        better acceleration. This value should always be a multiple of 64, currently fixed as 128 as 
 *        a tradeoff. It is also the skip interval used to accelerate {@link DocsEnum#advance(int)}.
 *    <li>DocFPDelta determines the position of this term's TermFreqs within the .doc file. 
 *        In particular, it is the difference of file offset between this term's
 *        data and previous term's data (or zero, for the first term in the block).On disk it is 
 *        stored as the difference from previous value in sequence. </li>
 *    <li>PosFPDelta determines the position of this term's TermPositions within the .pos file.
 *        While PayFPDelta determines the position of this term's &lt;TermPayloads, TermOffsets?&gt; within 
 *        the .pay file. Similar to DocFPDelta, it is the difference between two file positions (or 
 *        neglected, for fields that omit payloads and offsets).</li>
 *    <li>PosVIntBlockFPDelta determines the position of this term's last TermPosition in last pos packed
 *        block within the .pos file. It is synonym for PayVIntBlockFPDelta or OffsetVIntBlockFPDelta. 
 *        This is actually used to indicate whether it is necessary to load following
 *        payloads and offsets from .pos instead of .pay. Every time a new block of positions are to be 
 *        loaded, the PostingsReader will use this value to check whether current block is packed format
 *        or VInt. When packed format, payloads and offsets are fetched from .pay, otherwise from .pos. 
 *        (this value is neglected when total number of positions i.e. totalTermFreq is less or equal 
 *        to PackedBlockSize).
 *    <li>SkipFPDelta determines the position of this term's SkipData within the .doc
 *        file. In particular, it is the length of the TermFreq data.
 *        SkipDelta is only stored if DocFreq is not smaller than SkipMinimum
 *        (i.e. 8 in BlockPostingsFormat).</li>
 * </ul>
 * </dd>
 * </dl>
 *
 * <a name="Termindex" id="Termindex"></a>
 * <dl>
 * <dd>
 * <b>Term Index</b>
 * <p>The .tim file format is mentioned in
 *   <a href="../lucene40/Lucene40PostingsFormat.html#Termindex">Lucene40PostingsFormat:TermIndex</a>
 * </dd>
 * </dl>
 *
 *
 * <a name="Frequencies" id="Frequencies"></a>
 * <dl>
 * <dd>
 * <b>Frequencies and Skip Data</b>
 *
 * <p>The .doc file contains the lists of documents which contain each term, along
 * with the frequency of the term in that document (except when frequencies are
 * omitted: {@link IndexOptions#DOCS_ONLY}). It also saves skip data to the beginning of 
 * each packed or VInt block, when the length of document list is larger than packed block size.</p>
 *
 * <ul>
 *   <li>docFile(.doc) --&gt; Header, &lt;TermFreqs, SkipData?&gt;<sup>TermCount</sup></li>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <li>TermFreqs --&gt; &lt;PackedBlock&gt; <sup>PackedDocBlockNum</sup>,  
 *                        VIntBlock? </li>
 *   <li>PackedBlock --&gt; PackedDocDeltaBlock, PackedFreqBlock?
 *   <li>VIntBlock --&gt; &lt;DocDelta[, Freq?]&gt;<sup>DocFreq-PackedBlockSize*PackedDocBlockNum</sup>
 *   <li>SkipData --&gt; &lt;&lt;SkipLevelLength, SkipLevel&gt;
 *       <sup>NumSkipLevels-1</sup>, SkipLevel&gt;, SkipDatum?</li>
 *   <li>SkipLevel --&gt; &lt;SkipDatum&gt; <sup>TrimmedDocFreq/(PackedBlockSize^(Level + 1))</sup></li>
 *   <li>SkipDatum --&gt; DocSkip, DocFPSkip, &lt;PosFPSkip, PosBlockOffset, PayLength?, 
 *                        OffsetStart?, PayFPSkip?&gt;?, SkipChildLevelPointer?</li>
 *   <li>PackedDocDeltaBlock, PackedFreqBlock --&gt; {@link PackedInts PackedInts}</li>
 *   <li>DocDelta, Freq, DocSkip, DocFPSkip, PosFPSkip, PosBlockOffset, PayLength, OffsetStart, PayFPSkip 
 *       --&gt; 
 *   {@link DataOutput#writeVInt VInt}</li>
 *   <li>SkipChildLevelPointer --&gt; {@link DataOutput#writeVLong VLong}</li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 *   <li>PackedDocDeltaBlock is theoretically generated from two steps: 
 *     <ol>
 *       <li>Calculate the difference between each document number and previous one, 
 *           and get a d-gaps list (for the first document, use absolute value); </li>
 *       <li>For those d-gaps from first one to PackedDocBlockNum*PackedBlockSize<sup>th</sup>, 
 *           separately encode as packed blocks.</li>
 *     </ol>
 *     If frequencies are not omitted, PackedFreqBlock will be generated without d-gap step.
 *   </li>
 *   <li>VIntBlock stores remaining d-gaps (along with frequencies when possible) with a format 
 *       mentioned in
 *   <a href="../lucene40/Lucene40PostingsFormat.html#Frequencies">Lucene40PostingsFormat:Frequencies</a>
 *   </li>
 *   <li>PackedDocBlockNum is the number of packed blocks for current term's docids or frequencies. 
 *       In particular, PackedDocBlockNum = floor(DocFreq/PackedBlockSize) </li>
 *   <li>TrimmedDocFreq = DocFreq % PackedBlockSize == 0 ? DocFreq - 1 : DocFreq. 
 *       We use this trick since the definition of skip entry is a little different from base interface.
 *       In {@link MultiLevelSkipListWriter}, skip data is assumed to be saved for
 *       skipInterval<sup>th</sup>, 2*skipInterval<sup>th</sup> ... posting in the list. However, 
 *       in BlockPostingsFormat, the skip data is saved for skipInterval+1<sup>th</sup>, 
 *       2*skipInterval+1<sup>th</sup> ... posting (skipInterval==PackedBlockSize in this case). 
 *       When DocFreq is multiple of PackedBlockSize, MultiLevelSkipListWriter will expect one 
 *       more skip data than BlockSkipWriter. </li>
 *   <li>SkipDatum is the metadata of one skip entry.
 *      For the first block (no matter packed or VInt), it is omitted.</li>
 *   <li>DocSkip records the document number of every PackedBlockSize<sup>th</sup> document number in
 *       the postings (i.e. last document number in each packed block). On disk it is stored as the 
 *       difference from previous value in the sequence. </li>
 *   <li>DocFPSkip records the file offsets of each block (excluding )posting at 
 *       PackedBlockSize+1<sup>th</sup>, 2*PackedBlockSize+1<sup>th</sup> ... , in DocFile. 
 *       The file offsets are relative to the start of current term's TermFreqs. 
 *       On disk it is also stored as the difference from previous SkipDatum in the sequence.</li>
 *   <li>Since positions and payloads are also block encoded, the skip should skip to related block first,
 *       then fetch the values according to in-block offset. PosFPSkip and PayFPSkip record the file 
 *       offsets of related block in .pos and .pay, respectively. While PosBlockOffset indicates
 *       which value to fetch inside the related block (PayBlockOffset is unnecessary since it is always
 *       equal to PosBlockOffset). Same as DocFPSkip, the file offsets are relative to the start of 
 *       current term's TermFreqs, and stored as a difference sequence.</li>
 *   <li>PayLength indicates the length of last payload.</li>
 *   <li>OffsetStart indicates the first value of last offset pair.</li>
 * </ul>
 * </dd>
 * </dl>
 *
 * <a name="Positions" id="Positions"></a>
 * <dl>
 * <dd>
 * <b>Positions</b>
 * <p>The .pos file contains the lists of positions that each term occurs at within documents. It also
 *    sometimes stores part of payloads and offsets for speedup.</p>
 * <ul>
 *   <li>PosFile(.pos) --&gt; Header, &lt;TermPositions&gt; <sup>TermCount</sup></li>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <li>TermPositions --&gt; &lt;PackedPosDeltaBlock&gt; <sup>PackedPosBlockNum</sup>,  
 *                            VIntBlock? </li>
 *   <li>VIntBlock --&gt; PosVIntCount, &lt;PosDelta[, PayLength?], PayData?, 
 *                        OffsetStartDelta?, OffsetLength?&gt;<sup>PosVIntCount</sup>
 *   <li>PackedPosDeltaBlock --&gt; {@link PackedInts PackedInts}</li>
 *   <li>PosVIntCount, PosDelta, OffsetStartDelta, OffsetLength --&gt; 
 *       {@link DataOutput#writeVInt VInt}</li>
 *   <li>PayData --&gt; {@link DataOutput#writeByte byte}<sup>PayLength</sup></li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 *   <li>TermPositions are order by term (terms are implicit, from the term dictionary), and position 
 *       values for each term document pair are incremental, and ordered by document number.</li>
 *   <li>PackedPosBlockNum is the number of packed blocks for current term's positions, payloads or offsets. 
 *       In particular, PackedPosBlockNum = floor(totalTermFreq/PackedBlockSize) </li>
 *   <li>PosVIntCount is the number of positions encoded as VInt format. In particular, 
 *       PosVIntCount = totalTermFreq - PackedPosBlockNum*PackedBlockSize</li>
 *   <li>The procedure how PackedPosDeltaBlock is generated is the same as PackedDocDeltaBlock 
 *       in chapter <a href="#Frequencies">Frequencies and Skip Data</a>.</li>
 *   <li>PosDelta is the same as the format mentioned in 
 *   <a href="../lucene40/Lucene40PostingsFormat.html#Positions">Lucene40PostingsFormat:Positions</a>
 *   </li>
 *   <li>OffsetStartDelta is the difference between this position's startOffset from the previous 
 *       occurrence (or zero, if this is the first occurrence in this document).</li>
 *   <li>OffsetLength indicates the length of the current offset (endOffset-startOffset).</li>
 *   <li>PayloadData is the blob of metadata associated with current position.</li>
 * </ul>
 * </dd>
 * </dl>
 *
 * <a name="Payloads" id="Payloads"></a>
 * <dl>
 * <dd>
 * <b>Payloads and Offsets</b>
 * <p>The .pay file will store payloads and offsets associated with certain term-document positions. 
 *    Some payloads and offsets will be separated out into .pos file, for speedup reason.</p>
 * <ul>
 *   <li>PayFile(.pay): --&gt; Header, &lt;TermPayloads, TermOffsets?&gt; <sup>TermCount</sup></li>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <li>TermPayloads --&gt; &lt;PackedPayLengthBlock, SumPayLength, PayData&gt; <sup>PackedPayBlockNum</sup>
 *   <li>TermOffsets --&gt; &lt;PackedOffsetStartDeltaBlock, PackedOffsetLengthBlock&gt; <sup>PackedPayBlockNum</sup>
 *   <li>PackedPayLengthBlock, PackedOffsetStartDeltaBlock, PackedOffsetLengthBlock --&gt; {@link PackedInts PackedInts}</li>
 *   <li>SumPayLength --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>PayData --&gt; {@link DataOutput#writeByte byte}<sup>SumPayLength</sup></li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 *   <li>The order of TermPayloads/TermOffsets will be the same as TermPositions, note that part of 
 *       payload/offsets are stored in .pos.</li>
 *   <li>The procedure how PackedPayLengthBlock and PackedOffsetLengthBlock are generated is the 
 *       same as PackedFreqBlock in chapter <a href="#Frequencies">Frequencies and Skip Data</a>. 
 *       While PackedStartDeltaBlock follows a same procedure as PackedDocDeltaBlock.</li>
 *   <li>PackedPayBlockNum is always equal to PackedPosBlockNum, for the same term. It is also synonym 
 *       for PackedOffsetBlockNum.</li>
 *   <li>SumPayLength is the total length of payloads written within one block, should be the sum
 *       of PayLengths in one packed block.</li>
 *   <li>PayLength in PackedPayLengthBlock is the length of each payload, associated with current 
 *       position.</li>
 * </ul>
 * </dd>
 * </dl>
 * </p>
 *
 * @lucene.experimental
 */

public final class BlockPostingsFormat extends PostingsFormat {
  /**
   * Filename extension for document number, frequencies, and skip data.
   * See chapter: <a href="#Frequencies">Frequencies and Skip Data</a>
   */
  public static final String DOC_EXTENSION = "doc";

  /**
   * Filename extension for positions. 
   * See chapter: <a href="#Positions">Positions</a>
   */
  public static final String POS_EXTENSION = "pos";

  /**
   * Filename extension for payloads and offsets.
   * See chapter: <a href="#Payloads">Payloads and Offsets</a>
   */
  public static final String PAY_EXTENSION = "pay";

  private final int minTermBlockSize;
  private final int maxTermBlockSize;

  /**
   * Fixed packed block size, number of integers encoded in 
   * a single packed block.
   */
  // NOTE: must be multiple of 64 because of PackedInts long-aligned encoding/decoding
  public final static int BLOCK_SIZE = 128;

  public BlockPostingsFormat() {
    this(BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  public BlockPostingsFormat(int minTermBlockSize, int maxTermBlockSize) {
    super("Block");
    this.minTermBlockSize = minTermBlockSize;
    assert minTermBlockSize > 1;
    this.maxTermBlockSize = maxTermBlockSize;
    assert minTermBlockSize <= maxTermBlockSize;
  }

  @Override
  public String toString() {
    return getName() + "(blocksize=" + BLOCK_SIZE + ")";
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new BlockPostingsWriter(state);

    boolean success = false;
    try {
      FieldsConsumer ret = new BlockTreeTermsWriter(state, 
                                                    postingsWriter,
                                                    minTermBlockSize, 
                                                    maxTermBlockSize);
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
    PostingsReaderBase postingsReader = new BlockPostingsReader(state.dir,
                                                                state.fieldInfos,
                                                                state.segmentInfo,
                                                                state.context,
                                                                state.segmentSuffix);
    boolean success = false;
    try {
      FieldsProducer ret = new BlockTreeTermsReader(state.dir,
                                                    state.fieldInfos,
                                                    state.segmentInfo,
                                                    postingsReader,
                                                    state.context,
                                                    state.segmentSuffix,
                                                    state.termsIndexDivisor);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsReader);
      }
    }
  }
}
