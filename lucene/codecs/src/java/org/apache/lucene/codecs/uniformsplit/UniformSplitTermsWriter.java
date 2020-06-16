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

package org.apache.lucene.codecs.uniformsplit;

import java.io.IOException;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.codecs.uniformsplit.UniformSplitPostingsFormat.NAME;
import static org.apache.lucene.codecs.uniformsplit.UniformSplitPostingsFormat.TERMS_BLOCKS_EXTENSION;
import static org.apache.lucene.codecs.uniformsplit.UniformSplitPostingsFormat.TERMS_DICTIONARY_EXTENSION;
import static org.apache.lucene.codecs.uniformsplit.UniformSplitPostingsFormat.VERSION_CURRENT;

/**
 * A block-based terms index and dictionary that assigns terms to nearly
 * uniform length blocks. This technique is called Uniform Split.
 * <p>
 * The block construction is driven by two parameters, {@code targetNumBlockLines}
 * and {@code deltaNumLines}.
 * Each block size (number of terms) is {@code targetNumBlockLines}+-{@code deltaNumLines}.
 * The algorithm computes the minimal distinguishing prefix (MDP) between
 * each term and its previous term (alphabetically ordered). Then it selects
 * in the neighborhood of the {@code targetNumBlockLines}, and within the
 * {@code deltaNumLines}, the term with the minimal MDP. This term becomes
 * the first term of the next block and its MDP is the block key. This block
 * key is added to the terms dictionary trie.
 * <p>
 * We call dictionary the trie structure in memory, and block file the disk file
 * containing the block lines, with one term and its corresponding term state
 * details per line.
 * <p>
 * When seeking a term, the dictionary seeks the floor leaf of the trie for the
 * searched term and jumps to the corresponding file pointer in the block file.
 * There, the block terms are scanned for the exact searched term.
 * <p>
 * The terms inside a block do not need to share a prefix. Only the block
 * key is used to find the block from the dictionary trie. And the block key
 * is selected because it is the locally smallest MDP. This makes the dictionary
 * trie very compact.
 * <p>
 * An interesting property of the Uniform Split technique is the very linear
 * balance between memory usage and lookup performance. By decreasing
 * the target block size, the block scan becomes faster, and since there are
 * more blocks, the dictionary trie memory usage increases. Additionally,
 * small blocks are faster to read from disk. A good sweet spot for the target
 * block size is 32 with delta of 3 (10%) (default values). This can be tuned
 * in the constructor.
 * <p>
 * There are additional optimizations:
 * <ul>
 * <li>Each block has a header that allows the lookup to jump directly to
 * the middle term with a fast comparison. This reduces the linear scan
 * by 2 for a small disk size increase.</li>
 * <li>Each block term is incrementally encoded according to its previous
 * term. This both reduces the disk size and speeds up the block scan.</li>
 * <li>All term line details (the terms states) are written after all terms. This
 * allows faster term scan without needing to decode the term states.</li>
 * <li>All file pointers are base-encoded. Their value is relative to the block
 * base file pointer (not to the previous file pointer), this allows to read the
 * term state of any term independently.</li>
 * </ul>
 * <p>
 * Blocks can be compressed or encrypted with an optional {@link BlockEncoder}
 * provided in the {@link #UniformSplitTermsWriter(PostingsWriterBase, SegmentWriteState, int, int, BlockEncoder) constructor}.
 * <p>
 * The {@link UniformSplitPostingsFormat#TERMS_BLOCKS_EXTENSION block file}
 * contains all the term blocks for each field sequentially. It also contains
 * the fields metadata at the end of the file.
 * <p>
 * The {@link UniformSplitPostingsFormat#TERMS_DICTIONARY_EXTENSION dictionary file}
 * contains the trie ({@link org.apache.lucene.util.fst.FST} bytes) for each
 * field sequentially.
 *
 * @lucene.experimental
 */
public class UniformSplitTermsWriter extends FieldsConsumer {

  /**
   * Default value for the target block size (number of terms per block).
   */
  public static final int DEFAULT_TARGET_NUM_BLOCK_LINES = 32;
  /**
   * Default value for the maximum allowed delta variation of the block size (delta of the number of terms per block).
   * The block size will be [target block size]+-[allowed delta].
   */
  public static final int DEFAULT_DELTA_NUM_LINES = (int) (DEFAULT_TARGET_NUM_BLOCK_LINES * 0.1);
  /**
   * Upper limit of the block size (maximum number of terms per block).
   */
  protected static final int MAX_NUM_BLOCK_LINES = 1_000;

  protected final FieldInfos fieldInfos;
  protected final PostingsWriterBase postingsWriter;
  protected final int maxDoc;

  protected final int targetNumBlockLines;
  protected final int deltaNumLines;

  protected final BlockEncoder blockEncoder;
  protected final FieldMetadata.Serializer fieldMetadataWriter;
  protected final IndexOutput blockOutput;
  protected final IndexOutput dictionaryOutput;

  /**
   * @param blockEncoder Optional block encoder, may be null if none.
   *                     It can be used for compression or encryption.
   */
  public UniformSplitTermsWriter(PostingsWriterBase postingsWriter, SegmentWriteState state,
                          BlockEncoder blockEncoder) throws IOException {
    this(postingsWriter, state, DEFAULT_TARGET_NUM_BLOCK_LINES, DEFAULT_DELTA_NUM_LINES, blockEncoder);
  }

  /**
   * @param blockEncoder Optional block encoder, may be null if none.
   *                     It can be used for compression or encryption.
   */
  public UniformSplitTermsWriter(PostingsWriterBase postingsWriter, SegmentWriteState state,
                          int targetNumBlockLines, int deltaNumLines, BlockEncoder blockEncoder) throws IOException {
    this(postingsWriter, state, targetNumBlockLines, deltaNumLines, blockEncoder, FieldMetadata.Serializer.INSTANCE,
        NAME, VERSION_CURRENT, TERMS_BLOCKS_EXTENSION, TERMS_DICTIONARY_EXTENSION);
  }


  /**
   * @param targetNumBlockLines Target number of lines per block.
   *                            Must be strictly greater than 0.
   *                            The parameters can be pre-validated with {@link #validateSettings(int, int)}.
   *                            There is one term per block line, with its corresponding details ({@link org.apache.lucene.index.TermState}).
   * @param deltaNumLines       Maximum allowed delta variation of the number of lines per block.
   *                            Must be greater than or equal to 0 and strictly less than {@code targetNumBlockLines}.
   *                            The block size will be {@code targetNumBlockLines}+-{@code deltaNumLines}.
   *                            The block size must always be less than or equal to {@link #MAX_NUM_BLOCK_LINES}.
   * @param blockEncoder        Optional block encoder, may be null if none.
   *                            It can be used for compression or encryption.
   */
  protected UniformSplitTermsWriter(PostingsWriterBase postingsWriter, SegmentWriteState state,
                          int targetNumBlockLines, int deltaNumLines, BlockEncoder blockEncoder, FieldMetadata.Serializer fieldMetadataWriter,
                          String codecName, int versionCurrent, String termsBlocksExtension, String dictionaryExtension) throws IOException {
    validateSettings(targetNumBlockLines, deltaNumLines);
    IndexOutput blockOutput = null;
    IndexOutput dictionaryOutput = null;
    boolean success = false;
    try {
      this.fieldInfos = state.fieldInfos;
      this.postingsWriter = postingsWriter;
      this.maxDoc = state.segmentInfo.maxDoc();
      this.targetNumBlockLines = targetNumBlockLines;
      this.deltaNumLines = deltaNumLines;
      this.blockEncoder = blockEncoder;
      this.fieldMetadataWriter = fieldMetadataWriter;

      String termsName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, termsBlocksExtension);
      blockOutput = state.directory.createOutput(termsName, state.context);
      CodecUtil.writeIndexHeader(blockOutput, codecName, versionCurrent, state.segmentInfo.getId(), state.segmentSuffix);

      String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dictionaryExtension);
      dictionaryOutput = state.directory.createOutput(indexName, state.context);
      CodecUtil.writeIndexHeader(dictionaryOutput, codecName, versionCurrent, state.segmentInfo.getId(), state.segmentSuffix);

      postingsWriter.init(blockOutput, state);

      this.blockOutput = blockOutput;
      this.dictionaryOutput = dictionaryOutput;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(blockOutput, dictionaryOutput);
      }
    }
  }

  /**
   * Validates the {@link #UniformSplitTermsWriter(PostingsWriterBase, SegmentWriteState, int, int, BlockEncoder) constructor}
   * settings.
   *
   * @param targetNumBlockLines Target number of lines per block.
   *                            Must be strictly greater than 0.
   * @param deltaNumLines       Maximum allowed delta variation of the number of lines per block.
   *                            Must be greater than or equal to 0 and strictly less than {@code targetNumBlockLines}.
   *                            Additionally, {@code targetNumBlockLines} + {@code deltaNumLines} must be less than
   *                            or equal to {@link #MAX_NUM_BLOCK_LINES}.
   */
  protected static void validateSettings(int targetNumBlockLines, int deltaNumLines) {
    if (targetNumBlockLines <= 0) {
      throw new IllegalArgumentException("Invalid negative or nul targetNumBlockLines=" + targetNumBlockLines);
    }
    if (deltaNumLines < 0) {
      throw new IllegalArgumentException("Invalid negative deltaNumLines=" + deltaNumLines);
    }
    if (deltaNumLines >= targetNumBlockLines) {
      throw new IllegalArgumentException("Invalid too large deltaNumLines=" + deltaNumLines
          + ", it must be < targetNumBlockLines=" + targetNumBlockLines);
    }
    if (targetNumBlockLines + deltaNumLines > UniformSplitTermsWriter.MAX_NUM_BLOCK_LINES) {
      throw new IllegalArgumentException("Invalid (targetNumBlockLines + deltaNumLines)="
          + (targetNumBlockLines + deltaNumLines) + ", it must be <= MAX_NUM_BLOCK_LINES="
          + UniformSplitTermsWriter.MAX_NUM_BLOCK_LINES);
    }
  }

  @Override
  public void write(Fields fields, NormsProducer normsProducer) throws IOException {
    BlockWriter blockWriter = new BlockWriter(blockOutput, targetNumBlockLines, deltaNumLines, blockEncoder);
    ByteBuffersDataOutput fieldsOutput = new ByteBuffersDataOutput();
    int fieldsNumber = 0;
    for (String field : fields) {
      Terms terms = fields.terms(field);
      if (terms != null) {
        TermsEnum termsEnum = terms.iterator();
        FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        fieldsNumber += writeFieldTerms(blockWriter, fieldsOutput, termsEnum, fieldInfo, normsProducer);
      }
    }
    writeFieldsMetadata(fieldsNumber, fieldsOutput);
    CodecUtil.writeFooter(dictionaryOutput);
  }

  protected void writeFieldsMetadata(int fieldsNumber, ByteBuffersDataOutput fieldsOutput) throws IOException {
    long fieldsStartPosition = blockOutput.getFilePointer();
    blockOutput.writeVInt(fieldsNumber);
    if (blockEncoder == null) {
      writeUnencodedFieldsMetadata(fieldsOutput);
    } else {
      writeEncodedFieldsMetadata(fieldsOutput);
    }
    // Must be a fixed length. Read by UniformSplitTermsReader when seeking fields metadata.
    blockOutput.writeLong(fieldsStartPosition);
    CodecUtil.writeFooter(blockOutput);
  }

  protected void writeUnencodedFieldsMetadata(ByteBuffersDataOutput fieldsOutput) throws IOException {
    fieldsOutput.copyTo(blockOutput);
  }

  protected void writeEncodedFieldsMetadata(ByteBuffersDataOutput fieldsOutput) throws IOException {
    BlockEncoder.WritableBytes encodedBytes = blockEncoder.encode(fieldsOutput.toDataInput(), fieldsOutput.size());
    blockOutput.writeVLong(encodedBytes.size());
    encodedBytes.writeTo(blockOutput);
  }

  /**
   * @return 1 if the field was written; 0 otherwise.
   */
  protected int writeFieldTerms(BlockWriter blockWriter, DataOutput fieldsOutput, TermsEnum termsEnum,
                              FieldInfo fieldInfo, NormsProducer normsProducer) throws IOException {

    FieldMetadata fieldMetadata = new FieldMetadata(fieldInfo, maxDoc);
    fieldMetadata.setDictionaryStartFP(dictionaryOutput.getFilePointer());

    postingsWriter.setField(fieldInfo);
    blockWriter.setField(fieldMetadata);
    IndexDictionary.Builder dictionaryBuilder = new FSTDictionary.Builder();
    BytesRef lastTerm = null;
    while (termsEnum.next() != null) {
      BlockTermState blockTermState = writePostingLine(termsEnum, fieldMetadata, normsProducer);
      if (blockTermState != null) {
        lastTerm = BytesRef.deepCopyOf(termsEnum.term());
        blockWriter.addLine(lastTerm, blockTermState, dictionaryBuilder);
      }
    }

    // Flush remaining terms.
    blockWriter.finishLastBlock(dictionaryBuilder);

    if (fieldMetadata.getNumTerms() > 0) {
      fieldMetadata.setLastTerm(lastTerm);
      fieldMetadataWriter.write(fieldsOutput, fieldMetadata);
      writeDictionary(dictionaryBuilder);
      return 1;
    }
    return 0;
  }

  /**
   * Writes the posting values for the current term in the given {@link TermsEnum}
   * and updates the {@link FieldMetadata} stats.
   *
   * @return the written {@link BlockTermState}; or null if none.
   */
  protected BlockTermState writePostingLine(TermsEnum termsEnum, FieldMetadata fieldMetadata, NormsProducer normsProducer) throws IOException {
    BlockTermState state = postingsWriter.writeTerm(termsEnum.term(), termsEnum, fieldMetadata.getDocsSeen(), normsProducer);
    if (state == null) {
      // No doc for this term.
      return null;
    }
    fieldMetadata.updateStats(state);
    return state;
  }

  /**
   * Writes the dictionary index (FST) to disk.
   */
  protected void writeDictionary(IndexDictionary.Builder dictionaryBuilder) throws IOException {
    dictionaryBuilder.build().write(dictionaryOutput, blockEncoder);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(blockOutput, dictionaryOutput, postingsWriter);
  }
}
