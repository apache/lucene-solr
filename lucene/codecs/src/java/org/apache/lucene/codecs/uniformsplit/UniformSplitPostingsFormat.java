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

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsReader;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;

/**
 * {@link PostingsFormat} based on the Uniform Split technique.
 *
 * @see UniformSplitTermsWriter
 * @lucene.experimental
 */
public class UniformSplitPostingsFormat extends PostingsFormat {

  /**
   * Extension of the file containing the terms dictionary (the FST "trie").
   */
  public static final String TERMS_DICTIONARY_EXTENSION = "ustd";
  /**
   * Extension of the file containing the terms blocks for each field and the fields metadata.
   */
  public static final String TERMS_BLOCKS_EXTENSION = "ustb";

  public static final int VERSION_START = 0;
  public static final int VERSION_ENCODABLE_FIELDS_METADATA = 1;
  public static final int VERSION_CURRENT = VERSION_ENCODABLE_FIELDS_METADATA;

  public static final String NAME = "UniformSplit";

  protected final int targetNumBlockLines;
  protected final int deltaNumLines;
  protected final BlockEncoder blockEncoder;
  protected final BlockDecoder blockDecoder;
  protected final boolean dictionaryOnHeap;

  /**
   * Creates a {@link UniformSplitPostingsFormat} with default settings.
   */
  public UniformSplitPostingsFormat() {
    this(UniformSplitTermsWriter.DEFAULT_TARGET_NUM_BLOCK_LINES, UniformSplitTermsWriter.DEFAULT_DELTA_NUM_LINES,
        null, null, false);
  }

  /**
   * @param targetNumBlockLines Target number of lines per block.
   *                            Must be strictly greater than 0.
   *                            The parameters can be pre-validated with {@link UniformSplitTermsWriter#validateSettings(int, int)}.
   *                            There is one term per block line, with its corresponding details ({@link org.apache.lucene.index.TermState}).
   * @param deltaNumLines       Maximum allowed delta variation of the number of lines per block.
   *                            Must be greater than or equal to 0 and strictly less than {@code targetNumBlockLines}.
   *                            The block size will be {@code targetNumBlockLines}+-{@code deltaNumLines}.
   *                            The block size must always be less than or equal to {@link UniformSplitTermsWriter#MAX_NUM_BLOCK_LINES}.
   * @param blockEncoder        Optional block encoder, may be null if none. If present, it is used to encode all terms
   *                            blocks, as well as the FST dictionary and the fields metadata.
   * @param blockDecoder        Optional block decoder, may be null if none. If present, it is used to decode all terms
   *                            blocks, as well as the FST dictionary and the fields metadata.
   * @param dictionaryOnHeap    Whether to force loading the terms dictionary on-heap. By default it is kept off-heap without
   *                            impact on performance. If block encoding/decoding is used, then the dictionary is always
   *                            loaded on-heap whatever this parameter value is.
   */
  public UniformSplitPostingsFormat(int targetNumBlockLines, int deltaNumLines, BlockEncoder blockEncoder, BlockDecoder blockDecoder,
                                    boolean dictionaryOnHeap) {
    this(NAME, targetNumBlockLines, deltaNumLines, blockEncoder, blockDecoder, dictionaryOnHeap);
  }

  /**
   * @see #UniformSplitPostingsFormat(int, int, BlockEncoder, BlockDecoder, boolean)
   */
  protected UniformSplitPostingsFormat(String name, int targetNumBlockLines, int deltaNumLines, BlockEncoder blockEncoder,
                                       BlockDecoder blockDecoder, boolean dictionaryOnHeap) {
    super(name);
    UniformSplitTermsWriter.validateSettings(targetNumBlockLines, deltaNumLines);
    validateBlockEncoder(blockEncoder, blockDecoder);
    this.targetNumBlockLines = targetNumBlockLines;
    this.deltaNumLines = deltaNumLines;
    this.blockEncoder = blockEncoder;
    this.blockDecoder = blockDecoder;
    this.dictionaryOnHeap = dictionaryOnHeap;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new Lucene84PostingsWriter(state);
    boolean success = false;
    try {
      FieldsConsumer termsWriter = createUniformSplitTermsWriter(postingsWriter, state, targetNumBlockLines, deltaNumLines, blockEncoder);
      success = true;
      return termsWriter;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsWriter);
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    PostingsReaderBase postingsReader = new Lucene84PostingsReader(state);
    boolean success = false;
    try {
      FieldsProducer termsReader = createUniformSplitTermsReader(postingsReader, state, blockDecoder);
      success = true;
      return termsReader;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsReader);
      }
    }
  }

  protected FieldsConsumer createUniformSplitTermsWriter(PostingsWriterBase postingsWriter, SegmentWriteState state,
                                               int targetNumBlockLines, int deltaNumLines, BlockEncoder blockEncoder) throws IOException {
    return new UniformSplitTermsWriter(postingsWriter, state, targetNumBlockLines, deltaNumLines, blockEncoder);
  }

  protected FieldsProducer createUniformSplitTermsReader(PostingsReaderBase postingsReader, SegmentReadState state,
                                               BlockDecoder blockDecoder) throws IOException {
    return new UniformSplitTermsReader(postingsReader, state, blockDecoder, dictionaryOnHeap);
  }

  private static void validateBlockEncoder(BlockEncoder blockEncoder, BlockDecoder blockDecoder) {
    if (blockEncoder != null && blockDecoder == null || blockEncoder == null && blockDecoder != null) {
      throw new IllegalArgumentException("Invalid blockEncoder=" + blockEncoder + " and blockDecoder=" + blockDecoder + ", both must be null or both must be non-null");
    }
  }
}
