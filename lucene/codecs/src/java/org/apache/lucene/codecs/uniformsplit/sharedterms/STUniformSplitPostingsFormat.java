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

package org.apache.lucene.codecs.uniformsplit.sharedterms;

import java.io.IOException;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.uniformsplit.BlockDecoder;
import org.apache.lucene.codecs.uniformsplit.BlockEncoder;
import org.apache.lucene.codecs.uniformsplit.UniformSplitPostingsFormat;
import org.apache.lucene.codecs.uniformsplit.UniformSplitTermsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * {@link PostingsFormat} based on the Uniform Split technique and supporting
 * Shared Terms.
 * <p>
 * Shared Terms means the terms of all fields are stored in the same block file,
 * with multiple fields associated to one term (one block line). In the same way,
 * the dictionary trie is also shared between all fields. This highly reduces
 * the memory required by the field dictionary compared to having one separate
 * dictionary per field.
 *
 * @lucene.experimental
 */
public class STUniformSplitPostingsFormat extends UniformSplitPostingsFormat {

  /**
   * Extension of the file containing the terms dictionary (the FST "trie").
   */
  public static final String TERMS_DICTIONARY_EXTENSION = "stustd";
  /**
   * Extension of the file containing the terms blocks for each field and the fields metadata.
   */
  public static final String TERMS_BLOCKS_EXTENSION = "stustb";

  public static final int VERSION_CURRENT = UniformSplitPostingsFormat.VERSION_CURRENT;

  public static final String NAME = "SharedTermsUniformSplit";

  /**
   * Creates a {@link STUniformSplitPostingsFormat} with default settings.
   */
  public STUniformSplitPostingsFormat() {
    this(UniformSplitTermsWriter.DEFAULT_TARGET_NUM_BLOCK_LINES, UniformSplitTermsWriter.DEFAULT_DELTA_NUM_LINES,
        null, null, false);
  }

  /**
   * @see UniformSplitPostingsFormat#UniformSplitPostingsFormat(int, int, BlockEncoder, BlockDecoder, boolean)
   */
  public STUniformSplitPostingsFormat(int targetNumBlockLines, int deltaNumLines, BlockEncoder blockEncoder, BlockDecoder blockDecoder,
                                      boolean dictionaryOnHeap) {
    this(NAME, targetNumBlockLines, deltaNumLines, blockEncoder, blockDecoder, dictionaryOnHeap);
  }

  protected STUniformSplitPostingsFormat(String name, int targetNumBlockLines, int deltaNumLines, BlockEncoder blockEncoder,
                                         BlockDecoder blockDecoder, boolean dictionaryOnHeap) {
    super(name, targetNumBlockLines, deltaNumLines, blockEncoder, blockDecoder, dictionaryOnHeap);
  }

  @Override
  protected FieldsConsumer createUniformSplitTermsWriter(PostingsWriterBase postingsWriter, SegmentWriteState state,
                                               int targetNumBlockLines, int deltaNumLines, BlockEncoder blockEncoder) throws IOException {
    return new STUniformSplitTermsWriter(postingsWriter, state, targetNumBlockLines, deltaNumLines, blockEncoder);
  }

  @Override
  protected FieldsProducer createUniformSplitTermsReader(PostingsReaderBase postingsReader, SegmentReadState state,
                                               BlockDecoder blockDecoder) throws IOException {
    return new STUniformSplitTermsReader(postingsReader, state, blockDecoder, dictionaryOnHeap);
  }
}