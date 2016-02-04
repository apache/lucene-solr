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
package org.apache.lucene.codecs.autoprefix;


import java.io.IOException;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsReader;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;

/**
 * Just like {@link Lucene50PostingsFormat} except this format
 * exposes the experimental auto-prefix terms.
 *
 * @lucene.experimental
 */

public final class AutoPrefixPostingsFormat extends PostingsFormat {

  private final int minItemsInBlock;
  private final int maxItemsInBlock;
  private final int minItemsInAutoPrefix;
  private final int maxItemsInAutoPrefix;

  /** Creates {@code AutoPrefixPostingsFormat} with default settings. */
  public AutoPrefixPostingsFormat() {
    this(BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE,
         BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE,
         25, 48);
  }

  /** Creates {@code Lucene50PostingsFormat} with custom
   *  values for {@code minBlockSize} and {@code
   *  maxBlockSize} passed to block terms dictionary.
   *  @see BlockTreeTermsWriter#BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int) */
  public AutoPrefixPostingsFormat(int minItemsInAutoPrefix, int maxItemsInAutoPrefix) {
    this(BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE,
         BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE,
         minItemsInAutoPrefix,
         maxItemsInAutoPrefix);
  }

  /** Creates {@code Lucene50PostingsFormat} with custom
   *  values for {@code minBlockSize}, {@code
   *  maxBlockSize}, {@code minItemsInAutoPrefix} and {@code maxItemsInAutoPrefix}, passed
   *  to block tree terms dictionary.
   *  @see BlockTreeTermsWriter#BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int,int,int) */
  public AutoPrefixPostingsFormat(int minItemsInBlock, int maxItemsInBlock, int minItemsInAutoPrefix, int maxItemsInAutoPrefix) {
    super("AutoPrefix");
    BlockTreeTermsWriter.validateSettings(minItemsInBlock,
                                          maxItemsInBlock);
    BlockTreeTermsWriter.validateAutoPrefixSettings(minItemsInAutoPrefix,
                                                    maxItemsInAutoPrefix);
    this.minItemsInBlock = minItemsInBlock;
    this.maxItemsInBlock = maxItemsInBlock;
    this.minItemsInAutoPrefix = minItemsInAutoPrefix;
    this.maxItemsInAutoPrefix = maxItemsInAutoPrefix;
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new Lucene50PostingsWriter(state);

    boolean success = false;
    try {
      FieldsConsumer ret = new BlockTreeTermsWriter(state, 
                                                    postingsWriter,
                                                    minItemsInBlock, 
                                                    maxItemsInBlock,
                                                    minItemsInAutoPrefix,
                                                    maxItemsInAutoPrefix);
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
    PostingsReaderBase postingsReader = new Lucene50PostingsReader(state);
    boolean success = false;
    try {
      FieldsProducer ret = new BlockTreeTermsReader(postingsReader, state);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsReader);
      }
    }
  }
}
