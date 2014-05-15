package org.apache.lucene.codecs.idversion;

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
import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;

/** A PostingsFormat for primary-key (ID) fields, that associates a
 *  long version with each ID and enables fast (using only the terms index)
 *  lookup for whether a given ID may have a version > N.
 *
 *  The field is indexed as DOCS_ONLY, but the user must feed in the
 *  version as a payload on the first token.
 *
 *  The docID and version for each ID is inlined into the terms dict.
 *
 *  @lucene.experimental */

public abstract class IDVersionPostingsFormat extends PostingsFormat {

  private final int minTermsInBlock;
  private final int maxTermsInBlock;

  public IDVersionPostingsFormat() {
    this(BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  public IDVersionPostingsFormat(int minTermsInBlock, int maxTermsInBlock) {
    super("IDVersion");
    this.minTermsInBlock = minTermsInBlock;
    this.maxTermsInBlock = maxTermsInBlock;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new IDVersionPostingsWriter();
    boolean success = false;
    try {
      FieldsConsumer ret = new BlockTreeTermsWriter(state, 
                                                    postingsWriter,
                                                    minTermsInBlock, 
                                                    maxTermsInBlock);
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
    PostingsReaderBase postingsReader = new IDVersionPostingsReader();
    boolean success = false;
     try {
       FieldsProducer ret = new BlockTreeTermsReader(state.directory,
                                                     state.fieldInfos,
                                                     state.segmentInfo,
                                                     postingsReader,
                                                     state.context,
                                                     state.segmentSuffix);
       success = true;
       return ret;
     } finally {
       if (!success) {
         IOUtils.closeWhileHandlingException(postingsReader);
       }
     }
  }
}
