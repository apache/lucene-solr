package org.apache.lucene.codecs.lucene40;

import java.io.IOException;

import org.apache.lucene.codecs.temp.*;
import org.apache.lucene.codecs.BlockTreeTermsReader;
import org.apache.lucene.codecs.BlockTreeTermsWriter;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TempPostingsReaderBase;
import org.apache.lucene.codecs.TempPostingsWriterBase;
import org.apache.lucene.index.DocsEnum; // javadocs
import org.apache.lucene.index.FieldInfo.IndexOptions; // javadocs
import org.apache.lucene.index.FieldInfos; // javadocs
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput; // javadocs
import org.apache.lucene.util.fst.FST; // javadocs


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

/**
 * Read-write version of {@link Lucene40PostingsFormat} for testing.
 */
public class Temp40RWPostingsFormat extends PostingsFormat {
  /** minimum items (terms or sub-blocks) per block for BlockTree */
  protected final int minBlockSize;
  /** maximum items (terms or sub-blocks) per block for BlockTree */
  protected final int maxBlockSize;

  /** Creates {@code Lucene40PostingsFormat} with default
   *  settings. */
  public Temp40RWPostingsFormat() {
    this(BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  /** Creates {@code Lucene40PostingsFormat} with custom
   *  values for {@code minBlockSize} and {@code
   *  maxBlockSize} passed to block terms dictionary.
   *  @see BlockTreeTermsWriter#BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int) */
  private Temp40RWPostingsFormat(int minBlockSize, int maxBlockSize) {
    super("Temp40RW");
    this.minBlockSize = minBlockSize;
    assert minBlockSize > 1;
    this.maxBlockSize = maxBlockSize;
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    TempPostingsReaderBase postings = new Temp40PostingsReader(state.directory, state.fieldInfos, state.segmentInfo, state.context, state.segmentSuffix);

    boolean success = false;
    try {
      FieldsProducer ret = new TempBlockTreeTermsReader(
                                                    state.directory,
                                                    state.fieldInfos,
                                                    state.segmentInfo,
                                                    postings,
                                                    state.context,
                                                    state.segmentSuffix);
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

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    TempPostingsWriterBase docs = new Temp40PostingsWriter(state);

    // TODO: should we make the terms index more easily
    // pluggable?  Ie so that this codec would record which
    // index impl was used, and switch on loading?
    // Or... you must make a new Codec for this?
    boolean success = false;
    try {
      FieldsConsumer ret = new TempBlockTreeTermsWriter(state, docs, minBlockSize, maxBlockSize);
      success = true;
      return ret;
    } finally {
      if (!success) {
        docs.close();
      }
    }
  }
}
