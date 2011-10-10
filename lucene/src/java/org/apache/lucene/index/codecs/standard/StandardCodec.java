package org.apache.lucene.index.codecs.standard;

/**
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
import java.util.Set;

import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.PostingsReaderBase;
import org.apache.lucene.index.codecs.PostingsWriterBase;
import org.apache.lucene.index.codecs.BlockTreeTermsReader;
import org.apache.lucene.index.codecs.BlockTreeTermsWriter;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.DefaultDocValuesConsumer;
import org.apache.lucene.index.codecs.DefaultDocValuesProducer;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.PerDocConsumer;
import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.store.Directory;

/** Default codec. 
 *  @lucene.experimental */
public class StandardCodec extends Codec {

  private final int minBlockSize;
  private final int maxBlockSize;

  public StandardCodec() {
    this(BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  public StandardCodec(int minBlockSize, int maxBlockSize) {
    super("Standard");
    this.minBlockSize = minBlockSize;
    assert minBlockSize > 1;
    this.maxBlockSize = maxBlockSize;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase docs = new StandardPostingsWriter(state);

    // TODO: should we make the terms index more easily
    // pluggable?  Ie so that this codec would record which
    // index impl was used, and switch on loading?
    // Or... you must make a new Codec for this?
    boolean success = false;
    try {
      FieldsConsumer ret = new BlockTreeTermsWriter(state, docs, minBlockSize, maxBlockSize);
      success = true;
      return ret;
    } finally {
      if (!success) {
        docs.close();
      }
    }
  }

  public final static int TERMS_CACHE_SIZE = 1024;

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    PostingsReaderBase postings = new StandardPostingsReader(state.dir, state.segmentInfo, state.context, state.codecId);

    boolean success = false;
    try {
      FieldsProducer ret = new BlockTreeTermsReader(
                                                    state.dir,
                                                    state.fieldInfos,
                                                    state.segmentInfo.name,
                                                    postings,
                                                    state.context,
                                                    state.codecId,
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
  public void files(Directory dir, SegmentInfo segmentInfo, int codecID, Set<String> files) throws IOException {
    StandardPostingsReader.files(dir, segmentInfo, codecID, files);
    BlockTreeTermsReader.files(dir, segmentInfo, codecID, files);
    DefaultDocValuesConsumer.files(dir, segmentInfo, codecID, files);
  }

  @Override
  public void getExtensions(Set<String> extensions) {
    getStandardExtensions(extensions);
  }

  public static void getStandardExtensions(Set<String> extensions) {
    extensions.add(FREQ_EXTENSION);
    extensions.add(PROX_EXTENSION);
    BlockTreeTermsReader.getExtensions(extensions);
    DefaultDocValuesConsumer.getExtensions(extensions);
  }

  @Override
  public String toString() {
    return name + "(minBlockSize=" + minBlockSize + " maxBlockSize=" + maxBlockSize + ")";
  }

  @Override
  public PerDocConsumer docsConsumer(PerDocWriteState state) throws IOException {
    return new DefaultDocValuesConsumer(state);
  }

  @Override
  public PerDocValues docsProducer(SegmentReadState state) throws IOException {
    return new DefaultDocValuesProducer(state);
  }
}
