package org.apache.lucene.codecs.appending;

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

import org.apache.lucene.codecs.BlockTreeTermsWriter;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.CodecUtil;

/**
 * Append-only version of {@link BlockTreeTermsWriter}
 * @lucene.experimental
 */
public class AppendingTermsWriter extends BlockTreeTermsWriter {
  final static String TERMS_CODEC_NAME = "APPENDING_TERMS_DICT";
  final static int TERMS_VERSION_START = 0;
  final static int TERMS_VERSION_CURRENT = TERMS_VERSION_START;
  
  final static String TERMS_INDEX_CODEC_NAME = "APPENDING_TERMS_INDEX";
  final static int TERMS_INDEX_VERSION_START = 0;
  final static int TERMS_INDEX_VERSION_CURRENT = TERMS_INDEX_VERSION_START;
  
  public AppendingTermsWriter(SegmentWriteState state, PostingsWriterBase postingsWriter, int minItemsInBlock, int maxItemsInBlock) throws IOException {
    super(state, postingsWriter, minItemsInBlock, maxItemsInBlock);
  }

  @Override
  protected void writeHeader(IndexOutput out) throws IOException {
    CodecUtil.writeHeader(out, TERMS_CODEC_NAME, TERMS_VERSION_CURRENT);
  }

  @Override
  protected void writeIndexHeader(IndexOutput out) throws IOException {
    CodecUtil.writeHeader(out, TERMS_INDEX_CODEC_NAME, TERMS_INDEX_VERSION_CURRENT);
  }

  @Override
  protected void writeTrailer(IndexOutput out, long dirStart) throws IOException {
    out.writeLong(dirStart);
  }

  @Override
  protected void writeIndexTrailer(IndexOutput indexOut, long dirStart) throws IOException {
    indexOut.writeLong(dirStart);
  }
}
