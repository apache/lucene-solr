package org.apache.lucene.codecs.appending;

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
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

/**
 * Reads append-only terms from AppendingTermsWriter.
 * @lucene.experimental
 * @deprecated Only for reading old Appending segments
 */
@Deprecated
public class AppendingTermsReader extends BlockTreeTermsReader {

  final static String APPENDING_TERMS_CODEC_NAME = "APPENDING_TERMS_DICT";
  final static int APPENDING_TERMS_VERSION_START = 0;
  final static int APPENDING_TERMS_VERSION_CURRENT = APPENDING_TERMS_VERSION_START;
  
  final static String APPENDING_TERMS_INDEX_CODEC_NAME = "APPENDING_TERMS_INDEX";
  final static int APPENDING_TERMS_INDEX_VERSION_START = 0;
  final static int APPENDING_TERMS_INDEX_VERSION_CURRENT = APPENDING_TERMS_INDEX_VERSION_START;
  
  public AppendingTermsReader(Directory dir, FieldInfos fieldInfos, SegmentInfo info, PostingsReaderBase postingsReader, 
      IOContext ioContext, String segmentSuffix, int indexDivisor) throws IOException {
    super(dir, fieldInfos, info, postingsReader, ioContext, segmentSuffix, indexDivisor);
  }

  @Override
  protected int readHeader(IndexInput input) throws IOException {
    return CodecUtil.checkHeader(input, APPENDING_TERMS_CODEC_NAME,
        APPENDING_TERMS_VERSION_START,
        APPENDING_TERMS_VERSION_CURRENT);  
  }

  @Override
  protected int readIndexHeader(IndexInput input) throws IOException {
    return CodecUtil.checkHeader(input, APPENDING_TERMS_INDEX_CODEC_NAME,
        APPENDING_TERMS_INDEX_VERSION_START,
        APPENDING_TERMS_INDEX_VERSION_CURRENT);
  }
  
  @Override
  protected void seekDir(IndexInput input, long dirOffset) throws IOException {
    input.seek(input.length() - Long.SIZE / 8);
    long offset = input.readLong();
    input.seek(offset);
  }
}
