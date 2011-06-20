package org.apache.lucene.index.codecs.appending;

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

import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IOContext;
import org.apache.lucene.index.codecs.PostingsReaderBase;
import org.apache.lucene.index.codecs.BlockTermsReader;
import org.apache.lucene.index.codecs.BlockTermsWriter;
import org.apache.lucene.index.codecs.TermsIndexReaderBase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.CodecUtil;

public class AppendingTermsDictReader extends BlockTermsReader {

  public AppendingTermsDictReader(TermsIndexReaderBase indexReader,
          Directory dir, FieldInfos fieldInfos, String segment,
          PostingsReaderBase postingsReader, IOContext context,
          int termsCacheSize, int codecId) throws IOException {
    super(indexReader, dir, fieldInfos, segment, postingsReader, context,
          termsCacheSize, codecId);
  }
  
  @Override
  protected void readHeader(IndexInput in) throws IOException {
    CodecUtil.checkHeader(in, AppendingTermsDictWriter.CODEC_NAME,
      BlockTermsWriter.VERSION_START, BlockTermsWriter.VERSION_CURRENT);    
  }

  @Override
  protected void seekDir(IndexInput in, long dirOffset) throws IOException {
    in.seek(in.length() - Long.SIZE / 8);
    long offset = in.readLong();
    in.seek(offset);
  }

}
