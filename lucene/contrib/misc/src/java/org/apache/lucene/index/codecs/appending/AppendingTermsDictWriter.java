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

import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.PostingsWriterBase;
import org.apache.lucene.index.codecs.BlockTermsWriter;
import org.apache.lucene.index.codecs.TermsIndexWriterBase;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.CodecUtil;

public class AppendingTermsDictWriter extends BlockTermsWriter {
  final static String CODEC_NAME = "APPENDING_TERMS_DICT";

  public AppendingTermsDictWriter(TermsIndexWriterBase indexWriter,
                                  SegmentWriteState state, PostingsWriterBase postingsWriter)
    throws IOException {
    super(indexWriter, state, postingsWriter);
  }
  
  @Override
  protected void writeHeader(IndexOutput out) throws IOException {
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT); 
  }

  @Override
  protected void writeTrailer(long dirStart) throws IOException {
    out.writeLong(dirStart);
  }
}
