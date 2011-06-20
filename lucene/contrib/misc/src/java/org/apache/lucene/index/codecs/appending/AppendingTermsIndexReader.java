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
import java.util.Comparator;

import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IOContext;
import org.apache.lucene.index.codecs.FixedGapTermsIndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;

public class AppendingTermsIndexReader extends FixedGapTermsIndexReader {

  public AppendingTermsIndexReader(Directory dir, FieldInfos fieldInfos,
          String segment, int indexDivisor, Comparator<BytesRef> termComp, int codecId)
          throws IOException {
    super(dir, fieldInfos, segment, indexDivisor, termComp, codecId, IOContext.DEFAULT);
  }
  
  @Override
  protected void readHeader(IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, AppendingTermsIndexWriter.CODEC_NAME,
      AppendingTermsIndexWriter.VERSION_START, AppendingTermsIndexWriter.VERSION_START);    
  }

  @Override
  protected void seekDir(IndexInput input, long dirOffset) throws IOException {
    input.seek(input.length() - Long.SIZE / 8);
    long offset = input.readLong();
    input.seek(offset);
  }
}
