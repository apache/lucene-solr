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

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.Bits;

final class IDVersionPostingsReader extends PostingsReaderBase {

  @Override
  public void init(IndexInput termsIn) throws IOException {
    // Make sure we are talking to the matching postings writer
    CodecUtil.checkHeader(termsIn,
                          IDVersionPostingsWriter.TERMS_CODEC,
                          IDVersionPostingsWriter.VERSION_START,
                          IDVersionPostingsWriter.VERSION_CURRENT);
  }

  @Override
  public BlockTermState newTermState() {
    return new IDVersionTermState();
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void decodeTerm(long[] longs, DataInput in, FieldInfo fieldInfo, BlockTermState _termState, boolean absolute)
    throws IOException {
    final IDVersionTermState termState = (IDVersionTermState) _termState;
    termState.docID = in.readVInt();
    if (absolute) {
      termState.idVersion = in.readVLong();
    } else {
      termState.idVersion += in.readZLong();
    }
  }

  @Override
  public DocsEnum docs(FieldInfo fieldInfo, BlockTermState termState, Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
    SingleDocsEnum docsEnum;

    if (reuse instanceof SingleDocsEnum) {
      docsEnum = (SingleDocsEnum) reuse;
    } else {
      docsEnum = new SingleDocsEnum();
    }
    docsEnum.reset(((IDVersionTermState) termState).docID, liveDocs);

    return docsEnum;
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(FieldInfo fieldInfo, BlockTermState _termState, Bits liveDocs,
                                               DocsAndPositionsEnum reuse, int flags) {
    SingleDocsAndPositionsEnum posEnum;

    if (reuse instanceof SingleDocsAndPositionsEnum) {
      posEnum = (SingleDocsAndPositionsEnum) reuse;
    } else {
      posEnum = new SingleDocsAndPositionsEnum();
    }
    IDVersionTermState termState = (IDVersionTermState) _termState;
    posEnum.reset(termState.docID, termState.idVersion, liveDocs);
    return posEnum;
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }

  @Override
  public void checkIntegrity() throws IOException {
  }
}
