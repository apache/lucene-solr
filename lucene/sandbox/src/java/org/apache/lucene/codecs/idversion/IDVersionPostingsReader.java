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
package org.apache.lucene.codecs.idversion;

import java.io.IOException;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;

final class IDVersionPostingsReader extends PostingsReaderBase {

  @Override
  public void init(IndexInput termsIn, SegmentReadState state) throws IOException {
    // Make sure we are talking to the matching postings writer
    CodecUtil.checkIndexHeader(termsIn,
                                 IDVersionPostingsWriter.TERMS_CODEC,
                                 IDVersionPostingsWriter.VERSION_START,
                                 IDVersionPostingsWriter.VERSION_CURRENT,
                                 state.segmentInfo.getId(), state.segmentSuffix);
  }

  @Override
  public BlockTermState newTermState() {
    return new IDVersionTermState();
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void decodeTerm(DataInput in, FieldInfo fieldInfo, BlockTermState _termState, boolean absolute)
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
  public PostingsEnum postings(FieldInfo fieldInfo, BlockTermState termState, PostingsEnum reuse, int flags) throws IOException {
    SingleDocsEnum docsEnum;

    if (PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) {
      SinglePostingsEnum posEnum;

      if (reuse instanceof SinglePostingsEnum) {
        posEnum = (SinglePostingsEnum) reuse;
      } else {
        posEnum = new SinglePostingsEnum();
      }
      IDVersionTermState _termState = (IDVersionTermState) termState;
      posEnum.reset(_termState.docID, _termState.idVersion);
      return posEnum;
    }

    if (reuse instanceof SingleDocsEnum) {
      docsEnum = (SingleDocsEnum) reuse;
    } else {
      docsEnum = new SingleDocsEnum();
    }
    docsEnum.reset(((IDVersionTermState) termState).docID);

    return docsEnum;
  }

  @Override
  public ImpactsEnum impacts(FieldInfo fieldInfo, BlockTermState state, int flags) throws IOException {
    throw new UnsupportedOperationException("Should never be called, IDVersionSegmentTermsEnum implements impacts directly");
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }

  @Override
  public void checkIntegrity() throws IOException {
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
