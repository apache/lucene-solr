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
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.TermState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

public final class IDVersionPostingsWriter extends PushPostingsWriterBase {

  final static String TERMS_CODEC = "IDVersionPostingsWriterTerms";

  // Increment version to change it
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  final static IDVersionTermState emptyState = new IDVersionTermState();
  IDVersionTermState lastState;

  private int lastDocID;
  private int lastPosition;
  private long lastVersion;

  @Override
  public IDVersionTermState newTermState() {
    return new IDVersionTermState();
  }

  @Override
  public void init(IndexOutput termsOut) throws IOException {
    CodecUtil.writeHeader(termsOut, TERMS_CODEC, VERSION_CURRENT);
  }

  @Override
  public int setField(FieldInfo fieldInfo) {
    super.setField(fieldInfo);
    if (fieldInfo.getIndexOptions() != FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
      throw new IllegalArgumentException("field must be index using IndexOptions.DOCS_AND_FREQS_AND_POSITIONS");
    }
    lastState = emptyState;
    return 0;
  }

  @Override
  public void startTerm() {
    lastDocID = -1;
  }

  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    if (lastDocID != -1) {
      // nocommit need test
      throw new IllegalArgumentException("term appears in more than one document");
    }
    if (termDocFreq != 1) {
      // nocommit need test
      throw new IllegalArgumentException("term appears more than once in the document");
    }

    lastDocID = docID;
    lastPosition = -1;
  }

  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
    if (lastPosition != -1) {
      // nocommit need test
      throw new IllegalArgumentException("term appears more than once in document");
    }
    lastPosition = position;
    if (payload == null) {
      // nocommit need test
      throw new IllegalArgumentException("missing payload");
    }
    if (payload.length == 0) {
      // nocommit need test
      throw new IllegalArgumentException("payload.length == 0");
    }
    // nocommit decode payload to long here ... PayloadHelper!?  or keep as byte[]?
    lastVersion = 0;
  }

  @Override
  public void finishDoc() throws IOException {
    if (lastPosition == -1) {
      // nocommit need test
      throw new IllegalArgumentException("missing addPosition");
    }
  }

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(BlockTermState _state) throws IOException {
    IDVersionTermState state = (IDVersionTermState) _state;
    assert state.docFreq > 0;

    assert lastDocID != -1;
    state.docID = lastDocID;
    state.idVersion = lastVersion;
  }
  
  @Override
  public void encodeTerm(long[] longs, DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute) throws IOException {
    IDVersionTermState state = (IDVersionTermState) _state;
    out.writeVInt(state.docID);
    out.writeVLong(state.idVersion);
  }

  @Override
  public void close() throws IOException {
  }
}
