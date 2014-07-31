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
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

final class IDVersionPostingsWriter extends PostingsWriterBase {

  final static String TERMS_CODEC = "IDVersionPostingsWriterTerms";

  // Increment version to change it
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  final static IDVersionTermState emptyState = new IDVersionTermState();
  IDVersionTermState lastState;

  private int curDocID;
  int lastDocID;
  private int lastPosition;
  private long lastVersion;

  private final SegmentWriteState state;

  public IDVersionPostingsWriter(SegmentWriteState state) {
    this.state = state;
  }

  @Override
  public BlockTermState newTermState() {
    return new IDVersionTermState();
  }

  @Override
  public void init(IndexOutput termsOut) throws IOException {
    CodecUtil.writeHeader(termsOut, TERMS_CODEC, VERSION_CURRENT);
  }

  @Override
  public int setField(FieldInfo fieldInfo) {
    if (fieldInfo.getIndexOptions() != FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
      throw new IllegalArgumentException("field must be index using IndexOptions.DOCS_AND_FREQS_AND_POSITIONS");
    }
    // LUCENE-5693: because CheckIndex cross-checks term vectors with postings even for deleted docs, and because our PF only indexes the
    // non-deleted documents on flush, CheckIndex will see this as corruption:
    if (fieldInfo.hasVectors()) {
      throw new IllegalArgumentException("field cannot index term vectors: CheckIndex will report this as index corruption");
    }
    lastState = emptyState;
    return 0;
  }

  @Override
  public void startTerm() {
    lastDocID = -1;
    curDocID = -1;
  }

  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    // TODO: LUCENE-5693: we don't need this check if we fix IW to not send deleted docs to us on flush:
    //if (state.liveDocs != null && state.liveDocs.get(docID) == false) {
    //return;
    //}
    /*
    if (lastDocID != -1) {
      throw new IllegalArgumentException("term appears in more than one document");
    }
    */
    if (termDocFreq != 1) {
      throw new IllegalArgumentException("term appears more than once in the document");
    }

    curDocID = docID;
    lastPosition = -1;
    lastVersion = -1;
  }

  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
    //if (lastDocID == -1) {
    //// Doc is deleted; skip it
    //return;
    //}
    if (lastPosition != -1) {
      throw new IllegalArgumentException("term appears more than once in document");
    }
    lastPosition = position;
    if (payload == null) {
      throw new IllegalArgumentException("token doens't have a payload");
    }
    if (payload.length != 8) {
      throw new IllegalArgumentException("payload.length != 8 (got " + payload.length + ")");
    }

    long newVersion = IDVersionPostingsFormat.bytesToLong(payload);
    if (newVersion < IDVersionPostingsFormat.MIN_VERSION) {
      throw new IllegalArgumentException("version must be >= MIN_VERSION=" + IDVersionPostingsFormat.MIN_VERSION + " (got: " + lastVersion + "; payload=" + payload + ")");
    }
    if (newVersion > IDVersionPostingsFormat.MAX_VERSION) {
      throw new IllegalArgumentException("version must be <= MAX_VERSION=" + IDVersionPostingsFormat.MAX_VERSION + " (got: " + lastVersion + "; payload=" + payload + ")");
    }
    // In 5.0, IW's flush tells us which docs are deleted, so we know which doc to ignore during flush (because it was updated with a newer
    // version), but in 4.x IW doesn't tell us the deleted docs, so we instead only update if the version is newer.
    if (newVersion > lastVersion) {
      lastVersion = newVersion;
      lastDocID = curDocID;
    }
  }

  @Override
  public void finishDoc() throws IOException {
    //if (lastDocID == -1) {
    //// Doc is deleted; skip it
    //return;
    //}
    if (lastPosition == -1) {
      throw new IllegalArgumentException("missing addPosition");
    }
  }

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(BlockTermState _state) throws IOException {
    //if (lastDocID == -1) {
    //return;
    //}
    IDVersionTermState state = (IDVersionTermState) _state;
    assert state.docFreq > 0: "lastDocID=" + lastDocID;

    state.docID = lastDocID;
    state.idVersion = lastVersion;
  }

  private long lastEncodedVersion;

  @Override
  public void encodeTerm(long[] longs, DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute) throws IOException {
    IDVersionTermState state = (IDVersionTermState) _state;
    out.writeVInt(state.docID);
    if (absolute) {
      out.writeVLong(state.idVersion);
    } else {
      long delta = state.idVersion - lastEncodedVersion;
      out.writeZLong(delta);
    }
    lastEncodedVersion = state.idVersion;
  }

  @Override
  public void close() throws IOException {
  }
}
