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

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.LiveFieldValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/** A PostingsFormat optimized for primary-key (ID) fields that also
 *  record a version (long) for each ID, delivered as a payload
 *  created by {@link #longToBytes} during indexing.  At search time,
 *  the TermsEnum implementation {@link IDVersionSegmentTermsEnum}
 *  enables fast (using only the terms index when possible) lookup for
 *  whether a given ID was previously indexed with version &gt; N (see
 *  {@link IDVersionSegmentTermsEnum#seekExact(BytesRef,long)}.
 *
 *  <p>This is most effective if the app assigns monotonically
 *  increasing global version to each indexed doc.  Then, during
 *  indexing, use {@link
 *  IDVersionSegmentTermsEnum#seekExact(BytesRef,long)} (along with
 *  {@link LiveFieldValues}) to decide whether the document you are
 *  about to index was already indexed with a higher version, and skip
 *  it if so.
 *
 *  <p>The field is effectively indexed as DOCS_ONLY and the docID is
 *  pulsed into the terms dictionary, but the user must feed in the
 *  version as a payload on the first token.
 *
 *  <p>NOTE: term vectors cannot be indexed with this field (not that
 *  you should really ever want to do this).
 *
 *  @lucene.experimental */

public class IDVersionPostingsFormat extends PostingsFormat {

  /** version must be &gt;= this. */
  public static final long MIN_VERSION = 0;

  // TODO: we could delta encode instead, and keep the last bit:

  /** version must be &lt;= this, because we encode with ZigZag. */
  public static final long MAX_VERSION = 0x3fffffffffffffffL;

  private final int minTermsInBlock;
  private final int maxTermsInBlock;

  public IDVersionPostingsFormat() {
    this(BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  public IDVersionPostingsFormat(int minTermsInBlock, int maxTermsInBlock) {
    super("IDVersion");
    this.minTermsInBlock = minTermsInBlock;
    this.maxTermsInBlock = maxTermsInBlock;
    BlockTreeTermsWriter.validateSettings(minTermsInBlock, maxTermsInBlock);
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new IDVersionPostingsWriter(state.liveDocs);
    boolean success = false;
    try {
      FieldsConsumer ret = new VersionBlockTreeTermsWriter(state, 
                                                           postingsWriter,
                                                           minTermsInBlock, 
                                                           maxTermsInBlock);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsWriter);
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    PostingsReaderBase postingsReader = new IDVersionPostingsReader();
    boolean success = false;
     try {
       FieldsProducer ret = new VersionBlockTreeTermsReader(postingsReader, state);
       success = true;
       return ret;
     } finally {
       if (!success) {
         IOUtils.closeWhileHandlingException(postingsReader);
       }
     }
  }

  public static long bytesToLong(BytesRef bytes) {
    return ((bytes.bytes[bytes.offset]&0xFFL) << 56) |
      ((bytes.bytes[bytes.offset+1]&0xFFL) << 48) |
      ((bytes.bytes[bytes.offset+2]&0xFFL) << 40) |
      ((bytes.bytes[bytes.offset+3]&0xFFL) << 32) |
      ((bytes.bytes[bytes.offset+4]&0xFFL) << 24) |
      ((bytes.bytes[bytes.offset+5]&0xFFL) << 16) |
      ((bytes.bytes[bytes.offset+6]&0xFFL) << 8) |
      (bytes.bytes[bytes.offset+7]&0xFFL);
  }

  public static void longToBytes(long v, BytesRef bytes) {
    if (v > MAX_VERSION || v < MIN_VERSION) {
      throw new IllegalArgumentException("version must be >= MIN_VERSION=" + MIN_VERSION + " and <= MAX_VERSION=" + MAX_VERSION + " (got: " + v + ")");
    }
    bytes.offset = 0;
    bytes.length = 8;
    bytes.bytes[0] = (byte) (v >> 56);
    bytes.bytes[1] = (byte) (v >> 48);
    bytes.bytes[2] = (byte) (v >> 40);
    bytes.bytes[3] = (byte) (v >> 32);
    bytes.bytes[4] = (byte) (v >> 24);
    bytes.bytes[5] = (byte) (v >> 16);
    bytes.bytes[6] = (byte) (v >> 8);
    bytes.bytes[7] = (byte) v;
    assert bytesToLong(bytes) == v: bytesToLong(bytes) + " vs " + v + " bytes=" + bytes;
  }
}
