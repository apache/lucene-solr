package org.apache.lucene.index;

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

/** Low level bulk iterator through postings (documents,
 *  term freq, positions).  This API shifts much
 *  responsibility to the caller, in order to maximize
 *  performance (so that low-level intblock codecs can
 *  directly share their internal buffer with the caller
 *  without any further processing after decode):
 *
 *    * Caller must track and enforce docFreq limit
 *    * If omitTFAP is on, caller must handle null
 *      freq/pos reader
 *    * Accum docDeltas to get docIDs
 *    * Accum posDeltas to get positions
 *    * Correlate positions to docs by adding up termFreqs
 *    * Enforce skipDocs
 *    * Jump is not precise -- caller must still scan after
 *      a successful jump
 *
 *  When you first obtain a BulkPostingsEnum, and also after
 *  a call to {@link #jump} that returns a non-null result,
 *  the shared int[] buffers will contain valid data; use
 *  offset/end to get the bounds.
 *
 *  @lucene.experimental */

// nocommit -- should we expose whether doc/freq intblocks
// are aligned?  consumers like TermScorer can specialize in
// this case -- end() is always BLOCK_SIZE, and they will
// alway fill in synchrony
public abstract class BulkPostingsEnum {

  /** NOTE: on first obtaining the BlockReader it's possible
   *  there's data in the buffer.  Use offset/end to check. */
  public static abstract class BlockReader {

    /** Returns shared int[] that holds each block. Call
     *  this once up front before you start iterating. */
    public abstract int[] getBuffer();

    /** Read the next block.  Returns the count read, always
     *  greater than 0 (it is the caller's responsibility to
     *  not call this beyond the last block; there is no
     *  internal bounds checking in implementations).
     *  After fill, the offset (where valid data starts) is
     *  always 0. */
    public abstract int fill() throws IOException;

    // nocommit -- need a skip(int count)

    /** End index plus 1 of valid data in the buffer. */
    public abstract int end();

    /** Start index of valid data in the buffer.  This is
     *  only valid after first obtaining a BulkPostingsEnum
     *  or after a successful jump.  Once you call fill,
     *  you should not call this method anymore (the offset
     *  is always 0). */
    public abstract int offset();

    // nocommit messy
    public abstract void setOffset(int offset);
  }

  public abstract BlockReader getDocDeltasReader() throws IOException;

  /** Returns null if per-document term freq is not indexed */
  public abstract BlockReader getFreqsReader() throws IOException;

  /** Returns null if positions are not indexed */
  public abstract BlockReader getPositionDeltasReader() throws IOException;

  public static class JumpResult {
    public int count;
    public int docID;
  }

  /** Only call this if the docID you seek is after the last
   *  document in the buffer.  This call does not position
   *  exactly; instead, it jumps forward when possible,
   *  returning the docID and ord it had jumped to, seeking
   *  all of the BlockReaders accordingly.  Note that if a
   *  seek did occur, you must call .offset() and .limit()
   *  on each BlockReader.  If null is returned then
   *  skipping is not possible, ie you should just scan
   *  yourself). The returned JumpResult will only be valid until
   *  the next call to jump. */
  abstract public JumpResult jump(int targetDocID, int curCount) throws IOException;
}
