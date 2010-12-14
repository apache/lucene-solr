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
 *  term freq and positions).  This API shifts much
 *  responsibility to the caller, in order to maximize
 *  performance:
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
 *    * Avoid reading too many ints, ie, impls of this API
 *      do not do bounds checking
 *
 *  @lucene.experimental */
public abstract class BulkPostingsEnum {

  /** NOTE: on first obtaining the BlockReader it's possible
   *  there's data in the buffer.  Use offset/end to check. */
  public static abstract class BlockReader {

    /** Returns int[] that holds each block. Call this once
     *  up front before you start iterating. */
    public abstract int[] getBuffer();
    
    /** Read another block. Returns the count read, or 0 on
     *  EOF. */
    public abstract int fill() throws IOException;

    /** End index plus 1 of valid data in the buffer */
    public abstract int end();

    /** Start index of valid data in the buffer */
    public abstract int offset();

    // nocommit messy
    public abstract void setOffset(int offset);

    // nocommit messy
    public int next() throws IOException {
      final int[] buffer = getBuffer();
      int offset = offset();
      int end = end();
      if (offset >= end) {
        offset = 0;
        end = fill();
        if (offset >= end) {
          // nocommit cleanup
          throw new IOException("no more ints");
        }
      }
      setOffset(1+offset);
      return buffer[offset];
    }

    /** Reads long as 1 or 2 ints, and can only use 61 of
     *  the 64 long bits. */
    public long readVLong() throws IOException {
      int offset = offset();
      
      final int v = next();
      if ((v & 1) == 0) {
        return v >> 1;
      } else {
        final long v2 = next();
        return (v2 << 30) | (v >> 1);
      }
    }
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
  abstract public JumpResult jump(int target, int curCount) throws IOException;
}
