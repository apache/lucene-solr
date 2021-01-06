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

package org.apache.lucene.luke.models.documents;

import java.io.IOException;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.BytesRef;

/** Holder for a term's position information, and optionally, offsets and payloads. */
public final class TermPosting {

  // position
  private int position = -1;

  // start and end offset (optional)
  private int startOffset = -1;
  private int endOffset = -1;

  // payload (optional)
  private BytesRef payload;

  static TermPosting of(int position, PostingsEnum penum) throws IOException {
    TermPosting posting = new TermPosting();

    // set position
    posting.position = position;

    // set offset (if available)
    int sOffset = penum.startOffset();
    int eOffset = penum.endOffset();
    if (sOffset >= 0 && eOffset >= 0) {
      posting.startOffset = sOffset;
      posting.endOffset = eOffset;
    }

    // set payload (if available)
    if (penum.getPayload() != null) {
      posting.payload = BytesRef.deepCopyOf(penum.getPayload());
    }

    return posting;
  }

  public int getPosition() {
    return position;
  }

  public int getStartOffset() {
    return startOffset;
  }

  public int getEndOffset() {
    return endOffset;
  }

  public BytesRef getPayload() {
    return payload;
  }

  @Override
  public String toString() {
    return "TermPosting{"
        + "position="
        + position
        + ", startOffset="
        + startOffset
        + ", endOffset="
        + endOffset
        + ", payload="
        + payload
        + '}';
  }

  private TermPosting() {}
}
