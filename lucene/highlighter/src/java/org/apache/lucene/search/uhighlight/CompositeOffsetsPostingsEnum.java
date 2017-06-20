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
package org.apache.lucene.search.uhighlight;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

/**
 * Provides a view over several underlying PostingsEnums for the iteration of offsets on the current document only.
 * It's not general purpose; the position returned is always -1 and it doesn't iterate the documents.
 */
final class CompositeOffsetsPostingsEnum extends PostingsEnum {

  private final int docId;
  private final int freq;
  private final PriorityQueue<BoundsCheckingPostingsEnum> queue;
  private boolean firstPositionConsumed = false;

  /**
   * This class is used to ensure we don't over iterate the underlying
   * postings enum by keeping track of the position relative to the
   * frequency.
   * Ideally this would've been an implementation of a PostingsEnum
   * but it would have to delegate most methods and it seemed easier
   * to just wrap the tweaked method.
   */
  private static final class BoundsCheckingPostingsEnum {

    private final PostingsEnum postingsEnum;
    private int remainingPositions;

    BoundsCheckingPostingsEnum(PostingsEnum postingsEnum) throws IOException {
      this.postingsEnum = postingsEnum;
      this.remainingPositions = postingsEnum.freq();
      nextPosition();
    }

    /** Advances to the next position and returns true, or returns false if it can't. */
    private boolean nextPosition() throws IOException {
      if (remainingPositions-- > 0) {
        postingsEnum.nextPosition(); // ignore the actual position; we don't care.
        return true;
      } else {
        return false;
      }
    }

  }

  /** The provided {@link PostingsEnum}s must all be positioned to the same document, and must have offsets. */
  CompositeOffsetsPostingsEnum(List<PostingsEnum> postingsEnums) throws IOException {
    queue = new PriorityQueue<BoundsCheckingPostingsEnum>(postingsEnums.size()) {
      @Override
      protected boolean lessThan(BoundsCheckingPostingsEnum a, BoundsCheckingPostingsEnum b) {
        try {
          return a.postingsEnum.startOffset() < b.postingsEnum.startOffset();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };

    int freqAdd = 0;
    for (PostingsEnum postingsEnum : postingsEnums) {
      queue.add(new BoundsCheckingPostingsEnum(postingsEnum));
      freqAdd += postingsEnum.freq();
    }
    freq = freqAdd;
    this.docId = queue.top().postingsEnum.docID();
  }

  @Override
  public int freq() throws IOException {
    return freq;
  }

  /** Advances to the next position. Always returns -1; the caller is assumed not to care for the highlighter.  */
  @Override
  public int nextPosition() throws IOException {
    if (!firstPositionConsumed) {
      firstPositionConsumed = true;
    } else if (queue.size() == 0) {
      throw new IllegalStateException("nextPosition called too many times");
    } else if (queue.top().nextPosition()) { // advance head
      queue.updateTop(); //the new position may be behind another postingsEnum in the queue
    } else {
      queue.pop(); //this postingsEnum is consumed; get rid of it. Another will take it's place.
    }
    assert queue.size() > 0;
    return -1;
  }

  @Override
  public int startOffset() throws IOException {
    return queue.top().postingsEnum.startOffset();
  }

  @Override
  public int endOffset() throws IOException {
    return queue.top().postingsEnum.endOffset();
  }

  @Override
  public BytesRef getPayload() throws IOException {
    return queue.top().postingsEnum.getPayload();
  }

  @Override
  public int docID() {
    return docId;
  }

  @Override
  public int nextDoc() throws IOException {
    return NO_MORE_DOCS;
  }

  @Override
  public int advance(int target) throws IOException {
    return NO_MORE_DOCS;
  }

  @Override
  public long cost() {
    return 1L; //at most 1 doc is returned
  }
}
