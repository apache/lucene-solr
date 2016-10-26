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


final class CompositePostingsEnum extends PostingsEnum {

  private static final int NO_MORE_POSITIONS = -2;
  private final BytesRef term;
  private final int freq;
  private final PriorityQueue<BoundsCheckingPostingsEnum> queue;


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
    private final int freq;
    private int position;
    private int nextPosition;
    private int positionInc = 1;

    private int startOffset;
    private int endOffset;

    BoundsCheckingPostingsEnum(PostingsEnum postingsEnum) throws IOException {
      this.postingsEnum = postingsEnum;
      this.freq = postingsEnum.freq();
      nextPosition = postingsEnum.nextPosition();
      position = nextPosition;
      startOffset = postingsEnum.startOffset();
      endOffset = postingsEnum.endOffset();
    }

    private boolean hasMorePositions() throws IOException {
      return positionInc < freq;
    }

    /**
     * Returns the next position of the underlying postings enum unless
     * it cannot iterate further and returns NO_MORE_POSITIONS;
     * @return
     * @throws IOException
     */
    private int nextPosition() throws IOException {
      position = nextPosition;
      startOffset = postingsEnum.startOffset();
      endOffset = postingsEnum.endOffset();
      if (hasMorePositions()) {
        positionInc++;
        nextPosition = postingsEnum.nextPosition();
      } else {
        nextPosition = NO_MORE_POSITIONS;
      }
      return position;
    }

  }

  CompositePostingsEnum(BytesRef term, List<PostingsEnum> postingsEnums) throws IOException {
    this.term = term;
    queue = new PriorityQueue<BoundsCheckingPostingsEnum>(postingsEnums.size()) {
      @Override
      protected boolean lessThan(BoundsCheckingPostingsEnum a, BoundsCheckingPostingsEnum b) {
        return a.position < b.position;
      }
    };

    int freqAdd = 0;
    for (PostingsEnum postingsEnum : postingsEnums) {
      queue.add(new BoundsCheckingPostingsEnum(postingsEnum));
      freqAdd += postingsEnum.freq();
    }
    freq = freqAdd;
  }

  @Override
  public int freq() throws IOException {
    return freq;
  }

  @Override
  public int nextPosition() throws IOException {
    int position = NO_MORE_POSITIONS;
    while (queue.size() >= 1) {
      queue.top().nextPosition();
      queue.updateTop(); //the new position may be behind another postingsEnum in the queue
      position = queue.top().position;

      if (position == NO_MORE_POSITIONS) {
        queue.pop(); //this postingsEnum is consumed, let's get rid of it
      } else {
        break; //we got a new position
      }

    }
    return position;
  }

  @Override
  public int startOffset() throws IOException {
    return queue.top().startOffset;
  }

  @Override
  public int endOffset() throws IOException {
    return queue.top().endOffset;
  }

  @Override
  public BytesRef getPayload() throws IOException {
    //The UnifiedHighlighter depends on the payload for a wildcard
    //being the term representing it
    return term;
  }

  @Override
  public int docID() {
    return queue.top().postingsEnum.docID();
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
