package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

/**
 * Copyright (c) 2013 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class PositionQueue extends PriorityQueue<PositionQueue.DocsEnumRef> {

  public class DocsEnumRef implements Comparable<DocsEnumRef> {

    public final DocsEnum docsEnum;
    public final int ord;
    public int start;
    public int end;

    public DocsEnumRef(DocsEnum docsEnum, int ord) {
      this.docsEnum = docsEnum;
      this.ord = ord;
    }

    public int nextPosition() throws IOException {
      assert docsEnum.docID() != -1;
      if (docsEnum.docID() == DocsEnum.NO_MORE_DOCS || docsEnum.docID() != docId
          || docsEnum.nextPosition() == DocsEnum.NO_MORE_POSITIONS) {
        start = end = DocsEnum.NO_MORE_POSITIONS;
      }
      else {
        start = docsEnum.startPosition();
        end = docsEnum.endPosition();
      }
      return start;
    }

    @Override
    public int compareTo(DocsEnumRef o) {
      if (this.docsEnum.docID() < o.docsEnum.docID())
        return -1;
      if (this.docsEnum.docID() > o.docsEnum.docID())
        return 1;
      return Integer.compare(this.start, o.start);
    }
  }

  boolean positioned = false;
  int pos = -1;
  int docId = -1;
  protected int queuesize;

  public PositionQueue(DocsEnum... subDocsEnums) {
    super(subDocsEnums.length);
    for (int i = 0; i < subDocsEnums.length; i++) {
      add(new DocsEnumRef(subDocsEnums[i], i));
    }
    queuesize = subDocsEnums.length;
  }

  protected void init() throws IOException {
    queuesize = 0;
    for (Object scorerRef : getHeapArray()) {
      if (scorerRef != null) {
        ((DocsEnumRef) scorerRef).nextPosition();
        queuesize++;
      }
    }
    updateTop();
  }

  public int nextPosition() throws IOException {
    if (!positioned) {
      init();
      positioned = true;
      return pos = top().start;
    }
    if (pos == DocsEnum.NO_MORE_POSITIONS)
      return DocsEnum.NO_MORE_POSITIONS;
    if (top().nextPosition() == DocsEnum.NO_MORE_POSITIONS)
      queuesize--;
    else
      updateInternalIntervals();
    updateTop();
    return pos = top().start;
  }

  @Override
  protected boolean lessThan(DocsEnumRef a, DocsEnumRef b) {
    return a.compareTo(b) < 0;
  }

  protected void updateInternalIntervals() throws IOException {}

  /**
   * Must be called after the scorers have been advanced
   */
  public void advanceTo(int doc) {
    positioned = false;
    this.docId = doc;
    this.queuesize = this.size();
  }

  public int startPosition() throws IOException {
    return top().docsEnum.startPosition();
  }

  public int endPosition() throws IOException {
    return top().docsEnum.endPosition();
  }

  public int startOffset() throws IOException {
    return top().docsEnum.startOffset();
  }

  public int endOffset() throws IOException {
    return top().docsEnum.endOffset();
  }

  public BytesRef getPayload() throws IOException {
    return top().docsEnum.getPayload();
  }


}
