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

package org.apache.lucene.codecs.uniformsplit.sharedterms;

import java.io.IOException;
import java.util.List;
import java.util.RandomAccess;

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

/**
 * Combines {@link PostingsEnum} for the same term for a given field from
 * multiple segments. It is used during segment merging.
 *
 * @lucene.experimental
 */
public class STMergingTermsEnum extends TermsEnum {

  protected final String fieldName;
  protected final MultiSegmentsPostingsEnum multiPostingsEnum;
  protected BytesRef term;

  /**
   * Constructs a {@link STMergingTermsEnum} for a given field.
   */
  protected STMergingTermsEnum(String fieldName, int numSegments) {
    this.fieldName = fieldName;
    multiPostingsEnum = new MultiSegmentsPostingsEnum(numSegments);
  }

  /**
   * Resets this {@link STMergingTermsEnum} with a new term and its list of
   * {@link STUniformSplitTermsWriter.SegmentPostings} to combine.
   *
   * @param segmentPostings List sorted by segment index.
   */
  protected void reset(BytesRef term, List<STUniformSplitTermsWriter.SegmentPostings> segmentPostings) {
    this.term = term;
    multiPostingsEnum.reset(segmentPostings);
  }

  @Override
  public AttributeSource attributes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean seekExact(BytesRef text) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SeekStatus seekCeil(BytesRef text) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seekExact(long ord) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seekExact(BytesRef term, TermState state) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public BytesRef term() {
    return term;
  }

  @Override
  public long ord() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int docFreq() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long totalTermFreq() {
    throw new UnsupportedOperationException();
  }

  @Override
  public PostingsEnum postings(PostingsEnum reuse, int flags) {
    multiPostingsEnum.setPostingFlags(flags);
    return multiPostingsEnum;
  }

  @Override
  public ImpactsEnum impacts(int flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TermState termState() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BytesRef next() {
    throw new UnsupportedOperationException();
  }

  /**
   * Combines multiple segments {@link PostingsEnum} as a single {@link PostingsEnum},
   * for one field and one term.
   * <p>
   * This {@link PostingsEnum} does not extend {@link org.apache.lucene.index.FilterLeafReader.FilterPostingsEnum}
   * because it updates the delegate for each segment.
   */
  protected class MultiSegmentsPostingsEnum extends PostingsEnum {

    protected final PostingsEnum[] reusablePostingsEnums;
    protected List<STUniformSplitTermsWriter.SegmentPostings> segmentPostingsList;
    protected int segmentIndex;
    protected PostingsEnum postingsEnum;
    protected boolean postingsEnumExhausted;
    protected MergeState.DocMap docMap;
    protected int docId;
    protected int postingsFlags;

    protected MultiSegmentsPostingsEnum(int numSegments) {
      reusablePostingsEnums = new PostingsEnum[numSegments];
    }

    /**
     * Resets/reuse this {@link PostingsEnum}.
     * @param segmentPostingsList List of segment postings ordered by segment index.
     */
    protected void reset(List<STUniformSplitTermsWriter.SegmentPostings> segmentPostingsList) {
      assert segmentPostingsList instanceof RandomAccess
          : "We optimize by accessing the list elements instead of creating an Iterator";
      this.segmentPostingsList = segmentPostingsList;
      segmentIndex = -1;
      postingsEnumExhausted = true;
      docId = -1;
    }

    protected void setPostingFlags(int flags) {
      this.postingsFlags = flags;
    }

    @Override
    public int freq() throws IOException {
      return postingsEnum.freq();
    }

    @Override
    public int nextPosition() throws IOException {
      return postingsEnum.nextPosition();
    }

    @Override
    public int startOffset() throws IOException {
      return postingsEnum.startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      return postingsEnum.endOffset();
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return postingsEnum.getPayload();
    }

    @Override
    public int docID() {
      return docId;
    }

    @Override
    public int nextDoc() throws IOException {
      assert segmentPostingsList != null : "reset not called";
      while (true) {
        if (postingsEnumExhausted) {
          if (segmentIndex == segmentPostingsList.size() - 1) {
            return docId = NO_MORE_DOCS;
          } else {
            segmentIndex++;
            STUniformSplitTermsWriter.SegmentPostings segmentPostings =
                segmentPostingsList.get(segmentIndex);
            postingsEnum = getPostings(segmentPostings);
            postingsEnumExhausted = false;
            docMap = segmentPostings.docMap;
          }
        }
        int docId = postingsEnum.nextDoc();
        if (docId == NO_MORE_DOCS) {
          postingsEnumExhausted = true;
        } else {
          docId = docMap.get(docId);
          if (docId != -1) {
            assert docId > this.docId : "next docId=" + docId + ", current docId=" + this.docId;
            return this.docId = docId;
          }
        }
      }
    }

    protected PostingsEnum getPostings(STUniformSplitTermsWriter.SegmentPostings segmentPostings) throws IOException {
      // The field is present in the segment because it is one of the segments provided in the reset() method.
      return reusablePostingsEnums[segmentPostings.segmentIndex] =
                      segmentPostings.getPostings(fieldName, reusablePostingsEnums[segmentPostings.segmentIndex], postingsFlags);
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return 0; // Cost is not useful here.
    }
  }
}
