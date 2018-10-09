/*
 * This software was produced for the U. S. Government
 * under Contract No. W15P7T-11-C-F600, and is
 * subject to the Rights in Noncommercial Computer Software
 * and Noncommercial Computer Software Documentation
 * Clause 252.227-7014 (JUN 1995)
 *
 * Copyright 2013 The MITRE Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.tagger;

import java.util.Arrays;

import com.carrotsearch.hppc.IntArrayList;

public abstract class OffsetCorrector {

  //TODO support a streaming style of consuming input text so that we need not take a
  // String. Trickier because we need to keep more information as we parse to know when tags
  // are adjacent with/without whitespace

  //Data structure requirements:
  // Given a character offset:
  //   * determine what tagId is it's parent.
  //   * determine if it is adjacent to the parent open tag, ignoring whitespace
  //   * determine if it is adjacent to the parent close tag, ignoring whitespace
  // Given a tagId:
  //   * What is it's parent tagId
  //   * What's the char offset of the start and end of the open tag
  //   * What's the char offset of the start and end of the close tag

  /** Document text. */
  protected final String docText;

  /** Array of tag info comprised of 5 int fields:
   *    [int parentTag, int openStartOff, int openEndOff, int closeStartOff, int closeEndOff].
   * It's size indicates how many tags there are. Tag's are ID'ed sequentially from 0. */
  protected final IntArrayList tagInfo;

  /** offsets of parent tag id change (ascending order) */
  protected final IntArrayList parentChangeOffsets;
  /** tag id; parallel array to parentChangeOffsets */
  protected final IntArrayList parentChangeIds;

  protected final int[] offsetPair = new int[] { -1, -1};//non-thread-safe state

  /** Disjoint start and end span offsets (inclusive) of non-taggable sections. Null if none. */
  protected final IntArrayList nonTaggableOffsets;

  /**
   * Initialize based on the document text.
   * @param docText non-null structured content.
   * @param hasNonTaggable if there may be "non-taggable" tags to track
   */
  protected OffsetCorrector(String docText, boolean hasNonTaggable) {
    this.docText = docText;
    final int guessNumElements = Math.max(docText.length() / 20, 4);

    tagInfo = new IntArrayList(guessNumElements * 5);
    parentChangeOffsets = new IntArrayList(guessNumElements * 2);
    parentChangeIds = new IntArrayList(guessNumElements * 2);
    nonTaggableOffsets = hasNonTaggable ? new IntArrayList(guessNumElements / 5) : null;
  }

  /** Corrects the start and end offset pair. It will return null if it can't
   * due to a failure to keep the offsets balance-able, or if it spans "non-taggable" tags.
   * The start (left) offset is pulled left as needed over whitespace and opening tags. The end
   * (right) offset is pulled right as needed over whitespace and closing tags. It's returned as
   * a 2-element array.
   * <p>Note that the returned array is internally reused; just use it to examine the response.
   */
  public int[] correctPair(int leftOffset, int rightOffset) {
    rightOffset = correctEndOffsetForCloseElement(rightOffset);
    if (spansNonTaggable(leftOffset, rightOffset))
      return null;

    int startTag = lookupTag(leftOffset);
    //offsetPair[0] = Math.max(offsetPair[0], getOpenStartOff(startTag));
    int endTag = lookupTag(rightOffset-1);
    //offsetPair[1] = Math.min(offsetPair[1], getCloseStartOff(endTag));

    // Find the ancestor tag enclosing offsetPair.  And bump out left offset along the way.
    int iTag = startTag;
    for (; !tagEnclosesOffset(iTag, rightOffset); iTag = getParentTag(iTag)) {
      //Ensure there is nothing except whitespace thru OpenEndOff
      int tagOpenEndOff = getOpenEndOff(iTag);
      if (hasNonWhitespace(tagOpenEndOff, leftOffset))
        return null;
      leftOffset = getOpenStartOff(iTag);
    }
    final int ancestorTag = iTag;
    // Bump out rightOffset until we get to ancestorTag.
    for (iTag = endTag; iTag != ancestorTag; iTag = getParentTag(iTag)) {
      //Ensure there is nothing except whitespace thru CloseStartOff
      int tagCloseStartOff = getCloseStartOff(iTag);
      if (hasNonWhitespace(rightOffset, tagCloseStartOff))
        return null;
      rightOffset = getCloseEndOff(iTag);
    }

    offsetPair[0] = leftOffset;
    offsetPair[1] = rightOffset;
    return offsetPair;
  }

  /** Correct endOffset for adjacent element at the right side.  E.g. offsetPair might point to:
   * <pre>
   *   foo&lt;/tag&gt;
   * </pre>
   * and this method pulls the end offset left to the '&lt;'. This is necessary for use with
   * {@link org.apache.lucene.analysis.charfilter.HTMLStripCharFilter}.
   *
   * See https://issues.apache.org/jira/browse/LUCENE-5734 */
  protected int correctEndOffsetForCloseElement(int endOffset) {
    if (docText.charAt(endOffset-1) == '>') {
      final int newEndOffset = docText.lastIndexOf('<', endOffset - 2);
      if (newEndOffset > offsetPair[0])//just to be sure
        return newEndOffset;
    }
    return endOffset;
  }

  protected boolean hasNonWhitespace(int start, int end) {
    for (int i = start; i < end; i++) {
      if (!Character.isWhitespace(docText.charAt(i)))
        return true;
    }
    return false;
  }

  protected boolean tagEnclosesOffset(int tag, int off) {
    return off >= getOpenStartOff(tag) && off < getCloseEndOff(tag);
  }

  protected int getParentTag(int tag) { return tagInfo.get(tag * 5 + 0); }
  protected int getOpenStartOff(int tag) { return tagInfo.get(tag * 5 + 1); }
  protected int getOpenEndOff(int tag) { return tagInfo.get(tag * 5 + 2); }
  protected int getCloseStartOff(int tag) { return tagInfo.get(tag * 5 + 3); }
  protected int getCloseEndOff(int tag) { return tagInfo.get(tag * 5 + 4); }

  protected int lookupTag(int off) {
    int idx = Arrays.binarySearch(parentChangeOffsets.buffer, 0, parentChangeOffsets.size(), off);
    if (idx < 0)
      idx = (-idx - 1) - 1;//round down
    return parentChangeIds.get(idx);
  }

  protected boolean spansNonTaggable(int startOff, int endOff) {
    if (nonTaggableOffsets == null)
      return false;
    int idx = Arrays.binarySearch(nonTaggableOffsets.buffer, 0, nonTaggableOffsets.size(), startOff);
    //if tag start coincides with first or last char of non-taggable span then result is true.
    // (probably never happens since those characters are actual element markup)
    if (idx >= 0)
      return true;
    idx = -idx - 1;//modify for where we would insert
    //if idx is odd then our span intersects a non-taggable span; return true
    if ((idx & 1) == 1)
      return true;
    //it's non-taggable if the next non-taggable start span is before our endOff
    if (idx == nonTaggableOffsets.size())
      return false;
    return nonTaggableOffsets.get(idx) < endOff;
  }
}
