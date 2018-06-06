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

public interface TagClusterReducer {
  /**
   * Reduces the linked-list to only those tags that should be emitted
   * @param head not null; 1-element array to head which isn't null either
   */
  void reduce(TagLL[] head);

  static final TagClusterReducer ALL = new TagClusterReducer() {
    @Override
    public void reduce(TagLL[] head) {
    }
  };

  static final TagClusterReducer NO_SUB = new TagClusterReducer() {
    @Override
    public void reduce(TagLL[] head) {
      //loop forward over all tags
      for (TagLL tag = head[0].nextTag; tag != null; tag = tag.nextTag) {
        //loop backwards over prev tags from this tag
        for (TagLL tPrev = tag.prevTag; tPrev != null; tPrev = tPrev.prevTag) {
          assert tPrev.startOffset <= tag.startOffset;
          //if a previous tag's endOffset is <= this one's, tForward can be removed
          if (tPrev.endOffset >= tag.endOffset) {
            tag.removeLL();
            break;
          } else if (tPrev.startOffset == tag.startOffset) {
            tPrev.removeLL();
            //continue; 'tag' is still valid
          }
        }
      }
    }
  };

  static final TagClusterReducer LONGEST_DOMINANT_RIGHT = new TagClusterReducer() {
    @Override
    public void reduce(TagLL[] head) {

      //--Optimize for common single-tag case
      if (head[0].nextTag == null)
        return;

      while (true) {
        //--Find longest not already marked
        TagLL longest = null;
        for (TagLL t = head[0]; t != null; t = t.nextTag) {
          if (!t.mark && (longest == null || t.charLen() >= longest.charLen()))
            longest = t;
        }
        if (longest == null)
          break;
        //--Mark longest (so we return it eventually)
        longest.mark = true;
        //--Remove tags overlapping this longest
        for (TagLL t = head[0]; t != null; t = t.nextTag) {
          if (t.mark)
            continue;

          if (t.overlaps(longest)) {
            t.removeLL();
          } else if (t.startOffset >= longest.endOffset) {
            break;//no subsequent can possibly overlap
          }
        }
      }//loop

      //all-remaining should be marked
//      for (TagLL t = head; t != null; t = t.nextTag) {
//        assert t.mark;
////        if (!t.mark) {
////          t.removeLL();
////          if (head == t)
////            head = t.nextTag;
////        }
//      }
      assert head[0].mark;
    }
  };
}
