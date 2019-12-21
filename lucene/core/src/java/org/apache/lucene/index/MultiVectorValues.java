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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.List;

public class MultiVectorValues {

  /** Returns a vector values for a reader */
  public static VectorValues getVectorValues(final IndexReader r, final String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
    final int size = leaves.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return leaves.get(0).reader().getVectorValues(field);
    }

    boolean anyReal = false;
    for(LeafReaderContext leaf : leaves) {
      FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo(field);
      if (fieldInfo != null) {
        if (fieldInfo.getVectorNumDimensions() != 0) {
          anyReal = true;
          break;
        }
      }
    }

    if (anyReal == false) {
      return null;
    }


    return new VectorValues() {
      private int nextLeaf;
      private VectorValues currentValues;
      private LeafReaderContext currentLeaf;
      private int docID = -1;

      @Override
      public int docID() {
        return docID;
      }

      @Override
      public int nextDoc() throws IOException {
        while (true) {
          while (currentValues == null) {
            if (nextLeaf == leaves.size()) {
              docID = NO_MORE_DOCS;
              return docID;
            }
            currentLeaf = leaves.get(nextLeaf);
            currentValues = currentLeaf.reader().getVectorValues(field);
            nextLeaf++;
          }

          int newDocID = currentValues.nextDoc();

          if (newDocID == NO_MORE_DOCS) {
            currentValues = null;
            continue;
          } else {
            docID = currentLeaf.docBase + newDocID;
            return docID;
          }
        }
      }

      @Override
      public int advance(int target) throws IOException {
        if (target <= docID) {
          throw new IllegalArgumentException("can only advance beyond current document: on docID=" + docID + " but targetDocID=" + target);
        }
        int readerIndex = ReaderUtil.subIndex(target, leaves);
        if (readerIndex >= nextLeaf) {
          if (readerIndex == leaves.size()) {
            currentValues = null;
            docID = NO_MORE_DOCS;
            return docID;
          }
          currentLeaf = leaves.get(readerIndex);
          currentValues = currentLeaf.reader().getVectorValues(field);
          nextLeaf = readerIndex+1;
          if (currentValues == null) {
            return nextDoc();
          }
        }
        int newDocID = currentValues.advance(target - currentLeaf.docBase);
        if (newDocID == NO_MORE_DOCS) {
          currentValues = null;
          return nextDoc();
        } else {
          docID = currentLeaf.docBase + newDocID;
          return docID;
        }
      }


      @Override
      public float[] vectorValue() throws IOException {
        return currentValues.vectorValue();
      }

      @Override
      public boolean seek(int target) throws IOException {
        nextLeaf = 0;
        for (int i = 0; i < leaves.size(); i++) {
          currentLeaf = leaves.get(i);
          currentValues = currentLeaf.reader().getVectorValues(field);
          if (currentValues.seek(target - currentLeaf.docBase)) {
            return true;
          }
          nextLeaf++;
        }
        return false;
      }

      @Override
      public long cost() {
        // TODO
        return 0;
      }
    };
  }


}
