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
package org.apache.solr.search.facet;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSetCollector;
import org.apache.solr.search.QueryContext;

/** @lucene.experimental */
public class BlockJoin {

  /** acceptDocs will normally be used to avoid deleted documents from being generated as part of the answer DocSet (just use *:*)
   *  although it can be used to further constrain the generated documents.
   */
  public static DocSet toChildren(DocSet parentInput, BitDocSet parentList, DocSet acceptDocs, QueryContext qcontext) throws IOException {
    FixedBitSet parentBits = parentList.getBits();
    DocSetCollector collector = new DocSetCollector(qcontext.searcher().maxDoc());
    DocIterator iter = parentInput.iterator();
    while (iter.hasNext()) {
      int parentDoc = iter.nextDoc();
      if (!parentList.exists(parentDoc) || parentDoc == 0) { // test for parentDoc==0 here to avoid passing -1 to prevSetBit later on
        // not a parent, or parent has no children
        continue;
      }
      int prevParent = parentBits.prevSetBit(parentDoc - 1);
      for (int childDoc = prevParent+1; childDoc<parentDoc; childDoc++) {
        if (acceptDocs != null && !acceptDocs.exists(childDoc)) continue;  // only select live docs
        collector.collect(childDoc);
      }
    }
    return collector.getDocSet();
  }

  /** childInput may also contain parents (i.e. a parent or below will all roll up to that parent) */
  public static DocSet toParents(DocSet childInput, BitDocSet parentList, QueryContext qcontext) throws IOException {
    FixedBitSet parentBits = parentList.getBits();
    DocSetCollector collector = new DocSetCollector(qcontext.searcher().maxDoc());
    DocIterator iter = childInput.iterator();
    int currentParent = -1;
    while (iter.hasNext()) {
      int childDoc = iter.nextDoc(); // TODO: skipping
      if (childDoc <= currentParent) { // use <= since we also allow parents in the input
        // we already visited this parent
        continue;
      }
      currentParent = parentBits.nextSetBit(childDoc);
      if (currentParent != DocIdSetIterator.NO_MORE_DOCS) {
        // only collect the parent the first time we skip to it
        collector.collect( currentParent );
      }
    }
    return collector.getDocSet();
  }

}
