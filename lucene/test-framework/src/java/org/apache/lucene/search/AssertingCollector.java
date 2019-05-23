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
package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;

/**
 * A collector that asserts that it is used correctly.
 */
class AssertingCollector extends FilterCollector {

  private int maxDoc = -1;
  private int previousLeafMaxDoc = 0;

  /** Wrap the given collector in order to add assertions. */
  public static Collector wrap(Collector in) {
    if (in instanceof AssertingCollector) {
      return in;
    }
    return new AssertingCollector(in);
  }

  private AssertingCollector(Collector in) {
    super(in);
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    assert context.docBase >= previousLeafMaxDoc;
    previousLeafMaxDoc = context.docBase + context.reader().maxDoc();

    final LeafCollector in = super.getLeafCollector(context);
    final int docBase = context.docBase;
    return new AssertingLeafCollector(in, 0, DocIdSetIterator.NO_MORE_DOCS) {
      @Override
      public void collect(int doc) throws IOException {
        // check that documents are scored in order globally,
        // not only per segment
        assert docBase + doc >= maxDoc : "collection is not in order: current doc="
            + (docBase + doc) + " while " + maxDoc + " has already been collected";

        super.collect(doc);
        maxDoc = docBase + doc;
      }
    };
  }

}
