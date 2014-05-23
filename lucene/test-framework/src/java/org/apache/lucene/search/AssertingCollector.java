package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.index.AtomicReaderContext;

/** Wraps another Collector and checks that
 *  acceptsDocsOutOfOrder is respected. */

public class AssertingCollector extends FilterCollector {

  public static Collector wrap(Random random, Collector other, boolean inOrder) {
    return other instanceof AssertingCollector ? other : new AssertingCollector(random, other, inOrder);
  }

  final Random random;
  final boolean inOrder;

  AssertingCollector(Random random, Collector in, boolean inOrder) {
    super(in);
    this.random = random;
    this.inOrder = inOrder;
  }

  @Override
  public LeafCollector getLeafCollector(AtomicReaderContext context) throws IOException {
    return new FilterLeafCollector(super.getLeafCollector(context)) {

      int lastCollected = -1;

      @Override
      public void setScorer(Scorer scorer) throws IOException {
        super.setScorer(AssertingScorer.getAssertingScorer(random, scorer));
      }

      @Override
      public void collect(int doc) throws IOException {
        if (inOrder || !acceptsDocsOutOfOrder()) {
          assert doc > lastCollected : "Out of order : " + lastCollected + " " + doc;
        }
        in.collect(doc);
        lastCollected = doc;
      }

    };
  }

}

