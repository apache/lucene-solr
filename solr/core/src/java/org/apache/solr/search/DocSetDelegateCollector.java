package org.apache.solr.search;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.FixedBitSet;

/**
 *
 */
public class DocSetDelegateCollector extends DocSetCollector {
  final Collector collector;

  public DocSetDelegateCollector(int smallSetSize, int maxDoc, Collector collector) {
    super(smallSetSize, maxDoc);
    this.collector = collector;
  }

  @Override
  public void collect(int doc) throws IOException {
    collector.collect(doc);

    doc += base;
    // optimistically collect the first docs in an array
    // in case the total number will be small enough to represent
    // as a small set like SortedIntDocSet instead...
    // Storing in this array will be quicker to convert
    // than scanning through a potentially huge bit vector.
    // FUTURE: when search methods all start returning docs in order, maybe
    // we could have a ListDocSet() and use the collected array directly.
    if (pos < scratch.length) {
      scratch[pos]=doc;
    } else {
      // this conditional could be removed if BitSet was preallocated, but that
      // would take up more memory, and add more GC time...
      if (bits==null) bits = new FixedBitSet(maxDoc);
      bits.set(doc);
    }

    pos++;
  }

  @Override
  public DocSet getDocSet() {
    if (pos<=scratch.length) {
      // assumes docs were collected in sorted order!
      return new SortedIntDocSet(scratch, pos);
    } else {
      // set the bits for ids that were collected in the array
      for (int i=0; i<scratch.length; i++) bits.set(scratch[i]);
      return new BitDocSet(bits,pos);
    }
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    collector.setScorer(scorer);
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    collector.setNextReader(context);
    this.base = context.docBase;
  }
}
