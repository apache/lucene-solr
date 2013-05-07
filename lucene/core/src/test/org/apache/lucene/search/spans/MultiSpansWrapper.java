package org.apache.lucene.search.spans;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * 
 * A wrapper to perform span operations on a non-leaf reader context
 * <p>
 * NOTE: This should be used for testing purposes only
 * @lucene.internal
 */
public class MultiSpansWrapper extends Spans { // can't be package private due to payloads

  private SpanQuery query;
  private List<AtomicReaderContext> leaves;
  private int leafOrd = 0;
  private Spans current;
  private Map<Term,TermContext> termContexts;
  private final int numLeaves;

  private MultiSpansWrapper(List<AtomicReaderContext> leaves, SpanQuery query, Map<Term,TermContext> termContexts) {
    this.query = query;
    this.leaves = leaves;
    this.numLeaves = leaves.size();
    this.termContexts = termContexts;
  }
  
  public static Spans wrap(IndexReaderContext topLevelReaderContext, SpanQuery query) throws IOException {
    Map<Term,TermContext> termContexts = new HashMap<Term,TermContext>();
    TreeSet<Term> terms = new TreeSet<Term>();
    query.extractTerms(terms);
    for (Term term : terms) {
      termContexts.put(term, TermContext.build(topLevelReaderContext, term, true));
    }
    final List<AtomicReaderContext> leaves = topLevelReaderContext.leaves();
    if(leaves.size() == 1) {
      final AtomicReaderContext ctx = leaves.get(0);
      return query.getSpans(ctx, ctx.reader().getLiveDocs(), termContexts);
    }
    return new MultiSpansWrapper(leaves, query, termContexts);
  }

  @Override
  public boolean next() throws IOException {
    if (leafOrd >= numLeaves) {
      return false;
    }
    if (current == null) {
      final AtomicReaderContext ctx = leaves.get(leafOrd);
      current = query.getSpans(ctx, ctx.reader().getLiveDocs(), termContexts);
    }
    while(true) {
      if (current.next()) {
        return true;
      }
      if (++leafOrd < numLeaves) {
        final AtomicReaderContext ctx = leaves.get(leafOrd);
        current = query.getSpans(ctx, ctx.reader().getLiveDocs(), termContexts);
      } else {
        current = null;
        break;
      }
    }
    return false;
  }

  @Override
  public boolean skipTo(int target) throws IOException {
    if (leafOrd >= numLeaves) {
      return false;
    }

    int subIndex = ReaderUtil.subIndex(target, leaves);
    assert subIndex >= leafOrd;
    if (subIndex != leafOrd) {
      final AtomicReaderContext ctx = leaves.get(subIndex);
      current = query.getSpans(ctx, ctx.reader().getLiveDocs(), termContexts);
      leafOrd = subIndex;
    } else if (current == null) {
      final AtomicReaderContext ctx = leaves.get(leafOrd);
      current = query.getSpans(ctx, ctx.reader().getLiveDocs(), termContexts);
    }
    while (true) {
      if (target < leaves.get(leafOrd).docBase) {
        // target was in the previous slice
        if (current.next()) {
          return true;
        }
      } else if (current.skipTo(target - leaves.get(leafOrd).docBase)) {
        return true;
      }
      if (++leafOrd < numLeaves) {
        final AtomicReaderContext ctx = leaves.get(leafOrd);
        current = query.getSpans(ctx, ctx.reader().getLiveDocs(), termContexts);
      } else {
        current = null;
        break;
      }
    }

    return false;
  }

  @Override
  public int doc() {
    if (current == null) {
      return DocIdSetIterator.NO_MORE_DOCS;
    }
    return current.doc() + leaves.get(leafOrd).docBase;
  }

  @Override
  public int start() {
    if (current == null) {
      return DocIdSetIterator.NO_MORE_DOCS;
    }
    return current.start();
  }

  @Override
  public int end() {
    if (current == null) {
      return DocIdSetIterator.NO_MORE_DOCS;
    }
    return current.end();
  }

  @Override
  public Collection<byte[]> getPayload() throws IOException {
    if (current == null) {
      return Collections.emptyList();
    }
    return current.getPayload();
  }

  @Override
  public boolean isPayloadAvailable() throws IOException {
    if (current == null) {
      return false;
    }
    return current.isPayloadAvailable();
  }

  @Override
  public long cost() {
    return Integer.MAX_VALUE; // just for tests
  }

}
