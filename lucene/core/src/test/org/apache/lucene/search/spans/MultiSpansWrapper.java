package org.apache.lucene.search.spans;

/**
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
import java.util.Map;
import java.util.TreeSet;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.TermContext;

/**
 * 
 * A wrapper to perform span operations on a non-leaf reader context
 * <p>
 * NOTE: This should be used for testing purposes only
 * @lucene.internal
 */
public class MultiSpansWrapper extends Spans { // can't be package private due to payloads

  private SpanQuery query;
  private AtomicReaderContext[] leaves;
  private int leafOrd = 0;
  private Spans current;
  private Map<Term,TermContext> termContexts;

  private MultiSpansWrapper(AtomicReaderContext[] leaves, SpanQuery query, Map<Term,TermContext> termContexts) {
    this.query = query;
    this.leaves = leaves;
    this.termContexts = termContexts;

  }
  
  public static Spans wrap(IndexReaderContext topLevelReaderContext, SpanQuery query) throws IOException {
    Map<Term,TermContext> termContexts = new HashMap<Term,TermContext>();
    TreeSet<Term> terms = new TreeSet<Term>();
    query.extractTerms(terms);
    for (Term term : terms) {
      termContexts.put(term, TermContext.build(topLevelReaderContext, term, true));
    }
    AtomicReaderContext[] leaves = topLevelReaderContext.leaves();
    if(leaves.length == 1) {
      return query.getSpans(leaves[0], leaves[0].reader().getLiveDocs(), termContexts);
    }
    return new MultiSpansWrapper(leaves, query, termContexts);
  }

  @Override
  public boolean next() throws IOException {
    if (leafOrd >= leaves.length) {
      return false;
    }
    if (current == null) {
      current = query.getSpans(leaves[leafOrd], leaves[leafOrd].reader().getLiveDocs(), termContexts);
    }
    while(true) {
      if (current.next()) {
        return true;
      }
      if (++leafOrd < leaves.length) {
        current = query.getSpans(leaves[leafOrd], leaves[leafOrd].reader().getLiveDocs(), termContexts);
      } else {
        current = null;
        break;
      }
    }
    return false;
  }

  @Override
  public boolean skipTo(int target) throws IOException {
    if (leafOrd >= leaves.length) {
      return false;
    }

    int subIndex = ReaderUtil.subIndex(target, leaves);
    assert subIndex >= leafOrd;
    if (subIndex != leafOrd) {
      current = query.getSpans(leaves[subIndex], leaves[subIndex].reader().getLiveDocs(), termContexts);
      leafOrd = subIndex;
    } else if (current == null) {
      current = query.getSpans(leaves[leafOrd], leaves[leafOrd].reader().getLiveDocs(), termContexts);
    }
    while (true) {
      if (current.skipTo(target - leaves[leafOrd].docBase)) {
        return true;
      }
      if (++leafOrd < leaves.length) {
        current = query.getSpans(leaves[leafOrd], leaves[leafOrd].reader().getLiveDocs(), termContexts);
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
    return current.doc() + leaves[leafOrd].docBase;
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
  public boolean isPayloadAvailable() {
    if (current == null) {
      return false;
    }
    return current.isPayloadAvailable();
  }

}
