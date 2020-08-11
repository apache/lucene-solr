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
package org.apache.solr.search.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocValuesTermsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.DaciukMihovAutomatonBuilder;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSet;

/**
 * A graph hit collector.  This accumulates the edges for a given graph traversal.
 * On each collect method, the collector skips edge extraction for nodes that it has
 * already traversed.
 * @lucene.internal
 */
abstract class GraphEdgeCollector extends SimpleCollector implements Collector {
  // For graph traversal, the result set that has already been visited and thus can be skipped for during value collection.
  DocSet skipSet;
  // known leaf nodes
  DocSet leafNodes;

  int numHits = 0;    // number of documents visited
  BitSet bits;  // if not null, used to collect documents visited

  int base;

  SchemaField collectField;

  // skipSet and leafNodes may be null
  GraphEdgeCollector(SchemaField collectField, DocSet skipSet, DocSet leafNodes) {
    this.collectField = collectField;
    this.skipSet = skipSet;
    this.leafNodes = leafNodes;
  }

  // Set to use to collect docs being visited
  // TODO: this should be replaced with a more general delegating collector
  public void setCollectDocs(FixedBitSet target) {
    this.bits = target;
  }

  // the number of docs visited
  public int getNumHits() {
    return numHits;
  }

  public void collect(int segDoc) throws IOException {
    int doc = segDoc + base;
    if (skipSet != null && skipSet.exists(doc)) {
      // when skipSet == all nodes visited so far, then this represents a cycle and we can
      // keep track of that here in the future if we need to.
      return;
    }

    if (bits != null) bits.set(doc);
    // increment the hit count so we know how many docs we traversed this time.
    numHits++;

    // Optimization to not look up edges for a document that is a leaf node (i.e. has no outgoing edges)
    if (leafNodes == null || !leafNodes.exists(doc)) {
      addEdgeIdsToResult(segDoc);
    }
    // Note: tracking links in for each result would be a huge memory hog... so not implementing at this time.
  }

  abstract void addEdgeIdsToResult(int doc) throws IOException;

  private void addDocToResult(int docWithBase) {
    // this document is part of the traversal. mark it in our bitmap.
    bits.set(docWithBase);
    // increment the hit count so we know how many docs we traversed this time.
    numHits++;
  }

  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    base = context.docBase;
  }

  public abstract Query getResultQuery(SchemaField matchField, boolean useAutomaton);

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }


  static class GraphTermsCollector extends GraphEdgeCollector {
    // all the collected terms
    private BytesRefHash collectorTerms;
    private SortedSetDocValues docTermOrds;


    GraphTermsCollector(SchemaField collectField, DocSet skipSet, DocSet leafNodes) {
      super(collectField, skipSet, leafNodes);
      this.collectorTerms = new BytesRefHash();
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      // Grab the updated doc values.
      docTermOrds = DocValues.getSortedSet(context.reader(), collectField.getName());
    }

    @Override
    void addEdgeIdsToResult(int doc) throws IOException {
      // set the doc to pull the edges ids for.
      if (doc > docTermOrds.docID()) {
        docTermOrds.advance(doc);
      }
      if (doc == docTermOrds.docID()) {
        BytesRef edgeValue = new BytesRef();
        long ord;
        while ((ord = docTermOrds.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          edgeValue = docTermOrds.lookupOrd(ord);
          // add the edge id to the collector terms.
          collectorTerms.add(edgeValue);
        }
      }
    }

    @Override
    public Query getResultQuery(SchemaField matchField, boolean useAutomaton) {
      if (collectorTerms == null || collectorTerms.size() == 0) {
        // return null if there are no terms (edges) to traverse.
        return null;
      } else {
        // Create a query
        Query q = null;

        // TODO: see if we should dynamically select this based on the frontier size.
        if (useAutomaton) {
          // build an automaton based query for the frontier.
          Automaton autn = buildAutomaton(collectorTerms);
          AutomatonQuery autnQuery = new AutomatonQuery(new Term(matchField.getName()), autn);
          q = autnQuery;
        } else {
          List<BytesRef> termList = new ArrayList<>(collectorTerms.size());
          for (int i = 0; i < collectorTerms.size(); i++) {
            BytesRef ref = new BytesRef();
            collectorTerms.get(i, ref);
            termList.add(ref);
          }
          q = (matchField.hasDocValues() && !matchField.indexed())
                  ? new DocValuesTermsQuery(matchField.getName(), termList)
                  : new TermInSetQuery(matchField.getName(), termList);
        }

        return q;
      }
    }


    /**
     * Build an automaton to represent the frontier query
     */
    private Automaton buildAutomaton(BytesRefHash termBytesHash) {
      // need top pass a sorted set of terms to the autn builder (maybe a better way to avoid this?)
      final TreeSet<BytesRef> terms = new TreeSet<BytesRef>();
      for (int i = 0; i < termBytesHash.size(); i++) {
        BytesRef ref = new BytesRef();
        termBytesHash.get(i, ref);
        terms.add(ref);
      }
      final Automaton a = DaciukMihovAutomatonBuilder.build(terms);
      return a;
    }

  }
}


