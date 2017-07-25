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

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.DaciukMihovAutomatonBuilder;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocSet;
import org.apache.solr.util.LongIterator;
import org.apache.solr.util.LongSet;

/**
 * A graph hit collector.  This accumulates the edges for a given graph traversal.
 * On each collect method, the collector skips edge extraction for nodes that it has
 * already traversed.
 * @lucene.internal
 */
abstract class GraphEdgeCollector extends SimpleCollector implements Collector {

  GraphQuery.GraphQueryWeight weight;

  // the result set that is being collected.
  Bits currentResult;
  // known leaf nodes
  DocSet leafNodes;
  // number of hits discovered at this level.
  int numHits=0;
  BitSet bits;
  final int maxDoc;
  int base;
  int baseInParent;
  // if we care to track this.
  boolean hasCycles = false;

  GraphEdgeCollector(GraphQuery.GraphQueryWeight weight, int maxDoc, Bits currentResult, DocSet leafNodes) {
    this.weight = weight;
    this.maxDoc = maxDoc;
    this.currentResult = currentResult;
    this.leafNodes = leafNodes;
    if (bits==null) {
      // create a bitset at the start that will hold the graph traversal result set 
      bits = new FixedBitSet(maxDoc);
    }
  }
  
  public void collect(int doc) throws IOException {    
    doc += base;
    if (currentResult.get(doc)) {
      // cycle detected / already been here.
      // knowing if your graph had a cycle might be useful and it's lightweight to implement here.
      hasCycles = true;
      return;
    }
    // collect the docs
    addDocToResult(doc);
    // Optimization to not look up edges for a document that is a leaf node
    if (leafNodes == null || !leafNodes.exists(doc)) {
      addEdgeIdsToResult(doc-base);
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
  
  public BitDocSet getDocSet() {
    if (bits == null) {
      // TODO: this shouldn't happen
      bits = new FixedBitSet(maxDoc);
    }
    return new BitDocSet((FixedBitSet)bits,numHits);
  }
  
  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    base = context.docBase;
    baseInParent = context.docBaseInParent;
  }

  protected abstract Query getResultQuery();

  public Query getFrontierQuery() {
    Query q = getResultQuery();
    if (q == null) return null;

    // If there is a filter to be used while crawling the graph, add that.
    if (weight.getGraphQuery().getTraversalFilter() != null) {
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.add(q, BooleanClause.Occur.MUST);
      builder.add(weight.getGraphQuery().getTraversalFilter(), BooleanClause.Occur.MUST);
      q = builder.build();
    }

    return q;
  }
  
  @Override
  public boolean needsScores() {
    return false;
  }
  
}

class GraphTermsCollector extends GraphEdgeCollector {
  // all the collected terms
  private BytesRefHash collectorTerms;
  private SortedSetDocValues docTermOrds;


  GraphTermsCollector(GraphQuery.GraphQueryWeight weight, int maxDoc, Bits currentResult, DocSet leafNodes) {
    super(weight, maxDoc, currentResult, leafNodes);
    this.collectorTerms =  new BytesRefHash();
  }

  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    super.doSetNextReader(context);
    // Grab the updated doc values.
    docTermOrds = DocValues.getSortedSet(context.reader(), weight.getGraphQuery().getToField());
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
  protected Query getResultQuery() {
    if (collectorTerms == null || collectorTerms.size() == 0) {
      // return null if there are no terms (edges) to traverse.
      return null;
    } else {
      // Create a query
      Query q = null;

      GraphQuery gq = weight.getGraphQuery();
      // TODO: see if we should dynamically select this based on the frontier size.
      if (gq.isUseAutn()) {
        // build an automaton based query for the frontier.
        Automaton autn = buildAutomaton(collectorTerms);
        AutomatonQuery autnQuery = new AutomatonQuery(new Term(gq.getFromField()), autn);
        q = autnQuery;
      } else {
        List<BytesRef> termList = new ArrayList<>(collectorTerms.size());
        for (int i = 0 ; i < collectorTerms.size(); i++) {
          BytesRef ref = new BytesRef();
          collectorTerms.get(i, ref);
          termList.add(ref);
        }
        q = new TermInSetQuery(gq.getFromField(), termList);
      }

      return q;
    }
  }


  /** Build an automaton to represent the frontier query */
  private Automaton buildAutomaton(BytesRefHash termBytesHash) {
    // need top pass a sorted set of terms to the autn builder (maybe a better way to avoid this?)
    final TreeSet<BytesRef> terms = new TreeSet<BytesRef>();
    for (int i = 0 ; i < termBytesHash.size(); i++) {
      BytesRef ref = new BytesRef();
      termBytesHash.get(i, ref);
      terms.add(ref);
    }
    final Automaton a = DaciukMihovAutomatonBuilder.build(terms);
    return a;
  }
}


class GraphPointsCollector extends GraphEdgeCollector {
  final LongSet set = new LongSet(256);

  SortedNumericDocValues values = null;

  GraphPointsCollector(GraphQuery.GraphQueryWeight weight, int maxDoc, Bits currentResult, DocSet leafNodes) {
    super(weight, maxDoc, currentResult, leafNodes);
  }

  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    super.doSetNextReader(context);
    values = DocValues.getSortedNumeric(context.reader(), weight.getGraphQuery().getToField());
  }

  @Override
  void addEdgeIdsToResult(int doc) throws IOException {
    // set the doc to pull the edges ids for.
    int valuesDoc = values.docID();
    if (valuesDoc < doc) {
      valuesDoc = values.advance(doc);
    }
    if (valuesDoc == doc) {
      int count = values.docValueCount();
      for (int i = 0; i < count; i++) {
        long v = values.nextValue();
        set.add(v);
      }
    }
  }

  @Override
  protected Query getResultQuery() {
    if (set.cardinality() == 0) return null;

    Query q = null;
    SchemaField sfield = weight.fromSchemaField;
    NumberType ntype = sfield.getType().getNumberType();
    boolean multiValued = sfield.multiValued();

    if (ntype == NumberType.LONG || ntype == NumberType.DATE) {
      long[] vals = new long[set.cardinality()];
      int i = 0;
      for (LongIterator iter = set.iterator(); iter.hasNext(); ) {
        long bits = iter.next();
        long v = bits;
        vals[i++] = v;
      }
      q = LongPoint.newSetQuery(sfield.getName(), vals);
    } else if (ntype == NumberType.INTEGER) {
      int[] vals = new int[set.cardinality()];
      int i = 0;
      for (LongIterator iter = set.iterator(); iter.hasNext(); ) {
        long bits = iter.next();
        int v = (int)bits;
        vals[i++] = v;
      }
      q = IntPoint.newSetQuery(sfield.getName(), vals);
    } else if (ntype == NumberType.DOUBLE) {
      double[] vals = new double[set.cardinality()];
      int i = 0;
      for (LongIterator iter = set.iterator(); iter.hasNext(); ) {
        long bits = iter.next();
        double v = multiValued ? NumericUtils.sortableLongToDouble(bits) : Double.longBitsToDouble(bits);
        vals[i++] = v;
      }
      q = DoublePoint.newSetQuery(sfield.getName(), vals);
    } else if (ntype == NumberType.FLOAT) {
      float[] vals = new float[set.cardinality()];
      int i = 0;
      for (LongIterator iter = set.iterator(); iter.hasNext(); ) {
        long bits = iter.next();
        float v = multiValued ? NumericUtils.sortableIntToFloat((int) bits) : Float.intBitsToFloat((int) bits);
        vals[i++] = v;
      }
      q = FloatPoint.newSetQuery(sfield.getName(), vals);
    }

    return q;
  }


  /** Build an automaton to represent the frontier query */
  private Automaton buildAutomaton(BytesRefHash termBytesHash) {
    // need top pass a sorted set of terms to the autn builder (maybe a better way to avoid this?)
    final TreeSet<BytesRef> terms = new TreeSet<BytesRef>();
    for (int i = 0 ; i < termBytesHash.size(); i++) {
      BytesRef ref = new BytesRef();
      termBytesHash.get(i, ref);
      terms.add(ref);
    }
    final Automaton a = DaciukMihovAutomatonBuilder.build(terms);
    return a;
  }
}
