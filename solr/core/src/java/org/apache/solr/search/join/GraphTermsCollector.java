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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocSet;

/**
 * A graph hit collector.  This accumulates the edges for a given graph traversal.
 * On each collect method, the collector skips edge extraction for nodes that it has
 * already traversed.
 * @lucene.internal
 */
class GraphTermsCollector extends SimpleCollector implements Collector {
  
  // the field to collect edge ids from
  private String field;
  // all the collected terms
  private BytesRefHash collectorTerms;
  private SortedSetDocValues docTermOrds;
  // the result set that is being collected.
  private Bits currentResult;
  // known leaf nodes
  private DocSet leafNodes;
  // number of hits discovered at this level.
  int numHits=0;
  BitSet bits;
  final int maxDoc;
  int base;
  int baseInParent;
  // if we care to track this.
  boolean hasCycles = false;
  
  GraphTermsCollector(String field,int maxDoc, Bits currentResult, DocSet leafNodes) {
    this.field = field;
    this.maxDoc = maxDoc;
    this.collectorTerms =  new BytesRefHash();
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
    if (!leafNodes.exists(doc)) {
      addEdgeIdsToResult(doc-base);
    } 
    // Note: tracking links in for each result would be a huge memory hog... so not implementing at this time.
    
  }
  
  private void addEdgeIdsToResult(int doc) throws IOException {
    // set the doc to pull the edges ids for.
    docTermOrds.setDocument(doc);
    BytesRef edgeValue = new BytesRef();
    long ord;
    while ((ord = docTermOrds.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
      // TODO: handle non string type fields.
      edgeValue = docTermOrds.lookupOrd(ord);
      // add the edge id to the collector terms.
      collectorTerms.add(edgeValue);
    }
  }
  
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
    // Grab the updated doc values.
    docTermOrds = DocValues.getSortedSet(context.reader(), field);
    base = context.docBase;
    baseInParent = context.docBaseInParent;
  }
  
  public BytesRefHash getCollectorTerms() {
    return collectorTerms;
  }
  
  @Override
  public boolean needsScores() {
    return false;
  }
  
}
