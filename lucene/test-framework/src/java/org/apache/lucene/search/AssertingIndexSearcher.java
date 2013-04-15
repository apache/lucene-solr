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
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.index.AssertingAtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VirtualMethod;
import org.apache.lucene.util._TestUtil;

/**
 * Helper class that adds some extra checks to ensure correct
 * usage of {@code IndexSearcher} and {@code Weight}.
 */
public class AssertingIndexSearcher extends IndexSearcher {
  final Random random;
  public  AssertingIndexSearcher(Random random, IndexReader r) {
    super(r);
    this.random = new Random(random.nextLong());
  }
  
  public  AssertingIndexSearcher(Random random, IndexReaderContext context) {
    super(context);
    this.random = new Random(random.nextLong());
  }
  
  public  AssertingIndexSearcher(Random random, IndexReader r, ExecutorService ex) {
    super(r, ex);
    this.random = new Random(random.nextLong());
  }
  
  public  AssertingIndexSearcher(Random random, IndexReaderContext context, ExecutorService ex) {
    super(context, ex);
    this.random = new Random(random.nextLong());
  }
  
  /** Ensures, that the returned {@code Weight} is not normalized again, which may produce wrong scores. */
  @Override
  public Weight createNormalizedWeight(Query query) throws IOException {
    final Weight w = super.createNormalizedWeight(query);
    return new AssertingWeight(random, w) {

      @Override
      public void normalize(float norm, float topLevelBoost) {
        throw new IllegalStateException("Weight already normalized.");
      }

      @Override
      public float getValueForNormalization() {
        throw new IllegalStateException("Weight already normalized.");
      }

    };
  }

  @Override
  public Query rewrite(Query original) throws IOException {
    // TODO: use the more sophisticated QueryUtils.check sometimes!
    QueryUtils.check(original);
    Query rewritten = super.rewrite(original);
    QueryUtils.check(rewritten);
    return rewritten;
  }

  @Override
  protected Query wrapFilter(Query query, Filter filter) {
    if (random.nextBoolean())
      return super.wrapFilter(query, filter);
    return (filter == null) ? query : new FilteredQuery(query, filter, _TestUtil.randomFilterStrategy(random));
  }

  @Override
  protected void search(List<AtomicReaderContext> leaves, Weight weight, Collector collector) throws IOException {
    super.search(leaves, AssertingWeight.wrap(random, weight), collector);
  }

  static class AssertingWeight extends Weight {

    static Weight wrap(Random random, Weight other) {
      return other instanceof AssertingWeight ? other : new AssertingWeight(random, other);
    }

    final Random random;
    final Weight in;

    AssertingWeight(Random random, Weight in) {
      this.random = random;
      this.in = in;
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      return in.explain(context, doc);
    }

    @Override
    public Query getQuery() {
      return in.getQuery();
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return in.getValueForNormalization();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      in.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
        boolean topScorer, Bits acceptDocs) throws IOException {
      // if the caller asks for in-order scoring or if the weight does not support
      // out-of order scoring then collection will have to happen in-order.
      final boolean inOrder = scoreDocsInOrder || !scoresDocsOutOfOrder();
      final Scorer inScorer = in.scorer(context, scoreDocsInOrder, topScorer, acceptDocs);
      return AssertingScorer.wrap(new Random(random.nextLong()), inScorer, topScorer, inOrder);
    }

    @Override
    public boolean scoresDocsOutOfOrder() {
      return in.scoresDocsOutOfOrder();
    }

  }

  enum TopScorer {
    YES, NO, UNKNOWN;
  }

  /** Wraps a Scorer with additional checks */
  public static class AssertingScorer extends Scorer {

    private static final VirtualMethod<Scorer> SCORE_COLLECTOR = new VirtualMethod<Scorer>(Scorer.class, "score", Collector.class);
    private static final VirtualMethod<Scorer> SCORE_COLLECTOR_RANGE = new VirtualMethod<Scorer>(Scorer.class, "score", Collector.class, int.class, int.class);

    // we need to track scorers using a weak hash map because otherwise we
    // could loose references because of eg.
    // AssertingScorer.score(Collector) which needs to delegate to work correctly
    private static Map<Scorer, WeakReference<AssertingScorer>> ASSERTING_INSTANCES = Collections.synchronizedMap(new WeakHashMap<Scorer, WeakReference<AssertingScorer>>());

    private static Scorer wrap(Random random, Scorer other, TopScorer topScorer, boolean inOrder) {
      if (other == null || other instanceof AssertingScorer) {
        return other;
      }
      final AssertingScorer assertScorer = new AssertingScorer(random, other, topScorer, inOrder);
      ASSERTING_INSTANCES.put(other, new WeakReference<AssertingScorer>(assertScorer));
      return assertScorer;
    }

    static Scorer wrap(Random random, Scorer other, boolean topScorer, boolean inOrder) {
      return wrap(random, other, topScorer ? TopScorer.YES : TopScorer.NO, inOrder);
    }

    static Scorer getAssertingScorer(Random random, Scorer other) {
      if (other == null || other instanceof AssertingScorer) {
        return other;
      }
      final WeakReference<AssertingScorer> assertingScorerRef = ASSERTING_INSTANCES.get(other);
      final AssertingScorer assertingScorer = assertingScorerRef == null ? null : assertingScorerRef.get();
      if (assertingScorer == null) {
        // can happen in case of memory pressure or if
        // scorer1.score(collector) calls
        // collector.setScorer(scorer2) with scorer1 != scorer2, such as
        // BooleanScorer. In that case we can't enable all assertions
        return new AssertingScorer(random, other, TopScorer.UNKNOWN, false);
      } else {
        return assertingScorer;
      }
    }

    final Random random;
    final Scorer in;
    final AssertingAtomicReader.AssertingDocsEnum docsEnumIn;
    final TopScorer topScorer;
    final boolean inOrder;
    final boolean canCallNextDoc;

    private AssertingScorer(Random random, Scorer in, TopScorer topScorer, boolean inOrder) {
      super(in.weight);
      this.random = random;
      this.in = in;
      this.topScorer = topScorer;
      this.inOrder = inOrder;
      this.docsEnumIn = new AssertingAtomicReader.AssertingDocsEnum(in, topScorer == TopScorer.NO);
      this.canCallNextDoc = topScorer != TopScorer.YES // not a top scorer
          || !SCORE_COLLECTOR_RANGE.isOverriddenAsOf(in.getClass()) // the default impl relies upon nextDoc()
          || !SCORE_COLLECTOR.isOverriddenAsOf(in.getClass()); // the default impl relies upon nextDoc()
    }

    public Scorer getIn() {
      return in;
    }

    boolean iterating() {
      switch (docID()) {
        case -1:
        case NO_MORE_DOCS:
          return false;
        default:
          return true;
      }
    }

    @Override
    public float score() throws IOException {
      assert iterating();
      final float score = in.score();
      assert !Float.isNaN(score);
      assert !Float.isNaN(score);
      return score;
    }

    @Override
    public void score(Collector collector) throws IOException {
      assert topScorer != TopScorer.NO;
      if (SCORE_COLLECTOR.isOverriddenAsOf(this.in.getClass())) {
        if (random.nextBoolean()) {
          try {
            final boolean remaining = in.score(collector, DocsEnum.NO_MORE_DOCS, in.nextDoc());
            assert !remaining;
          } catch (UnsupportedOperationException e) {
            in.score(collector);
          }
        } else {
          in.score(collector);
        }
      } else {
        // score(Collector) has not been overridden, use the super method in
        // order to benefit from all assertions
        super.score(collector);
      }
    }

    @Override
    public boolean score(Collector collector, int max, int firstDocID) throws IOException {
      assert topScorer != TopScorer.NO;
      if (SCORE_COLLECTOR_RANGE.isOverriddenAsOf(this.in.getClass())) {
        return in.score(collector, max, firstDocID);
      } else {
        // score(Collector,int,int) has not been overridden, use the super
        // method in order to benefit from all assertions
        return super.score(collector, max, firstDocID);
      }
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      return in.getChildren();
    }

    @Override
    public int freq() throws IOException {
      assert iterating();
      return in.freq();
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      assert canCallNextDoc : "top scorers should not call nextDoc()";
      return docsEnumIn.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      assert canCallNextDoc : "top scorers should not call advance(target)";
      return docsEnumIn.advance(target);
    }

    @Override
    public long cost() {
      return in.cost();
    }
  }

  static class AssertingCollector extends Collector {

    static Collector wrap(Random random, Collector other, boolean inOrder) {
      return other instanceof AssertingCollector ? other : new AssertingCollector(random, other, inOrder);
    }

    final Random random;
    final Collector in;
    final boolean inOrder;
    int lastCollected;

    AssertingCollector(Random random, Collector in, boolean inOrder) {
      this.random = random;
      this.in = in;
      this.inOrder = inOrder;
      lastCollected = -1;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      in.setScorer(AssertingScorer.getAssertingScorer(random, scorer));
    }

    @Override
    public void collect(int doc) throws IOException {
      if (inOrder || !acceptsDocsOutOfOrder()) {
        assert doc > lastCollected : "Out of order : " + lastCollected + " " + doc;
      }
      in.collect(doc);
      lastCollected = doc;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      lastCollected = -1;
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return in.acceptsDocsOutOfOrder();
    }

  }

}
