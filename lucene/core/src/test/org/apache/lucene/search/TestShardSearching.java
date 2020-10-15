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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.TestUtil;

// TODO
//   - other queries besides PrefixQuery & TermQuery (but:
//     FuzzyQ will be problematic... the top N terms it
//     takes means results will differ)
//   - NRQ/F
//   - BQ, negated clauses, negated prefix clauses
//   - test pulling docs in 2nd round trip...
//   - filter too

@SuppressCodecs({ "SimpleText", "Direct" })
public class TestShardSearching extends ShardSearchingTestBase {

  private static class PreviousSearchState {
    public final long searchTimeNanos;
    public final long[] versions;
    public final ScoreDoc searchAfterLocal;
    public final ScoreDoc searchAfterShard;
    public final Sort sort;
    public final Query query;
    public final int numHitsPaged;

    public PreviousSearchState(Query query, Sort sort, ScoreDoc searchAfterLocal, ScoreDoc searchAfterShard, long[] versions, int numHitsPaged) {
      this.versions = versions.clone();
      this.searchAfterLocal = searchAfterLocal;
      this.searchAfterShard = searchAfterShard;
      this.sort = sort;
      this.query = query;
      this.numHitsPaged = numHitsPaged;
      searchTimeNanos = System.nanoTime();
    }
  }

  public void testSimple() throws Exception {
    final int numNodes = TestUtil.nextInt(random(), 1, 10);

    final double runTimeSec = TEST_NIGHTLY ? atLeast(5) : atLeast(1);

    final int minDocsToMakeTerms = TestUtil.nextInt(random(), 5, 20);

    final int maxSearcherAgeSeconds = TestUtil.nextInt(random(), 1, 3);

    if (VERBOSE) {
      System.out.println("TEST: numNodes=" + numNodes + " runTimeSec=" + runTimeSec + " maxSearcherAgeSeconds=" + maxSearcherAgeSeconds);
    }

    start(numNodes,
          runTimeSec,
          maxSearcherAgeSeconds
          );

    final List<PreviousSearchState> priorSearches = new ArrayList<>();
    List<BytesRef> terms = null;
    while (System.nanoTime() < endTimeNanos) {

      final boolean doFollowon = priorSearches.size() > 0 && random().nextInt(7) == 1;

      // Pick a random node; we will run the query on this node:
      final int myNodeID = random().nextInt(numNodes);

      final NodeState.ShardIndexSearcher localShardSearcher;

      final PreviousSearchState prevSearchState;

      if (doFollowon) {
        // Pretend user issued a followon query:
        prevSearchState = priorSearches.get(random().nextInt(priorSearches.size()));

        if (VERBOSE) {
          System.out.println("\nTEST: follow-on query age=" + ((System.nanoTime() - prevSearchState.searchTimeNanos)/1000000000.0));
        }

        try {
          localShardSearcher = nodes[myNodeID].acquire(prevSearchState.versions);
        } catch (SearcherExpiredException see) {
          // Expected, sometimes; in a "real" app we would
          // either forward this error to the user ("too
          // much time has passed; please re-run your
          // search") or sneakily just switch to newest
          // searcher w/o telling them...
          if (VERBOSE) {
            System.out.println("  searcher expired during local shard searcher init: " + see);
          }
          priorSearches.remove(prevSearchState);
          continue;
        }
      } else {
        if (VERBOSE) {
          System.out.println("\nTEST: fresh query");
        }
        // Do fresh query:
        localShardSearcher = nodes[myNodeID].acquire();
        prevSearchState = null;
      }

      final IndexReader[] subs = new IndexReader[numNodes];

      PreviousSearchState searchState = null;

      try {

        // Mock: now make a single reader (MultiReader) from all node
        // searchers.  In a real shard env you can't do this... we
        // do it to confirm results from the shard searcher
        // are correct:
        int docCount = 0;
        try {
          for(int nodeID=0;nodeID<numNodes;nodeID++) {
            final long subVersion = localShardSearcher.nodeVersions[nodeID];
            final IndexSearcher sub = nodes[nodeID].searchers.acquire(subVersion);
            if (sub == null) {
              nodeID--;
              while(nodeID >= 0) {
                subs[nodeID].decRef();
                subs[nodeID] = null;
                nodeID--;
              }
              throw new SearcherExpiredException("nodeID=" + nodeID + " version=" + subVersion);
            }
            subs[nodeID] = sub.getIndexReader();
            docCount += subs[nodeID].maxDoc();
          }
        } catch (SearcherExpiredException see) {
          // Expected
          if (VERBOSE) {
            System.out.println("  searcher expired during mock reader init: " + see);
          }
          continue;
        }

        final IndexReader mockReader = new MultiReader(subs);
        final IndexSearcher mockSearcher = new IndexSearcher(mockReader);

        Query query;
        Sort sort;

        if (prevSearchState != null) {
          query = prevSearchState.query;
          sort = prevSearchState.sort;
        } else {
          if (terms == null && docCount > minDocsToMakeTerms) {
            // TODO: try to "focus" on high freq terms sometimes too
            // TODO: maybe also periodically reset the terms...?
            final TermsEnum termsEnum = MultiTerms.getTerms(mockReader, "body").iterator();
            terms = new ArrayList<>();
            while(termsEnum.next() != null) {
              terms.add(BytesRef.deepCopyOf(termsEnum.term()));
            }
            if (VERBOSE) {
              System.out.println("TEST: init terms: " + terms.size() + " terms");
            }
            if (terms.size() == 0) {
              terms = null;
            }
          }

          if (VERBOSE) {
            System.out.println("  maxDoc=" + mockReader.maxDoc());
          }

          if (terms != null) {
            if (random().nextBoolean()) {
              query = new TermQuery(new Term("body", terms.get(random().nextInt(terms.size()))));
            } else {
              final String t = terms.get(random().nextInt(terms.size())).utf8ToString();
              final String prefix;
              if (t.length() <= 1) {
                prefix = t;
              } else {
                prefix = t.substring(0, TestUtil.nextInt(random(), 1, 2));
              }
              query = new PrefixQuery(new Term("body", prefix));
            }
            
            if (random().nextBoolean()) {
              sort = null;
            } else {
              // TODO: sort by more than 1 field
              final int what = random().nextInt(3);
              if (what == 0) {
                sort = new Sort(SortField.FIELD_SCORE);
              } else if (what == 1) {
                // TODO: this sort doesn't merge
                // correctly... it's tricky because you
                // could have > 2.1B docs across all shards: 
                //sort = new Sort(SortField.FIELD_DOC);
                sort = null;
              } else if (what == 2) {
                sort = new Sort(new SortField[] {new SortField("docid_intDV", SortField.Type.INT, random().nextBoolean())});
              } else {
                sort = new Sort(new SortField[] {new SortField("titleDV", SortField.Type.STRING, random().nextBoolean())});
              }
            }
          } else {
            query = null;
            sort = null;
          }
        }

        if (query != null) {

          try {
            searchState = assertSame(mockSearcher, localShardSearcher, query, sort, prevSearchState);
          } catch (SearcherExpiredException see) {
            // Expected; in a "real" app we would
            // either forward this error to the user ("too
            // much time has passed; please re-run your
            // search") or sneakily just switch to newest
            // searcher w/o telling them...
            if (VERBOSE) {
              System.out.println("  searcher expired during search: " + see);
              see.printStackTrace(System.out);
            }
            // We can't do this in general: on a very slow
            // computer it's possible the local searcher
            // expires before we can finish our search:
            // assert prevSearchState != null;
            if (prevSearchState != null) {
              priorSearches.remove(prevSearchState);
            }
          }
        }
      } finally {
        nodes[myNodeID].release(localShardSearcher);
        for(IndexReader sub : subs) {
          if (sub != null) {
            sub.decRef();
          }
        }
      }

      if (searchState != null && searchState.searchAfterLocal != null && random().nextInt(5) == 3) {
        priorSearches.add(searchState);
        if (priorSearches.size() > 200) {
          Collections.shuffle(priorSearches, random());
          priorSearches.subList(100, priorSearches.size()).clear();
        }
      }
    }

    finish();
  }

  private PreviousSearchState assertSame(IndexSearcher mockSearcher, NodeState.ShardIndexSearcher shardSearcher, Query q, Sort sort, PreviousSearchState state) throws IOException {

    int numHits = TestUtil.nextInt(random(), 1, 100);
    if (state != null && state.searchAfterLocal == null) {
      // In addition to what we last searched:
      numHits += state.numHitsPaged;
    }

    if (VERBOSE) {
      System.out.println("TEST: query=" + q + " sort=" + sort + " numHits=" + numHits);
      if (state != null) {
        System.out.println("  prev: searchAfterLocal=" + state.searchAfterLocal + " searchAfterShard=" + state.searchAfterShard + " numHitsPaged=" + state.numHitsPaged);
      }
    }

    // Single (mock local) searcher:
    final TopDocs hits;
    if (sort == null) {
      if (state != null && state.searchAfterLocal != null) {
        hits = mockSearcher.searchAfter(state.searchAfterLocal, q, numHits);
      } else {
        hits = mockSearcher.search(q, numHits);
      }
    } else {
      hits = mockSearcher.search(q, numHits, sort);
    }

    // Shard searcher
    final TopDocs shardHits;
    if (sort == null) {
      if (state != null && state.searchAfterShard != null) {
        shardHits = shardSearcher.searchAfter(state.searchAfterShard, q, numHits);
      } else {
        shardHits = shardSearcher.search(q, numHits);
      }
    } else {
      shardHits = shardSearcher.search(q, numHits, sort);
    }

    final int numNodes = shardSearcher.nodeVersions.length;
    int[] base = new int[numNodes];
    final List<IndexReaderContext> subs = mockSearcher.getTopReaderContext().children();
    assertEquals(numNodes, subs.size());

    for(int nodeID=0;nodeID<numNodes;nodeID++) {
      base[nodeID] = subs.get(nodeID).docBaseInParent;
    }

    if (VERBOSE) {
      /*
      for(int shardID=0;shardID<shardSearchers.length;shardID++) {
        System.out.println("  shard=" + shardID + " maxDoc=" + shardSearchers[shardID].searcher.getIndexReader().maxDoc());
      }
      */
      System.out.println("  single searcher: " + hits.totalHits.value);
      for(int i=0;i<hits.scoreDocs.length;i++) {
        final ScoreDoc sd = hits.scoreDocs[i];
        System.out.println("    doc=" + sd.doc + " score=" + sd.score);
      }
      System.out.println("  shard searcher: " + shardHits.totalHits.value);
      for(int i=0;i<shardHits.scoreDocs.length;i++) {
        final ScoreDoc sd = shardHits.scoreDocs[i];
        System.out.println("    doc=" + sd.doc + " (rebased: " + (sd.doc + base[sd.shardIndex]) + ") score=" + sd.score + " shard=" + sd.shardIndex);
      }
    }

    int numHitsPaged;
    if (state != null && state.searchAfterLocal != null) {
      numHitsPaged = hits.scoreDocs.length; 
      if (state != null) {
        numHitsPaged += state.numHitsPaged;
      }
    } else {
      numHitsPaged = hits.scoreDocs.length;
    }

    final boolean moreHits;

    final ScoreDoc bottomHit;
    final ScoreDoc bottomHitShards;

    if (numHitsPaged < hits.totalHits.value) {
      // More hits to page through
      moreHits = true;
      if (sort == null) {
        bottomHit = hits.scoreDocs[hits.scoreDocs.length-1];
        final ScoreDoc sd = shardHits.scoreDocs[shardHits.scoreDocs.length-1];
        // Must copy because below we rebase:
        bottomHitShards = new ScoreDoc(sd.doc, sd.score, sd.shardIndex);
        if (VERBOSE) {
          System.out.println("  save bottomHit=" + bottomHit);
        }
      } else {
        bottomHit = null;
        bottomHitShards = null;
      }

    } else {
      assertEquals(hits.totalHits.value, numHitsPaged);
      bottomHit = null;
      bottomHitShards = null;
      moreHits = false;
    }

    // Must rebase so assertEquals passes:
    for(int hitID=0;hitID<shardHits.scoreDocs.length;hitID++) {
      final ScoreDoc sd = shardHits.scoreDocs[hitID];
      sd.doc += base[sd.shardIndex];
    }

    TestUtil.assertConsistent(hits, shardHits);

    if (moreHits) {
      // Return a continuation:
      return new PreviousSearchState(q, sort, bottomHit, bottomHitShards, shardSearcher.nodeVersions, numHitsPaged);
    } else {
      return null;
    }
  }
}
