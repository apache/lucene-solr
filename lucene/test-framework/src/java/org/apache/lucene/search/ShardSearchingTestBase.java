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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.TestUtil;

// TODO
//   - doc blocks?  so we can test joins/grouping...
//   - controlled consistency (NRTMgr)

/**
 * Base test class for simulating distributed search across multiple shards.
 */
public abstract class ShardSearchingTestBase extends LuceneTestCase {

  // TODO: maybe SLM should throw this instead of returning null...
  /**
   * Thrown when the lease for a searcher has expired.
   */
  public static class SearcherExpiredException extends RuntimeException {
    public SearcherExpiredException(String message) {
      super(message);
    }
  }

  private static class FieldAndShardVersion {
    private final long version;
    private final int nodeID;
    private final String field;

    public FieldAndShardVersion(int nodeID, long version, String field) {
      this.nodeID = nodeID;
      this.version = version;
      this.field = field;
    }

    @Override
    public int hashCode() {
      return (int) (version * nodeID + field.hashCode());
    }

    @Override
    public boolean equals(Object _other) {
      if (!(_other instanceof FieldAndShardVersion)) {
        return false;
      }

      final FieldAndShardVersion other = (FieldAndShardVersion) _other;

      return field.equals(other.field) && version == other.version && nodeID == other.nodeID;
    }

    @Override
    public String toString() {
      return "FieldAndShardVersion(field=" + field + " nodeID=" + nodeID + " version=" + version+ ")";
    }
  }

  private static class TermAndShardVersion {
    private final long version;
    private final int nodeID;
    private final Term term;

    public TermAndShardVersion(int nodeID, long version, Term term) {
      this.nodeID = nodeID;
      this.version = version;
      this.term = term;
    }

    @Override
    public int hashCode() {
      return (int) (version * nodeID + term.hashCode());
    }

    @Override
    public boolean equals(Object _other) {
      if (!(_other instanceof TermAndShardVersion)) {
        return false;
      }

      final TermAndShardVersion other = (TermAndShardVersion) _other;

      return term.equals(other.term) && version == other.version && nodeID == other.nodeID;
    }
  }

  // We share collection stats for these fields on each node
  // reopen:
  private final String[] fieldsToShare = new String[] {"body", "title"};

  // Called by one node once it has reopened, to notify all
  // other nodes.  This is just a mock (since it goes and
  // directly updates all other nodes, in RAM)... in a real
  // env this would hit the wire, sending version &
  // collection stats to all other nodes:
  void broadcastNodeReopen(int nodeID, long version, IndexSearcher newSearcher) throws IOException {

    if (VERBOSE) {
      System.out.println("REOPEN: nodeID=" + nodeID + " version=" + version + " maxDoc=" + newSearcher.getIndexReader().maxDoc());
    }

    // Broadcast new collection stats for this node to all
    // other nodes:
    for(String field : fieldsToShare) {
      final CollectionStatistics stats = newSearcher.collectionStatistics(field);
      if (stats != null) {
        for (NodeState node : nodes) {
          // Don't put my own collection stats into the cache;
          // we pull locally:
          if (node.myNodeID != nodeID) {
            node.collectionStatsCache.put(new FieldAndShardVersion(nodeID, version, field), stats);
          }
        }
      }
    }
    for (NodeState node : nodes) {
      node.updateNodeVersion(nodeID, version);
    }
  }

  // TODO: broadcastNodeExpire?  then we can purge the
  // known-stale cache entries...

  // MOCK: in a real env you have to hit the wire
  // (send this query to all remote nodes
  // concurrently):
  TopDocs searchNode(int nodeID, long[] nodeVersions, Query q, Sort sort, int numHits, ScoreDoc searchAfter) throws IOException {
    final NodeState.ShardIndexSearcher s = nodes[nodeID].acquire(nodeVersions);
    try {
      if (sort == null) {
        if (searchAfter != null) {
          return s.localSearchAfter(searchAfter, q, numHits);
        } else {
          return s.localSearch(q, numHits);
        }
      } else {
        assert searchAfter == null;  // not supported yet
        return s.localSearch(q, numHits, sort);
      }
    } finally {
      nodes[nodeID].release(s);
    }
  }

  // Mock: in a real env, this would hit the wire and get
  // term stats from remote node
  Map<Term,TermStatistics> getNodeTermStats(Set<Term> terms, int nodeID, long version) throws IOException {
    final NodeState node = nodes[nodeID];
    final Map<Term,TermStatistics> stats = new HashMap<>();
    final IndexSearcher s = node.searchers.acquire(version);
    if (s == null) {
      throw new SearcherExpiredException("node=" + nodeID + " version=" + version);
    }
    try {
      for(Term term : terms) {
        final TermStates ts = TermStates.build(s.getIndexReader().getContext(), term, true);
        if (ts.docFreq() > 0) {
          stats.put(term, s.termStatistics(term, ts.docFreq(), ts.totalTermFreq()));
        }
      }
    } finally {
      node.searchers.release(s);
    }
    return stats;
  }

  protected final class NodeState implements Closeable {
    public final Directory dir;
    public final IndexWriter writer;
    public final SearcherLifetimeManager searchers;
    public final SearcherManager mgr;
    public final int myNodeID;
    public final long[] currentNodeVersions;

    // TODO: nothing evicts from here!!!  Somehow, on searcher
    // expiration on remote nodes we must evict from our
    // local cache...?  And still LRU otherwise (for the
    // still-live searchers).

    private final Map<FieldAndShardVersion,CollectionStatistics> collectionStatsCache = new ConcurrentHashMap<>();
    private final Map<TermAndShardVersion,TermStatistics> termStatsCache = new ConcurrentHashMap<>();

    /** Matches docs in the local shard but scores based on
     *  aggregated stats ("mock distributed scoring") from all
     *  nodes. */ 

    public class ShardIndexSearcher extends IndexSearcher {
      // Version for the node searchers we search:
      public final long[] nodeVersions;
      public final int myNodeID;

      public ShardIndexSearcher(long[] nodeVersions, IndexReader localReader, int nodeID) {
        super(localReader);
        this.nodeVersions = nodeVersions;
        myNodeID = nodeID;
        assert myNodeID == NodeState.this.myNodeID: "myNodeID=" + nodeID + " NodeState.this.myNodeID=" + NodeState.this.myNodeID;
      }

      @Override
      public Query rewrite(Query original) throws IOException {
        final IndexSearcher localSearcher = new IndexSearcher(getIndexReader());
        original = localSearcher.rewrite(original);
        final Set<Term> terms = new HashSet<>();
        original.visit(QueryVisitor.termCollector(terms));

        // Make a single request to remote nodes for term
        // stats:
        for(int nodeID=0;nodeID<nodeVersions.length;nodeID++) {
          if (nodeID == myNodeID) {
            continue;
          }

          final Set<Term> missing = new HashSet<>();
          for(Term term : terms) {
            final TermAndShardVersion key = new TermAndShardVersion(nodeID, nodeVersions[nodeID], term);
            if (!termStatsCache.containsKey(key)) {
              missing.add(term);
            }
          }
          if (missing.size() != 0) {
            for(Map.Entry<Term,TermStatistics> ent : getNodeTermStats(missing, nodeID, nodeVersions[nodeID]).entrySet()) {
              if (ent.getValue() != null) {
                final TermAndShardVersion key = new TermAndShardVersion(nodeID, nodeVersions[nodeID], ent.getKey());
                termStatsCache.put(key, ent.getValue());
              }
            }
          }
        }

        return original;
      }

      @Override
      public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq) throws IOException {
        assert term != null;
        long distributedDocFreq = 0;
        long distributedTotalTermFreq = 0;
        for(int nodeID=0;nodeID<nodeVersions.length;nodeID++) {

          final TermStatistics subStats;
          if (nodeID == myNodeID) {
            subStats = super.termStatistics(term, docFreq, totalTermFreq);
          } else {
            final TermAndShardVersion key = new TermAndShardVersion(nodeID, nodeVersions[nodeID], term);
            subStats = termStatsCache.get(key);
            if (subStats == null) {
              continue; // term not found
            }
          }

          long nodeDocFreq = subStats.docFreq();
          distributedDocFreq += nodeDocFreq;
          
          long nodeTotalTermFreq = subStats.totalTermFreq();
          distributedTotalTermFreq += nodeTotalTermFreq;
        }
        assert distributedDocFreq > 0;
        return new TermStatistics(term.bytes(), distributedDocFreq, distributedTotalTermFreq);
      }

      @Override
      public CollectionStatistics collectionStatistics(String field) throws IOException {
        // TODO: we could compute this on init and cache,
        // since we are re-inited whenever any nodes have a
        // new reader
        long docCount = 0;
        long sumTotalTermFreq = 0;
        long sumDocFreq = 0;
        long maxDoc = 0;

        for(int nodeID=0;nodeID<nodeVersions.length;nodeID++) {
          final FieldAndShardVersion key = new FieldAndShardVersion(nodeID, nodeVersions[nodeID], field);
          final CollectionStatistics nodeStats;
          if (nodeID == myNodeID) {
            nodeStats = super.collectionStatistics(field);
          } else {
            nodeStats = collectionStatsCache.get(key);
          }
          if (nodeStats == null) {
            continue; // field not in sub at all
          }
          
          long nodeDocCount = nodeStats.docCount();
          docCount += nodeDocCount;
          
          long nodeSumTotalTermFreq = nodeStats.sumTotalTermFreq();
          sumTotalTermFreq += nodeSumTotalTermFreq;
          
          long nodeSumDocFreq = nodeStats.sumDocFreq();
          sumDocFreq += nodeSumDocFreq;
          
          assert nodeStats.maxDoc() >= 0;
          maxDoc += nodeStats.maxDoc();
        }

        if (maxDoc == 0) {
          return null; // field not found across any node whatsoever
        } else {
          return new CollectionStatistics(field, maxDoc, docCount, sumTotalTermFreq, sumDocFreq);
        }
      }

      @Override
      public TopDocs search(Query query, int numHits) throws IOException {
        final TopDocs[] shardHits = new TopDocs[nodeVersions.length];
        for(int nodeID=0;nodeID<nodeVersions.length;nodeID++) {
          if (nodeID == myNodeID) {
            // My node; run using local shard searcher we
            // already aquired:
            shardHits[nodeID] = localSearch(query, numHits);
          } else {
            shardHits[nodeID] = searchNode(nodeID, nodeVersions, query, null, numHits, null);
          }

          for (int i = 0; i < shardHits[nodeID].scoreDocs.length; i++) {
            shardHits[nodeID].scoreDocs[i].shardIndex = nodeID;
          }
        }

        // Merge:
        return TopDocs.merge(numHits, shardHits);
      }

      public TopDocs localSearch(Query query, int numHits) throws IOException {
        return super.search(query, numHits);
      }

      @Override
      public TopDocs searchAfter(ScoreDoc after, Query query, int numHits) throws IOException {
        if (after == null) {
          return super.searchAfter(after, query, numHits);
        }
        final TopDocs[] shardHits = new TopDocs[nodeVersions.length];
        // results are merged in that order: score, shardIndex, doc. therefore we set
        // after to after.score and depending on the nodeID we set doc to either:
        // - not collect any more documents with that score (only with worse score)
        // - collect more documents with that score (and worse) following the last collected document
        // - collect all documents with that score (and worse)
        ScoreDoc shardAfter = new ScoreDoc(after.doc, after.score);
        for (int nodeID = 0; nodeID < nodeVersions.length; nodeID++) {
          if (nodeID < after.shardIndex) {
            // all documents with after.score were already collected, so collect
            // only documents with worse scores.
            final NodeState.ShardIndexSearcher s = nodes[nodeID].acquire(nodeVersions);
            try {
              // Setting after.doc to reader.maxDoc-1 is a way to tell
              // TopScoreDocCollector that no more docs with that score should
              // be collected. note that in practice the shard which sends the
              // request to a remote shard won't have reader.maxDoc at hand, so
              // it will send some arbitrary value which will be fixed on the
              // other end.
              shardAfter.doc = s.getIndexReader().maxDoc() - 1;
            } finally {
              nodes[nodeID].release(s);
            }
          } else if (nodeID == after.shardIndex) {
            // collect all documents following the last collected doc with
            // after.score + documents with worse scores.  
            shardAfter.doc = after.doc;
          } else {
            // all documents with after.score (and worse) should be collected
            // because they didn't make it to top-N in the previous round.
            shardAfter.doc = -1;
          }
          if (nodeID == myNodeID) {
            // My node; run using local shard searcher we
            // already aquired:
            shardHits[nodeID] = localSearchAfter(shardAfter, query, numHits);
          } else {
            shardHits[nodeID] = searchNode(nodeID, nodeVersions, query, null, numHits, shardAfter);
          }

          for (int i = 0; i < shardHits[nodeID].scoreDocs.length; i++) {
            shardHits[nodeID].scoreDocs[i].shardIndex = nodeID;
          }
          //System.out.println("  node=" + nodeID + " totHits=" + shardHits[nodeID].totalHits);
        }

        // Merge:
        return TopDocs.merge(numHits, shardHits);
      }

      public TopDocs localSearchAfter(ScoreDoc after, Query query, int numHits) throws IOException {
        return super.searchAfter(after, query, numHits);
      }

      @Override
      public TopFieldDocs search(Query query, int numHits, Sort sort) throws IOException {
        assert sort != null;
        final TopFieldDocs[] shardHits = new TopFieldDocs[nodeVersions.length];
        for(int nodeID=0;nodeID<nodeVersions.length;nodeID++) {
          if (nodeID == myNodeID) {
            // My node; run using local shard searcher we
            // already aquired:
            shardHits[nodeID] = localSearch(query, numHits, sort);
          } else {
            shardHits[nodeID] = (TopFieldDocs) searchNode(nodeID, nodeVersions, query, sort, numHits, null);
          }

          for (int i = 0; i < shardHits[nodeID].scoreDocs.length; i++) {
            shardHits[nodeID].scoreDocs[i].shardIndex = nodeID;
          }
        }

        // Merge:
        return TopDocs.merge(sort, numHits, shardHits);
      }

      public TopFieldDocs localSearch(Query query, int numHits, Sort sort) throws IOException {
        return super.search(query, numHits, sort);
      }

    }

    private volatile ShardIndexSearcher currentShardSearcher;

    public NodeState(Random random, int nodeID, int numNodes) throws IOException {
      myNodeID = nodeID;
      dir = newFSDirectory(createTempDir("ShardSearchingTestBase"));
      // TODO: set warmer
      MockAnalyzer analyzer = new MockAnalyzer(random());
      analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));
      IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
      if (VERBOSE) {
        iwc.setInfoStream(new PrintStreamInfoStream(System.out));
      }
      writer = new IndexWriter(dir, iwc);
      mgr = new SearcherManager(writer, null);
      searchers = new SearcherLifetimeManager();

      // Init w/ 0s... caller above will do initial
      // "broadcast" by calling initSearcher:
      currentNodeVersions = new long[numNodes];
    }

    public void initSearcher(long[] nodeVersions) throws IOException {
      assert currentShardSearcher == null;
      System.arraycopy(nodeVersions, 0, currentNodeVersions, 0, currentNodeVersions.length);
      currentShardSearcher = new ShardIndexSearcher(currentNodeVersions.clone(),
                                                    mgr.acquire().getIndexReader(),
                                                    myNodeID);
    }

    public void updateNodeVersion(int nodeID, long version) throws IOException {
      currentNodeVersions[nodeID] = version;
      if (currentShardSearcher != null) {
        currentShardSearcher.getIndexReader().decRef();
      }
      currentShardSearcher = new ShardIndexSearcher(currentNodeVersions.clone(), 
                                                    mgr.acquire().getIndexReader(),
                                                    myNodeID);
    }

    // Get the current (fresh) searcher for this node
    public ShardIndexSearcher acquire() {
      while(true) {
        final ShardIndexSearcher s = currentShardSearcher;
        // In theory the reader could get decRef'd to 0
        // before we have a chance to incRef, ie if a reopen
        // happens right after the above line, this thread
        // gets stalled, and the old IR is closed.  So we
        // must try/retry until incRef succeeds:
        if (s.getIndexReader().tryIncRef()) {
          return s;
        }
      }
    }

    public void release(ShardIndexSearcher s) throws IOException {
      s.getIndexReader().decRef();
    }

    // Get and old searcher matching the specified versions:
    public ShardIndexSearcher acquire(long[] nodeVersions) {
      final IndexSearcher s = searchers.acquire(nodeVersions[myNodeID]);
      if (s == null) {
        throw new SearcherExpiredException("nodeID=" + myNodeID + " version=" + nodeVersions[myNodeID]);
      }
      return new ShardIndexSearcher(nodeVersions, s.getIndexReader(), myNodeID);
    }

    // Reopen local reader
    public void reopen() throws IOException {
      final IndexSearcher before = mgr.acquire();
      mgr.release(before);

      mgr.maybeRefresh();
      final IndexSearcher after = mgr.acquire();
      try {
        if (after != before) {
          // New searcher was opened
          final long version = searchers.record(after);
          searchers.prune(new SearcherLifetimeManager.PruneByAge(maxSearcherAgeSeconds));
          broadcastNodeReopen(myNodeID, version, after);
        }
      } finally {
        mgr.release(after);
      }
    }

    @Override
    public void close() throws IOException {
      if (currentShardSearcher != null) {
        currentShardSearcher.getIndexReader().decRef();
      }
      searchers.close();
      mgr.close();
      writer.close();
      dir.close();
    }
  }

  // TODO: make this more realistic, ie, each node should
  // have its own thread, so we have true node to node
  // concurrency
  private final class ChangeIndices extends Thread {
    @Override
    public void run() {
      try {
        final LineFileDocs docs = new LineFileDocs(random());
        int numDocs = 0;
        while (System.nanoTime() < endTimeNanos) {
          final int what = random().nextInt(3);
          final NodeState node = nodes[random().nextInt(nodes.length)];
          if (numDocs == 0 || what == 0) {
            node.writer.addDocument(docs.nextDoc());
            numDocs++;
          } else if (what == 1) {
            node.writer.updateDocument(new Term("docid", ""+random().nextInt(numDocs)),
                                        docs.nextDoc());
            numDocs++;
          } else {
            node.writer.deleteDocuments(new Term("docid", ""+random().nextInt(numDocs)));
          }
          // TODO: doc blocks too

          if (random().nextInt(17) == 12) {
            node.writer.commit();
          }

          if (random().nextInt(17) == 12) {
            nodes[random().nextInt(nodes.length)].reopen();
          }
        }
      } catch (Throwable t) {
        System.out.println("FAILED:");
        t.printStackTrace(System.out);
        throw new RuntimeException(t);
      }
    }
  }

  protected NodeState[] nodes;
  int maxSearcherAgeSeconds;
  long endTimeNanos;
  private Thread changeIndicesThread;

  protected void start(int numNodes, double runTimeSec, int maxSearcherAgeSeconds) throws IOException {

    endTimeNanos = System.nanoTime() + (long) (runTimeSec*1000000000);
    this.maxSearcherAgeSeconds = maxSearcherAgeSeconds;

    nodes = new NodeState[numNodes];
    for(int nodeID=0;nodeID<numNodes;nodeID++) {
      nodes[nodeID] = new NodeState(random(), nodeID, numNodes);
    }

    long[] nodeVersions = new long[nodes.length];
    for(int nodeID=0;nodeID<numNodes;nodeID++) {
      final IndexSearcher s = nodes[nodeID].mgr.acquire();
      try {
        nodeVersions[nodeID] = nodes[nodeID].searchers.record(s);
      } finally {
        nodes[nodeID].mgr.release(s);
      }
    }

    for(int nodeID=0;nodeID<numNodes;nodeID++) {
      final IndexSearcher s = nodes[nodeID].mgr.acquire();
      assert nodeVersions[nodeID] == nodes[nodeID].searchers.record(s);
      assert s != null;
      try {
        broadcastNodeReopen(nodeID, nodeVersions[nodeID], s);
      } finally {
        nodes[nodeID].mgr.release(s);
      }
    }

    changeIndicesThread = new ChangeIndices();
    changeIndicesThread.start();
  }

  protected void finish() throws InterruptedException, IOException {
    changeIndicesThread.join();
    for(NodeState node : nodes) {
      node.close();
    }
  }

  /**
   * An IndexSearcher and associated version (lease)
   */
  protected static class SearcherAndVersion {
    public final IndexSearcher searcher;
    public final long version;

    public SearcherAndVersion(IndexSearcher searcher, long version) {
      this.searcher = searcher;
      this.version = version;
    }
  }
}
