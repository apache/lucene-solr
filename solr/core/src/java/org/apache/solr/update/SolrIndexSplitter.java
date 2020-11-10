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
package org.apache.solr.update;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.HardlinkCopyDirectoryWrapper;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.HashBasedRouter;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.IndexFetcher;
import org.apache.solr.handler.SnapShooter;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.BitsFilteredPostingsEnum;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RTimerTree;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrIndexSplitter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String INDEX_PREFIX = "index.";

  public enum SplitMethod {
    REWRITE,
    LINK;

    public static SplitMethod get(String p) {
      if (p != null) {
        try {
          return SplitMethod.valueOf(p.toUpperCase(Locale.ROOT));
        } catch (Exception ex) {
          return null;
        }
      }
      return null;
    }

    public String toLower() {
      return toString().toLowerCase(Locale.ROOT);
    }
  }

  SolrIndexSearcher searcher;
  SchemaField field;
  List<DocRouter.Range> ranges;
  DocRouter.Range[] rangesArr; // same as ranges list, but an array for extra speed in inner loops
  List<String> paths;
  List<SolrCore> cores;
  HashBasedRouter hashRouter;
  int numPieces;
  String routeFieldName;
  String splitKey;
  SplitMethod splitMethod;
  RTimerTree timings = new RTimerTree();

  public SolrIndexSplitter(SplitIndexCommand cmd) {
    searcher = cmd.getReq().getSearcher();
    ranges = cmd.ranges;
    paths = cmd.paths;
    cores = cmd.cores;
    hashRouter = cmd.router instanceof HashBasedRouter ? (HashBasedRouter)cmd.router : null;

    if (ranges == null) {
      numPieces =  paths != null ? paths.size() : cores.size();
    } else  {
      numPieces = ranges.size();
      rangesArr = ranges.toArray(new DocRouter.Range[ranges.size()]);
    }
    routeFieldName = cmd.routeFieldName;
    if (routeFieldName == null) {
      field = searcher.getSchema().getUniqueKeyField();
    } else  {
      field = searcher.getSchema().getField(routeFieldName);
    }
    if (cmd.splitKey != null) {
      splitKey = getRouteKey(cmd.splitKey);
    }
    if (cores == null) {
      this.splitMethod = SplitMethod.REWRITE;
    } else {
      this.splitMethod = cmd.splitMethod;
    }
  }

  public void split(NamedList<Object> results) throws IOException {
    SolrCore parentCore = searcher.getCore();
    Directory parentDirectory = searcher.getRawReader().directory();
    Lock parentDirectoryLock = null;
    UpdateLog ulog = parentCore.getUpdateHandler().getUpdateLog();
    if (ulog == null && splitMethod == SplitMethod.LINK) {
      log.warn("No updateLog in parent core, switching to use potentially slower 'splitMethod=rewrite'");
      splitMethod = SplitMethod.REWRITE;
    }
    if (splitMethod == SplitMethod.LINK) {
      RTimerTree t = timings.sub("closeParentIW");
      try {
        // start buffering updates
        ulog.bufferUpdates();
        parentCore.getSolrCoreState().closeIndexWriter(parentCore, false);
        // make sure we can lock the directory for our exclusive use
        parentDirectoryLock = parentDirectory.obtainLock(IndexWriter.WRITE_LOCK_NAME);
        log.info("Splitting in 'link' mode: closed parent IndexWriter...");
        t.stop();
      } catch (Exception e) {
        if (parentDirectoryLock != null) {
          IOUtils.closeWhileHandlingException(parentDirectoryLock);
        }
        try {
          parentCore.getSolrCoreState().openIndexWriter(parentCore);
          ulog.applyBufferedUpdates();
        } catch (Exception e1) {
          log.error("Error reopening IndexWriter after failed close", e1);
          log.error("Original error closing IndexWriter:", e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error reopening IndexWriter after failed close", e1);
        }
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error closing current IndexWriter, aborting 'link' split...", e);
      }
    }
    boolean success = false;
    try {
      RTimerTree t = timings.sub("doSplit");
      doSplit();
      t.stop();
      success = true;
    } catch (Exception e) {
      results.add("failed", e.toString());
      throw e;
    } finally {
      if (splitMethod == SplitMethod.LINK) {
        IOUtils.closeWhileHandlingException(parentDirectoryLock);
        RTimerTree t = timings.sub("reopenParentIW");
        parentCore.getSolrCoreState().openIndexWriter(parentCore);
        t.stop();
        t = timings.sub("parentApplyBufferedUpdates");
        ulog.applyBufferedUpdates();
        t.stop();
        if (log.isInfoEnabled()) {
          log.info("Splitting in 'link' mode {}: re-opened parent IndexWriter.", (success ? "finished" : "FAILED"));
        }
      }
    }
    results.add(CommonParams.TIMING, timings.asNamedList());
  }

  public void doSplit() throws IOException {

    List<LeafReaderContext> leaves = searcher.getRawReader().leaves();
    Directory parentDirectory = searcher.getRawReader().directory();
    List<FixedBitSet[]> segmentDocSets = new ArrayList<>(leaves.size());
    SolrIndexConfig parentConfig = searcher.getCore().getSolrConfig().indexConfig;
    String timestamp = new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.ROOT).format(new Date());

    if (log.isInfoEnabled()) {
      log.info("SolrIndexSplitter: partitions={} segments={}", numPieces, leaves.size());
    }
    RTimerTree t;

    // this tracks round-robin assignment of docs to partitions
    AtomicInteger currentPartition = new AtomicInteger();

    if (splitMethod != SplitMethod.LINK) {
      t = timings.sub("findDocSetsPerLeaf");
      for (LeafReaderContext readerContext : leaves) {
        assert readerContext.ordInParent == segmentDocSets.size();  // make sure we're going in order
        FixedBitSet[] docSets = split(readerContext, numPieces, field, rangesArr, splitKey, hashRouter, currentPartition, false);
        segmentDocSets.add(docSets);
      }
      t.stop();
    }


    Map<IndexReader.CacheKey, FixedBitSet[]> docsToDeleteCache = new ConcurrentHashMap<>();

    // would it be more efficient to write segment-at-a-time to each new index?
    // - need to worry about number of open descriptors
    // - need to worry about if IW.addIndexes does a sync or not...
    // - would be more efficient on the read side, but prob less efficient merging
    for (int partitionNumber=0; partitionNumber<numPieces; partitionNumber++) {
      String partitionName = "SolrIndexSplitter:partition=" + partitionNumber + ",partitionCount=" + numPieces + (ranges != null ? ",range=" + ranges.get(partitionNumber) : "");
      log.info(partitionName);

      boolean success = false;

      RefCounted<IndexWriter> iwRef = null;
      IndexWriter iw;
      if (cores != null && splitMethod != SplitMethod.LINK) {
        SolrCore subCore = cores.get(partitionNumber);
        iwRef = subCore.getUpdateHandler().getSolrCoreState().getIndexWriter(subCore);
        iw = iwRef.get();
      } else {
        if (splitMethod == SplitMethod.LINK) {
          SolrCore subCore = cores.get(partitionNumber);
          String path = subCore.getDataDir() + INDEX_PREFIX + timestamp;
          t = timings.sub("hardLinkCopy");
          t.resume();
          // copy by hard-linking
          Directory splitDir = subCore.getDirectoryFactory().get(path, DirectoryFactory.DirContext.DEFAULT, subCore.getSolrConfig().indexConfig.lockType);
          // the wrapper doesn't hold any resources itself so it doesn't need closing
          HardlinkCopyDirectoryWrapper hardLinkedDir = new HardlinkCopyDirectoryWrapper(splitDir);
          boolean copiedOk = false;
          try {
            for (String file : parentDirectory.listAll()) {
              // we've closed the IndexWriter, so ignore write.lock
              // its file may be present even when IndexWriter is closed but
              // we've already checked that the lock is not held by anyone else
              if (file.equals(IndexWriter.WRITE_LOCK_NAME)) {
                continue;
              }
              hardLinkedDir.copyFrom(parentDirectory, file, file, IOContext.DEFAULT);
            }
            copiedOk = true;
          } finally {
            if (!copiedOk) {
              subCore.getDirectoryFactory().doneWithDirectory(splitDir);
              subCore.getDirectoryFactory().remove(splitDir);
            }
          }
          t.pause();
          IndexWriterConfig iwConfig = parentConfig.toIndexWriterConfig(subCore);
          // don't run merges at this time
          iwConfig.setMergePolicy(NoMergePolicy.INSTANCE);
          t = timings.sub("createSubIW");
          t.resume();
          iw = new SolrIndexWriter(partitionName, splitDir, iwConfig);
          t.pause();
        } else {
          SolrCore core = searcher.getCore();
          String path = paths.get(partitionNumber);
          t = timings.sub("createSubIW");
          t.resume();
          iw = SolrIndexWriter.create(core, partitionName, path,
              core.getDirectoryFactory(), true, core.getLatestSchema(),
              core.getSolrConfig().indexConfig, core.getDeletionPolicy(), core.getCodec());
          t.pause();
        }
      }

      try {
        if (splitMethod == SplitMethod.LINK) {
          t = timings.sub("deleteDocuments");
          t.resume();
          // apply deletions specific to this partition. As a side-effect on the first call this also populates
          // a cache of docsets to delete per leaf reader per partition, which is reused for subsequent partitions.
          iw.deleteDocuments(new SplittingQuery(partitionNumber, field, rangesArr, hashRouter, splitKey, docsToDeleteCache, currentPartition));
          t.pause();
        } else {
          // This removes deletions but optimize might still be needed because sub-shards will have the same number of segments as the parent shard.
          t = timings.sub("addIndexes");
          t.resume();
          for (int segmentNumber = 0; segmentNumber<leaves.size(); segmentNumber++) {
            if (log.isInfoEnabled()) {
              log.info("SolrIndexSplitter: partition # {} partitionCount={} {} segment #={} segmentCount={}", partitionNumber, numPieces
                  , (ranges != null ? " range=" + ranges.get(partitionNumber) : ""), segmentNumber, leaves.size()); // nowarn
            }
            CodecReader subReader = SlowCodecReaderWrapper.wrap(leaves.get(segmentNumber).reader());
            iw.addIndexes(new LiveDocsReader(subReader, segmentDocSets.get(segmentNumber)[partitionNumber]));
          }
          t.pause();
        }
        // we commit explicitly instead of sending a CommitUpdateCommand through the processor chain
        // because the sub-shard cores will just ignore such a commit because the update log is not
        // in active state at this time.
        //TODO no commitUpdateCommand
        SolrIndexWriter.setCommitData(iw, -1);
        t = timings.sub("subIWCommit");
        t.resume();
        iw.commit();
        t.pause();
        success = true;
      } finally {
        if (iwRef != null) {
          iwRef.decref();
        } else {
          if (success) {
            t = timings.sub("subIWClose");
            t.resume();
            iw.close();
            t.pause();
          } else {
            IOUtils.closeWhileHandlingException(iw);
          }
          if (splitMethod == SplitMethod.LINK) {
            SolrCore subCore = cores.get(partitionNumber);
            subCore.getDirectoryFactory().release(iw.getDirectory());
          }
        }
      }
    }
    // all sub-indexes created ok
    // when using hard-linking switch directories & refresh cores
    if (splitMethod == SplitMethod.LINK && cores != null) {
      boolean switchOk = true;
      t = timings.sub("switchSubIndexes");
      for (int partitionNumber = 0; partitionNumber < numPieces; partitionNumber++) {
        SolrCore subCore = cores.get(partitionNumber);
        String indexDirPath = subCore.getIndexDir();

        log.debug("Switching directories");
        String hardLinkPath = subCore.getDataDir() + INDEX_PREFIX + timestamp;
        subCore.modifyIndexProps(INDEX_PREFIX + timestamp);
        try {
          subCore.getUpdateHandler().newIndexWriter(false);
          openNewSearcher(subCore);
        } catch (Exception e) {
          log.error("Failed to switch sub-core {} to {}, split will fail", indexDirPath, hardLinkPath, e);
          switchOk = false;
          break;
        }
      }
      t.stop();
      if (!switchOk) {
        t = timings.sub("rollbackSubIndexes");
        // rollback the switch
        for (int partitionNumber = 0; partitionNumber < numPieces; partitionNumber++) {
          SolrCore subCore = cores.get(partitionNumber);
          Directory dir = null;
          try {
            dir = subCore.getDirectoryFactory().get(subCore.getDataDir(), DirectoryFactory.DirContext.META_DATA,
                subCore.getSolrConfig().indexConfig.lockType);
            dir.deleteFile(IndexFetcher.INDEX_PROPERTIES);
          } finally {
            if (dir != null) {
              subCore.getDirectoryFactory().release(dir);
            }
          }
          // switch back if necessary and remove the hardlinked dir
          String hardLinkPath = subCore.getDataDir() + INDEX_PREFIX + timestamp;
          try {
            dir = subCore.getDirectoryFactory().get(hardLinkPath, DirectoryFactory.DirContext.DEFAULT,
                subCore.getSolrConfig().indexConfig.lockType);
            subCore.getDirectoryFactory().doneWithDirectory(dir);
            subCore.getDirectoryFactory().remove(dir);
          } finally {
            if (dir != null) {
              subCore.getDirectoryFactory().release(dir);
            }
          }
          subCore.getUpdateHandler().newIndexWriter(false);
          try {
            openNewSearcher(subCore);
          } catch (Exception e) {
            log.warn("Error rolling back failed split of {}", hardLinkPath, e);
          }
        }
        t.stop();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "There were errors during index split");
      } else {
        // complete the switch - remove original index
        t = timings.sub("cleanSubIndex");
        for (int partitionNumber = 0; partitionNumber < numPieces; partitionNumber++) {
          SolrCore subCore = cores.get(partitionNumber);
          String oldIndexPath = subCore.getDataDir() + "index";
          Directory indexDir = null;
          try {
            indexDir = subCore.getDirectoryFactory().get(oldIndexPath,
                DirectoryFactory.DirContext.DEFAULT, subCore.getSolrConfig().indexConfig.lockType);
            subCore.getDirectoryFactory().doneWithDirectory(indexDir);
            subCore.getDirectoryFactory().remove(indexDir);
          } finally {
            if (indexDir != null) {
              subCore.getDirectoryFactory().release(indexDir);
            }
          }
        }
        t.stop();
      }
    }
  }

  private void openNewSearcher(SolrCore core) throws Exception {
    @SuppressWarnings({"rawtypes"})
    Future[] waitSearcher = new Future[1];
    core.getSearcher(true, false, waitSearcher, true);
    if (waitSearcher[0] != null) {
      waitSearcher[0].get();
    }
  }

  private class SplittingQuery extends Query {
    private final int partition;
    private final SchemaField field;
    private final DocRouter.Range[] rangesArr;
    private final HashBasedRouter hashRouter;
    private final String splitKey;
    private final Map<IndexReader.CacheKey, FixedBitSet[]> docsToDelete;
    private final AtomicInteger currentPartition;

    SplittingQuery(int partition, SchemaField field, DocRouter.Range[] rangesArr, HashBasedRouter hashRouter, String splitKey,
                   Map<IndexReader.CacheKey, FixedBitSet[]> docsToDelete, AtomicInteger currentPartition) {
      this.partition = partition;
      this.field = field;
      this.rangesArr = rangesArr;
      this.hashRouter = hashRouter;
      this.splitKey = splitKey;
      this.docsToDelete = docsToDelete;
      this.currentPartition = currentPartition;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      return new ConstantScoreWeight(this, boost) {

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          RTimerTree t = timings.sub("findDocsToDelete");
          t.resume();
          FixedBitSet set = findDocsToDelete(context);
          t.pause();
          if (log.isInfoEnabled()) {
            log.info("### partition={}, leaf={}, maxDoc={}, numDels={}, setLen={}, setCard={}"
            , partition, context, context.reader().maxDoc()
            ,context.reader().numDeletedDocs(), set.length(), set.cardinality());
          }
          Bits liveDocs = context.reader().getLiveDocs();
          if (liveDocs != null) {
            // check that we don't delete already deleted docs
            FixedBitSet dels = FixedBitSet.copyOf(liveDocs);
            dels.flip(0, dels.length());
            dels.and(set);
            if (dels.cardinality() > 0) {
              log.error("### INVALID DELS {}", dels.cardinality());
            }
          }
          return new ConstantScoreScorer(this, score(), scoreMode, new BitSetIterator(set, set.length()));
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return false;
        }

        @Override
        public String toString() {
          return "weight(shardSplittingQuery,part" + partition + ")";
        }
      };
    }

    private FixedBitSet findDocsToDelete(LeafReaderContext readerContext) throws IOException {
      // check whether a cached copy of bitsets already exists for this reader
      FixedBitSet[] perPartition = docsToDelete.get(readerContext.reader().getCoreCacheHelper().getKey());
      if (perPartition != null) {
        return perPartition[partition];
      }
      synchronized (docsToDelete) {
        perPartition = docsToDelete.get(readerContext.reader().getCoreCacheHelper().getKey());
        if (perPartition != null) {
          return perPartition[partition];
        }

        perPartition = split(readerContext, numPieces, field, rangesArr, splitKey, hashRouter, currentPartition, true);
        docsToDelete.put(readerContext.reader().getCoreCacheHelper().getKey(), perPartition);
        return perPartition[partition];
      }
    }

    @Override
    public String toString(String field) {
      return "shardSplittingQuery";
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof SplittingQuery)) {
        return false;
      }
      SplittingQuery q = (SplittingQuery)obj;
      return partition == q.partition;
    }

    @Override
    public int hashCode() {
      return partition;
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }
  }

  static FixedBitSet[] split(LeafReaderContext readerContext, int numPieces, SchemaField field, DocRouter.Range[] rangesArr,
                             String splitKey, HashBasedRouter hashRouter, AtomicInteger currentPartition, boolean delete) throws IOException {
    LeafReader reader = readerContext.reader();
    FixedBitSet[] docSets = new FixedBitSet[numPieces];
    for (int i=0; i<docSets.length; i++) {
      docSets[i] = new FixedBitSet(reader.maxDoc());
      if (delete) {
        docSets[i].set(0, reader.maxDoc());
      }
    }
    Bits liveDocs = reader.getLiveDocs();
    if (liveDocs != null && delete) {
      FixedBitSet liveDocsSet = FixedBitSet.copyOf(liveDocs);
      for (FixedBitSet set : docSets) {
        set.and(liveDocsSet);
      }
    }

    Terms terms = reader.terms(field.getName());
    TermsEnum termsEnum = terms==null ? null : terms.iterator();
    if (termsEnum == null) return docSets;

    BytesRef term = null;
    PostingsEnum postingsEnum = null;

    int[] docsMatchingRanges = null;
    if (rangesArr != null) {
      // +1 because documents can belong to *zero*, one, several or all ranges in rangesArr
      docsMatchingRanges = new int[rangesArr.length+1];
    }

    CharsRefBuilder idRef = new CharsRefBuilder();
    for (;;) {
      term = termsEnum.next();
      if (term == null) break;

      // figure out the hash for the term

      // FUTURE: if conversion to strings costs too much, we could
      // specialize and use the hash function that can work over bytes.
      field.getType().indexedToReadable(term, idRef);
      String idString = idRef.toString();

      if (splitKey != null) {
        // todo have composite routers support these kind of things instead
        String part1 = getRouteKey(idString);
        if (part1 == null)
          continue;
        if (!splitKey.equals(part1))  {
          continue;
        }
      }

      int hash = 0;
      if (hashRouter != null && rangesArr != null) {
        hash = hashRouter.sliceHash(idString, null, null, null);
      }

      postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
      postingsEnum = BitsFilteredPostingsEnum.wrap(postingsEnum, liveDocs);
      for (;;) {
        int doc = postingsEnum.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) break;
        if (rangesArr == null) {
          if (delete) {
            docSets[currentPartition.get()].clear(doc);
          } else {
            docSets[currentPartition.get()].set(doc);
          }
          currentPartition.set((currentPartition.get() + 1) % numPieces);
        } else  {
          int matchingRangesCount = 0;
          for (int i=0; i < rangesArr.length; i++) {      // inner-loop: use array here for extra speed.
            if (rangesArr[i].includes(hash)) {
              if (delete) {
                docSets[i].clear(doc);
              } else {
                docSets[i].set(doc);
              }
              ++matchingRangesCount;
            }
          }
          docsMatchingRanges[matchingRangesCount]++;
        }
      }
    }

    if (docsMatchingRanges != null) {
      for (int ii = 0; ii < docsMatchingRanges.length; ii++) {
        if (0 == docsMatchingRanges[ii]) continue;
        switch (ii) {
          case 0:
            // document loss
            log.error("Splitting {}: {} documents belong to no shards and will be dropped",
                reader, docsMatchingRanges[ii]);
            break;
          case 1:
            // normal case, each document moves to one of the sub-shards
            log.info("Splitting {}: {} documents will move into a sub-shard",
                reader, docsMatchingRanges[ii]);
            break;
          default:
            // document duplication
            log.error("Splitting {}: {} documents will be moved to multiple ({}) sub-shards",
                reader, docsMatchingRanges[ii], ii);
            break;
        }
      }
    }

    return docSets;
  }

  public static String getRouteKey(String idString) {
    int idx = idString.indexOf(CompositeIdRouter.SEPARATOR);
    if (idx <= 0) return null;
    String part1 = idString.substring(0, idx);
    int commaIdx = part1.indexOf(CompositeIdRouter.bitsSeparator);
    if (commaIdx > 0 && commaIdx + 1 < part1.length())  {
      char ch = part1.charAt(commaIdx + 1);
      if (ch >= '0' && ch <= '9') {
        part1 = part1.substring(0, commaIdx);
      }
    }
    return part1;
  }


  // change livedocs on the reader to delete those docs we don't want
  static class LiveDocsReader extends FilterCodecReader {
    final FixedBitSet liveDocs;
    final int numDocs;

    public LiveDocsReader(CodecReader in, FixedBitSet liveDocs) {
      super(in);
      this.liveDocs = liveDocs;
      this.numDocs = liveDocs.cardinality();
    }

    @Override
    public int numDocs() {
      return numDocs;
    }

    @Override
    public Bits getLiveDocs() {
      return liveDocs;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

}
