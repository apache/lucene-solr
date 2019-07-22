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

package org.apache.solr.handler.admin;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkShardTerms;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.SolrIndexSplitter;
import org.apache.solr.update.SplitIndexCommand;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.DocCollection.DOC_ROUTER;
import static org.apache.solr.common.params.CommonParams.PATH;
import static org.apache.solr.common.params.CoreAdminParams.GET_RANGES;


class SplitOp implements CoreAdminHandler.CoreAdminOp {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    SolrParams params = it.req.getParams();

    String splitKey = params.get("split.key");
    String[] newCoreNames = params.getParams("targetCore");
    String cname = params.get(CoreAdminParams.CORE, "");

    if ( params.getBool(GET_RANGES, false) ) {
      handleGetRanges(it, cname);
      return;
    }

    List<DocRouter.Range> ranges = null;

    String[] pathsArr = params.getParams(PATH);
    String rangesStr = params.get(CoreAdminParams.RANGES);    // ranges=a-b,c-d,e-f
    if (rangesStr != null) {
      String[] rangesArr = rangesStr.split(",");
      if (rangesArr.length == 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "There must be at least one range specified to split an index");
      } else {
        ranges = new ArrayList<>(rangesArr.length);
        for (String r : rangesArr) {
          try {
            ranges.add(DocRouter.DEFAULT.fromString(r));
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Exception parsing hexadecimal hash range: " + r, e);
          }
        }
      }
    }

    if ((pathsArr == null || pathsArr.length == 0) && (newCoreNames == null || newCoreNames.length == 0)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Either path or targetCore param must be specified");
    }

    log.info("Invoked split action for core: " + cname);
    String methodStr = params.get(CommonAdminParams.SPLIT_METHOD, SolrIndexSplitter.SplitMethod.REWRITE.toLower());
    SolrIndexSplitter.SplitMethod splitMethod = SolrIndexSplitter.SplitMethod.get(methodStr);
    if (splitMethod == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unsupported value of '" + CommonAdminParams.SPLIT_METHOD + "': " + methodStr);
    }
    SolrCore parentCore = it.handler.coreContainer.getCore(cname);
    List<SolrCore> newCores = null;
    SolrQueryRequest req = null;

    try {
      // TODO: allow use of rangesStr in the future
      List<String> paths = null;
      int partitions = pathsArr != null ? pathsArr.length : newCoreNames.length;

      DocRouter router = null;
      String routeFieldName = null;
      if (it.handler.coreContainer.isZooKeeperAware()) {
        ClusterState clusterState = it.handler.coreContainer.getZkController().getClusterState();
        String collectionName = parentCore.getCoreDescriptor().getCloudDescriptor().getCollectionName();
        DocCollection collection = clusterState.getCollection(collectionName);
        String sliceName = parentCore.getCoreDescriptor().getCloudDescriptor().getShardId();
        Slice slice = collection.getSlice(sliceName);
        router = collection.getRouter() != null ? collection.getRouter() : DocRouter.DEFAULT;
        if (ranges == null) {
          DocRouter.Range currentRange = slice.getRange();
          ranges = currentRange != null ? router.partitionRange(partitions, currentRange) : null;
        }
        Object routerObj = collection.get(DOC_ROUTER); // for back-compat with Solr 4.4
        if (routerObj instanceof Map) {
          Map routerProps = (Map) routerObj;
          routeFieldName = (String) routerProps.get("field");
        }
      }

      if (pathsArr == null) {
        newCores = new ArrayList<>(partitions);
        for (String newCoreName : newCoreNames) {
          SolrCore newcore = it.handler.coreContainer.getCore(newCoreName);
          if (newcore != null) {
            newCores.add(newcore);
            if (it.handler.coreContainer.isZooKeeperAware()) {
              // this core must be the only replica in its shard otherwise
              // we cannot guarantee consistency between replicas because when we add data to this replica
              CloudDescriptor cd = newcore.getCoreDescriptor().getCloudDescriptor();
              ClusterState clusterState = it.handler.coreContainer.getZkController().getClusterState();
              if (clusterState.getCollection(cd.getCollectionName()).getSlice(cd.getShardId()).getReplicas().size() != 1) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "Core with core name " + newCoreName + " must be the only replica in shard " + cd.getShardId());
              }
            }
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Core with core name " + newCoreName + " expected but doesn't exist.");
          }
        }
      } else {
        paths = Arrays.asList(pathsArr);
      }

      req = new LocalSolrQueryRequest(parentCore, params);

      SplitIndexCommand cmd = new SplitIndexCommand(req, it.rsp, paths, newCores, ranges, router, routeFieldName, splitKey, splitMethod);
      parentCore.getUpdateHandler().split(cmd);

      if (it.handler.coreContainer.isZooKeeperAware()) {
        for (SolrCore newcore : newCores) {
          // the index of the core changed from empty to have some data, its term must be not zero
          CloudDescriptor cd = newcore.getCoreDescriptor().getCloudDescriptor();
          ZkShardTerms zkShardTerms = it.handler.coreContainer.getZkController().getShardTerms(cd.getCollectionName(), cd.getShardId());
          zkShardTerms.ensureHighestTermsAreNotZero();
        }
      }

      // After the split has completed, someone (here?) should start the process of replaying the buffered updates.
    } catch (Exception e) {
      log.error("ERROR executing split:", e);
      throw e;
    } finally {
      if (req != null) req.close();
      if (parentCore != null) parentCore.close();
      if (newCores != null) {
        for (SolrCore newCore : newCores) {
          newCore.close();
        }
      }
    }
  }




  // This is called when splitByPrefix is used.
  // The overseer called us to get recommended splits taking into
  // account actual document distribution over the hash space.
  private void handleGetRanges(CoreAdminHandler.CallInfo it, String coreName) throws Exception {

    SolrCore parentCore = it.handler.coreContainer.getCore(coreName);
    if (parentCore == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown core " + coreName);
    }

    RefCounted<SolrIndexSearcher> searcherHolder = parentCore.getRealtimeSearcher();

    try {
      if (!it.handler.coreContainer.isZooKeeperAware()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Shard splitByPrefix requires SolrCloud mode.");
      } else {
        String routeFieldName = "id";
        String prefixField = "id_prefix";

        ClusterState clusterState = it.handler.coreContainer.getZkController().getClusterState();
        String collectionName = parentCore.getCoreDescriptor().getCloudDescriptor().getCollectionName();
        DocCollection collection = clusterState.getCollection(collectionName);
        String sliceName = parentCore.getCoreDescriptor().getCloudDescriptor().getShardId();
        Slice slice = collection.getSlice(sliceName);
        DocRouter router = collection.getRouter() != null ? collection.getRouter() : DocRouter.DEFAULT;
        DocRouter.Range currentRange = slice.getRange();

        Object routerObj = collection.get(DOC_ROUTER); // for back-compat with Solr 4.4
        if (routerObj instanceof Map) {
          Map routerProps = (Map) routerObj;
          routeFieldName = (String) routerProps.get("field");
        }

        Collection<RangeCount> counts = getHashHistogram(searcherHolder.get(), prefixField, router, collection);
        Collection<DocRouter.Range> splits = getSplits(counts, currentRange);
        String splitString = toSplitString(splits);

        if (splitString == null) {
          return;
        }

        it.rsp.add(CoreAdminParams.RANGES, splitString);
      }
    } finally {
      if (searcherHolder != null) searcherHolder.decref();
      if (parentCore != null) parentCore.close();
    }
  }



  static class RangeCount implements Comparable<RangeCount> {
    DocRouter.Range range;
    int count;

    public RangeCount(DocRouter.Range range, int count) {
      this.range = range;
      this.count = count;
    }

    @Override
    public int hashCode() {
      return range.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof RangeCount)) return false;
      return this.range.equals( ((RangeCount)obj).range );
    }

    @Override
    public int compareTo(RangeCount o) {
      return this.range.compareTo(o.range);
    }
  }


  static String toSplitString(Collection<DocRouter.Range> splits) throws Exception {
    if (splits == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    for (DocRouter.Range range : splits) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(range);
    }


    return sb.toString();
  }


  // Returns a list of range counts sorted by the range lower bound
  static Collection<RangeCount> getHashHistogram(SolrIndexSearcher searcher, String prefixField, DocRouter router, DocCollection collection) throws IOException {
    TreeMap<DocRouter.Range,RangeCount> counts = new TreeMap<>();

    Terms terms = MultiTerms.getTerms(searcher.getIndexReader(), prefixField);
    if (terms == null) {
      return counts.values();
    }

    TermsEnum termsEnum = terms.iterator();
    for (;;) {
      BytesRef term = termsEnum.next();
      if (term == null) break;

      String termStr = term.utf8ToString();
      int firstSep = termStr.indexOf(CompositeIdRouter.SEPARATOR);
      // truncate to first separator since we don't support multiple levels currently
      if (firstSep != termStr.length()-1 && firstSep > 0) {
        termStr = termStr.substring(0, firstSep+1);
      }
      DocRouter.Range range = router.getSearchRangeSingle(termStr, null, collection);
      int numDocs = termsEnum.docFreq();

      RangeCount rangeCount = new RangeCount(range, numDocs);

      RangeCount prev = counts.put(rangeCount.range, rangeCount);
      if (prev != null) {
        // we hit a hash collision or truncated a prefix to first level, so add the buckets together.
        rangeCount.count += prev.count;
      }
    }

    return counts.values();
  }


  // returns the list of recommended splits, or null if there is not enough information
  static Collection<DocRouter.Range> getSplits(Collection<RangeCount> rawCounts, DocRouter.Range currentRange) throws Exception {

    int totalCount = 0;
    RangeCount biggest = null; // keep track of the largest in case we need to split it out into it's own shard
    RangeCount last = null;  // keep track of what the last range is

    // Remove counts that don't overlap with currentRange (can happen if someone overrode document routing)
    List<RangeCount> counts = new ArrayList<>(rawCounts.size());
    for (RangeCount rangeCount : rawCounts) {
      if (!rangeCount.range.overlaps(currentRange)) {
        continue;
      }
      totalCount += rangeCount.count;
      if (biggest == null || rangeCount.count > biggest.count) {
        biggest = rangeCount;
      }
      counts.add(rangeCount);
      last = rangeCount;
    }

    if (counts.size() == 0) {
      // we don't have any data to go off of, so do the split the normal way
      return null;
    }


    List<DocRouter.Range> targetRanges = new ArrayList<>();

    if (counts.size() == 1) {
      // We have a single range, so we should split it.

      // It may already be a partial range, so figure that out
      int lower = Math.max(last.range.min, currentRange.min);
      int upper = Math.min(last.range.max, currentRange.max);
      int mid = lower + (upper-lower)/2;
      if (mid == lower || mid == upper) {
        // shard too small... this should pretty much never happen, but use default split logic if it does.
        return null;
      }

      // Make sure to include the shard's current range in the new ranges so we don't create useless empty shards.
      DocRouter.Range lowerRange = new DocRouter.Range(currentRange.min, mid);
      DocRouter.Range upperRange = new DocRouter.Range(mid+1, currentRange.max);
      targetRanges.add(lowerRange);
      targetRanges.add(upperRange);

      return targetRanges;
    }

    // We have at least two ranges, so we want to partition the ranges
    // and avoid splitting any individual range.

    int targetCount = totalCount / 2;
    RangeCount middle = null;
    RangeCount prev = null;
    int currCount = 0;
    for (RangeCount rangeCount : counts) {
      currCount += rangeCount.count;
      if (currCount >= targetCount) {  // this should at least be true on the last range
        middle = rangeCount;
        break;
      }
      prev = rangeCount;
    }

    // check if using the range before the middle one would make a better split point
    int overError = currCount - targetCount;  // error if we include middle in first split
    int underError = targetCount - (currCount - middle.count); // error if we include middle in second split
    if (underError < overError) {
      middle = prev;
    }

    // if the middle range turned out to be the last one, pick the one before it instead
    if (middle == last) {
      middle = prev;
    }

    // Make sure to include the shard's current range in the new ranges so we don't create useless empty shards.
    DocRouter.Range lowerRange = new DocRouter.Range(currentRange.min, middle.range.max);
    DocRouter.Range upperRange = new DocRouter.Range(middle.range.max+1, currentRange.max);
    targetRanges.add(lowerRange);
    targetRanges.add(upperRange);

    return targetRanges;
  }


}
