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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergePolicy.MergeSpecification;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;

import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * This handler exposes information about last commit generation segments
 */
public class SegmentsInfoRequestHandler extends RequestHandlerBase {

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
      throws Exception {
    rsp.add("segments", getSegmentsInfo(req, rsp));
    rsp.setHttpCaching(false);
  }

  private SimpleOrderedMap<Object> getSegmentsInfo(SolrQueryRequest req, SolrQueryResponse rsp)
      throws Exception {
    SolrIndexSearcher searcher = req.getSearcher();

    SegmentInfos infos =
        SegmentInfos.readLatestCommit(searcher.getIndexReader().directory());

    List<String> mergeCandidates = getMergeCandidatesNames(req, infos);

    SimpleOrderedMap<Object> segmentInfos = new SimpleOrderedMap<>();
    SimpleOrderedMap<Object> segmentInfo = null;
    for (SegmentCommitInfo segmentCommitInfo : infos) {
      segmentInfo = getSegmentInfo(segmentCommitInfo);
      if (mergeCandidates.contains(segmentCommitInfo.info.name)) {
        segmentInfo.add("mergeCandidate", true);
      }
      segmentInfos.add((String) segmentInfo.get(NAME), segmentInfo);
    }

    return segmentInfos;
  }

  private SimpleOrderedMap<Object> getSegmentInfo(
      SegmentCommitInfo segmentCommitInfo) throws IOException {
    SimpleOrderedMap<Object> segmentInfoMap = new SimpleOrderedMap<>();

    segmentInfoMap.add(NAME, segmentCommitInfo.info.name);
    segmentInfoMap.add("delCount", segmentCommitInfo.getDelCount());
    segmentInfoMap.add("sizeInBytes", segmentCommitInfo.sizeInBytes());
    segmentInfoMap.add("size", segmentCommitInfo.info.maxDoc());
    Long timestamp = Long.parseLong(segmentCommitInfo.info.getDiagnostics()
        .get("timestamp"));
    segmentInfoMap.add("age", new Date(timestamp));
    segmentInfoMap.add("source",
        segmentCommitInfo.info.getDiagnostics().get("source"));
    segmentInfoMap.add("version", segmentCommitInfo.info.getVersion().toString());

    return segmentInfoMap;
  }

  private List<String> getMergeCandidatesNames(SolrQueryRequest req, SegmentInfos infos) throws IOException {
    List<String> result = new ArrayList<String>();
    RefCounted<IndexWriter> refCounted = req.getCore().getSolrCoreState().getIndexWriter(req.getCore());
    try {
      IndexWriter indexWriter = refCounted.get();
      //get chosen merge policy
      MergePolicy mp = indexWriter.getConfig().getMergePolicy();
      //Find merges
      MergeSpecification findMerges = mp.findMerges(MergeTrigger.EXPLICIT, infos, indexWriter);
      if (findMerges != null && findMerges.merges != null && findMerges.merges.size() > 0) {
        for (OneMerge merge : findMerges.merges) {
          //TODO: add merge grouping
          for (SegmentCommitInfo mergeSegmentInfo : merge.segments) {
            result.add(mergeSegmentInfo.info.name);
          }
        }
      }

      return result;
    } finally {
      refCounted.decref();
    }
  }

  @Override
  public String getDescription() {
    return "Lucene segments info.";
  }
}
