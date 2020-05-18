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
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergePolicy.MergeSpecification;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.Version;
import org.apache.solr.common.luke.FieldFlag;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.lucene.index.IndexOptions.DOCS;
import static org.apache.lucene.index.IndexOptions.DOCS_AND_FREQS;
import static org.apache.lucene.index.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * This handler exposes information about last commit generation segments
 */
public class SegmentsInfoRequestHandler extends RequestHandlerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String FIELD_INFO_PARAM = "fieldInfo";
  public static final String CORE_INFO_PARAM = "coreInfo";
  public static final String SIZE_INFO_PARAM = "sizeInfo";
  public static final String RAW_SIZE_PARAM = "rawSize";
  public static final String RAW_SIZE_SUMMARY_PARAM = "rawSizeSummary";
  public static final String RAW_SIZE_DETAILS_PARAM = "rawSizeDetails";
  public static final String RAW_SIZE_SAMPLING_PERCENT_PARAM = "rawSizeSamplingPercent";

  private static final List<String> FI_LEGEND;

  static {
    FI_LEGEND = Arrays.asList(
        FieldFlag.INDEXED.toString(),
        FieldFlag.DOC_VALUES.toString(),
        "xxx - DocValues type",
        FieldFlag.TERM_VECTOR_STORED.toString(),
        FieldFlag.OMIT_NORMS.toString(),
        FieldFlag.OMIT_TF.toString(),
        FieldFlag.OMIT_POSITIONS.toString(),
        FieldFlag.STORE_OFFSETS_WITH_POSITIONS.toString(),
        "p - field has payloads",
        "s - field uses soft deletes",
        ":x:x:x - point data dim : index dim : num bytes");
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
      throws Exception {
    getSegmentsInfo(req, rsp);
    rsp.setHttpCaching(false);
  }

  private static final double GB = 1024.0 * 1024.0 * 1024.0;

  private void getSegmentsInfo(SolrQueryRequest req, SolrQueryResponse rsp)
      throws Exception {
    boolean withFieldInfo = req.getParams().getBool(FIELD_INFO_PARAM, false);
    boolean withCoreInfo = req.getParams().getBool(CORE_INFO_PARAM, false);
    boolean withSizeInfo = req.getParams().getBool(SIZE_INFO_PARAM, false);
    boolean withRawSizeInfo = req.getParams().getBool(RAW_SIZE_PARAM, false);
    boolean withRawSizeSummary = req.getParams().getBool(RAW_SIZE_SUMMARY_PARAM, false);
    boolean withRawSizeDetails = req.getParams().getBool(RAW_SIZE_DETAILS_PARAM, false);
    if (withRawSizeSummary || withRawSizeDetails) {
      withRawSizeInfo  = true;
    }
    SolrIndexSearcher searcher = req.getSearcher();

    SegmentInfos infos =
        SegmentInfos.readLatestCommit(searcher.getIndexReader().directory());

    SimpleOrderedMap<Object> segmentInfos = new SimpleOrderedMap<>();

    SolrCore core = req.getCore();
    SimpleOrderedMap<Object> infosInfo = new SimpleOrderedMap<>();
    Version minVersion = infos.getMinSegmentLuceneVersion();
    if (minVersion != null) {
      infosInfo.add("minSegmentLuceneVersion", minVersion.toString());
    }
    Version commitVersion = infos.getCommitLuceneVersion();
    if (commitVersion != null) {
      infosInfo.add("commitLuceneVersion", commitVersion.toString());
    }
    infosInfo.add("numSegments", infos.size());
    infosInfo.add("segmentsFileName", infos.getSegmentsFileName());
    infosInfo.add("totalMaxDoc", infos.totalMaxDoc());
    infosInfo.add("userData", infos.userData);
    if (withCoreInfo) {
      SimpleOrderedMap<Object> coreInfo = new SimpleOrderedMap<>();
      infosInfo.add("core", coreInfo);
      coreInfo.add("startTime", core.getStartTimeStamp().getTime() + "(" + core.getStartTimeStamp() + ")");
      coreInfo.add("dataDir", core.getDataDir());
      coreInfo.add("indexDir", core.getIndexDir());
      coreInfo.add("sizeInGB", (double)core.getIndexSize() / GB);

      RefCounted<IndexWriter> iwRef = core.getSolrCoreState().getIndexWriter(core);
      if (iwRef != null) {
        try {
          IndexWriter iw = iwRef.get();
          String iwConfigStr = iw.getConfig().toString();
          SimpleOrderedMap<Object> iwConfig = new SimpleOrderedMap<>();
          // meh ...
          String[] lines = iwConfigStr.split("\\n");
          for (String line : lines) {
            String[] parts = line.split("=");
            if (parts.length < 2) {
              continue;
            }
            iwConfig.add(parts[0], parts[1]);
          }
          coreInfo.add("indexWriterConfig", iwConfig);
        } finally {
          iwRef.decref();
        }
      }
    }
    SimpleOrderedMap<Object> segmentInfo = null;
    List<SegmentCommitInfo> sortable = new ArrayList<>(infos.asList());
    // Order by the number of live docs. The display is logarithmic so it is a little jumbled visually
    sortable.sort((s1, s2) ->
      (s2.info.maxDoc() - s2.getDelCount()) - (s1.info.maxDoc() - s1.getDelCount())
    );

    List<String> mergeCandidates = new ArrayList<>();
    SimpleOrderedMap<Object> runningMerges = getMergeInformation(req, infos, mergeCandidates);
    List<LeafReaderContext> leafContexts = searcher.getIndexReader().leaves();
    IndexSchema schema = req.getSchema();
    for (SegmentCommitInfo segmentCommitInfo : sortable) {
      segmentInfo = getSegmentInfo(segmentCommitInfo, withSizeInfo, withFieldInfo, leafContexts, schema);
      if (mergeCandidates.contains(segmentCommitInfo.info.name)) {
        segmentInfo.add("mergeCandidate", true);
      }
      segmentInfos.add((String) segmentInfo.get(NAME), segmentInfo);
    }

    rsp.add("info", infosInfo);
    if (runningMerges.size() > 0) {
      rsp.add("runningMerges", runningMerges);
    }
    if (withFieldInfo) {
      rsp.add("fieldInfoLegend", FI_LEGEND);
    }
    rsp.add("segments", segmentInfos);
    if (withRawSizeInfo) {
      IndexSizeEstimator estimator = new IndexSizeEstimator(searcher.getRawReader(), 20, 100, withRawSizeSummary, withRawSizeDetails);
      Object samplingPercentVal = req.getParams().get(RAW_SIZE_SAMPLING_PERCENT_PARAM);
      if (samplingPercentVal != null) {
        estimator.setSamplingPercent(Float.parseFloat(String.valueOf(samplingPercentVal)));
      }
      IndexSizeEstimator.Estimate estimate = estimator.estimate();
      SimpleOrderedMap<Object> estimateMap = new SimpleOrderedMap<>();
      // make the units more user-friendly
      estimateMap.add(IndexSizeEstimator.FIELDS_BY_SIZE, estimate.getHumanReadableFieldsBySize());
      estimateMap.add(IndexSizeEstimator.TYPES_BY_SIZE, estimate.getHumanReadableTypesBySize());
      if (estimate.getSummary() != null) {
        estimateMap.add(IndexSizeEstimator.SUMMARY, estimate.getSummary());
      }
      if (estimate.getDetails() != null) {
        estimateMap.add(IndexSizeEstimator.DETAILS, estimate.getDetails());
      }
      rsp.add("rawSize", estimateMap);
    }
  }

  private SimpleOrderedMap<Object> getSegmentInfo(
      SegmentCommitInfo segmentCommitInfo, boolean withSizeInfo, boolean withFieldInfos,
      List<LeafReaderContext> leafContexts, IndexSchema schema) throws IOException {
    SimpleOrderedMap<Object> segmentInfoMap = new SimpleOrderedMap<>();

    segmentInfoMap.add(NAME, segmentCommitInfo.info.name);
    segmentInfoMap.add("delCount", segmentCommitInfo.getDelCount());
    segmentInfoMap.add("softDelCount", segmentCommitInfo.getSoftDelCount());
    segmentInfoMap.add("hasFieldUpdates", segmentCommitInfo.hasFieldUpdates());
    segmentInfoMap.add("sizeInBytes", segmentCommitInfo.sizeInBytes());
    segmentInfoMap.add("size", segmentCommitInfo.info.maxDoc());
    Long timestamp = Long.parseLong(segmentCommitInfo.info.getDiagnostics()
        .get("timestamp"));
    segmentInfoMap.add("age", new Date(timestamp));
    segmentInfoMap.add("source",
        segmentCommitInfo.info.getDiagnostics().get("source"));
    segmentInfoMap.add("version", segmentCommitInfo.info.getVersion().toString());
    // don't open a new SegmentReader - try to find the right one from the leaf contexts
    SegmentReader seg = null;
    for (LeafReaderContext lrc : leafContexts) {
      LeafReader leafReader = lrc.reader();
      leafReader = FilterLeafReader.unwrap(leafReader);
      if (leafReader instanceof SegmentReader) {
        SegmentReader sr = (SegmentReader)leafReader;
        if (sr.getSegmentInfo().info.equals(segmentCommitInfo.info)) {
          seg = sr;
          break;
        }
      }
    }
    if (seg != null) {
      LeafMetaData metaData = seg.getMetaData();
      if (metaData != null) {
        segmentInfoMap.add("createdVersionMajor", metaData.getCreatedVersionMajor());
        segmentInfoMap.add("minVersion", metaData.getMinVersion().toString());
        if (metaData.getSort() != null) {
          segmentInfoMap.add("sort", metaData.getSort().toString());
        }
      }
    }
    if (!segmentCommitInfo.info.getDiagnostics().isEmpty()) {
      segmentInfoMap.add("diagnostics", segmentCommitInfo.info.getDiagnostics());
    }
    if (!segmentCommitInfo.info.getAttributes().isEmpty()) {
      segmentInfoMap.add("attributes", segmentCommitInfo.info.getAttributes());
    }
    if (withSizeInfo) {
      Directory dir = segmentCommitInfo.info.dir;
      List<Pair<String, Long>> files = segmentCommitInfo.files().stream()
          .map(f -> {
            long size = -1;
            try {
              size = dir.fileLength(f);
            } catch (IOException e) {
            }
            return new Pair<String, Long>(f, size);
          }).sorted((p1, p2) -> {
            if (p1.second() > p2.second()) {
              return -1;
            } else if (p1.second() < p2.second()) {
              return 1;
            } else {
              return 0;
            }
          }).collect(Collectors.toList());
      if (!files.isEmpty()) {
        SimpleOrderedMap<Object> topFiles = new SimpleOrderedMap<>();
        for (int i = 0; i < Math.min(files.size(), 5); i++) {
          Pair<String, Long> p = files.get(i);
          topFiles.add(p.first(), RamUsageEstimator.humanReadableUnits(p.second()));
        }
        segmentInfoMap.add("largestFiles", topFiles);
      }
    }
    if (seg != null && withSizeInfo) {
      SimpleOrderedMap<Object> ram = new SimpleOrderedMap<>();
      ram.add("total", seg.ramBytesUsed());
      for (Accountable ac : seg.getChildResources()) {
        accountableToMap(ac, ram::add);
      }
      segmentInfoMap.add("ramBytesUsed", ram);
    }
    if (withFieldInfos) {
      if (seg == null) {
        log.debug("Skipping segment info - not available as a SegmentReader: {}", segmentCommitInfo);
      } else {
        FieldInfos fis = seg.getFieldInfos();
        SimpleOrderedMap<Object> fields = new SimpleOrderedMap<>();
        for (FieldInfo fi : fis) {
          fields.add(fi.name, getFieldInfo(seg, fi, schema));
        }
        segmentInfoMap.add("fields", fields);
      }
    }

    return segmentInfoMap;
  }

  private void accountableToMap(Accountable accountable, BiConsumer<String, Object> consumer) {
    Collection<Accountable> children = accountable.getChildResources();
    if (children != null && !children.isEmpty()) {
      LinkedHashMap<String, Object> map = new LinkedHashMap<>();
      map.put("total", accountable.ramBytesUsed());
      for (Accountable child : children) {
        accountableToMap(child, map::put);
      }
      consumer.accept(accountable.toString(), map);
    } else {
      consumer.accept(accountable.toString(), accountable.ramBytesUsed());
    }
  }

  private SimpleOrderedMap<Object> getFieldInfo(SegmentReader reader, FieldInfo fi, IndexSchema schema) {
    SimpleOrderedMap<Object> fieldFlags = new SimpleOrderedMap<>();
    StringBuilder flags = new StringBuilder();
    IndexOptions opts = fi.getIndexOptions();
    flags.append( (opts != IndexOptions.NONE) ? FieldFlag.INDEXED.getAbbreviation() : '-' );
    DocValuesType dvt = fi.getDocValuesType();
    if (dvt != DocValuesType.NONE) {
      flags.append(FieldFlag.DOC_VALUES.getAbbreviation());
      switch (dvt) {
        case NUMERIC:
          flags.append("num");
          break;
        case BINARY:
          flags.append("bin");
          break;
        case SORTED:
          flags.append("srt");
          break;
        case SORTED_NUMERIC:
          flags.append("srn");
          break;
        case SORTED_SET:
          flags.append("srs");
          break;
        default:
          flags.append("???"); // should not happen
      }
    } else {
      flags.append("----");
    }
    flags.append( (fi.hasVectors()) ? FieldFlag.TERM_VECTOR_STORED.getAbbreviation() : '-' );
    flags.append( (fi.omitsNorms()) ? FieldFlag.OMIT_NORMS.getAbbreviation() : '-' );

    flags.append( (DOCS == opts ) ?
        FieldFlag.OMIT_TF.getAbbreviation() : '-' );

    flags.append((DOCS_AND_FREQS == opts) ?
        FieldFlag.OMIT_POSITIONS.getAbbreviation() : '-');

    flags.append((DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS == opts) ?
        FieldFlag.STORE_OFFSETS_WITH_POSITIONS.getAbbreviation() : '-');

    flags.append( (fi.hasPayloads() ? "p" : "-"));
    flags.append( (fi.isSoftDeletesField() ? "s" : "-"));
    if (fi.getPointDimensionCount() > 0 || fi.getPointIndexDimensionCount() > 0) {
      flags.append(":");
      flags.append(fi.getPointDimensionCount()).append(':');
      flags.append(fi.getPointIndexDimensionCount()).append(':');
      flags.append(fi.getPointNumBytes());
    }

    fieldFlags.add("flags", flags.toString());
    try {
      Terms terms = reader.terms(fi.name);
      if (terms != null) {
        fieldFlags.add("docCount", terms.getDocCount());
        fieldFlags.add("sumDocFreq", terms.getSumDocFreq());
        fieldFlags.add("sumTotalTermFreq", terms.getSumTotalTermFreq());
      }
    } catch (Exception e) {
      log.debug("Exception retrieving term stats for field {}", fi.name, e);
    }

    // probably too much detail?
//    Map<String, String> attributes = fi.attributes();
//    if (!attributes.isEmpty()) {
//      fieldFlags.add("attributes", attributes);
//    }

    // check compliance of the index with the current schema
    SchemaField sf = schema.getFieldOrNull(fi.name);
    boolean hasPoints = fi.getPointDimensionCount() > 0 || fi.getPointIndexDimensionCount() > 0;

    if (sf != null) {
      fieldFlags.add("schemaType", sf.getType().getTypeName());
      SimpleOrderedMap<Object> nonCompliant = new SimpleOrderedMap<>();
      if (sf.hasDocValues() &&
          fi.getDocValuesType() == DocValuesType.NONE &&
          fi.getIndexOptions() != IndexOptions.NONE) {
        nonCompliant.add("docValues", "schema=" + sf.getType().getUninversionType(sf) + ", segment=false");
      }
      if (!sf.hasDocValues() &&
          fi.getDocValuesType() != DocValuesType.NONE) {
        nonCompliant.add("docValues", "schema=false, segment=" + fi.getDocValuesType().toString());
      }
      if (!sf.isPolyField()) { // difficult to find all sub-fields in a general way
        if (sf.indexed() != ((fi.getIndexOptions() != IndexOptions.NONE) || hasPoints)) {
          nonCompliant.add("indexed", "schema=" + sf.indexed() + ", segment=" + fi.getIndexOptions());
        }
      }
      if (!hasPoints && (sf.omitNorms() != fi.omitsNorms())) {
        nonCompliant.add("omitNorms", "schema=" + sf.omitNorms() + ", segment=" + fi.omitsNorms());
      }
      if (sf.storeTermVector() != fi.hasVectors()) {
        nonCompliant.add("termVectors", "schema=" + sf.storeTermVector() + ", segment=" + fi.hasVectors());
      }
      if (sf.storeOffsetsWithPositions() != (fi.getIndexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)) {
        nonCompliant.add("storeOffsetsWithPositions", "schema=" + sf.storeOffsetsWithPositions() + ", segment=" + fi.getIndexOptions());
      }

      if (nonCompliant.size() > 0) {
        nonCompliant.add("schemaField", sf.toString());
        fieldFlags.add("nonCompliant", nonCompliant);
      }
    } else {
      fieldFlags.add("schemaType", "(UNKNOWN)");
    }
    return fieldFlags;
  }

  // returns a map of currently running merges, and populates a list of candidate segments for merge
  private SimpleOrderedMap<Object> getMergeInformation(SolrQueryRequest req, SegmentInfos infos, List<String> mergeCandidates) throws IOException {
    SimpleOrderedMap<Object> result = new SimpleOrderedMap<>();
    RefCounted<IndexWriter> refCounted = req.getCore().getSolrCoreState().getIndexWriter(req.getCore());
    try {
      IndexWriter indexWriter = refCounted.get();
      if (indexWriter instanceof SolrIndexWriter) {
        result.addAll(((SolrIndexWriter)indexWriter).getRunningMerges());
      }
      //get chosen merge policy
      MergePolicy mp = indexWriter.getConfig().getMergePolicy();
      //Find merges
      MergeSpecification findMerges = mp.findMerges(MergeTrigger.EXPLICIT, infos, indexWriter);
      if (findMerges != null && findMerges.merges != null && findMerges.merges.size() > 0) {
        for (OneMerge merge : findMerges.merges) {
          //TODO: add merge grouping
          for (SegmentCommitInfo mergeSegmentInfo : merge.segments) {
            mergeCandidates.add(mergeSegmentInfo.info.name);
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

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }
}
