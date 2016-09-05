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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocList;
import org.apache.solr.search.QParser;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.update.IndexFingerprint;
import org.apache.solr.update.PeerSync;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RealTimeGetComponent extends SearchComponent
{
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String COMPONENT_NAME = "get";

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    // Set field flags
    ReturnFields returnFields = new SolrReturnFields( rb.req );
    rb.rsp.setReturnFields( returnFields );
  }


  @Override
  public void process(ResponseBuilder rb) throws IOException
  {
    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;
    SolrParams params = req.getParams();

    if (!params.getBool(COMPONENT_NAME, true)) {
      return;
    }
    
    // This seems rather kludgey, may there is better way to indicate
    // that replica can support handling version ranges
    String val = params.get("checkCanHandleVersionRanges");
    if(val != null) {
      rb.rsp.add("canHandleVersionRanges", true);
      return;
    }
    
    val = params.get("getVersions");
    if (val != null) {
      processGetVersions(rb);
      return;
    }

    val = params.get("getUpdates");
    if (val != null) {
      // solrcloud_debug
      if (log.isDebugEnabled()) {
        try {
          RefCounted<SolrIndexSearcher> searchHolder = req.getCore()
              .getNewestSearcher(false);
          SolrIndexSearcher searcher = searchHolder.get();
          try {
            log.debug(req.getCore().getCoreDescriptor()
                .getCoreContainer().getZkController().getNodeName()
                + " min count to sync to (from most recent searcher view) "
                + searcher.search(new MatchAllDocsQuery(), 1).totalHits);
          } finally {
            searchHolder.decref();
          }
        } catch (Exception e) {
          log.debug("Error in solrcloud_debug block", e);
        }
      }
      
      processGetUpdates(rb);
      return;
    }

    final IdsRequsted reqIds = IdsRequsted.parseParams(req);
    
    if (reqIds.allIds.isEmpty()) {
      return;
    }

    // parse any existing filters
    try {
      String[] fqs = req.getParams().getParams(CommonParams.FQ);
      if (fqs!=null && fqs.length!=0) {
        List<Query> filters = rb.getFilters();
        // if filters already exists, make a copy instead of modifying the original
        filters = filters == null ? new ArrayList<Query>(fqs.length) : new ArrayList<>(filters);
        for (String fq : fqs) {
          if (fq != null && fq.trim().length()!=0) {
            QParser fqp = QParser.getParser(fq, req);
            filters.add(fqp.getQuery());
          }
        }
        if (!filters.isEmpty()) {
          rb.setFilters( filters );
        }
      }
    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

    SolrCore core = req.getCore();
    SchemaField idField = core.getLatestSchema().getUniqueKeyField();
    FieldType fieldType = idField.getType();

    SolrDocumentList docList = new SolrDocumentList();
    UpdateLog ulog = core.getUpdateHandler().getUpdateLog();

    RefCounted<SolrIndexSearcher> searcherHolder = null;
    
    // this is initialized & set on the context *after* any searcher (re-)opening
    ResultContext resultContext = null;
    final DocTransformer transformer = rsp.getReturnFields().getTransformer();

    // true in any situation where we have to use a realtime searcher rather then returning docs
    // directly from the UpdateLog
    final boolean mustUseRealtimeSearcher =
      // if we have filters, we need to check those against the indexed form of the doc
      (rb.getFilters() != null)
      || ((null != transformer) && transformer.needsSolrIndexSearcher());

   try {
     SolrIndexSearcher searcher = null;

     BytesRefBuilder idBytes = new BytesRefBuilder();
     for (String idStr : reqIds.allIds) {
       fieldType.readableToIndexed(idStr, idBytes);
       if (ulog != null) {
         Object o = ulog.lookup(idBytes.get());
         if (o != null) {
           // should currently be a List<Oper,Ver,Doc/Id>
           List entry = (List)o;
           assert entry.size() >= 3;
           int oper = (Integer)entry.get(0) & UpdateLog.OPERATION_MASK;
           switch (oper) {
             case UpdateLog.ADD:

               if (mustUseRealtimeSearcher) {
                 if (searcherHolder != null) {
                   // close handles to current searchers & result context
                   searcher = null;
                   searcherHolder.decref();
                   searcherHolder = null;
                   resultContext = null;
                 }
                 ulog.openRealtimeSearcher();  // force open a new realtime searcher
                 o = null;  // pretend we never found this record and fall through to use the searcher
                 break;
               }

               SolrDocument doc = toSolrDoc((SolrInputDocument)entry.get(entry.size()-1), core.getLatestSchema());
               if (transformer!=null) {
                 transformer.transform(doc, -1, 0); // unknown docID
               }
              docList.add(doc);
              break;
             case UpdateLog.DELETE:
              break;
             default:
               throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,  "Unknown Operation! " + oper);
           }
           if (o != null) continue;
         }
       }

       // didn't find it in the update log, so it should be in the newest searcher opened
       if (searcher == null) {
         searcherHolder = core.getRealtimeSearcher();
         searcher = searcherHolder.get();
         // don't bother with ResultContext yet, we won't need it if doc doesn't match filters
       }

       int docid = -1;
       long segAndId = searcher.lookupId(idBytes.get());
       if (segAndId >= 0) {
         int segid = (int) segAndId;
         LeafReaderContext ctx = searcher.getTopReaderContext().leaves().get((int) (segAndId >> 32));
         docid = segid + ctx.docBase;

         if (rb.getFilters() != null) {
           for (Query raw : rb.getFilters()) {
             Query q = raw.rewrite(searcher.getIndexReader());
             Scorer scorer = searcher.createWeight(q, false, 1f).scorer(ctx);
             if (scorer == null || segid != scorer.iterator().advance(segid)) {
               // filter doesn't match.
               docid = -1;
               break;
             }
           }
         }
       }

       if (docid < 0) continue;
       
       Document luceneDocument = searcher.doc(docid, rsp.getReturnFields().getLuceneFieldNames());
       SolrDocument doc = toSolrDoc(luceneDocument,  core.getLatestSchema());
       searcher.decorateDocValueFields(doc, docid, searcher.getNonStoredDVs(true));
       if ( null != transformer) {
         if (null == resultContext) {
           // either first pass, or we've re-opened searcher - either way now we setContext
           resultContext = new RTGResultContext(rsp.getReturnFields(), searcher, req);
           transformer.setContext(resultContext);
         }
         transformer.transform(doc, docid, 0);
       }
       docList.add(doc);
     }

   } finally {
     if (searcherHolder != null) {
       searcherHolder.decref();
     }
   }

   addDocListToResponse(rb, docList);
  }


  public static SolrInputDocument DELETED = new SolrInputDocument();

  /** returns the SolrInputDocument from the current tlog, or DELETED if it has been deleted, or
   * null if there is no record of it in the current update log.  If null is returned, it could
   * still be in the latest index.
   */
  public static SolrInputDocument getInputDocumentFromTlog(SolrCore core, BytesRef idBytes) {

    UpdateLog ulog = core.getUpdateHandler().getUpdateLog();

    if (ulog != null) {
      Object o = ulog.lookup(idBytes);
      if (o != null) {
        // should currently be a List<Oper,Ver,Doc/Id>
        List entry = (List)o;
        assert entry.size() >= 3;
        int oper = (Integer)entry.get(0) & UpdateLog.OPERATION_MASK;
        switch (oper) {
          case UpdateLog.ADD:
            return (SolrInputDocument)entry.get(entry.size()-1);
          case UpdateLog.DELETE:
            return DELETED;
          default:
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,  "Unknown Operation! " + oper);
        }
      }
    }

    return null;
  }

  public static SolrInputDocument getInputDocument(SolrCore core, BytesRef idBytes) throws IOException {
    SolrInputDocument sid = null;
    RefCounted<SolrIndexSearcher> searcherHolder = null;
    try {
      SolrIndexSearcher searcher = null;
      sid = getInputDocumentFromTlog(core, idBytes);
      if (sid == DELETED) {
        return null;
      }

      if (sid == null) {
        // didn't find it in the update log, so it should be in the newest searcher opened
        if (searcher == null) {
          searcherHolder = core.getRealtimeSearcher();
          searcher = searcherHolder.get();
        }

        // SolrCore.verbose("RealTimeGet using searcher ", searcher);
        SchemaField idField = core.getLatestSchema().getUniqueKeyField();

        int docid = searcher.getFirstMatch(new Term(idField.getName(), idBytes));
        if (docid < 0) return null;
        Document luceneDocument = searcher.doc(docid);
        sid = toSolrInputDocument(luceneDocument, core.getLatestSchema());
        searcher.decorateDocValueFields(sid, docid, searcher.getNonStoredDVsWithoutCopyTargets());
      }
    } finally {
      if (searcherHolder != null) {
        searcherHolder.decref();
      }
    }

    return sid;
  }

  private static SolrInputDocument toSolrInputDocument(Document doc, IndexSchema schema) {
    SolrInputDocument out = new SolrInputDocument();
    for( IndexableField f : doc.getFields() ) {
      String fname = f.name();
      SchemaField sf = schema.getFieldOrNull(f.name());
      Object val = null;
      if (sf != null) {
        if ((!sf.hasDocValues() && !sf.stored()) || schema.isCopyFieldTarget(sf)) continue;
        val = sf.getType().toObject(f);   // object or external string?
      } else {
        val = f.stringValue();
        if (val == null) val = f.numericValue();
        if (val == null) val = f.binaryValue();
        if (val == null) val = f;
      }

      // todo: how to handle targets of copy fields (including polyfield sub-fields)?
      out.addField(fname, val);
    }
    return out;
  }


  private static SolrDocument toSolrDoc(Document doc, IndexSchema schema) {
    SolrDocument out = new SolrDocument();
    for( IndexableField f : doc.getFields() ) {
      // Make sure multivalued fields are represented as lists
      Object existing = out.get(f.name());
      if (existing == null) {
        SchemaField sf = schema.getFieldOrNull(f.name());

        // don't return copyField targets
        if (sf != null && schema.isCopyFieldTarget(sf)) continue;

        if (sf != null && sf.multiValued()) {
          List<Object> vals = new ArrayList<>();
          vals.add( f );
          out.setField( f.name(), vals );
        }
        else{
          out.setField( f.name(), f );
        }
      }
      else {
        out.addField( f.name(), f );
      }
    }
    return out;
  }

  private static SolrDocument toSolrDoc(SolrInputDocument sdoc, IndexSchema schema) {
    // TODO: do something more performant than this double conversion
    Document doc = DocumentBuilder.toDocument(sdoc, schema);

    // copy the stored fields only
    Document out = new Document();
    for (IndexableField f : doc.getFields()) {
      if (f.fieldType().stored()) {
        out.add(f);
      } else if (f.fieldType().docValuesType() != DocValuesType.NONE) {
        SchemaField schemaField = schema.getFieldOrNull(f.name());
        if (schemaField != null && !schemaField.stored() && schemaField.useDocValuesAsStored()) {
          out.add(f);
        }
      } else {
        log.debug("Don't know how to handle field " + f);
      }
    }

    return toSolrDoc(out, schema);
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    if (rb.stage < ResponseBuilder.STAGE_GET_FIELDS)
      return ResponseBuilder.STAGE_GET_FIELDS;
    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      return createSubRequests(rb);
    }
    return ResponseBuilder.STAGE_DONE;
  }

  public int createSubRequests(ResponseBuilder rb) throws IOException {
    
    final IdsRequsted reqIds = IdsRequsted.parseParams(rb.req);
    if (reqIds.allIds.isEmpty()) {
      return ResponseBuilder.STAGE_DONE;
    }
    
    SolrParams params = rb.req.getParams();

    // TODO: handle collection=...?

    ZkController zkController = rb.req.getCore().getCoreDescriptor().getCoreContainer().getZkController();

    // if shards=... then use that
    if (zkController != null && params.get(ShardParams.SHARDS) == null) {
      CloudDescriptor cloudDescriptor = rb.req.getCore().getCoreDescriptor().getCloudDescriptor();

      String collection = cloudDescriptor.getCollectionName();
      ClusterState clusterState = zkController.getClusterState();
      DocCollection coll = clusterState.getCollection(collection);


      Map<String, List<String>> sliceToId = new HashMap<>();
      for (String id : reqIds.allIds) {
        Slice slice = coll.getRouter().getTargetSlice(id, null, null, params, coll);

        List<String> idsForShard = sliceToId.get(slice.getName());
        if (idsForShard == null) {
          idsForShard = new ArrayList<>(2);
          sliceToId.put(slice.getName(), idsForShard);
        }
        idsForShard.add(id);
      }

      for (Map.Entry<String,List<String>> entry : sliceToId.entrySet()) {
        String shard = entry.getKey();

        ShardRequest sreq = createShardRequest(rb, entry.getValue());
        // sreq.shards = new String[]{shard};    // TODO: would be nice if this would work...
        sreq.shards = sliceToShards(rb, collection, shard);
        sreq.actualShards = sreq.shards;
        
        rb.addRequest(this, sreq);
      }      
    } else {
      ShardRequest sreq = createShardRequest(rb, reqIds.allIds);
      sreq.shards = null;  // ALL
      sreq.actualShards = sreq.shards;

      rb.addRequest(this, sreq);
    }

    return ResponseBuilder.STAGE_DONE;
  }

  /**
   * Helper method for creating a new ShardRequest for the specified ids, based on the params 
   * specified for the current request.  The new ShardRequest does not yet know anything about 
   * which shard/slice it will be sent to.
   */
  private ShardRequest createShardRequest(final ResponseBuilder rb, final List<String> ids) {
    final ShardRequest sreq = new ShardRequest();
    sreq.purpose = 1;
    sreq.params = new ModifiableSolrParams(rb.req.getParams());

    // TODO: how to avoid hardcoding this and hit the same handler?
    sreq.params.set(ShardParams.SHARDS_QT,"/get");      
    sreq.params.set("distrib",false);

    sreq.params.remove(ShardParams.SHARDS);
    sreq.params.remove("id");
    sreq.params.remove("ids");
    sreq.params.set("ids", StrUtils.join(ids, ','));
    
    return sreq;
  }
  
  private String[] sliceToShards(ResponseBuilder rb, String collection, String slice) {
    String lookup = collection + '_' + slice;  // seems either form may be filled in rb.slices?
    
    // We use this since the shard handler already filled in the slice to shards mapping.
    // A better approach would be to avoid filling out every slice each time, or to cache
    // the mappings.

    for (int i=0; i<rb.slices.length; i++) {
      log.info("LOOKUP_SLICE:" + rb.slices[i] + "=" + rb.shards[i]);
      if (lookup.equals(rb.slices[i]) || slice.equals(rb.slices[i])) {
        return new String[]{rb.shards[i]};
      }
    }


    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Can't find shard '" + lookup + "'");
  }

  /***
  private void handleRegularResponses(ResponseBuilder rb, ShardRequest sreq) {
  }
  ***/

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.stage != ResponseBuilder.STAGE_GET_FIELDS) {
      return;
    }
    
    mergeResponses(rb);
  }
  
  private void mergeResponses(ResponseBuilder rb) {
    SolrDocumentList docList = new SolrDocumentList();
    
    for (ShardRequest sreq : rb.finished) {
      // if shards=shard1,shard2 was used, then  we query both shards for each id and
      // can get more than one response
      for (ShardResponse srsp : sreq.responses) {
        SolrResponse sr = srsp.getSolrResponse();
        NamedList nl = sr.getResponse();
        SolrDocumentList subList = (SolrDocumentList)nl.get("response");
        docList.addAll(subList);
      }
    }
    
    addDocListToResponse(rb, docList);
  }

  /**
   * Encapsulates logic for how a {@link SolrDocumentList} should be added to the response
   * based on the request params used
   */
  private void addDocListToResponse(final ResponseBuilder rb, final SolrDocumentList docList) {
    assert null != docList;
    
    final SolrQueryResponse rsp = rb.rsp;
    final IdsRequsted reqIds = IdsRequsted.parseParams(rb.req);
    
    if (reqIds.useSingleDocResponse) {
      assert docList.size() <= 1;
      // if the doc was not found, then use a value of null.
      rsp.add("doc", docList.size() > 0 ? docList.get(0) : null);
    } else {
      docList.setNumFound(docList.size());
      rsp.addResponse(docList);
    }
  }

                                                                                               

  ////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "query";
  }

  @Override
  public URL[] getDocs() {
    return null;
  }


  ///////////////////////////////////////////////////////////////////////////////////
  // Returns last versions added to index
  ///////////////////////////////////////////////////////////////////////////////////


  public void processGetVersions(ResponseBuilder rb) throws IOException
  {
    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;
    SolrParams params = req.getParams();

    if (!params.getBool(COMPONENT_NAME, true)) {
      return;
    }

    int nVersions = params.getInt("getVersions", -1);
    if (nVersions == -1) return;

    boolean doFingerprint = params.getBool("fingerprint", false);

    String sync = params.get("sync");
    if (sync != null) {
      processSync(rb, nVersions, sync);
      return;
    }

    UpdateLog ulog = req.getCore().getUpdateHandler().getUpdateLog();
    if (ulog == null) return;

    // get fingerprint first as it will cause a soft commit
    // and would avoid mismatch if documents are being actively index especially during PeerSync
    if (doFingerprint) {
      IndexFingerprint fingerprint = IndexFingerprint.getFingerprint(req.getCore(), Long.MAX_VALUE);
      rb.rsp.add("fingerprint", fingerprint.toObject());
    }

    try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
      List<Long> versions = recentUpdates.getVersions(nVersions);
      rb.rsp.add("versions", versions);
    }
  }

  
  public void processSync(ResponseBuilder rb, int nVersions, String sync) {
    
    boolean onlyIfActive = rb.req.getParams().getBool("onlyIfActive", false);
    
    if (onlyIfActive) {
      if (rb.req.getCore().getCoreDescriptor().getCloudDescriptor().getLastPublished() != Replica.State.ACTIVE) {
        log.info("Last published state was not ACTIVE, cannot sync.");
        rb.rsp.add("sync", "false");
        return;
      }
    }
    
    List<String> replicas = StrUtils.splitSmart(sync, ",", true);
    
    boolean cantReachIsSuccess = rb.req.getParams().getBool("cantReachIsSuccess", false);
    
    PeerSync peerSync = new PeerSync(rb.req.getCore(), replicas, nVersions, cantReachIsSuccess, true);
    boolean success = peerSync.sync();
    
    // TODO: more complex response?
    rb.rsp.add("sync", success);
  }
  

  public void processGetUpdates(ResponseBuilder rb) throws IOException
  {
    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;
    SolrParams params = req.getParams();

    if (!params.getBool(COMPONENT_NAME, true)) {
      return;
    }

    String versionsStr = params.get("getUpdates");
    if (versionsStr == null) return;

    UpdateLog ulog = req.getCore().getUpdateHandler().getUpdateLog();
    if (ulog == null) return;

    // handle version ranges
    List<Long> versions = null;
    if (versionsStr.indexOf("...") != -1) {
      versions = resolveVersionRanges(versionsStr, ulog);
    } else {
      versions = StrUtils.splitSmart(versionsStr, ",", true).stream().map(Long::parseLong)
          .collect(Collectors.toList());
    }

    // find fingerprint for max version for which updates are requested
    boolean doFingerprint = params.getBool("fingerprint", false);
    if (doFingerprint) {
      long maxVersionForUpdate = Collections.min(versions, PeerSync.absComparator);
      IndexFingerprint fingerprint = IndexFingerprint.getFingerprint(req.getCore(), Math.abs(maxVersionForUpdate));
      rb.rsp.add("fingerprint", fingerprint.toObject());
    }

    List<Object> updates = new ArrayList<>(versions.size());

    long minVersion = Long.MAX_VALUE;

    // TODO: get this from cache instead of rebuilding?
    try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
      for (Long version : versions) {
        try {
          Object o = recentUpdates.lookup(version);
          if (o == null) continue;

          if (version > 0) {
            minVersion = Math.min(minVersion, version);
          }

          // TODO: do any kind of validation here?
          updates.add(o);

        } catch (SolrException | ClassCastException e) {
          log.warn("Exception reading log for updates", e);
        }
      }

      // Must return all delete-by-query commands that occur after the first add requested
      // since they may apply.
      updates.addAll(recentUpdates.getDeleteByQuery(minVersion));

      rb.rsp.add("updates", updates);

    }
  }
  
  
  private List<Long> resolveVersionRanges(String versionsStr, UpdateLog ulog) {
    if (StringUtils.isEmpty(versionsStr)) {
      return Collections.emptyList();
    }
    
    List<String> ranges = StrUtils.splitSmart(versionsStr, ",", true);
    
    // TODO merge ranges.
    
    // get all the versions from updatelog and sort them
    List<Long> versionAvailable = null;
    try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
      versionAvailable = recentUpdates.getVersions(ulog.getNumRecordsToKeep());
    }
    // sort versions
    Collections.sort(versionAvailable, PeerSync.absComparator);
    
    // This can be done with single pass over both ranges and versionsAvailable, that would require 
    // merging ranges. We currently use Set to ensure there are no duplicates.
    Set<Long> versionsToRet = new HashSet<>(ulog.getNumRecordsToKeep());
    for (String range : ranges) {
      String[] rangeBounds = range.split("\\.{3}");
      int indexStart = Collections.binarySearch(versionAvailable, Long.valueOf(rangeBounds[1]), PeerSync.absComparator);
      int indexEnd = Collections.binarySearch(versionAvailable, Long.valueOf(rangeBounds[0]), PeerSync.absComparator); 
      if(indexStart >=0 && indexEnd >= 0) {
        versionsToRet.addAll(versionAvailable.subList(indexStart, indexEnd + 1)); // indexEnd is exclusive
      }
    }
    // TODO do we need to sort versions using PeerSync.absComparator?
    return new ArrayList<>(versionsToRet);
  }

  /** 
   * Simple struct for tracking what ids were requested and what response format is expected 
   * acording to the request params
   */
  private final static class IdsRequsted {
    /** An List (which may be empty but will never be null) of the uniqueKeys requested. */
    public final List<String> allIds;
    /** 
     * true if the params provided by the user indicate that a single doc response structure 
     * should be used.  
     * Value is meaninless if <code>ids</code> is empty.
     */
    public final boolean useSingleDocResponse;
    private IdsRequsted(List<String> allIds, boolean useSingleDocResponse) {
      assert null != allIds;
      this.allIds = allIds;
      this.useSingleDocResponse = useSingleDocResponse;
    }
    
    /**
     * Parsers the <code>id</code> and <code>ids</code> params attached to the specified request object, 
     * and returns an <code>IdsRequsted</code> struct to use for this request.
     * The <code>IdsRequsted</code> is cached in the {@link SolrQueryRequest#getContext} so subsequent 
     * method calls on the same request will not re-parse the params.
     */
    public static IdsRequsted parseParams(SolrQueryRequest req) {
      final String contextKey = IdsRequsted.class.toString() + "_PARSED_ID_PARAMS";
      if (req.getContext().containsKey(contextKey)) {
        return (IdsRequsted)req.getContext().get(contextKey);
      }
      final SolrParams params = req.getParams();
      final String id[] = params.getParams("id");
      final String ids[] = params.getParams("ids");
      
      if (id == null && ids == null) {
        IdsRequsted result = new IdsRequsted(Collections.<String>emptyList(), true);
        req.getContext().put(contextKey, result);
        return result;
      }
      final List<String> allIds = new ArrayList<>((null == id ? 0 : id.length)
                                                  + (null == ids ? 0 : (2 * ids.length)));
      if (null != id) {
        for (String singleId : id) {
          allIds.add(singleId);
        }
      }
      if (null != ids) {
        for (String idList : ids) {
          allIds.addAll( StrUtils.splitSmart(idList, ",", true) );
        }
      }
      // if the client specified a single id=foo, then use "doc":{
      // otherwise use a standard doclist
      IdsRequsted result = new IdsRequsted(allIds, (ids == null && allIds.size() <= 1));
      req.getContext().put(contextKey, result);
      return result;
    }
  }

  
  /**
   * A lite weight ResultContext for use with RTG requests that can point at Realtime Searchers
   */
  private static final class RTGResultContext extends ResultContext {
    final ReturnFields returnFields;
    final SolrIndexSearcher searcher;
    final SolrQueryRequest req;
    public RTGResultContext(ReturnFields returnFields, SolrIndexSearcher searcher, SolrQueryRequest req) {
      this.returnFields = returnFields;
      this.searcher = searcher;
      this.req = req;
    }
    
    /** @returns null */
    public DocList getDocList() {
      return null;
    }
    
    public ReturnFields getReturnFields() {
      return this.returnFields;
    }
    
    public SolrIndexSearcher getSearcher() {
      return this.searcher;
    }
    
    /** @returns null */
    public Query getQuery() {
      return null;
    }
    
    public SolrQueryRequest getRequest() {
      return this.req;
    }
    
    /** @returns null */
    public Iterator<SolrDocument> getProcessedDocuments() {
      return null;
    }
  }
  
}
