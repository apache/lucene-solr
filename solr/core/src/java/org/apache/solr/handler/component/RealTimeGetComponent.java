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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.search.QueryUtils;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
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
import org.apache.solr.request.LocalSolrQueryRequest;
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
import org.apache.solr.search.SolrDocumentFetcher;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.update.CdcrUpdateLog;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.update.IndexFingerprint;
import org.apache.solr.update.PeerSync;
import org.apache.solr.update.PeerSyncWithLeader;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.processor.AtomicUpdateDocumentMerger;
import org.apache.solr.util.LongSet;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.TestInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.ID;
import static org.apache.solr.common.params.CommonParams.VERSION_FIELD;

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
    CloudDescriptor cloudDesc = req.getCore().getCoreDescriptor().getCloudDescriptor();

    if (cloudDesc != null) {
      Replica.Type replicaType = cloudDesc.getReplicaType();
      if (replicaType != null) {
        if (replicaType == Replica.Type.PULL) {
          throw new SolrException(ErrorCode.BAD_REQUEST, 
              String.format(Locale.ROOT, "%s can't handle realtime get requests. Replicas of type %s do not support these type of requests", 
                  cloudDesc.getCoreNodeName(),
                  Replica.Type.PULL));
        } 
        // non-leader TLOG replicas should not respond to distrib /get requests, but internal requests are OK
      }
    }
    
    if (!params.getBool(COMPONENT_NAME, true)) {
      return;
    }

    //TODO remove this at Solr 10
    //After SOLR-14641 other nodes won't call RTG with this param.
    //Just keeping here for backward-compatibility, if we remove this, nodes with older versions will
    //assume that this node can't handle version ranges.
    String val = params.get("checkCanHandleVersionRanges");
    if(val != null) {
      rb.rsp.add("canHandleVersionRanges", true);
      return;
    }

    val = params.get("getFingerprint");
    if(val != null) {
      processGetFingeprint(rb);
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
            if (log.isDebugEnabled()) {
              log.debug("{} min count to sync to (from most recent searcher view) {}"
                  , req.getCore().getCoreContainer().getZkController().getNodeName()
                  , searcher.count(new MatchAllDocsQuery()));
            }
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
    
    val = params.get("getInputDocument");
    if (val != null) {
      processGetInputDocument(rb);
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
            filters.add(QueryUtils.makeQueryable(fqp.getQuery()));
          }
        }
        if (!filters.isEmpty()) {
          rb.setFilters( filters );
        }
      }
    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

    final SolrCore core = req.getCore();
    SchemaField idField = core.getLatestSchema().getUniqueKeyField();
    FieldType fieldType = idField.getType();

    SolrDocumentList docList = new SolrDocumentList();
    UpdateLog ulog = core.getUpdateHandler().getUpdateLog();

    SearcherInfo searcherInfo =  new SearcherInfo(core);
    
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

     boolean opennedRealtimeSearcher = false;
     BytesRefBuilder idBytes = new BytesRefBuilder();
     for (String idStr : reqIds.allIds) {
       fieldType.readableToIndexed(idStr, idBytes);
       // if _route_ is passed, id is a child doc.  TODO remove in SOLR-15064
       if (!opennedRealtimeSearcher && !params.get(ShardParams._ROUTE_, idStr).equals(idStr)) {
         searcherInfo.clear();
         resultContext = null;
         ulog.openRealtimeSearcher();  // force open a new realtime searcher
         opennedRealtimeSearcher = true;
       } else if (ulog != null) {
         Object o = ulog.lookup(idBytes.get());
         if (o != null) {
           // should currently be a List<Oper,Ver,Doc/Id>
           @SuppressWarnings({"rawtypes"})
           List entry = (List)o;
           assert entry.size() >= 3;
           int oper = (Integer)entry.get(UpdateLog.FLAGS_IDX) & UpdateLog.OPERATION_MASK;
           switch (oper) {
             case UpdateLog.UPDATE_INPLACE: // fall through to ADD
             case UpdateLog.ADD:

               if (mustUseRealtimeSearcher) {
                 // close handles to current searchers & result context
                 if (!opennedRealtimeSearcher) {
                   searcherInfo.clear();
                   resultContext = null;
                   ulog.openRealtimeSearcher();  // force open a new realtime searcher
                   opennedRealtimeSearcher = true;
                 }
                 o = null;  // pretend we never found this record and fall through to use the searcher
                 break;
               }

               SolrDocument doc;
               if (oper == UpdateLog.ADD) {
                 doc = toSolrDoc((SolrInputDocument)entry.get(entry.size()-1), core.getLatestSchema());
                 // toSolrDoc filtered copy-field targets already
                 if (transformer!=null) {
                   transformer.transform(doc, -1); // unknown docID
                 }
               } else if (oper == UpdateLog.UPDATE_INPLACE) {
                 if (ulog instanceof CdcrUpdateLog) {
                   assert entry.size() == 6;
                 } else {
                   assert entry.size() == 5;
                 }
                 // For in-place update case, we have obtained the partial document till now. We need to
                 // resolve it to a full document to be returned to the user.
                 // resolveFullDocument applies the transformer, if present.
                 doc = resolveFullDocument(core, idBytes.get(), rsp.getReturnFields(), (SolrInputDocument)entry.get(entry.size()-1), entry);
                 if (doc == null) {
                   break; // document has been deleted as the resolve was going on
                 }
                 doc.visitSelfAndNestedDocs((label, d) -> removeCopyFieldTargets(d, req.getSchema()));
               } else {
                 throw new SolrException(ErrorCode.INVALID_STATE, "Expected ADD or UPDATE_INPLACE. Got: " + oper);
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
       searcherInfo.init();
       // don't bother with ResultContext yet, we won't need it if doc doesn't match filters

       int docid = -1;
       long segAndId = searcherInfo.getSearcher().lookupId(idBytes.get());
       if (segAndId >= 0) {
         int segid = (int) segAndId;
         LeafReaderContext ctx = searcherInfo.getSearcher().getTopReaderContext().leaves().get((int) (segAndId >> 32));
         docid = segid + ctx.docBase;

         if (rb.getFilters() != null) {
           for (Query raw : rb.getFilters()) {
             Query q = raw.rewrite(searcherInfo.getSearcher().getIndexReader());
             Scorer scorer = searcherInfo.getSearcher().createWeight(q, ScoreMode.COMPLETE_NO_SCORES, 1f).scorer(ctx);
             if (scorer == null || segid != scorer.iterator().advance(segid)) {
               // filter doesn't match.
               docid = -1;
               break;
             }
           }
         }
       }

       if (docid < 0) continue;
       
       Document luceneDocument = searcherInfo.getSearcher().doc(docid, rsp.getReturnFields().getLuceneFieldNames());
       SolrDocument doc = toSolrDoc(luceneDocument,  core.getLatestSchema());
       SolrDocumentFetcher docFetcher = searcherInfo.getSearcher().getDocFetcher();
       docFetcher.decorateDocValueFields(doc, docid, docFetcher.getNonStoredDVs(true));
       if ( null != transformer) {
         if (null == resultContext) {
           // either first pass, or we've re-opened searcher - either way now we setContext
           resultContext = new RTGResultContext(rsp.getReturnFields(), searcherInfo.getSearcher(), req);
           transformer.setContext(resultContext); // we avoid calling setContext unless searcher is new/changed
         }
         transformer.transform(doc, docid);
       }
       docList.add(doc);
     } // loop on ids

   } finally {
     searcherInfo.clear();
   }

   addDocListToResponse(rb, docList);
  }
  
  /**
   * Return the requested SolrInputDocument from the tlog/index. This will
   * always be a full document with children; partial / in-place documents will be resolved.
   * The id must be for a root document, not a child.
   */
  void processGetInputDocument(ResponseBuilder rb) throws IOException {
    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;
    SolrParams params = req.getParams();

    if (!params.getBool(COMPONENT_NAME, true)) {
      return;
    }

    String idStr = params.get("getInputDocument", null);
    if (idStr == null) return;
    BytesRef idBytes = req.getSchema().indexableUniqueKey(idStr);
    AtomicLong version = new AtomicLong();
    SolrInputDocument doc = getInputDocument(req.getCore(), idBytes, idBytes, version, null, Resolution.ROOT_WITH_CHILDREN);
    log.info("getInputDocument called for id={}, returning {}", idStr, doc);
    rb.rsp.add("inputDocument", doc);
    rb.rsp.add("version", version.get());
  }

  /**
   * A SearcherInfo provides mechanism for obtaining RT searcher, from
   * a SolrCore, and closing it, while taking care of the RefCounted references.
   */
  private static class SearcherInfo {
    private RefCounted<SolrIndexSearcher> searcherHolder = null;
    private SolrIndexSearcher searcher = null;
    final SolrCore core;
    
    public SearcherInfo(SolrCore core) {
      this.core = core;
    }
    
    void clear(){
      if (searcherHolder != null) {
        // close handles to current searchers
        searcher = null;
        searcherHolder.decref();
        searcherHolder = null;
      }
    }

    void init(){
      if (searcher == null) {
        searcherHolder = core.getRealtimeSearcher();
        searcher = searcherHolder.get();
      }
    }
    
    public SolrIndexSearcher getSearcher() {
      assert null != searcher : "init not called!";
      return searcher;
    }
  }

  /**
   * Given a partial document obtained from the transaction log (e.g. as a result of RTG), resolve to a full document
   * by populating all the partial updates that were applied on top of that last full document update.
   * Transformers are applied.
   * <p>TODO <em>Sometimes</em> there's copy-field target removal; it ought to be consistent.
   *
   * @param idBytes doc ID to find; never a child doc.
   * @param partialDoc partial doc (an in-place update).  Could be a child doc, thus not having idBytes.
   * @return Returns the merged document, i.e. the resolved full document, or null if the document was not found (deleted
   *          after the resolving began).  Never a child doc, since idBytes is never a child doc either.
   */
  private static SolrDocument resolveFullDocument(SolrCore core, BytesRef idBytes,
                                                  ReturnFields returnFields, SolrInputDocument partialDoc,
                                                  @SuppressWarnings({"rawtypes"}) List logEntry) throws IOException {
    Set<String> onlyTheseFields = returnFields.getExplicitlyRequestedFieldNames();
    if (idBytes == null || (logEntry.size() != 5 && logEntry.size() != 6)) {
      throw new SolrException(ErrorCode.INVALID_STATE, "Either Id field not present in partial document or log entry doesn't have previous version.");
    }
    long prevPointer = (long) logEntry.get(UpdateLog.PREV_POINTER_IDX);
    long prevVersion = (long) logEntry.get(UpdateLog.PREV_VERSION_IDX);
    final IndexSchema schema = core.getLatestSchema();

    // get the last full document from ulog
    long lastPrevPointer;
    // If partialDoc is NOT a child doc, then proceed and look into the ulog...
    if (schema.printableUniqueKey(idBytes).equals(schema.printableUniqueKey(partialDoc))) {
      UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
      lastPrevPointer = ulog.applyPartialUpdates(idBytes, prevPointer, prevVersion, onlyTheseFields, partialDoc);
    } else { // child doc.
      // TODO could make this smarter but it's complicated with nested docs
      lastPrevPointer = Long.MAX_VALUE; // results in reopenRealtimeSearcherAndGet
    }

    if (lastPrevPointer == -1) { // full document was not found in tlog, but exists in index
      return mergePartialDocWithFullDocFromIndex(core, idBytes, returnFields, partialDoc);
    } else if (lastPrevPointer > 0) {
      // We were supposed to have found the last full doc also in the tlogs, but the prevPointer links led to nowhere
      // We should reopen a new RT searcher and get the doc. This should be a rare occurrence
      Term idTerm = new Term(schema.getUniqueKeyField().getName(), idBytes);
      SolrDocument mergedDoc = reopenRealtimeSearcherAndGet(core, idTerm, returnFields);
      if (mergedDoc == null) {
        return null; // the document may have been deleted as the resolving was going on.
      }
      return mergedDoc;
    } else { // i.e. lastPrevPointer==0
      assert lastPrevPointer == 0;
      // We have successfully resolved the document based off the tlogs

      // determine whether we can use the in place document, if the caller specified onlyTheseFields
      // and those fields are all supported for in-place updates
      boolean forInPlaceUpdate = onlyTheseFields != null
          && onlyTheseFields.stream().map(schema::getField)
          .allMatch(f -> null!=f && AtomicUpdateDocumentMerger.isSupportedFieldForInPlaceUpdate(f));

      SolrDocument solrDoc = toSolrDoc(partialDoc, schema, forInPlaceUpdate); // filters copy-field targets TODO don't
      DocTransformer transformer = returnFields.getTransformer();
      if (transformer != null && !transformer.needsSolrIndexSearcher()) {
        transformer.transform(solrDoc, -1); // no docId when from the ulog
      } // if needs searcher, it must be [child]; tlog docs already have children
      return solrDoc;
    }
  }

  /**
   * Re-open the RT searcher and get the document, referred to by the idTerm, from that searcher. 
   * @return Returns the document or null if not found.
   */
  private static SolrDocument reopenRealtimeSearcherAndGet(SolrCore core, Term idTerm, ReturnFields returnFields) throws IOException {
    UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
    ulog.openRealtimeSearcher();
    RefCounted<SolrIndexSearcher> searcherHolder = core.getRealtimeSearcher();
    try {
      SolrIndexSearcher searcher = searcherHolder.get();

      int docid = searcher.getFirstMatch(idTerm);
      if (docid < 0) {
        return null;
      }
      return fetchSolrDoc(searcher, docid, returnFields);
    } finally {
      searcherHolder.decref();
    }
  }

  /**
   * Gets a document from the index by id. If a non-null partial document (for in-place update) is passed in,
   * this method obtains the document from the tlog/index by the given id, merges the partial document on top of it and then returns
   * the resultant document.
   *
   * @param core           A SolrCore instance, useful for obtaining a realtimesearcher and the schema
   * @param idBytes        Binary representation of the value of the unique key field
   * @param returnFields   Return fields, as requested
   * @param partialDoc     A partial document (containing an in-place update) used for merging against a full document
   *                       from index; this maybe be null.
   * @return If partial document is null, this returns document from the index or null if not found.
   *         If partial document is not null, this returns a document from index merged with the partial document, or null if
   *         document doesn't exist in the index.
   */
  private static SolrDocument mergePartialDocWithFullDocFromIndex(SolrCore core, BytesRef idBytes, ReturnFields returnFields,
                                                                  SolrInputDocument partialDoc) throws IOException {
    RefCounted<SolrIndexSearcher> searcherHolder = core.getRealtimeSearcher(); //Searcher();
    try {
      // now fetch last document from index, and merge partialDoc on top of it
      SolrIndexSearcher searcher = searcherHolder.get();
      SchemaField idField = core.getLatestSchema().getUniqueKeyField();
      Term idTerm = new Term(idField.getName(), idBytes);

      int docid = searcher.getFirstMatch(idTerm);
      if (docid < 0) {
        // The document was not found in index! Reopen a new RT searcher (to be sure) and get again.
        // This should be because the document was deleted recently.
        SolrDocument doc = reopenRealtimeSearcherAndGet(core, idTerm, returnFields);
        if (doc == null) {
          // Unable to resolve the last full doc in tlog fully,
          // and document not found in index even after opening new rt searcher.
          // This must be a case of deleted doc
          return null;
        }
        return doc;
      }

      SolrDocument doc = fetchSolrDoc(searcher, docid, returnFields);
      if (!doc.containsKey(VERSION_FIELD)) {
        searcher.getDocFetcher().decorateDocValueFields(doc, docid, Collections.singleton(VERSION_FIELD));
      }

      long docVersion = (long) doc.getFirstValue(VERSION_FIELD);
      Object partialVersionObj = partialDoc.getFieldValue(VERSION_FIELD);
      long partialDocVersion = partialVersionObj instanceof Field? ((Field) partialVersionObj).numericValue().longValue():
        partialVersionObj instanceof Number? ((Number) partialVersionObj).longValue(): Long.parseLong(partialVersionObj.toString());
      if (docVersion > partialDocVersion) {
        return doc;
      }
      for (String fieldName: partialDoc.getFieldNames()) {
        doc.setField(fieldName.toString(), partialDoc.getFieldValue(fieldName));  // since partial doc will only contain single valued fields, this is fine
      }

      return doc;
    } finally {
      if (searcherHolder != null) {
        searcherHolder.decref();
      }
    }
  }

  /**
   * Fetch the doc by the ID, returning the requested fields.
   */
  private static SolrDocument fetchSolrDoc(SolrIndexSearcher searcher, int docId, ReturnFields returnFields) throws IOException {
    final SolrDocumentFetcher docFetcher = searcher.getDocFetcher();
    final SolrDocument solrDoc = docFetcher.solrDoc(docId, (SolrReturnFields) returnFields);
    final DocTransformer transformer = returnFields.getTransformer();
    if (transformer != null) {
      transformer.setContext(new RTGResultContext(returnFields, searcher, null)); // we get away with null req
      transformer.transform(solrDoc, docId);
    }
    return solrDoc;
  }

  private static void removeCopyFieldTargets(SolrDocument solrDoc, IndexSchema schema) {
    // TODO ideally we wouldn't have fetched these in the first place!
    final Iterator<Map.Entry<String, Object>> iterator = solrDoc.iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Object> fieldVal =  iterator.next();
      String fieldName = fieldVal.getKey();
      SchemaField sf = schema.getFieldOrNull(fieldName);
      if (sf != null && schema.isCopyFieldTarget(sf)) {
        iterator.remove();
      }
    }
  }

  public static SolrInputDocument DELETED = new SolrInputDocument();

  @Deprecated // need Resolution
  public static SolrInputDocument getInputDocumentFromTlog(SolrCore core, BytesRef idBytes, AtomicLong versionReturned,
                                                           Set<String> onlyTheseNonStoredDVs, boolean resolveFullDocument) {
    return getInputDocumentFromTlog(core, idBytes, versionReturned, onlyTheseNonStoredDVs,
        resolveFullDocument ? Resolution.DOC : Resolution.PARTIAL);
  }

  /**
   * Specialized to pick out a child doc from a nested doc from the TLog.
   * @see #getInputDocumentFromTlog(SolrCore, BytesRef, AtomicLong, Set, Resolution)
   */
  private static SolrInputDocument getInputDocumentFromTlog(
      SolrCore core,
      BytesRef idBytes,
      BytesRef rootIdBytes,
      AtomicLong versionReturned,
      Set<String> onlyTheseFields,
      Resolution resolution) {
    if (idBytes.equals(rootIdBytes)) { // simple case; not looking for a child
      return getInputDocumentFromTlog(
          core, rootIdBytes, versionReturned, onlyTheseFields, resolution);
    }

    // Ensure we request the ID to pick out the child doc in the nest
    final String uniqueKeyField = core.getLatestSchema().getUniqueKeyField().getName();
    if (onlyTheseFields != null && !onlyTheseFields.contains(uniqueKeyField)) {
      onlyTheseFields = new HashSet<>(onlyTheseFields); // clone
      onlyTheseFields.add(uniqueKeyField);
    }

    SolrInputDocument iDoc =
        getInputDocumentFromTlog(
            core, rootIdBytes, versionReturned, onlyTheseFields, Resolution.ROOT_WITH_CHILDREN);
    if (iDoc == DELETED || iDoc == null) {
      return iDoc;
    }

    iDoc = findNestedDocById(iDoc, idBytes, core.getLatestSchema());
    if (iDoc == null) {
      return DELETED; // new nest overwrote the old nest without the ID we are looking for?
    }
    return iDoc;
  }

  /** returns the SolrInputDocument from the current tlog, or DELETED if it has been deleted, or
   * null if there is no record of it in the current update log.  If null is returned, it could
   * still be in the latest index.  Copy-field target fields are excluded.
   * @param idBytes doc ID to find; never a child doc.
   * @param versionReturned If a non-null AtomicLong is passed in, it is set to the version of the update returned from the TLog.
   */
  @SuppressWarnings({"fallthrough"})
  public static SolrInputDocument getInputDocumentFromTlog(SolrCore core, BytesRef idBytes, AtomicLong versionReturned,
      Set<String> onlyTheseFields, Resolution resolution) {
    UpdateLog ulog = core.getUpdateHandler().getUpdateLog();

    if (ulog != null) {
      Object o = ulog.lookup(idBytes);
      if (o != null) {
        // should currently be a List<Oper,Ver,Doc/Id>
        @SuppressWarnings({"rawtypes"})
        List entry = (List)o;
        assert entry.size() >= 3;
        int oper = (Integer)entry.get(0) & UpdateLog.OPERATION_MASK;
        if (versionReturned != null) {
          versionReturned.set((long)entry.get(UpdateLog.VERSION_IDX));
        }
        switch (oper) {
          case UpdateLog.UPDATE_INPLACE:
            if (ulog instanceof CdcrUpdateLog) {
              assert entry.size() == 6;
            } else {
              assert entry.size() == 5;
            }

            if (resolution != Resolution.PARTIAL) {
              SolrInputDocument doc = (SolrInputDocument)entry.get(entry.size()-1);
              try {
                // For in-place update case, we have obtained the partial document till now. We need to
                // resolve it to a full document to be returned to the user.
                SolrReturnFields returnFields = makeReturnFields(core, onlyTheseFields, resolution);
                SolrDocument sdoc = resolveFullDocument(core, idBytes, returnFields, doc, entry);
                if (sdoc == null) {
                  return DELETED;
                }
                return toSolrInputDocument(sdoc, core.getLatestSchema()); // filters copy-field
              } catch (IOException ex) {
                throw new SolrException(ErrorCode.SERVER_ERROR, "Error while resolving full document. ", ex);
              }
            } else {
              // fall through to ADD, so as to get only the partial document
            }
          case UpdateLog.ADD:
            return (SolrInputDocument) entry.get(entry.size()-1);
          case UpdateLog.DELETE:
            return DELETED;
          default:
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,  "Unknown Operation! " + oper);
        }
      }
    }

    return null;
  }

  @Deprecated // easy to use wrong
  public static SolrInputDocument getInputDocument(SolrCore core, BytesRef idBytes, Resolution lookupStrategy) throws IOException {
    return getInputDocument (core, idBytes, idBytes, null, null, lookupStrategy);
  }

  /**
   * Obtains the latest document for a given id from the tlog or through the realtime searcher (if not found in the tlog).
   * Fields that are targets of copy-fields are excluded.
   *
   * @param idBytes ID of the document to be fetched.
   * @param rootIdBytes the root ID of the document being looked up.
   *                    If there are no child docs, this is always the same as idBytes.
   * @param versionReturned If a non-null AtomicLong is passed in, it is set to the version of the update returned from the TLog.
   * @param onlyTheseFields If not-null, this limits the fields that are returned.  However it is only an optimization
   *                        hint since other fields may be returned.  Copy field targets are never returned.
   * @param resolveStrategy {@link Resolution#DOC} or {@link Resolution#ROOT_WITH_CHILDREN}.
   * @see Resolution
   */
  public static SolrInputDocument getInputDocument(SolrCore core, BytesRef idBytes, BytesRef rootIdBytes, AtomicLong versionReturned,
                                                   Set<String> onlyTheseFields, Resolution resolveStrategy) throws IOException {
    assert resolveStrategy != Resolution.PARTIAL;
    assert resolveStrategy == Resolution.DOC || idBytes.equals(rootIdBytes); // not needed (yet)

    SolrInputDocument sid =
        getInputDocumentFromTlog(
            core, idBytes, rootIdBytes, versionReturned, onlyTheseFields, resolveStrategy);
    if (sid == DELETED) {
      return null;
    }

    if (sid == null) {
      // didn't find it in the update log, so it should be in the newest searcher opened
      RefCounted<SolrIndexSearcher> searcherHolder = core.getRealtimeSearcher();
      try {
        SolrIndexSearcher searcher = searcherHolder.get();

        int docId =
            searcher.getFirstMatch(
                new Term(
                    core.getLatestSchema().getUniqueKeyField().getName(),
                    resolveStrategy == Resolution.ROOT_WITH_CHILDREN ? rootIdBytes : idBytes));
        if (docId < 0) return null;

        if (resolveStrategy == Resolution.ROOT_WITH_CHILDREN
            && core.getLatestSchema().isUsableForChildDocs()) {
          // check that this doc is in fact a root document as a prevention measure
          if (!hasRootTerm(searcher, rootIdBytes)) {
            throw new SolrException(
                ErrorCode.BAD_REQUEST,
                "Attempted an atomic/partial update to a child doc without indicating the _root_ somehow.");
          }
        }

        SolrDocument solrDoc =
            fetchSolrDoc(searcher, docId, makeReturnFields(core, onlyTheseFields, resolveStrategy));
        sid = toSolrInputDocument(solrDoc, core.getLatestSchema()); // filters copy-field targets
        // the assertions above furthermore guarantee the result corresponds to idBytes
      } finally {
        searcherHolder.decref();
      }
    }

    if (versionReturned != null) {
      if (sid.containsKey(VERSION_FIELD)) {
        versionReturned.set((long)sid.getFieldValue(VERSION_FIELD));
      }
    }
    return sid;
  }

  private static boolean hasRootTerm(SolrIndexSearcher searcher, BytesRef rootIdBytes) throws IOException {
    final String fieldName = IndexSchema.ROOT_FIELD_NAME;
    final List<LeafReaderContext> leafContexts = searcher.getTopReaderContext().leaves();
    for (final LeafReaderContext leaf : leafContexts) {
      final LeafReader reader = leaf.reader();

      final Terms terms = reader.terms(fieldName);
      if (terms == null) continue;

      TermsEnum te = terms.iterator();
      if (te.seekExact(rootIdBytes)) {
        return true;
      }
    }
    return false;
  }

  /** Traverse the doc looking for a doc with the specified ID. */
  private static SolrInputDocument findNestedDocById(SolrInputDocument iDoc, BytesRef idBytes, IndexSchema schema) {
    assert schema.printableUniqueKey(iDoc) != null : "need IDs";
    // traverse nested doc, looking for the node with the ID we are looking for
    SolrInputDocument[] found = new SolrInputDocument[1];
    String idStr = schema.printableUniqueKey(idBytes);
    BiConsumer<String, SolrInputDocument> finder = (label, childDoc) -> {
      if (found[0] == null && idStr.equals(schema.printableUniqueKey(childDoc))) {
        found[0] = childDoc;
      }
    };
    iDoc.visitSelfAndNestedDocs(finder);
    return found[0];
  }

  private static SolrReturnFields makeReturnFields(SolrCore core, Set<String> requestedFields, Resolution resolution) {
    DocTransformer docTransformer;
    if (resolution == Resolution.ROOT_WITH_CHILDREN && core.getLatestSchema().isUsableForChildDocs()) {
      SolrParams params = new ModifiableSolrParams().set("limit", "-1");
      try (LocalSolrQueryRequest req = new LocalSolrQueryRequest(core, params)) {
        docTransformer = core.getTransformerFactory("child").create(null, params, req);
      }
    } else {
      docTransformer = null;
    }
    // TODO optimization: add feature to SolrReturnFields to exclude copyFieldTargets from wildcard matching.
    //   Today, we filter this data out later before returning, but it's already been fetched.
    return new SolrReturnFields(requestedFields, docTransformer);
  }

  private static SolrInputDocument toSolrInputDocument(SolrDocument doc, IndexSchema schema) {
    SolrInputDocument out = new SolrInputDocument();
    for( String fname : doc.getFieldNames() ) {
      boolean fieldArrayListCreated = false;
      SchemaField sf = schema.getFieldOrNull(fname);
      if (sf != null) {
        if ((!sf.hasDocValues() && !sf.stored()) || schema.isCopyFieldTarget(sf)) continue;
      }
      for (Object val: doc.getFieldValues(fname)) {
        if (val instanceof IndexableField) {
          IndexableField f = (IndexableField) val;
          // materialize:
          if (sf != null) {
            val = sf.getType().toObject(f);   // object or external string?
          } else {
            val = f.stringValue();
            if (val == null) val = f.numericValue();
            if (val == null) val = f.binaryValue();
            if (val == null) val = f;
          }
        } else if(val instanceof SolrDocument) {
          val = toSolrInputDocument((SolrDocument) val, schema);
          if(!fieldArrayListCreated && doc.getFieldValue(fname) instanceof Collection) {
            // previous value was array so we must return as an array even if was a single value array
            out.setField(fname, Lists.newArrayList(val));
            fieldArrayListCreated = true;
            continue;
          }
        }
        out.addField(fname, val);
      }
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
          if (f.fieldType().docValuesType() == DocValuesType.SORTED_NUMERIC) {
            // SORTED_NUMERICS store sortable bits version of the value, need to retrieve the original
            vals.add(sf.getType().toObject(f)); // (will materialize by side-effect)
          } else {
            vals.add( materialize(f) );
          }
        out.setField(f.name(), vals);
      } else {
          out.setField( f.name(), materialize(f) );
        }
      }
      else {
        out.addField( f.name(), materialize(f) );
      }
    }
    return out;
  }

  /**
   * Ensure we don't have {@link org.apache.lucene.document.LazyDocument.LazyField} or equivalent.
   * It can pose problems if the searcher is about to be closed and we haven't fetched a value yet.
   */
  private static IndexableField materialize(IndexableField in) {
    if (in instanceof Field) { // already materialized
      return in;
    }
    return new ClonedField(in);
  }

  private static class ClonedField extends Field { // TODO Lucene Field has no copy constructor; maybe it should?
    ClonedField(IndexableField in) {
      super(in.name(), in.fieldType());
      this.fieldsData = in.numericValue();
      if (this.fieldsData == null) {
        this.fieldsData = in.binaryValue();
        if (this.fieldsData == null) {
          this.fieldsData = in.stringValue();
          if (this.fieldsData == null) {
            // fallback:
            assert false : in; // unexpected
          }
        }
      }
    }
  }

  /**
   * Converts a SolrInputDocument to SolrDocument, using an IndexSchema instance. 
   * @lucene.experimental
   */
  public static SolrDocument toSolrDoc(SolrInputDocument sdoc, IndexSchema schema) {
    return toSolrDoc(sdoc, schema, false);
  }

  /**
   * Converts a SolrInputDocument to SolrDocument, using an IndexSchema instance.
   *
   * @param sdoc The input document.
   * @param schema The index schema.
   * @param forInPlaceUpdate Whether the document is being used for an in place update,
   *                         see {@link DocumentBuilder#toDocument(SolrInputDocument, IndexSchema, boolean, boolean)}
   */
  public static SolrDocument toSolrDoc(SolrInputDocument sdoc, IndexSchema schema, boolean forInPlaceUpdate) {
    // TODO: do something more performant than this double conversion
    Document doc = DocumentBuilder.toDocument(sdoc, schema, forInPlaceUpdate, true);

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
        log.debug("Don't know how to handle field {}", f);
      }
    }

    SolrDocument solrDoc = toSolrDoc(out, schema);

    // add child docs
    for(SolrInputField solrInputField: sdoc) {
      if(solrInputField.getFirstValue() instanceof SolrInputDocument) {
        // is child doc
        Object val = solrInputField.getValue();
        Iterator<SolrDocument> childDocs = solrInputField.getValues().stream()
            .map(x -> toSolrDoc((SolrInputDocument) x, schema)).iterator();
        if(val instanceof Collection) {
          // add as collection even if single element collection
          solrDoc.setField(solrInputField.getName(), Lists.newArrayList(childDocs));
        } else {
          // single child doc
          solrDoc.setField(solrInputField.getName(), childDocs.next());
        }
      }
    }

    return solrDoc;
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

    ZkController zkController = rb.req.getCore().getCoreContainer().getZkController();

    // if shards=... then use that
    if (zkController != null && params.get(ShardParams.SHARDS) == null) {
      CloudDescriptor cloudDescriptor = rb.req.getCore().getCoreDescriptor().getCloudDescriptor();

      String collection = cloudDescriptor.getCollectionName();
      ClusterState clusterState = zkController.getClusterState();
      DocCollection coll = clusterState.getCollection(collection);


      Map<String, List<String>> sliceToId = new HashMap<>();
      for (String id : reqIds.allIds) {
        Slice slice = coll.getRouter().getTargetSlice(params.get(ShardParams._ROUTE_, id), null, null, params, coll);

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
    sreq.params.set(DISTRIB,false);

    sreq.params.remove(ShardParams.SHARDS);
    sreq.params.remove(ID);
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
      log.info("LOOKUP_SLICE:{}={}", rb.slices[i], rb.shards[i]);
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
        @SuppressWarnings({"rawtypes"})
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
  ///  SolrInfoBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "query";
  }

  @Override
  public Category getCategory() {
    return Category.QUERY;
  }

  public void processGetFingeprint(ResponseBuilder rb) throws IOException {
    TestInjection.injectFailIndexFingerprintRequests();

    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();

    long maxVersion = params.getLong("getFingerprint", Long.MAX_VALUE);
    if (TestInjection.injectWrongIndexFingerprint())  {
      maxVersion = -1;
    }
    IndexFingerprint fingerprint = IndexFingerprint.getFingerprint(req.getCore(), Math.abs(maxVersion));
    rb.rsp.add("fingerprint", fingerprint);
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
    String syncWithLeader = params.get("syncWithLeader");
    if (syncWithLeader != null) {
      List<Long> versions;
      try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
        versions = recentUpdates.getVersions(nVersions);
      }
      processSyncWithLeader(rb, nVersions, syncWithLeader, versions);
      return;
    }

    // get fingerprint first as it will cause a soft commit
    // and would avoid mismatch if documents are being actively index especially during PeerSync
    if (doFingerprint) {
      IndexFingerprint fingerprint = IndexFingerprint.getFingerprint(req.getCore(), Long.MAX_VALUE);
      rb.rsp.add("fingerprint", fingerprint);
    }

    try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
      List<Long> versions = recentUpdates.getVersions(nVersions);
      rb.rsp.add("versions", versions);
    }
  }

  public void processSyncWithLeader(ResponseBuilder rb, int nVersions, String syncWithLeader, List<Long> versions) {
    try (PeerSyncWithLeader peerSync = new PeerSyncWithLeader(rb.req.getCore(), syncWithLeader, nVersions)) {
      boolean success = peerSync.sync(versions).isSuccess();
      rb.rsp.add("syncWithLeader", success);
    } catch (IOException e) {
      log.error("Error while closing", e);
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
    try (PeerSync peerSync = new PeerSync(rb.req.getCore(), replicas, nVersions, cantReachIsSuccess)) {
      boolean success = peerSync.sync().isSuccess();
      // TODO: more complex response?
      rb.rsp.add("sync", success);
    } catch (IOException e) {
      log.error("Error while closing", e);
    }
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
      rb.rsp.add("fingerprint", fingerprint);
    }

    List<Object> updates = new ArrayList<>(versions.size());

    long minVersion = Long.MAX_VALUE;

    // TODO: get this from cache instead of rebuilding?
    try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
      LongSet updateVersions = new LongSet(versions.size());
      for (Long version : versions) {
        try {
          Object o = recentUpdates.lookup(version);
          if (o == null) continue;
          updateVersions.add(version);

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
      if (params.getBool("skipDbq", false)) {
        updates.addAll(recentUpdates.getDeleteByQuery(minVersion, updateVersions));
      }

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
   * Lookup strategy for some methods on this class.
   */
  public static enum Resolution {
    /** A partial update document.  Whole documents may still be returned. */
    PARTIAL,

    /** Resolve to a whole document, exclusive of children. */
    DOC,

    /** Resolves the whole nested hierarchy (look up root doc). */
    ROOT_WITH_CHILDREN
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
      final String id[] = params.getParams(ID);
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
