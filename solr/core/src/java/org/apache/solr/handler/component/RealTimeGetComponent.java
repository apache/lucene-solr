package org.apache.solr.handler.component;

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
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformContext;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.update.PeerSync;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RealTimeGetComponent extends SearchComponent
{
  public static Logger log = LoggerFactory.getLogger(UpdateLog.class);
  public static final String COMPONENT_NAME = "get";

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    // Set field flags
    ReturnFields returnFields = new ReturnFields( rb.req );
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

    String val = params.get("getVersions");
    if (val != null) {
      processGetVersions(rb);
      return;
    }

    val = params.get("getUpdates");
    if (val != null) {
      processGetUpdates(rb);
      return;
    }

    String id[] = params.getParams("id");
    String ids[] = params.getParams("ids");

    if (id == null && ids == null) {
      return;
    }

    String[] allIds = id==null ? new String[0] : id;

    if (ids != null) {
      List<String> lst = new ArrayList<String>();
      for (String s : allIds) {
        lst.add(s);
      }
      for (String idList : ids) {
        lst.addAll( StrUtils.splitSmart(idList, ",", true) );
      }
      allIds = lst.toArray(new String[lst.size()]);
    }

    SchemaField idField = req.getSchema().getUniqueKeyField();
    FieldType fieldType = idField.getType();

    SolrDocumentList docList = new SolrDocumentList();
    UpdateLog ulog = req.getCore().getUpdateHandler().getUpdateLog();

    RefCounted<SolrIndexSearcher> searcherHolder = null;

    DocTransformer transformer = rsp.getReturnFields().getTransformer();
    if (transformer != null) {
      TransformContext context = new TransformContext();
      context.req = req;
      transformer.setContext(context);
    }
   try {
     SolrIndexSearcher searcher = null;

     BytesRef idBytes = new BytesRef();
     for (String idStr : allIds) {
       fieldType.readableToIndexed(idStr, idBytes);
       if (ulog != null) {
         Object o = ulog.lookup(idBytes);
         if (o != null) {
           // should currently be a List<Oper,Ver,Doc/Id>
           List entry = (List)o;
           assert entry.size() >= 3;
           int oper = (Integer)entry.get(0) & UpdateLog.OPERATION_MASK;
           switch (oper) {
             case UpdateLog.ADD:
               SolrDocument doc = toSolrDoc((SolrInputDocument)entry.get(entry.size()-1), req.getSchema());
               if(transformer!=null) {
                 transformer.transform(doc, -1); // unknown docID
               }
              docList.add(doc);
              break;
             case UpdateLog.DELETE:
              break;
             default:
               throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,  "Unknown Operation! " + oper);
           }
           continue;
         }
       }

       // didn't find it in the update log, so it should be in the newest searcher opened
       if (searcher == null) {
         searcherHolder = req.getCore().getRealtimeSearcher();
         searcher = searcherHolder.get();
       }

       // SolrCore.verbose("RealTimeGet using searcher ", searcher);

       int docid = searcher.getFirstMatch(new Term(idField.getName(), idBytes));
       if (docid < 0) continue;
       StoredDocument luceneDocument = searcher.doc(docid);
       SolrDocument doc = toSolrDoc(luceneDocument,  req.getSchema());
       if( transformer != null ) {
         transformer.transform(doc, docid);
       }
       docList.add(doc);
     }

   } finally {
     if (searcherHolder != null) {
       searcherHolder.decref();
     }
   }


   // if the client specified a single id=foo, then use "doc":{
   // otherwise use a standard doclist

   if (ids ==  null && allIds.length <= 1) {
     // if the doc was not found, then use a value of null.
     rsp.add("doc", docList.size() > 0 ? docList.get(0) : null);
   } else {
     docList.setNumFound(docList.size());
     rsp.add("response", docList);
   }

  }

  public static SolrInputDocument getInputDocument(SolrCore core, BytesRef idBytes) throws IOException {
    SolrInputDocument sid = null;
    RefCounted<SolrIndexSearcher> searcherHolder = null;
    try {
      SolrIndexSearcher searcher = null;
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
              sid = (SolrInputDocument)entry.get(entry.size()-1);
              break;
            case UpdateLog.DELETE:
              return null;
            default:
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,  "Unknown Operation! " + oper);
          }
        }
      }

      if (sid == null) {
        // didn't find it in the update log, so it should be in the newest searcher opened
        if (searcher == null) {
          searcherHolder = core.getRealtimeSearcher();
          searcher = searcherHolder.get();
        }

        // SolrCore.verbose("RealTimeGet using searcher ", searcher);
        SchemaField idField = core.getSchema().getUniqueKeyField();

        int docid = searcher.getFirstMatch(new Term(idField.getName(), idBytes));
        if (docid < 0) return null;
        StoredDocument luceneDocument = searcher.doc(docid);
        sid = toSolrInputDocument(luceneDocument, core.getSchema());
      }
    } finally {
      if (searcherHolder != null) {
        searcherHolder.decref();
      }
    }

    return sid;
  }

  private static SolrInputDocument toSolrInputDocument(StoredDocument doc, IndexSchema schema) {
    SolrInputDocument out = new SolrInputDocument();
    for( StorableField f : doc.getFields() ) {
      String fname = f.name();
      SchemaField sf = schema.getFieldOrNull(f.name());
      Object val = null;
      if (sf != null) {
        if (!sf.stored() || schema.isCopyFieldTarget(sf)) continue;
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


  private static SolrDocument toSolrDoc(StoredDocument doc, IndexSchema schema) {
    SolrDocument out = new SolrDocument();
    for( StorableField f : doc.getFields() ) {
      // Make sure multivalued fields are represented as lists
      Object existing = out.get(f.name());
      if (existing == null) {
        SchemaField sf = schema.getFieldOrNull(f.name());

        // don't return copyField targets
        if (sf != null && schema.isCopyFieldTarget(sf)) continue;

        if (sf != null && sf.multiValued()) {
          List<Object> vals = new ArrayList<Object>();
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
    StoredDocument out = new StoredDocument();
    for (IndexableField f : doc.getFields()) {
      if (f.fieldType().stored() ) {
        out.add((StorableField) f);
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
    SolrParams params = rb.req.getParams();
    String id1[] = params.getParams("id");
    String ids[] = params.getParams("ids");

    if (id1 == null && ids == null) {
      return ResponseBuilder.STAGE_DONE;
    }

    List<String> allIds = new ArrayList<String>();
    if (id1 != null) {
      for (String s : id1) {
        allIds.add(s);
      }
    }
    if (ids != null) {
      for (String s : ids) {
        allIds.addAll( StrUtils.splitSmart(s, ",", true) );
      }
    }

    // TODO: handle collection=...?

    ZkController zkController = rb.req.getCore().getCoreDescriptor().getCoreContainer().getZkController();

    // if shards=... then use that
    if (zkController != null && params.get("shards") == null) {
      CloudDescriptor cloudDescriptor = rb.req.getCore().getCoreDescriptor().getCloudDescriptor();

      String collection = cloudDescriptor.getCollectionName();

      ClusterState clusterState = zkController.getClusterState();
      
      Map<String, List<String>> shardToId = new HashMap<String, List<String>>();
      for (String id : allIds) {
        int hash = Hash.murmurhash3_x86_32(id, 0, id.length(), 0);
        String shard = clusterState.getShard(hash,  collection);

        List<String> idsForShard = shardToId.get(shard);
        if (idsForShard == null) {
          idsForShard = new ArrayList<String>(2);
          shardToId.put(shard, idsForShard);
        }
        idsForShard.add(id);
      }

      for (Map.Entry<String,List<String>> entry : shardToId.entrySet()) {
        String shard = entry.getKey();
        String shardIdList = StrUtils.join(entry.getValue(), ',');

        ShardRequest sreq = new ShardRequest();

        sreq.purpose = 1;
        // sreq.shards = new String[]{shard};    // TODO: would be nice if this would work...
        sreq.shards = sliceToShards(rb, collection, shard);
        sreq.actualShards = sreq.shards;
        sreq.params = new ModifiableSolrParams();
        sreq.params.set(ShardParams.SHARDS_QT,"/get");      // TODO: how to avoid hardcoding this and hit the same handler?
        sreq.params.set("distrib",false);
        sreq.params.set("ids", shardIdList);

        rb.addRequest(this, sreq);
      }      
    } else {
      String shardIdList = StrUtils.join(allIds, ',');
      ShardRequest sreq = new ShardRequest();

      sreq.purpose = 1;
      sreq.shards = null;  // ALL
      sreq.actualShards = sreq.shards;
      sreq.params = new ModifiableSolrParams();
      sreq.params.set(ShardParams.SHARDS_QT,"/get");      // TODO: how to avoid hardcoding this and hit the same handler?
      sreq.params.set("distrib",false);
      sreq.params.set("ids", shardIdList);

      rb.addRequest(this, sreq);
    }

    return ResponseBuilder.STAGE_DONE;
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

    if (docList.size() <= 1 && rb.req.getParams().getParams("ids")==null) {
      // if the doc was not found, then use a value of null.
      rb.rsp.add("doc", docList.size() > 0 ? docList.get(0) : null);
    } else {
      docList.setNumFound(docList.size());
      rb.rsp.add("response", docList);
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
  public String getSource() {
    return "$URL$";
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

    String sync = params.get("sync");
    if (sync != null) {
      processSync(rb, nVersions, sync);
      return;
    }

    UpdateLog ulog = req.getCore().getUpdateHandler().getUpdateLog();
    if (ulog == null) return;

    UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates();
    try {
      rb.rsp.add("versions", recentUpdates.getVersions(nVersions));
    } finally {
      recentUpdates.close();  // cache this somehow?
    }
  }

  
  public void processSync(ResponseBuilder rb, int nVersions, String sync) {
    List<String> replicas = StrUtils.splitSmart(sync, ",", true);
    
    
    PeerSync peerSync = new PeerSync(rb.req.getCore(), replicas, nVersions);
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

    List<String> versions = StrUtils.splitSmart(versionsStr, ",", true);

    // TODO: get this from cache instead of rebuilding?
    UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates();

    List<Object> updates = new ArrayList<Object>(versions.size());

    long minVersion = Long.MAX_VALUE;
    
    try {
      for (String versionStr : versions) {
        long version = Long.parseLong(versionStr);
        try {
          Object o = recentUpdates.lookup(version);
          if (o == null) continue;

          if (version > 0) {
            minVersion = Math.min(minVersion, version);
          }
          
          // TODO: do any kind of validation here?
          updates.add(o);

        } catch (SolrException e) {
          log.warn("Exception reading log for updates", e);
        } catch (ClassCastException e) {
          log.warn("Exception reading log for updates", e);
        }
      }

      // Must return all delete-by-query commands that occur after the first add requested
      // since they may apply.
      updates.addAll( recentUpdates.getDeleteByQuery(minVersion));

      rb.rsp.add("updates", updates);

    } finally {
      recentUpdates.close();  // cache this somehow?
    }
  }

}
