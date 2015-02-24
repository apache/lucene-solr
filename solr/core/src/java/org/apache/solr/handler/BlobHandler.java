package org.apache.solr.handler;

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
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.QParser;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.SimplePostTool;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;

public class BlobHandler extends RequestHandlerBase  implements PluginInfoInitialized{
  protected static final Logger log = LoggerFactory.getLogger(BlobHandler.class);

  private static final long MAX_SZ = 5*1024*1024;//2MB
  private long maxSize = MAX_SZ;



  @Override
  public void handleRequestBody(final SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    String httpMethod = (String) req.getContext().get("httpMethod");
    String path = (String) req.getContext().get("path");
    SolrConfigHandler.setWt(req,"json");

    List<String> pieces = StrUtils.splitSmart(path, '/');
    String blobName = null;
    if(pieces.size()>=3) blobName = pieces.get(2);

    if("POST".equals(httpMethod)) {
      if (blobName == null || blobName.isEmpty()) {
        rsp.add("error","Name not found");
        return;
      }
      String err = SolrConfigHandler.validateName(blobName);
      if(err!=null){
        log.warn("no blob name");
        rsp.add("error", err);
        return;
      }
      if(req.getContentStreams() == null )  {
        log.warn("no content stream");
        rsp.add("error","No stream");
        return;
      }


      for (ContentStream stream : req.getContentStreams()) {
        ByteBuffer payload = SimplePostTool.inputStreamToByteArray(stream.getStream(), maxSize);
        MessageDigest m = MessageDigest.getInstance("MD5");
        m.update(payload.array(),payload.position(),payload.limit());
        String md5 = new BigInteger(1,m.digest()).toString(16);

        TopDocs duplicate = req.getSearcher().search(new TermQuery(new Term("md5", md5)), 1);
        if(duplicate.totalHits >0){
          rsp.add("error", "duplicate entry");
          req.forward(null,
              new MapSolrParams((Map) makeMap(
              "q", "md5:" + md5,
              "fl", "id,size,version,timestamp,blobName")),
              rsp);
          log.warn("duplicate entry for blob :"+blobName);
          return;
        }

        TopFieldDocs docs = req.getSearcher().search(new TermQuery(new Term("blobName", blobName)),
            1, new Sort(new SortField("version", SortField.Type.LONG, true)));

        long version = 0;
        if(docs.totalHits >0){
          StoredDocument doc = req.getSearcher().doc(docs.scoreDocs[0].doc);
          Number n = doc.getField("version").numericValue();
          version = n.longValue();
        }
        version++;
        String id = blobName+"/"+version;
        Map<String, Object> doc = makeMap(
            "id", id,
            "md5", md5,
            "blobName", blobName,
            "version", version,
            "timestamp", new Date(),
            "size", payload.limit(),
            "blob", payload);
        verifyWithRealtimeGet(blobName, version, req, doc);
        log.info(MessageFormat.format("inserting new blob {0} ,size {1}, md5 {2}",doc.get("id"), String.valueOf(payload.limit()),md5));
        indexMap(req, rsp, doc);
        log.info(" Successfully Added and committed a blob with id {} and size {} ",id, payload.limit());

        break;
      }

    } else {
      int version =-1;
      if(pieces.size()>3){
        try {
          version = Integer.parseInt(pieces.get(3));
        } catch (NumberFormatException e) {
          rsp.add("error", "Invalid version" + pieces.get(3));
          return;
        }

      }
      if(ReplicationHandler.FILE_STREAM.equals(req.getParams().get(CommonParams.WT))){
        if(blobName == null ){
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Please send the request in the format /blob/<blobName>/<version>");
        } else {
          String q = "blobName:{0}";
          if(version != -1) q = "id:{0}/{1}";
          QParser qparser =  QParser.getParser(MessageFormat.format(q,blobName,version) , "lucene", req);
          final TopDocs docs = req.getSearcher().search(qparser.parse(), 1, new Sort( new SortField("version", SortField.Type.LONG, true)));
          if(docs.totalHits>0){
            rsp.add(ReplicationHandler.FILE_STREAM, new SolrCore.RawWriter(){

              @Override
              public void write(OutputStream os) throws IOException {
                StoredDocument doc = req.getSearcher().doc(docs.scoreDocs[0].doc);
                StorableField sf = doc.getField("blob");
                FieldType fieldType = req.getSchema().getField("blob").getType();
                ByteBuffer buf = (ByteBuffer) fieldType.toObject(sf);
                if(buf == null){
                  //should never happen unless a user wrote this document directly
                  throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Invalid document . No field called blob");
                } else {
                  os.write(buf.array(),0,buf.limit());
                }
              }
            });

          } else {
            throw new SolrException(SolrException.ErrorCode.NOT_FOUND,
                MessageFormat.format("Invalid combination of blobName {0} and version {1}", blobName,String.valueOf(version)));
          }

        }
      } else {
        String q = "*:*";
        if(blobName != null){
          q = "blobName:{0}";
          if(version != -1){
            q = "id:{0}/{1}";
          }
        }

        req.forward(null,
            new MapSolrParams((Map) makeMap(
                "q", MessageFormat.format(q, blobName, version),
                "fl", "id,size,version,timestamp,blobName,md5",
                "sort", "version desc"))
            , rsp);
      }
    }
  }

  private void verifyWithRealtimeGet(String blobName, long version, SolrQueryRequest req, Map<String, Object> doc) {
    for(;;) {
      SolrQueryResponse response = new SolrQueryResponse();
      String id = blobName + "/" + version;
      req.forward("/get", new MapSolrParams(singletonMap("id", id)), response);
      if(response.getValues().get("doc") == null) {
        //ensure that the version does not exist
        return;
      } else {
        log.info("id {} already exists trying next ",id);
        version++;
        doc.put("version", version);
        id = blobName + "/" + version;
        doc.put("id", id);
      }
   }

  }

  public static void indexMap(SolrQueryRequest req, SolrQueryResponse rsp, Map<String, Object> doc) throws IOException {
    SolrInputDocument solrDoc = new SolrInputDocument();
    for (Map.Entry<String, Object> e : doc.entrySet()) solrDoc.addField(e.getKey(),e.getValue());
    UpdateRequestProcessorChain processorChain = req.getCore().getUpdateProcessingChain(req.getParams().get(UpdateParams.UPDATE_CHAIN));
    UpdateRequestProcessor processor = processorChain.createProcessor(req, rsp);
    AddUpdateCommand cmd = new AddUpdateCommand(req);
    cmd.solrDoc = solrDoc;
    log.info("Adding doc: "+doc);
    processor.processAdd(cmd);
    log.info("committing doc: "+doc);
    processor.processCommit(new CommitUpdateCommand(req, false));
    processor.finish();
  }

  @Override
  public SolrRequestHandler getSubHandler(String subPath) {
    if(StrUtils.splitSmart(subPath,'/').size()>4)  return null;
    return this;
  }


//////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Load Jars into a system index";
  }

  public static final String SCHEMA = "<?xml version='1.0' ?>\n" +
      "<schema name='_system collection or core' version='1.1'>\n" +
      "  <fieldtype name='string'  class='solr.StrField' sortMissingLast='true' omitNorms='true'/>\n" +
      "  <fieldType name='long' class='solr.TrieLongField' precisionStep='0' positionIncrementGap='0'/>\n" +
      "  <fieldType name='bytes' class='solr.BinaryField'/>\n" +
      "  <fieldType name='date' class='solr.TrieDateField'/>\n" +
      "  <field name='id'   type='string'   indexed='true'  stored='true'  multiValued='false' required='true'/>\n" +
      "  <field name='md5'   type='string'   indexed='true'  stored='true'  multiValued='false' required='true'/>\n" +
      "  <field name='blob'      type='bytes'   indexed='false' stored='true'  multiValued='false' />\n" +
      "  <field name='size'      type='long'   indexed='true' stored='true'  multiValued='false' />\n" +
      "  <field name='version'   type='long'     indexed='true'  stored='true'  multiValued='false' />\n" +
      "  <field name='timestamp'   type='date'   indexed='true'  stored='true'  multiValued='false' />\n" +
      "  <field name='blobName'      type='string'   indexed='true'  stored='true'  multiValued='false' />\n" +
      "  <field name='_version_' type='long'     indexed='true'  stored='true'/>\n" +
      "  <uniqueKey>id</uniqueKey>\n" +
      "</schema>" ;

  public static final String CONF = "<?xml version='1.0' ?>\n" +
      "<config>\n" +
      "<luceneMatchVersion>LATEST</luceneMatchVersion>\n" +
      "<directoryFactory name='DirectoryFactory' class='${solr.directoryFactory:solr.StandardDirectoryFactory}'/>\n" +
      "<updateHandler class='solr.DirectUpdateHandler2'>\n" +
      "  <updateLog>\n" +
      "    <str name='dir'>${solr.ulog.dir:}</str>\n" +
      "  </updateLog>\n     " +
      "  <autoCommit> \n" +
      "       <maxDocs>1</maxDocs> \n" +
      "       <openSearcher>true</openSearcher> \n" +
      "  </autoCommit>" +
      "</updateHandler>\n" +
      "<requestHandler name='standard' class='solr.StandardRequestHandler' default='true' />\n" +
      "<requestHandler name='/analysis/field' startup='lazy' class='solr.FieldAnalysisRequestHandler' />\n" +
      "<requestHandler name='/blob' class='solr.BlobHandler'>\n" +
      "  <lst name='invariants'>\n"+
           "<str name='maxSize'>${blob.max.size.mb:5}</str>\n"+
         "</lst>\n"+
      "</requestHandler>\n" +
      "</config>" ;

  @Override
  public void init(PluginInfo info) {
    super.init(info.initArgs);
    if(info.initArgs != null ){
      NamedList invariants = (NamedList) info.initArgs.get(PluginInfo.INVARIANTS);
      if(invariants != null){
        Object o = invariants.get("maxSize");
        if(o != null){
          maxSize = Long.parseLong(String.valueOf(o));
          maxSize = maxSize*1024*1024;
        }
      }

    }
  }
}
