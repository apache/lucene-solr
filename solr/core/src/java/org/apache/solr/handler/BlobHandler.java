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
package org.apache.solr.handler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.QParser;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.SimplePostTool;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;
import static org.apache.solr.common.params.CommonParams.ID;
import static org.apache.solr.common.params.CommonParams.JSON;
import static org.apache.solr.common.params.CommonParams.SORT;
import static org.apache.solr.common.params.CommonParams.VERSION;
import static org.apache.solr.common.util.Utils.makeMap;

public class BlobHandler extends RequestHandlerBase implements PluginInfoInitialized , PermissionNameProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long DEFAULT_MAX_SIZE = 5 * 1024 * 1024; // 5MB
  private long maxSize = DEFAULT_MAX_SIZE;

  @Override
  @SuppressWarnings({"unchecked"})
  public void handleRequestBody(final SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    String httpMethod = req.getHttpMethod();
    String path = (String) req.getContext().get("path");
    RequestHandlerUtils.setWt(req, JSON);

    List<String> pieces = StrUtils.splitSmart(path, '/');
    String blobName = null;
    if (pieces.size() >= 3) blobName = pieces.get(2);

    if ("POST".equals(httpMethod)) {
      if (blobName == null || blobName.isEmpty()) {
        rsp.add("error", "Name not found");
        return;
      }
      String err = SolrConfigHandler.validateName(blobName);
      if (err != null) {
        log.warn("no blob name");
        rsp.add("error", err);
        return;
      }
      if (req.getContentStreams() == null) {
        log.warn("no content stream");
        rsp.add("error", "No stream");
        return;
      }


      for (ContentStream stream : req.getContentStreams()) {
        ByteBuffer payload;
        try (InputStream is = stream.getStream()) {
          payload = SimplePostTool.inputStreamToByteArray(is, maxSize);
        }
        MessageDigest m = MessageDigest.getInstance("MD5");
        m.update(payload.array(), payload.arrayOffset() + payload.position(), payload.limit());
        String md5 = new String(Hex.encodeHex(m.digest()));

        int duplicateCount = req.getSearcher().count(new TermQuery(new Term("md5", md5)));
        if (duplicateCount > 0) {
          rsp.add("error", "duplicate entry");
          forward(req, null,
              new MapSolrParams((Map) makeMap(
                  "q", "md5:" + md5,
                  "fl", "id,size,version,timestamp,blobName")),
              rsp);
          log.warn("duplicate entry for blob : {}", blobName);
          return;
        }

        TopFieldDocs docs = req.getSearcher().search(new TermQuery(new Term("blobName", blobName)),
            1, new Sort(new SortField("version", SortField.Type.LONG, true)));

        long version = 0;
        if (docs.totalHits.value > 0) {
          Document doc = req.getSearcher().doc(docs.scoreDocs[0].doc);
          Number n = doc.getField("version").numericValue();
          version = n.longValue();
        }
        version++;
        String id = blobName + "/" + version;
        Map<String, Object> doc = makeMap(
            ID, id,
            CommonParams.TYPE, "blob",
            "md5", md5,
            "blobName", blobName,
            VERSION, version,
            "timestamp", new Date(),
            "size", payload.limit(),
            "blob", payload);
        verifyWithRealtimeGet(blobName, version, req, doc);
        if (log.isInfoEnabled()) {
          log.info(StrUtils.formatString("inserting new blob {0} ,size {1}, md5 {2}", doc.get(ID), String.valueOf(payload.limit()), md5));
        }
        indexMap(req, rsp, doc);
        if (log.isInfoEnabled()) {
          log.info(" Successfully Added and committed a blob with id {} and size {} ", id, payload.limit());
        }

        break;
      }

    } else {
      int version = -1;
      if (pieces.size() > 3) {
        try {
          version = Integer.parseInt(pieces.get(3));
        } catch (NumberFormatException e) {
          rsp.add("error", "Invalid version" + pieces.get(3));
          return;
        }

      }
      if (ReplicationHandler.FILE_STREAM.equals(req.getParams().get(CommonParams.WT))) {
        if (blobName == null) {
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Please send the request in the format /blob/<blobName>/<version>");
        } else {
          String q = "blobName:{0}";
          if (version != -1) q = "id:{0}/{1}";
          QParser qparser = QParser.getParser(StrUtils.formatString(q, blobName, version), req);
          final TopDocs docs = req.getSearcher().search(qparser.parse(), 1, new Sort(new SortField("version", SortField.Type.LONG, true)));
          if (docs.totalHits.value > 0) {
            rsp.add(ReplicationHandler.FILE_STREAM, new SolrCore.RawWriter() {

              @Override
              public void write(OutputStream os) throws IOException {
                Document doc = req.getSearcher().doc(docs.scoreDocs[0].doc);
                IndexableField sf = doc.getField("blob");
                FieldType fieldType = req.getSchema().getField("blob").getType();
                ByteBuffer buf = (ByteBuffer) fieldType.toObject(sf);
                if (buf == null) {
                  //should never happen unless a user wrote this document directly
                  throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Invalid document . No field called blob");
                } else {
                  os.write(buf.array(), buf.arrayOffset(), buf.limit());
                }
              }
            });

          } else {
            throw new SolrException(SolrException.ErrorCode.NOT_FOUND,
                StrUtils.formatString("Invalid combination of blobName {0} and version {1}", blobName, version));
          }

        }
      } else {
        String q = "*:*";
        if (blobName != null) {
          q = "blobName:{0}";
          if (version != -1) {
            q = "id:{0}/{1}";
          }
        }

        forward(req, null,
            new MapSolrParams((Map) makeMap(
                "q", StrUtils.formatString(q, blobName, version),
                "fl", "id,size,version,timestamp,blobName,md5",
                SORT, "version desc"))
            , rsp);
      }
    }
  }

  private void verifyWithRealtimeGet(String blobName, long version, SolrQueryRequest req, Map<String, Object> doc) {
    for (; ; ) {
      SolrQueryResponse response = new SolrQueryResponse();
      String id = blobName + "/" + version;
      forward(req, "/get", new MapSolrParams(singletonMap(ID, id)), response);
      if (response.getValues().get("doc") == null) {
        //ensure that the version does not exist
        return;
      } else {
        log.info("id {} already exists trying next ", id);
        version++;
        doc.put("version", version);
        id = blobName + "/" + version;
        doc.put(ID, id);
      }
    }

  }

  public static void indexMap(SolrQueryRequest req, SolrQueryResponse rsp, Map<String, Object> doc) throws IOException {
    SolrInputDocument solrDoc = new SolrInputDocument();
    for (Map.Entry<String, Object> e : doc.entrySet()) solrDoc.addField(e.getKey(), e.getValue());
    UpdateRequestProcessorChain processorChain = req.getCore().getUpdateProcessorChain(req.getParams());
    try (UpdateRequestProcessor processor = processorChain.createProcessor(req, rsp)) {
      AddUpdateCommand cmd = new AddUpdateCommand(req);
      cmd.solrDoc = solrDoc;
      log.info("Adding doc: {}", doc);
      processor.processAdd(cmd);
      log.info("committing doc: {}", doc);
      processor.processCommit(new CommitUpdateCommand(req, false));
      processor.finish();
    }
  }

  @Override
  public SolrRequestHandler getSubHandler(String subPath) {
    if (StrUtils.splitSmart(subPath, '/').size() > 4) return null;
    return this;
  }


//////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Load Jars into a system index";
  }


  @Override
  public void init(PluginInfo info) {
    super.init(info.initArgs);
    if (info.initArgs != null) {
      @SuppressWarnings({"rawtypes"})
      NamedList invariants = (NamedList) info.initArgs.get(PluginInfo.INVARIANTS);
      if (invariants != null) {
        Object o = invariants.get("maxSize");
        if (o != null) {
          maxSize = Long.parseLong(String.valueOf(o));
          maxSize = maxSize * 1024 * 1024;
        }
      }

    }
  }

  // This does not work for the general case of forwarding requests.  It probably currently
  // works OK for real-time get (which is all that BlobHandler uses it for).
  private static void forward(SolrQueryRequest req, String handler ,SolrParams params, SolrQueryResponse rsp){
    LocalSolrQueryRequest r = new LocalSolrQueryRequest(req.getCore(), params);
    SolrRequestInfo.getRequestInfo().addCloseHook( r );  // Close as late as possible...
    req.getCore().getRequestHandler(handler).handleRequest(r, rsp);
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Collection<Api> getApis() {
    return ApiBag.wrapRequestHandlers(this, "core.system.blob", "core.system.blob.upload");
  }

  @Override
  public Name getPermissionName(AuthorizationContext ctx) {
    switch (ctx.getHttpMethod()) {
      case "GET":
        return Name.READ_PERM;
      case "POST":
        return Name.UPDATE_PERM;
      default:
        return null;
    }

  }
}
