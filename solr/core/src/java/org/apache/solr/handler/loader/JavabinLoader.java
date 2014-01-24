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

package org.apache.solr.handler.loader;

import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Update handler which uses the JavaBin format
 *
 * @see org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec
 * @see org.apache.solr.common.util.JavaBinCodec
 */
public class JavabinLoader extends ContentStreamLoader {
  public static Logger log = LoggerFactory.getLogger(JavabinLoader.class);
  
  @Override
  public void load(SolrQueryRequest req, SolrQueryResponse rsp, ContentStream stream, UpdateRequestProcessor processor) throws Exception {
    InputStream is = null;
    try {
      is = stream.getStream();
      parseAndLoadDocs(req, rsp, is, processor);
    } finally {
      if(is != null) {
        is.close();
      }
    }
  }
  
  private void parseAndLoadDocs(final SolrQueryRequest req, SolrQueryResponse rsp, InputStream stream,
                                final UpdateRequestProcessor processor) throws IOException {
    UpdateRequest update = null;
    JavaBinUpdateRequestCodec.StreamingUpdateHandler handler = new JavaBinUpdateRequestCodec.StreamingUpdateHandler() {
      private AddUpdateCommand addCmd = null;

      @Override
      public void update(SolrInputDocument document, UpdateRequest updateRequest, Integer commitWithin, Boolean overwrite) {
        if (document == null) {
          // Perhaps commit from the parameters
          try {
            RequestHandlerUtils.handleCommit(req, processor, updateRequest.getParams(), false);
            RequestHandlerUtils.handleRollback(req, processor, updateRequest.getParams(), false);
          } catch (IOException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "ERROR handling commit/rollback");
          }
          return;
        }
        if (addCmd == null) {
          addCmd = getAddCommand(req, updateRequest.getParams());
        }
        addCmd.solrDoc = document;
        if (commitWithin != null) {
          addCmd.commitWithin = commitWithin;
        }
        if (overwrite != null) {
          addCmd.overwrite = overwrite;
        }
        
        try {
          processor.processAdd(addCmd);
          addCmd.clear();
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "ERROR adding document " + document);
        }
      }
    };
    FastInputStream in = FastInputStream.wrap(stream);
    for (; ; ) {
      try {
        update = new JavaBinUpdateRequestCodec().unmarshal(in, handler);
      } catch (EOFException e) {
        break; // this is expected
      }
      if (update.getDeleteByIdMap() != null || update.getDeleteQuery() != null) {
        delete(req, update, processor);
      }
    }
  }

  private AddUpdateCommand getAddCommand(SolrQueryRequest req, SolrParams params) {
    AddUpdateCommand addCmd = new AddUpdateCommand(req);

    addCmd.overwrite = params.getBool(UpdateParams.OVERWRITE, true);
    addCmd.commitWithin = params.getInt(UpdateParams.COMMIT_WITHIN, -1);
    return addCmd;
  }

  private void delete(SolrQueryRequest req, UpdateRequest update, UpdateRequestProcessor processor) throws IOException {
    SolrParams params = update.getParams();
    DeleteUpdateCommand delcmd = new DeleteUpdateCommand(req);
    if(params != null) {
      delcmd.commitWithin = params.getInt(UpdateParams.COMMIT_WITHIN, -1);
    }
    
    if(update.getDeleteByIdMap() != null) {
      Set<Entry<String,Map<String,Object>>> entries = update.getDeleteByIdMap().entrySet();
      for (Entry<String,Map<String,Object>> e : entries) {
        delcmd.id = e.getKey();
        Map<String,Object> map = e.getValue();
        if (map != null) {
          Long version = (Long) map.get("ver");
          if (version != null) {
            delcmd.setVersion(version);
          }
        }
        processor.processDelete(delcmd);
        delcmd.clear();
      }
    }
    
    if(update.getDeleteQuery() != null) {
      for (String s : update.getDeleteQuery()) {
        delcmd.query = s;
        processor.processDelete(delcmd);
      }
    }
  }
}
