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
package org.apache.solr.handler.dataimport;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;

/**
 * <p> Writes documents to SOLR. </p>
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 1.3
 */
public class SolrWriter extends DIHWriterBase implements DIHWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String LAST_INDEX_KEY = "last_index_time";

  private final UpdateRequestProcessor processor;
  private final int commitWithin;
  
  SolrQueryRequest req;

  public SolrWriter(UpdateRequestProcessor processor, SolrQueryRequest req) {
    this.processor = processor;
    this.req = req;
    commitWithin = (req != null) ? req.getParams().getInt(UpdateParams.COMMIT_WITHIN, -1): -1;
  }
  
  @Override
  public void close() {
    try {
      processor.finish();
    } catch (IOException e) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
          "Unable to call finish() on UpdateRequestProcessor", e);
    } finally {
      deltaKeys = null;
      try {
        processor.close();
      } catch (IOException e) {
        SolrException.log(log, e);
      }
    }
  }
  @Override
  public boolean upload(SolrInputDocument d) {
    try {
      AddUpdateCommand command = new AddUpdateCommand(req);
      command.solrDoc = d;
      command.commitWithin = commitWithin;
      processor.processAdd(command);
    } catch (Exception e) {
      log.warn("Error creating document : {}", d, e);
      return false;
    }

    return true;
  }
  
  @Override
  public void deleteDoc(Object id) {
    try {
      log.info("Deleting document: {}", id);
      DeleteUpdateCommand delCmd = new DeleteUpdateCommand(req);
      delCmd.setId(id.toString());
      processor.processDelete(delCmd);
    } catch (IOException e) {
      log.error("Exception while deleteing: {}", id, e);
    }
  }

  @Override
  public void deleteByQuery(String query) {
    try {
      log.info("Deleting documents from Solr with query: {}", query);
      DeleteUpdateCommand delCmd = new DeleteUpdateCommand(req);
      delCmd.query = query;
      processor.processDelete(delCmd);
    } catch (IOException e) {
      log.error("Exception while deleting by query: {}", query, e);
    }
  }

  @Override
  public void commit(boolean optimize) {
    try {
      CommitUpdateCommand commit = new CommitUpdateCommand(req,optimize);
      processor.processCommit(commit);
    } catch (Exception e) {
      log.error("Exception while solr commit.", e);
    }
  }

  @Override
  public void rollback() {
    try {
      RollbackUpdateCommand rollback = new RollbackUpdateCommand(req);
      processor.processRollback(rollback);
    } catch (Exception e) {
      log.error("Exception during rollback command.", e);
    }
  }

  @Override
  public void doDeleteAll() {
    try {
      DeleteUpdateCommand deleteCommand = new DeleteUpdateCommand(req);
      deleteCommand.query = "*:*";
      processor.processDelete(deleteCommand);
    } catch (IOException e) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "Exception in full dump while deleting all documents.", e);
    }
  }

  static String getResourceAsString(InputStream in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    byte[] buf = new byte[1024];
    int sz = 0;
    try {
      while ((sz = in.read(buf)) != -1) {
        baos.write(buf, 0, sz);
      }
    } finally {
      try {
        in.close();
      } catch (Exception e) {

      }
    }
    return new String(baos.toByteArray(), StandardCharsets.UTF_8);
  }

  static String getDocCount() {
    if (DocBuilder.INSTANCE.get() != null) {
      return ""
              + (DocBuilder.INSTANCE.get().importStatistics.docCount.get() + 1);
    } else {
      return null;
    }
  }
  @Override
  public void init(Context context) {
    /* NO-OP */
  }
}
