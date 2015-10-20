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

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.update.CdcrUpdateLog;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.processor.CdcrUpdateProcessor;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * The replication logic. Given a {@link org.apache.solr.handler.CdcrReplicatorState}, it reads all the new entries
 * in the update log and forward them to the target cluster. If an error occurs, the replication is stopped and
 * will be tried again later.
 */
public class CdcrReplicator implements Runnable {

  private final CdcrReplicatorState state;
  private final int batchSize;

  protected static Logger log = LoggerFactory.getLogger(CdcrReplicator.class);

  public CdcrReplicator(CdcrReplicatorState state, int batchSize) {
    this.state = state;
    this.batchSize = batchSize;
  }

  @Override
  public void run() {
    CdcrUpdateLog.CdcrLogReader logReader = state.getLogReader();
    CdcrUpdateLog.CdcrLogReader subReader = null;
    if (logReader == null) {
      log.warn("Log reader for target {} is not initialised, it will be ignored.", state.getTargetCollection());
      return;
    }

    try {
      // create update request
      UpdateRequest req = new UpdateRequest();
      // Add the param to indicate the {@link CdcrUpdateProcessor} to keep the provided version number
      req.setParam(CdcrUpdateProcessor.CDCR_UPDATE, "");

      // Start the benchmark timer
      state.getBenchmarkTimer().start();

      long counter = 0;
      subReader = logReader.getSubReader();

      for (int i = 0; i < batchSize; i++) {
        Object o = subReader.next();
        if (o == null) break; // we have reached the end of the update logs, we should close the batch

        if (isDelete(o)) {

          /*
          * Deletes are sent one at a time.
          */

          // First send out current batch of SolrInputDocument, the non-deletes.
          List<SolrInputDocument> docs = req.getDocuments();

          if (docs != null && docs.size() > 0) {
            subReader.resetToLastPosition(); // Push back the delete for now.
            this.sendRequest(req); // Send the batch update request
            logReader.forwardSeek(subReader); // Advance the main reader to just before the delete.
            o = subReader.next(); // Read the delete again
            counter += docs.size();
            req.clear();
          }

          // Process Delete
          this.processUpdate(o, req);
          this.sendRequest(req);
          logReader.forwardSeek(subReader);
          counter++;
          req.clear();

        } else {

          this.processUpdate(o, req);

        }
      }

      //Send the final batch out.
      List<SolrInputDocument> docs = req.getDocuments();

      if ((docs != null && docs.size() > 0)) {
        this.sendRequest(req);
        counter += docs.size();
      }

      // we might have read a single commit operation and reached the end of the update logs
      logReader.forwardSeek(subReader);

      log.debug("Forwarded {} updates to target {}", counter, state.getTargetCollection());
    } catch (Exception e) {
      // report error and update error stats
      this.handleException(e);
    } finally {
      // stop the benchmark timer
      state.getBenchmarkTimer().stop();
      // ensure that the subreader is closed and the associated pointer is removed
      if (subReader != null) subReader.close();
    }
  }

  private void sendRequest(UpdateRequest req) throws IOException, SolrServerException, CdcrReplicatorException {
    UpdateResponse rsp = req.process(state.getClient());
    if (rsp.getStatus() != 0) {
      throw new CdcrReplicatorException(req, rsp);
    }
    state.resetConsecutiveErrors();
  }

  private boolean isDelete(Object o) {
    List entry = (List) o;
    int operationAndFlags = (Integer) entry.get(0);
    int oper = operationAndFlags & UpdateLog.OPERATION_MASK;
    return oper == UpdateLog.DELETE_BY_QUERY || oper == UpdateLog.DELETE;
  }

  private void handleException(Exception e) {
    if (e instanceof CdcrReplicatorException) {
      UpdateRequest req = ((CdcrReplicatorException) e).req;
      UpdateResponse rsp = ((CdcrReplicatorException) e).rsp;
      log.warn("Failed to forward update request {}. Got response {}", req, rsp);
      state.reportError(CdcrReplicatorState.ErrorType.BAD_REQUEST);
    } else if (e instanceof CloudSolrClient.RouteException) {
      log.warn("Failed to forward update request", e);
      state.reportError(CdcrReplicatorState.ErrorType.BAD_REQUEST);
    } else {
      log.warn("Failed to forward update request", e);
      state.reportError(CdcrReplicatorState.ErrorType.INTERNAL);
    }
  }

  private UpdateRequest processUpdate(Object o, UpdateRequest req) {

    // should currently be a List<Oper,Ver,Doc/Id>
    List entry = (List) o;

    int operationAndFlags = (Integer) entry.get(0);
    int oper = operationAndFlags & UpdateLog.OPERATION_MASK;
    long version = (Long) entry.get(1);

    // record the operation in the benchmark timer
    state.getBenchmarkTimer().incrementCounter(oper);

    switch (oper) {
      case UpdateLog.ADD: {
        // the version is already attached to the document
        SolrInputDocument sdoc = (SolrInputDocument) entry.get(entry.size() - 1);
        req.add(sdoc);
        return req;
      }
      case UpdateLog.DELETE: {
        byte[] idBytes = (byte[]) entry.get(2);
        req.deleteById(new String(idBytes, Charset.forName("UTF-8")));
        req.setParam(DistributedUpdateProcessor.VERSION_FIELD, Long.toString(version));
        return req;
      }

      case UpdateLog.DELETE_BY_QUERY: {
        String query = (String) entry.get(2);
        req.deleteByQuery(query);
        req.setParam(DistributedUpdateProcessor.VERSION_FIELD, Long.toString(version));
        return req;
      }

      case UpdateLog.COMMIT: {
        return null;
      }

      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown Operation! " + oper);
    }
  }

  /**
   * Exception to catch update request issues with the target cluster.
   */
  public class CdcrReplicatorException extends Exception {

    private final UpdateRequest req;
    private final UpdateResponse rsp;

    public CdcrReplicatorException(UpdateRequest req, UpdateResponse rsp) {
      this.req = req;
      this.rsp = rsp;
    }

  }

}

