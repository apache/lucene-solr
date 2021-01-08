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
package org.apache.solr.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

/**
 * <p>
 *     This Factory generates an UpdateProcessor that will (by default) skip inserting new documents
 *     if there already exists a document with the same uniqueKey value in the index. It will also
 *     skip Atomic Updates to a document if that document does not already exist. This behaviour is applied
 *     to each document in turn, so adding a batch of documents can result in some being added and some
 *     ignored, depending on what is already in the index. If all of the documents are skipped, no changes
 *     to the index will occur.
 * </p>
 * These two forms of skipping can be switched on or off independently, by using init params:
 * <ul>
 *     <li><code>skipInsertIfExists</code> - This boolean parameter defaults to
 *          <code>true</code>, but if set to <code>false</code> then inserts (i.e. not Atomic Updates)
 *          will be passed through unchanged even if the document already exists.</li>
 *     <li><code>skipUpdateIfMissing</code> - This boolean parameter defaults to
 *         <code>true</code>, but if set to <code>false</code> then Atomic Updates
 *          will be passed through unchanged regardless of whether the document exists.</li>
 * </ul>
 * <p>
 *     These params can also be specified per-request, to override the configured behaviour
 *     for specific updates e.g. <code>/update?skipUpdateIfMissing=true</code>
 * </p>
 * <p>
 *     This implementation is a simpler alternative to {@link DocBasedVersionConstraintsProcessorFactory}
 *     when you are not concerned with versioning, and just want to quietly ignore duplicate documents and/or
 *     silently skip updates to non-existent documents (in the same way a database <code>UPDATE</code> would).
 *
 *     If your documents do have an explicit version field, and you want to ensure older versions are
 *     skipped instead of replacing the indexed document, you should consider {@link DocBasedVersionConstraintsProcessorFactory}
 *     instead.
 * </p>
 * <p>
 *     An example chain configuration to use this for skipping duplicate inserts, but not skipping updates to
 *     missing documents by default, is:
 * </p>
 * <pre class="prettyprint">
 * &lt;updateRequestProcessorChain name="skipexisting"&gt;
 *   &lt;processor class="solr.LogUpdateProcessorFactory" /&gt;
 *   &lt;processor class="solr.SkipExistingDocumentsProcessorFactory"&gt;
 *     &lt;bool name="skipInsertIfExists"&gt;true&lt;/bool&gt;
 *     &lt;bool name="skipUpdateIfMissing"&gt;false&lt;/bool&gt; &lt;!-- Can override this per-request --&gt;
 *   &lt;/processor&gt;
 *   &lt;processor class="solr.DistributedUpdateProcessorFactory" /&gt;
 *   &lt;processor class="solr.RunUpdateProcessorFactory" /&gt;
 * &lt;/updateRequestProcessorChain&gt;
 * </pre>
 * @since 6.4.0
 */
public class SkipExistingDocumentsProcessorFactory extends UpdateRequestProcessorFactory implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String PARAM_SKIP_INSERT_IF_EXISTS = "skipInsertIfExists";
  private static final String PARAM_SKIP_UPDATE_IF_MISSING = "skipUpdateIfMissing";

  private boolean skipInsertIfExists = true;
  private boolean skipUpdateIfMissing = true;

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args)  {
    Object tmp = args.remove(PARAM_SKIP_INSERT_IF_EXISTS);
    if (null != tmp) {
      if (! (tmp instanceof Boolean) ) {
        throw new SolrException(SERVER_ERROR, "'" + PARAM_SKIP_INSERT_IF_EXISTS + "' must be configured as a <bool>");
      }
      skipInsertIfExists = (Boolean)tmp;
    }
    tmp = args.remove(PARAM_SKIP_UPDATE_IF_MISSING);
    if (null != tmp) {
      if (! (tmp instanceof Boolean) ) {
        throw new SolrException(SERVER_ERROR, "'" + PARAM_SKIP_UPDATE_IF_MISSING + "' must be configured as a <bool>");
      }
      skipUpdateIfMissing = (Boolean)tmp;
    }

    super.init(args);
  }

  @Override
  public SkipExistingDocumentsUpdateProcessor getInstance(SolrQueryRequest req,
                                                          SolrQueryResponse rsp,
                                                          UpdateRequestProcessor next) {
    // Ensure the parameters are forwarded to the leader
    DistributedUpdateProcessorFactory.addParamToDistributedRequestWhitelist(req, PARAM_SKIP_INSERT_IF_EXISTS, PARAM_SKIP_UPDATE_IF_MISSING);

    // Allow the particular request to override the plugin's configured behaviour
    boolean skipInsertForRequest = req.getOriginalParams().getBool(PARAM_SKIP_INSERT_IF_EXISTS, this.skipInsertIfExists);
    boolean skipUpdateForRequest = req.getOriginalParams().getBool(PARAM_SKIP_UPDATE_IF_MISSING, this.skipUpdateIfMissing);

    return new SkipExistingDocumentsUpdateProcessor(req, next, skipInsertForRequest, skipUpdateForRequest);
  }

  @Override
  public void inform(SolrCore core) {

    if (core.getUpdateHandler().getUpdateLog() == null) {
      throw new SolrException(SERVER_ERROR, "updateLog must be enabled.");
    }

    if (core.getLatestSchema().getUniqueKeyField() == null) {
      throw new SolrException(SERVER_ERROR, "schema must have uniqueKey defined.");
    }
  }

  static class SkipExistingDocumentsUpdateProcessor extends UpdateRequestProcessor {

    private final boolean skipInsertIfExists;
    private final boolean skipUpdateIfMissing;
    private final SolrCore core;

    private DistributedUpdateProcessor distribProc;  // the distributed update processor following us
    private DistributedUpdateProcessor.DistribPhase phase;

    SkipExistingDocumentsUpdateProcessor(SolrQueryRequest req,
                                         UpdateRequestProcessor next,
                                         boolean skipInsertIfExists,
                                         boolean skipUpdateIfMissing) {
      super(next);
      this.skipInsertIfExists = skipInsertIfExists;
      this.skipUpdateIfMissing = skipUpdateIfMissing;
      this.core = req.getCore();

      for (UpdateRequestProcessor proc = next ;proc != null; proc = proc.next) {
        if (proc instanceof DistributedUpdateProcessor) {
          distribProc = (DistributedUpdateProcessor)proc;
          break;
        }
      }

      if (distribProc == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "DistributedUpdateProcessor must follow SkipExistingDocumentsUpdateProcessor");
      }

      phase = DistributedUpdateProcessor.DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));
    }

    boolean isSkipInsertIfExists() {
      return this.skipInsertIfExists;
    }

    boolean isSkipUpdateIfMissing() {
      return this.skipUpdateIfMissing;
    }

    boolean doesDocumentExist(BytesRef indexedDocId) throws IOException {
      assert null != indexedDocId;

      // we don't need any fields populated, we just need to know if the doc is in the tlog...
      SolrInputDocument oldDoc =
          RealTimeGetComponent.getInputDocumentFromTlog(
              core,
              indexedDocId,
              null,
              Collections.emptySet(),
              RealTimeGetComponent.Resolution.PARTIAL);
      if (oldDoc == RealTimeGetComponent.DELETED) {
        return false;
      }
      if (oldDoc != null) {
        return true;
      }

      // need to look up in index now...
      RefCounted<SolrIndexSearcher> newestSearcher = core.getRealtimeSearcher();
      try {
        SolrIndexSearcher searcher = newestSearcher.get();
        return searcher.lookupId(indexedDocId) >= 0L;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error reading document from index", e);
      } finally {
        if (newestSearcher != null) {
          newestSearcher.decref();
        }
      }
    }

    boolean isLeader(UpdateCommand cmd) {
      if ((cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0) {
        return false;
      }
      if (phase == DistributedUpdateProcessor.DistribPhase.FROMLEADER) {
        return false;
      }
      distribProc.setupRequest(cmd);
      return distribProc.isLeader();
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      BytesRef indexedDocId = cmd.getIndexedId();

      boolean isUpdate = AtomicUpdateDocumentMerger.isAtomicUpdate(cmd);

      // boolean existsByLookup = (RealTimeGetComponent.getInputDocument(core, indexedDocId) != null);
      // if (docExists != existsByLookup) {
      //   log.error("Found docExists {} but existsByLookup {} for doc {}", docExists, existsByLookup, indexedDocId.utf8ToString());
      // }

      if (log.isDebugEnabled()) {
        log.debug("Document ID {} ... exists already? {} ... isAtomicUpdate? {} ... isLeader? {}",
                  indexedDocId.utf8ToString(), doesDocumentExist(indexedDocId), isUpdate, isLeader(cmd));
      }

      if (skipInsertIfExists && !isUpdate && isLeader(cmd) && doesDocumentExist(indexedDocId)) {
        if (log.isDebugEnabled()) {
          log.debug("Skipping insert for pre-existing document ID {}", indexedDocId.utf8ToString());
        }
        return;
      }

      if (skipUpdateIfMissing && isUpdate && isLeader(cmd) && !doesDocumentExist(indexedDocId)) {
        if (log.isDebugEnabled()) {
          log.debug("Skipping update to non-existent document ID {}", indexedDocId.utf8ToString());
        }
        return;
      }

      if (log.isDebugEnabled()) {
        log.debug("Passing on document ID {}", indexedDocId.utf8ToString());
      }

      super.processAdd(cmd);
    }
  }
}
