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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.VersionInfo;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

/**
 * An update processor that will convert conventional field-value document to atomic update document
 * <p>
 * sample request:
 * curl -X POST -H Content-Type: application/json
 * http://localhost:8983/solr/test/update/json/docs?processor=atomic;ampersand;atomic.my_newfield=add;ampersand;atomic.subject=set;ampersand;atomic.count_i=inc;ampersand;commit=true
 * --data-binary {"id": 1,"title": "titleA"}
 * </p>
 * currently supports all types of atomic updates
 * @since 6.6.0
 */

public class AtomicUpdateProcessorFactory extends UpdateRequestProcessorFactory implements SolrCoreAware {

  private final static String ADD = "add";
  private final static String INC = "inc";
  private final static String REMOVE = "remove";
  private final static String SET = "set";
  private final static String REMOVEREGEX = "removeregex";
  private final static String ADDDISTINCT = "add-distinct";
  private final static Set<String> VALID_OPS = new HashSet<>(Arrays.asList(ADD, INC, REMOVE, SET, REMOVEREGEX, ADDDISTINCT));

  private final static String VERSION = "_version_";
  public static final String NAME = "atomic";
  public final static String ATOMIC_FIELD_PREFIX = "atomic.";
  private final static int MAX_ATTEMPTS = 25;

  private VersionInfo vinfo;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  @SuppressWarnings({"static-access", "rawtypes", "null"})
  @Override
  public void init(final NamedList args) {

  }

  @Override
  public void inform(SolrCore core) {
    this.vinfo = core.getUpdateHandler().getUpdateLog() == null ? null : core.getUpdateHandler().getUpdateLog().getVersionInfo();

  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp,
                                            UpdateRequestProcessor next) {
    if (vinfo == null) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "Atomic document updates are not supported unless <updateLog/> is configured");
    }
    return new AtomicUpdateProcessor(req, next);
  }

  private class AtomicUpdateProcessor extends UpdateRequestProcessor {

    @SuppressWarnings("unused")
    private final SolrQueryRequest req;
    private final UpdateRequestProcessor next;

    private AtomicUpdateProcessor(SolrQueryRequest req,
                                  UpdateRequestProcessor next) {
      super(next);
      this.next = next;
      this.req = req;
    }

    /*
     * 1. convert incoming update document to atomic-type update document 
     * for specified fields in processor definition.
     * 2. if incoming update document contains already atomic-type updates, skip
     * 3. fields not specified in processor param(s) in solrconfig.xml for atomic action
     * will be treated as conventional updates.
     * 4. retry when encounter version conflict
     */
    @SuppressWarnings("unchecked")
    @Override
    public void processAdd(AddUpdateCommand cmd)
        throws IOException {

      SolrInputDocument orgdoc = cmd.getSolrInputDocument();
      boolean isAtomicUpdateAddedByMe = false;

      Iterator<String> paramsIterator = req.getParams().getParameterNamesIterator();

      while (paramsIterator.hasNext()) {

        String param = paramsIterator.next();

        if (!param.startsWith(ATOMIC_FIELD_PREFIX)) continue;

        String field = param.substring(ATOMIC_FIELD_PREFIX.length(), param.length());
        String operation = req.getParams().get(param);

        if (!VALID_OPS.contains(operation)) {
          throw new SolrException(SERVER_ERROR,
              "Unexpected param(s) for AtomicUpdateProcessor, invalid atomic op passed: '" +
                  req.getParams().get(param) + "'");
        }
        if (orgdoc.get(field) == null || orgdoc.get(field).getValue() instanceof Map) {
          // no value for the field or it's already an atomic update operation
          //continue processing other fields
          continue;
        }

        orgdoc.setField(field, singletonMap(operation, orgdoc.get(field).getValue()));
        isAtomicUpdateAddedByMe = true;
      }

      // if atomic, put _version_ for optimistic concurrency if doc present in index
      if (isAtomicUpdateAddedByMe) {
        Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
        // if lastVersion is null then we put -1 to assert that document must not exist
        lastVersion = lastVersion == null ? -1 : lastVersion;
        orgdoc.setField(VERSION, lastVersion);
        processAddWithRetry(cmd, 1, cmd.getSolrInputDocument().deepCopy());
      } else {
        super.processAdd(cmd);
      }
      // else send it for doc to get inserted for the first time
    }

    private void processAddWithRetry(AddUpdateCommand cmd, int attempts, SolrInputDocument clonedOriginalDoc) throws IOException {
      try {
        super.processAdd(cmd);
      } catch (SolrException e) {
        if (attempts++ >= MAX_ATTEMPTS) {//maximum number of attempts allowed: 25
          throw new SolrException(SERVER_ERROR,
              "Atomic update failed after multiple attempts due to " + e.getMessage());
        }
        if (e.code() == ErrorCode.CONFLICT.code) { // version conflict
          log.warn("Atomic update failed. Retrying with new version .... ({})", attempts, e);

          Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
          // if lastVersion is null then we put -1 to assert that document must not exist
          lastVersion = lastVersion == null ? -1 : lastVersion;

          // The AtomicUpdateDocumentMerger modifies the AddUpdateCommand.solrDoc to populate the real values of the
          // modified fields. We don't want those absolute values because they are out-of-date due to the conflict
          // so we restore the original document created in processAdd method and set the right version on it
          cmd.solrDoc = clonedOriginalDoc;
          clonedOriginalDoc = clonedOriginalDoc.deepCopy(); // copy again because the old cloned ref will be modified during processAdd
          cmd.solrDoc.setField(VERSION, lastVersion);

          processAddWithRetry(cmd, attempts, clonedOriginalDoc);
        }
      }
    }
  }
}
