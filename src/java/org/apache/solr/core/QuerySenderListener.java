/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.core;

import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.util.NamedList;

import java.util.List;

/**
 * @author yonik
 * @version $Id$
 */
class QuerySenderListener extends AbstractSolrEventListener {

  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    SolrCore core = SolrCore.getSolrCore();
    log.info("QuerySenderListener sending requests to " + newSearcher);
    for (NamedList nlst : (List<NamedList>)args.get("queries")) {
      try {
        LocalSolrQueryRequest req = new LocalSolrQueryRequest(core, nlst);

        SolrQueryResponse rsp = new SolrQueryResponse();
        core.execute(req,rsp);
      } catch (Exception e) {
        // do nothing... we want to continue with the other requests.
        // the failure should have already been logged.
      }
      log.info("QuerySenderListener done.");
    }
  }



}
