/**
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

package org.apache.solr.core;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocIterator;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.List;

/**
 * @version $Id$
 */
public class QuerySenderListener extends AbstractSolrEventListener {
  public QuerySenderListener(SolrCore core) {
    super(core);
  }

  @Override
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    final SolrIndexSearcher searcher = newSearcher;
    log.info("QuerySenderListener sending requests to " + newSearcher);
    for (NamedList nlst : (List<NamedList>)args.get("queries")) {
      SolrQueryRequest req = null;

      try {
        // bind the request to a particular searcher (the newSearcher)
        NamedList params = addEventParms(currentSearcher, nlst);
        req = new LocalSolrQueryRequest(core,params) {
          @Override public SolrIndexSearcher getSearcher() { return searcher; }
          @Override public void close() { }
        };

        SolrQueryResponse rsp = new SolrQueryResponse();
        SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
        core.execute(core.getRequestHandler(req.getParams().get(CommonParams.QT)), req, rsp);

        // Retrieve the Document instances (not just the ids) to warm
        // the OS disk cache, and any Solr document cache.  Only the top
        // level values in the NamedList are checked for DocLists.
        NamedList values = rsp.getValues();
        for (int i=0; i<values.size(); i++) {
          Object o = values.getVal(i);
          if (o instanceof DocList) {
            DocList docs = (DocList)o;
            for (DocIterator iter = docs.iterator(); iter.hasNext();) {
              newSearcher.doc(iter.nextDoc());
            }
          }
        }

      } catch (Exception e) {
        // do nothing... we want to continue with the other requests.
        // the failure should have already been logged.
      } finally {
        if (req != null) req.close();
        SolrRequestInfo.clearRequestInfo();
      }
    }
    log.info("QuerySenderListener done.");
  }


}
