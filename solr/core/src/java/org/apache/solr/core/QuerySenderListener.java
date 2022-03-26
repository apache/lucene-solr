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
package org.apache.solr.core;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.DISTRIB;

/**
 *
 */
public class QuerySenderListener extends AbstractSolrEventListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public QuerySenderListener(SolrCore core) {
    super(core);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    final SolrIndexSearcher searcher = newSearcher;
    log.debug("QuerySenderListener sending requests to {}", newSearcher);

    Object queries = getArgs().getAll("queries");
    ArrayList<NamedList> allLists = queries2list(queries);

    if (allLists == null) return;
    for (NamedList nlst : allLists) {
      try {
        // bind the request to a particular searcher (the newSearcher)
        NamedList params = addEventParms(currentSearcher, nlst);
        // for this, we default to distrib = false
        if (params.get(DISTRIB) == null) {
          params.add(DISTRIB, false);
        }
        SolrQueryRequest req = new LocalSolrQueryRequest(getCore(),params) {
          @Override public SolrIndexSearcher getSearcher() { return searcher; }
          @Override public void close() { }
        };
        SolrQueryResponse rsp = new SolrQueryResponse();
        SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
        try {
          getCore().execute(getCore().getRequestHandler(req.getParams().get(CommonParams.QT)), req, rsp);

          // Retrieve the Document instances (not just the ids) to warm
          // the OS disk cache, and any Solr document cache.  Only the top
          // level values in the NamedList are checked for DocLists.
          NamedList values = rsp.getValues();
          for (int i=0; i<values.size(); i++) {
            Object o = values.getVal(i);
            if (o instanceof ResultContext) {
              o = ((ResultContext)o).getDocList();
            }
            if (o instanceof DocList) {
              DocList docs = (DocList)o;
              for (DocIterator iter = docs.iterator(); iter.hasNext();) {
                newSearcher.doc(iter.nextDoc());
              }
            }
          }
        } finally {
          try {
            req.close();
          } finally {
            SolrRequestInfo.clearRequestInfo();
          }
        }
      } catch (Exception e) {
        // do nothing... we want to continue with the other requests.
        // the failure should have already been logged.
      }
    }
    log.info("QuerySenderListener done.");
  }

  private ArrayList<NamedList> queries2list(Object queries) {

    String queries_t = queries.getClass().getSimpleName();
    // XML: ArrayList
    // JSON []: ArrayList
    log.debug("queries: " + queries_t + ": " + queries.toString());

    ArrayList<NamedList> allLists = new ArrayList<NamedList>();

    switch (queries_t) {
      case "ArrayList":
        {
          @SuppressWarnings("unchecked")
          ArrayList<Object> queriesLists = (ArrayList<Object>) queries;

          // XML: ArrayList
          // JSON []: ArrayList
          log.debug(
              "queriesLists: "
                  + queriesLists.getClass().getSimpleName()
                  + ":"
                  + queriesLists.toString());

          queriesLists.forEach(
              (o) -> {
                String o_t = o.getClass().getSimpleName();

                // XML: ArrayList
                // JSON []: NamedList
                log.debug("o: " + o_t + ": " + o.toString());

                switch (o_t) {
                  case "NamedList":
                    {

                      // JSON []
                      @SuppressWarnings("unchecked")
                      NamedList<Object> warmingQuery = (NamedList<Object>) o;

                      allLists.add(warmingQuery);

                      break;
                    }

                  case "ArrayList":
                    {
                      for (Object wqList : (ArrayList) o) {

                        String wqList_t = wqList.getClass().getSimpleName();

                        // XML: NamedList
                        log.debug(
                            "wqList: "
                                + wqList_t
                                + ": "
                                + wqList.toString());

                        if (wqList instanceof NamedList) {
                          @SuppressWarnings("unchecked")
                          NamedList<Object> query = (NamedList<Object>) wqList;
                          log.debug("query: " + query.toString());
                
                          allLists.add(query);
                        } else {
                          log.warn("unexpected wqList: " + wqList_t + ": " + wqList.toString());
                        }

                      //   if (wqList instanceof LinkedHashMap) {

                      //     @SuppressWarnings("unchecked")
                      //     LinkedHashMap<String, Object> warmingQueries =
                      //         (LinkedHashMap<String, Object>) wqList;

                      //     log.debug(
                      //         "warmingQueries: "
                      //             + warmingQueries.getClass().getSimpleName()
                      //             + ": "
                      //             + warmingQueries.toString());

                      //     NamedList<Object> warmingQuery = new NamedList<Object>();
                      //     warmingQueries.forEach((key, value) -> warmingQuery.add(key, value));

                          
                      //     log.debug(
                      //         "warmingQuery: "
                      //             + warmingQuery.getClass().getSimpleName()
                      //             + ": "
                      //             + warmingQuery.toString());

                      //     allLists.add(warmingQuery);
                      //   }
                      }

                      break;
                    }

                  default:
                    {
                      log.warn("unexpected o: " + o_t + ": " + o.toString());
                    }
                }
              });

          break;
        }

      // case "NamedList":
      //   {

      //     // this is used if JSON doesn't have nested array - but it only receives first entry in
      //     // JSON list so not useful
      //     log.warn(
      //         "Please send a nested array of queries - otherwise only the first will be used.");
      //     log.debug("queries: " + queries.toString());

      //     @SuppressWarnings("unchecked")
      //     NamedList<Object> query = (NamedList<Object>) queries;
      //     log.debug("query: " + query.toString());

      //     allLists.add(query);

      //     break;
      //   }

      default:
        {
          log.warn("Unsupported queries object - " + queries_t + ": " + queries.toString());
        }
    }

    return allLists;
  }

}
