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

package org.apache.solr.request;

import org.apache.lucene.search.*;
import org.apache.lucene.document.Document;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.regex.Pattern;
import java.io.IOException;
import java.net.URL;

import org.apache.solr.util.StrUtils;
import org.apache.solr.util.NamedList;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.CommonParams;
import org.apache.solr.search.*;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.core.SolrException;

/**
 * @author yonik
 * @version $Id$
 */
public class StandardRequestHandler implements SolrRequestHandler, SolrInfoMBean {

  // statistics
  // TODO: should we bother synchronizing these, or is an off-by-one error
  // acceptable every million requests or so?
  long numRequests;
  long numErrors;

  /** shorten the class referneces for utilities */
  private static class U extends SolrPluginUtils {
    /* :NOOP */
  }
  /** parameters garnered from config file */
  protected final CommonParams params = new CommonParams();


  public void init(NamedList args) {
    params.setValues(args);
  }

  public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
    numRequests++;


    // TODO: test if lucene will accept an escaped ';', otherwise
    // we need to un-escape them before we pass to QueryParser
    try {
      String sreq = req.getQueryString();
      String debug = U.getParam(req, params.DEBUG_QUERY, params.debugQuery);
      String defaultField = U.getParam(req, params.DF, params.df);

      // find fieldnames to return (fieldlist)
      String fl = U.getParam(req, params.FL, params.fl);
      int flags = 0; 
      if (fl != null) {
        flags |= U.setReturnFields(fl, rsp);
      }

      if (sreq==null) throw new SolrException(400,"Missing queryString");
      List<String> commands = StrUtils.splitSmart(sreq,';');

      String qs = commands.size() >= 1 ? commands.get(0) : "";
      Query query = QueryParsing.parseQuery(qs, defaultField, req.getSchema());

      // If the first non-query, non-filter command is a simple sort on an indexed field, then
      // we can use the Lucene sort ability.
      Sort sort = null;
      if (commands.size() >= 2) {
        QueryParsing.SortSpec sortSpec = QueryParsing.parseSort(commands.get(1), req.getSchema());
        if (sortSpec != null) {
          sort = sortSpec.getSort();
          // ignore the count for now... it's currently only controlled by start & limit on req
          // count = sortSpec.getCount();
        }
      }

      DocList results = req.getSearcher().getDocList(query, null, sort, req.getStart(), req.getLimit(), flags);
      rsp.add(null,results);

      try {
        NamedList dbg = U.doStandardDebug(req, qs, query, results, params);
        if (null != dbg) 
          rsp.add("debug", dbg);
      } catch (Exception e) {
        SolrException.logOnce(SolrCore.log, "Exception durring debug", e);
        rsp.add("exception_during_debug", SolrException.toStr(e));
      }

      NamedList sumData = SolrPluginUtils.doStandardHighlighting(
        results, query, req, params, new String[]{defaultField});
      if(sumData != null)
        rsp.add("highlighting", sumData);

    } catch (SolrException e) {
      rsp.setException(e);
      numErrors++;
      return;
    } catch (Exception e) {
      SolrException.log(SolrCore.log,e);
      rsp.setException(e);
      numErrors++;
      return;
    }
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////


  public String getName() {
    return StandardRequestHandler.class.getName();
  }

  public String getVersion() {
    return SolrCore.version;
  }

  public String getDescription() {
    return "The standard Solr request handler";
  }

  public Category getCategory() {
    return Category.QUERYHANDLER;
  }

  public String getSourceId() {
    return "$Id$";
  }

  public String getSource() {
    return "$URL$";
  }

  public URL[] getDocs() {
    return null;
  }

  public NamedList getStatistics() {
    NamedList lst = new NamedList();
    lst.add("requests", numRequests);
    lst.add("errors", numErrors);
    return lst;
  }
}

