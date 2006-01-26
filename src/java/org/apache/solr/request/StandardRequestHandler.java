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
import org.apache.solr.search.*;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.core.SolrException;

/**
 * @author yonik
 * @version $Id: StandardRequestHandler.java,v 1.17 2005/12/02 04:31:06 yonik Exp $
 */
public class StandardRequestHandler implements SolrRequestHandler, SolrInfoMBean {

  // statistics
  // TODO: should we bother synchronizing these, or is an off-by-one error
  // acceptable every million requests or so?
  long numRequests;
  long numErrors;


  public void init(NamedList args) {
    SolrCore.log.log(Level.INFO, "Unused request handler arguments:" + args);
  }


  private final Pattern splitList=Pattern.compile(",| ");

  public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
    numRequests++;


    // TODO: test if lucene will accept an escaped ';', otherwise
    // we need to un-escape them before we pass to QueryParser
    try {
      String sreq = req.getQueryString();
      String debug = req.getParam("debugQuery");

      // find fieldnames to return (fieldlist)
      String fl = req.getParam("fl");
      int flags=0;
      if (fl != null) {
        // TODO - this could become more efficient if widely used.
        // TODO - should field order be maintained?
        String[] flst = splitList.split(fl,0);
        if (flst.length > 0 && !(flst.length==1 && flst[0].length()==0)) {
          Set<String> set = new HashSet<String>();
          for (String fname : flst) {
            if ("score".equals(fname)) flags |= SolrIndexSearcher.GET_SCORES;
            set.add(fname);
          }
          rsp.setReturnFields(set);
        }
      }

      if (sreq==null) throw new SolrException(400,"Missing queryString");
      List<String> commands = StrUtils.splitSmart(sreq,';');

      String qs = commands.size() >= 1 ? commands.get(0) : "";
      Query query = QueryParsing.parseQuery(qs, req.getSchema());

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

      if (debug!=null) {
        NamedList dbg = new NamedList();
        try {
          dbg.add("querystring",qs);
          dbg.add("parsedquery",QueryParsing.toString(query,req.getSchema()));
          dbg.add("explain", getExplainList(query, results, req.getSearcher(), req.getSchema()));
          String otherQueryS = req.getParam("explainOther");
          if (otherQueryS != null && otherQueryS.length() > 0) {
            DocList otherResults = doQuery(otherQueryS,req.getSearcher(), req.getSchema(),0,10);
            dbg.add("otherQuery",otherQueryS);
            dbg.add("explainOther", getExplainList(query, otherResults, req.getSearcher(), req.getSchema()));
          }
        } catch (Exception e) {
          SolrException.logOnce(SolrCore.log,"Exception during debug:",e);
          dbg.add("exception_during_debug", SolrException.toStr(e));
        }
        rsp.add("debug",dbg);
      }

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

  private NamedList getExplainList(Query query, DocList results, SolrIndexSearcher searcher, IndexSchema schema) throws IOException {
    NamedList explainList = new NamedList();
    DocIterator iterator = results.iterator();
    for (int i=0; i<results.size(); i++) {
      int id = iterator.nextDoc();

      Explanation explain = searcher.explain(query, id);
      //explainList.add(Integer.toString(id), explain.toString().split("\n"));

      Document doc = searcher.doc(id);
      String strid = schema.printableUniqueKey(doc);
      String docname = "";
      if (strid != null) docname="id="+strid+",";
      docname = docname + "internal_docid="+id;

      explainList.add(docname, "\n" +explain.toString());
    }
    return explainList;
  }


  private DocList doQuery(String sreq, SolrIndexSearcher searcher, IndexSchema schema, int start, int limit) throws IOException {
    List<String> commands = StrUtils.splitSmart(sreq,';');

    String qs = commands.size() >= 1 ? commands.get(0) : "";
    Query query = QueryParsing.parseQuery(qs, schema);

    // If the first non-query, non-filter command is a simple sort on an indexed field, then
    // we can use the Lucene sort ability.
    Sort sort = null;
    if (commands.size() >= 2) {
      QueryParsing.SortSpec sortSpec = QueryParsing.parseSort(commands.get(1), schema);
      if (sortSpec != null) {
        sort = sortSpec.getSort();
        if (sortSpec.getCount() >= 0) {
          limit = sortSpec.getCount();
        }
      }
    }

    DocList results = searcher.getDocList(query,(DocSet)null, sort, start, limit);
    return results;
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

  public String getCvsId() {
    return "$Id: StandardRequestHandler.java,v 1.17 2005/12/02 04:31:06 yonik Exp $";
  }

  public String getCvsName() {
    return "$Name:  $";
  }

  public String getCvsSource() {
    return "$Source: /cvs/main/searching/solr/solarcore/src/solr/StandardRequestHandler.java,v $";
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

