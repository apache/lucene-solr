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

package org.apache.solr.handler;

import java.net.URL;

import org.apache.solr.core.SolrException;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.request.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.util.NamedList;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.SimpleOrderedMap;

/**
 *
 */
public abstract class RequestHandlerBase implements SolrRequestHandler, SolrInfoMBean {

  // statistics
  // TODO: should we bother synchronizing these, or is an off-by-one error
  // acceptable every million requests or so?
  long numRequests;
  long numErrors;
  protected SolrParams defaults;
  protected SolrParams appends;
  protected SolrParams invariants;

  /** shorten the class references for utilities */
  private static class U extends SolrPluginUtils {
    /* :NOOP */
  }

  public void init(NamedList args) {
    // Copied from StandardRequestHandler 
    if( args != null ) {
      Object o = args.get("defaults");
      if (o != null && o instanceof NamedList) {
        defaults = SolrParams.toSolrParams((NamedList)o);
      }
      o = args.get("appends");
      if (o != null && o instanceof NamedList) {
        appends = SolrParams.toSolrParams((NamedList)o);
      }
      o = args.get("invariants");
      if (o != null && o instanceof NamedList) {
        invariants = SolrParams.toSolrParams((NamedList)o);
      }
    }
  }
  
  public abstract void handleRequestBody( SolrQueryRequest req, SolrQueryResponse rsp ) throws Exception;

  public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
    numRequests++;

    try {
      U.setDefaults(req,defaults,appends,invariants);
      handleRequestBody( req, rsp );
    } 
    catch( SolrException se ) {
      numErrors++;
      throw se;
    }
    catch( Exception e) {
      numErrors++;
    }
  }
  

  //////////////////////// SolrInfoMBeans methods //////////////////////

  public String getName() {
    return this.getClass().getName();
  }

  public abstract String getDescription();
  public abstract String getSourceId();
  public abstract String getSource();
  public abstract String getVersion();
  
  public Category getCategory() {
    return Category.QUERYHANDLER;
  }

  public URL[] getDocs() {
    return null;  // this can be overridden, but not required
  }

  public NamedList getStatistics() {
    NamedList lst = new SimpleOrderedMap();
    lst.add("requests", numRequests);
    lst.add("errors", numErrors);
    return lst;
  }
}

