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

package org.apache.solr.servlet;

import org.apache.solr.core.*;
import org.apache.solr.request.*;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.StrUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.util.logging.Logger;
import java.util.Map;
import java.util.Set;

/**
 * @author yonik
 */

public class SolrServlet extends HttpServlet {
  final Logger log = Logger.getLogger(SolrServlet.class.getName());
  SolrCore core;
  private static String CONTENT_TYPE="text/xml;charset=UTF-8";

  XMLResponseWriter xmlResponseWriter;

  public void init() throws ServletException
  {
    log.info("user.dir=" + System.getProperty("user.dir"));
    core = SolrCore.getSolrCore();
    xmlResponseWriter=new XMLResponseWriter();
    log.info("SolrServlet.init() done");
  }

  public void destroy() {
    core.close();
    super.destroy();
  }

  public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    doGet(request,response);
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    SolrServletRequest solrReq =null;
    SolrQueryResponse solrRsp =null;
    try {
      solrRsp = new SolrQueryResponse();
      solrReq = new SolrServletRequest(core, request);
      // log.severe("REQUEST PARAMS:" + solrReq.getParamString());
      core.execute(solrReq, solrRsp);
      if (solrRsp.getException() == null) {
        response.setContentType(CONTENT_TYPE);
        PrintWriter writer = response.getWriter();
        // if (solrReq.getStrParam("version","2").charAt(0) == '1')
        xmlResponseWriter.write(writer, solrReq, solrRsp);
      } else {
        Exception e = solrRsp.getException();
        int rc=500;
        if (e instanceof SolrException) {
          rc=((SolrException)e).code();
        }
        sendErr(rc, SolrException.toStr(e), request, response);
      }
    } catch (SolrException e) {
      if (!e.logged) SolrException.log(log,e);
      sendErr(e.code(), SolrException.toStr(e), request, response);
    } catch (Throwable e) {
      SolrException.log(log,e);
      sendErr(500, SolrException.toStr(e), request, response);
    } finally {
      // This releases the IndexReader associated with the request
      solrReq.close();
    }

  }

  final void sendErr(int rc, String msg, HttpServletRequest request, HttpServletResponse response) {
    try {
      // hmmm, what if this was already set to text/xml?
      try{
        response.setContentType(CONTENT_TYPE);
        // response.setCharacterEncoding("UTF-8");
      } catch (Exception e) {}
      try{response.setStatus(rc);} catch (Exception e) {}
      PrintWriter writer = response.getWriter();
      writer.write(msg);
    } catch (IOException e) {
      SolrException.log(log,e);
    }
  }

  final int getParam(HttpServletRequest request, String param, int defval) {
    final String pval = request.getParameter(param);
    return (pval==null) ? defval : Integer.parseInt(pval);
  }

  final boolean paramExists(HttpServletRequest request, String param) {
    return request.getParameter(param)!=null ? true : false;
  }

}