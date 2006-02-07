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
  public static Logger log = Logger.getLogger(SolrServlet.class.getName());
  public SolrCore core;
  private static String CONTENT_TYPE="text/xml;charset=UTF-8";


  XMLResponseWriter xmlResponseWriter;

  public void init() throws ServletException
  {
    String configDir=getServletContext().getInitParameter("solr.configDir");
    String dataDir=getServletContext().getInitParameter("solr.dataDir");

    log.info("user.dir=" + System.getProperty("user.dir"));

    // TODO: find a way to allow configuration of the config and data
    // directories other than using CWD.  If it is done via servlet
    // params, then we must insure that this init() run before any
    // of the JSPs.
    core = SolrCore.getSolrCore();

    xmlResponseWriter=new XMLResponseWriter();

    getServletContext().setAttribute("SolrServlet",this);

    log.info("SolrServlet.init() done");
  }

  public void destroy() {
    core.close();
    getServletContext().removeAttribute("SolrServlet");
    super.destroy();
  }

  public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    // log.finer("Solr doPost()");
    // InputStream is = request.getInputStream();
    BufferedReader requestReader = request.getReader();

    response.setContentType(CONTENT_TYPE);
    PrintWriter responseWriter = response.getWriter();

    core.update(requestReader, responseWriter);
  }



  public  void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    // log.finer("Solr doGet: getQueryString:" + request.getQueryString());

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



class SolrServletRequest extends SolrQueryRequestBase {

  final HttpServletRequest req;

  public SolrServletRequest(SolrCore core, HttpServletRequest req) {
    super(core);
    this.req = req;
  }

  public String getParam(String name) {
    return req.getParameter(name);
  }


  public String getParamString() {
    StringBuilder sb = new StringBuilder(128);
    try {
      boolean first=true;

      for (Map.Entry<String,String[]> entry : (Set<Map.Entry<String,String[]>>)req.getParameterMap().entrySet()) {
        String key = entry.getKey();
        String[] valarr = entry.getValue();

        for (String val : valarr) {
          if (!first) sb.append('&');
          first=false;
          sb.append(key);
          sb.append('=');
          StrUtils.partialURLEncodeVal(sb, val);
        }
      }
    }
    catch (Exception e) {
      // should never happen... we only needed this because
      // partialURLEncodeVal can throw an IOException, but it
      // never will when adding to a StringBuilder.
      throw new RuntimeException(e);
    }

    return sb.toString();
  }

}