package org.apache.solr.servlet;/**
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

import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;
import org.apache.solr.request.XMLResponseWriter;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.QueryResponseWriter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;
import java.util.logging.Logger;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.PrintWriter;

/**
 * @author yonik
 * @version $Id$
 */
public class SolrUpdateServlet extends HttpServlet {
  final Logger log = Logger.getLogger(SolrUpdateServlet.class.getName());
  private SolrCore core;

  XMLResponseWriter xmlResponseWriter;

  public void init() throws ServletException
  {
    core = SolrCore.getSolrCore();
    log.info("SolrUpdateServlet.init() done");
  }

  public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    BufferedReader requestReader = request.getReader();
    response.setContentType(QueryResponseWriter.CONTENT_TYPE_XML_UTF8);
    PrintWriter responseWriter = response.getWriter();
    core.update(requestReader, responseWriter);
  }
}
