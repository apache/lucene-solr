package org.apache.solr.servlet;/**
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.handler.XmlUpdateRequestHandler;
import org.apache.solr.request.QueryResponseWriter;
import org.apache.solr.request.XMLResponseWriter;

/**
 * @version $Id$
 *
 * @deprecated Register a request handler to /update rather then use this servlet.  Add: &lt;requestHandler name="/update" class="solr.XmlUpdateRequestHandler" > to your solrconfig.xml
 */
@Deprecated
public class SolrUpdateServlet extends HttpServlet {
  final Logger log = Logger.getLogger(SolrUpdateServlet.class.getName());

  XmlUpdateRequestHandler legacyUpdateHandler;
  XMLResponseWriter xmlResponseWriter;

  @Override
  public void init() throws ServletException
  {
    legacyUpdateHandler = new XmlUpdateRequestHandler();
    legacyUpdateHandler.init( null );
    
    log.info("SolrUpdateServlet.init() done");
  }

  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    BufferedReader requestReader = request.getReader();
    response.setContentType(QueryResponseWriter.CONTENT_TYPE_XML_UTF8);

    if( request.getQueryString() != null ) {
      log.warning( 
          "The @Deprecated SolrUpdateServlet does not accept query parameters: "+request.getQueryString()+"\n"
          +"  If you are using solrj, make sure to register a request handler to /update rather then use this servlet.\n"
          +"  Add: <requestHandler name=\"/update\" class=\"solr.XmlUpdateRequestHandler\" > to your solrconfig.xml\n\n" );
    }
    PrintWriter writer = response.getWriter();
    legacyUpdateHandler.doLegacyUpdate(requestReader, writer);
  }
}
