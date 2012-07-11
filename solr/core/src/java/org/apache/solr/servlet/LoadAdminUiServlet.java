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

package org.apache.solr.servlet;

import java.io.InputStream;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.solr.core.CoreContainer;


/**
 * A simple servlet to load the Solr Admin UI
 * 
 * @since solr 4.0
 */
public final class LoadAdminUiServlet extends HttpServlet {

  @Override
  public void doGet(HttpServletRequest request,
                    HttpServletResponse response)
      throws IOException {
    response.setCharacterEncoding("UTF-8");
    response.setContentType("text/html");

    PrintWriter out = response.getWriter();
    InputStream in = getServletContext().getResourceAsStream("/admin.html");
    if(in != null) {
      try {
        // This attribute is set by the SolrDispatchFilter
        CoreContainer cores = (CoreContainer) request.getAttribute("org.apache.solr.CoreContainer");

        String html = IOUtils.toString(in, "UTF-8");

        String[] search = new String[] { 
            "${contextPath}", 
            "${adminPath}" 
        };
        String[] replace = new String[] {
            StringEscapeUtils.escapeJavaScript(request.getContextPath()),
            StringEscapeUtils.escapeJavaScript(cores.getAdminPath())
        };
        
        out.println( StringUtils.replaceEach(html, search, replace) );
      } finally {
        IOUtils.closeQuietly(in);
      }
    } else {
      out.println("solr");
    }
  }

  @Override
  public void doPost(HttpServletRequest request,
                     HttpServletResponse response)
      throws IOException {
    doGet(request, response);
  }
}
