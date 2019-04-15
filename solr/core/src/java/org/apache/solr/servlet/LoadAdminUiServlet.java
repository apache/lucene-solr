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

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

/**
 * A simple servlet to load the Solr Admin UI
 * 
 * @since solr 4.0
 */
public final class LoadAdminUiServlet extends BaseSolrServlet {

  @Override
  public void doGet(HttpServletRequest _request,
                    HttpServletResponse _response)
      throws IOException {
    HttpServletRequest request = SolrDispatchFilter.closeShield(_request, false);
    HttpServletResponse response = SolrDispatchFilter.closeShield(_response, false);
    
    response.addHeader("X-Frame-Options", "DENY"); // security: SOLR-7966 - avoid clickjacking for admin interface

    // This attribute is set by the SolrDispatchFilter
    String admin = request.getRequestURI().substring(request.getContextPath().length());
    CoreContainer cores = (CoreContainer) request.getAttribute("org.apache.solr.CoreContainer");
    InputStream in = getServletContext().getResourceAsStream(admin);
    Writer out = null;
    if(in != null && cores != null) {
      try {
        response.setCharacterEncoding("UTF-8");
        response.setContentType("text/html");

        // We have to close this to flush OutputStreamWriter buffer
        out = new OutputStreamWriter(new CloseShieldOutputStream(response.getOutputStream()), StandardCharsets.UTF_8);

        String html = IOUtils.toString(in, "UTF-8");
        Package pack = SolrCore.class.getPackage();

        String[] search = new String[] { 
            "${contextPath}", 
            "${adminPath}",
            "${version}" 
        };
        String[] replace = new String[] {
            StringEscapeUtils.escapeEcmaScript(request.getContextPath()),
            StringEscapeUtils.escapeEcmaScript(CommonParams.CORES_HANDLER_PATH),
            StringEscapeUtils.escapeEcmaScript(pack.getSpecificationVersion())
        };
        
        out.write( StringUtils.replaceEach(html, search, replace) );
      } finally {
        IOUtils.closeQuietly(in);
        IOUtils.closeQuietly(out);
      }
    } else {
      response.sendError(404);
    }
  }

}
