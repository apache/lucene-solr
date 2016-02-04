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

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A Simple redirection servlet to help us deprecate old UI elements
 */
public class RedirectServlet extends BaseSolrServlet {
  
  static final String CONTEXT_KEY = "${context}";
  
  String destination;
  int code = HttpServletResponse.SC_MOVED_PERMANENTLY;
  
  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    
    destination = config.getInitParameter("destination");
    if(destination==null) {
      throw new ServletException("RedirectServlet missing destination configuration");
    }
    if( "false".equals(config.getInitParameter("permanent") )) {
      code = HttpServletResponse.SC_MOVED_TEMPORARILY;
    }
    
    // Replace the context key
    if(destination.startsWith(CONTEXT_KEY)) {
      destination = config.getServletContext().getContextPath()
          +destination.substring(CONTEXT_KEY.length());
    }
  }
  
  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse res)
          throws ServletException,IOException {
      
    res.setStatus(code);
    res.setHeader("Location", destination);
  }

}