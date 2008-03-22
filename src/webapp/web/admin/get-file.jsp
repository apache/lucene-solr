<%@ page contentType="text/plain; charset=utf-8" pageEncoding="UTF-8" %>
<%--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
--%>
<%@ page import="org.apache.solr.core.Config,
                 org.apache.solr.core.SolrCore,
                 org.apache.solr.core.SolrConfig,
                 java.io.InputStream,
                 java.io.InputStreamReader,
                 java.io.Reader,
                 java.util.StringTokenizer,
                 java.util.logging.Logger"%>
<%!
  static Logger log = Logger.getLogger(SolrCore.class.getName());
%>
<%
  // NOTE -- this file will be removed in a future release
  log.warning("Using deprecated JSP: " + request.getRequestURL().append("?").append(request.getQueryString()) + " -- check the ShowFileRequestHandler"  );

  Object ocore = request.getAttribute("org.apache.solr.SolrCore");
  SolrCore core = ocore instanceof SolrCore? (SolrCore) ocore : SolrCore.getSolrCore();
  String fname = request.getParameter("file");
  String optional = request.getParameter("optional");
  String gettableFiles = core.getSolrConfig().get("admin/gettableFiles","");
  StringTokenizer st = new StringTokenizer(gettableFiles);
  InputStream is;
  boolean isValid = false;
  boolean isOptional = false;
  if (fname != null) {
    // Validate fname
    while(st.hasMoreTokens()) {
      if (st.nextToken().compareTo(fname) == 0) isValid = true;
    }
  }
  if (optional!=null && optional.equalsIgnoreCase("y")) {
    isOptional=true;
  }
  if (isValid) {
    try {
    is= core.getSolrConfig().openResource(fname);
    Reader input = new InputStreamReader(is);
    char[] buf = new char[4096];
    while (true) {
      int len = input.read(buf);
      if (len<=0) break;
      out.write(buf,0,len);
    }
    }
    catch (RuntimeException re) {
      if (!isOptional) {
        throw re;
      }
    }
  } else {
    out.println("<ERROR>");
    out.println("Permission denied for file "+ fname);
    out.println("</ERROR>");
  }
%>
