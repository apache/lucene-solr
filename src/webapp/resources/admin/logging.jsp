<%@ page contentType="text/html; charset=utf-8" pageEncoding="UTF-8"%>
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
<%@ page import="org.apache.solr.core.SolrCore,
                 org.apache.solr.schema.IndexSchema,
                 java.io.File,
                 java.net.InetAddress,
                 java.net.UnknownHostException"%>
<%@ page import="java.util.Date"%>
<%@ page import="java.util.logging.Level"%>
<%@ page import="java.util.logging.LogManager"%>
<%@ page import="java.util.logging.Logger"%>
<%@include file="header.jsp" %>
<%

  LogManager mgr = LogManager.getLogManager();
  Logger log = SolrCore.log;

  Logger parent = log.getParent();
  while(parent != null) {
    log = parent;
    parent = log.getParent();
  }
  Level lvl = log.getLevel();
      
%>
<br clear="all">
<h2>Solr Logging</h2>
<table>
  <tr>
    <td>
      <H3>Log Level:</H3>
    </td>
    <td>
<% if (lvl!=null) {%>
      <%= lvl.toString() %><br>
<% } else { %>
      null<br>
<% } %>
    </td>
  </tr>
  <tr>
    <td>
    Set Level
    </td>
    <td>
    [<a href=action.jsp?log=ALL>ALL</a>]
    [<a href=action.jsp?log=CONFIG>CONFIG</a>]
    [<a href=action.jsp?log=FINE>FINE</a>]
    [<a href=action.jsp?log=FINER>FINER</a>]
    [<a href=action.jsp?log=FINEST>FINEST</a>]
    [<a href=action.jsp?log=INFO>INFO</a>]
    [<a href=action.jsp?log=OFF>OFF</a>]
    [<a href=action.jsp?log=SEVERE>SEVERE</a>]
    [<a href=action.jsp?log=WARNING>WARNING</a>]
    </td>
  </tr>
</table>
<br><br>
    <a href=".">Return to Admin Page</a>
</body>
</html>
