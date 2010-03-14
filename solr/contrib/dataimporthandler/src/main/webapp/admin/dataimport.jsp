<%@ page import="org.apache.solr.request.SolrRequestHandler" %>
<%@ page import="java.util.Map" %>
<%@ page import="org.apache.solr.handler.dataimport.DataImportHandler" %>
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
<%-- do a verbatim include so we can use the local vars --%>
<%@include file="_info.jsp"%>
<html>
<%
  String handler = request.getParameter("handler");

  if (handler == null) {
    Map<String, SolrRequestHandler> handlers = core.getRequestHandlers();
%>
<head>
  <title>DataImportHandler Interactive Development</title>
  <link rel="stylesheet" type="text/css" href="solr-admin.css">
</head>
<body>
Select handler:
<ul>
<%
    for (String key : handlers.keySet()) {
      if (handlers.get(key) instanceof DataImportHandler) { %>
  <li><a href="dataimport.jsp?handler=<%=key%>"><%=key%></a></li>
<%
      }
    }
%>
</ul>
</body>
<% } else { %>

<frameset cols = "50%, 50%">
  <frame src ="debug.jsp?handler=<%=handler%>" />
  <frame src ="../select?qt=<%=handler%>&command=status"  name="result"/>
</frameset>
<% } %>
</html>
