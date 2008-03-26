<%@ page contentType="text/xml; charset=utf-8" pageEncoding="UTF-8" language="java" %>
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

<?xml-stylesheet type="text/xsl" href="logging.xsl"?>
<%@include file="_info.jsp" %>

<%
  Logger log = SolrCore.log;
  Logger parent = log.getParent();
  while(parent != null) {
    log = parent;
    parent = log.getParent();
  }
  Level lvl = log.getLevel();
      
%>
<solr>
  <core><%=core.getName()%></core>
  <logging>
<% if (lvl != null) {%>
      <logLevel><%= lvl.toString() %></logLevel>
<% } else { %>
      <logLevel>null</logLevel>
<% } %>
  </logging>
</solr>
