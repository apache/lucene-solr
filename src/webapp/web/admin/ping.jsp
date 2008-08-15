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
<%@ page import="org.apache.solr.core.SolrConfig,
                 org.apache.solr.core.SolrCore,
                 org.apache.solr.common.util.XML,
                 org.apache.solr.common.SolrException"%>
<%@ page import="org.apache.solr.request.LocalSolrQueryRequest"%>
<%@ page import="org.apache.solr.request.SolrQueryResponse"%>
<%@ page import="org.apache.solr.request.ServletSolrParams"%>
<%@ page import="org.apache.solr.request.SolrQueryRequest"%>

<%@include file="_info.jsp" %>
<?xml-stylesheet type="text/xsl" href="ping.xsl"?>

<solr>
  <core><%=core.getName()%></core>
  <ping>
<%
  SolrQueryRequest req = core.getPingQueryRequest();
  SolrQueryResponse resp = new SolrQueryResponse();
  try {
    core.execute(req,resp);
    if (resp.getException() == null) {
// No need for explicit status in the body, when the standard HTTP
// response codes already transmit success/failure message
//      out.println("<status>200</status>");
    }
    else if (resp.getException() != null) {
// No need for explicit status in the body, when the standard HTTP
// response codes already transmit success/failure message
//      out.println("<status>500</status>");
      out.println("<error>");
      XML.escapeCharData(SolrException.toStr(resp.getException()), out);
      out.println("</error>");
      response.sendError(500);
    }
  } catch (Throwable t) {
// No need for explicit status in the body, when the standard HTTP
// response codes already transmit success/failure message
//      out.println("<status>500</status>");
      out.println("<error>");
      XML.escapeCharData(SolrException.toStr(t), out);
      out.println("</error>");
      response.sendError(500);
  } finally {
      req.close();
  }
%>
  </ping>
</solr>
