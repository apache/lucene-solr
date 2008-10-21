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
<!-- $Id$ -->
<%@ page import="org.apache.solr.common.util.NamedList,
				 org.apache.solr.common.util.SimpleOrderedMap,
				 org.apache.solr.request.LocalSolrQueryRequest,
				 org.apache.solr.request.SolrQueryResponse,
				 org.apache.solr.request.SolrRequestHandler"%>

<html>
<head>

<%
request.setCharacterEncoding("UTF-8");
%>

<%@include file="../_info.jsp" %>

<script>
var host_name="<%= hostname %>"
</script>

<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<link rel="stylesheet" type="text/css" href="../solr-admin.css">
<link rel="icon" href="../favicon.ico" type="image/ico" />
<link rel="shortcut icon" href="../favicon.ico" type="image/ico" />
<title>Solr replication admin page</title>
<script type="text/javascript" src="../jquery-1.2.3.min.js"></script>

<%!
public NamedList executeCommand(String command, SolrCore core, SolrRequestHandler rh){
    NamedList namedlist = new SimpleOrderedMap();
    namedlist.add("command", command);
    LocalSolrQueryRequest solrqreq = new LocalSolrQueryRequest(core, namedlist);
    SolrQueryResponse rsp = new SolrQueryResponse();
    core.execute(rh, solrqreq, rsp);
    namedlist = rsp.getValues();
	return namedlist;
}
%>

<%

final SolrRequestHandler rh = core.getRequestHandler("/replication");
NamedList namedlist = executeCommand("details",core,rh);
NamedList detailsMap = (NamedList)namedlist.get("details");

if("false".equals((String)detailsMap.get("isMaster"))){
%>
	<meta http-equiv="refresh" content="2"/>
<%}%>

</head>

<body>
<a href=".."><img border="0" align="right" height="61" width="142" src="../solr-head.gif" alt="Solr"></a>
<h1>Solr replication (<%= collectionName %>) 
<%
if("true".equals((String)detailsMap.get("isMaster")))
	out.println(" Master");
  else
	out.println(" Slave");
%></h1>

<%= hostname %>:<%= port %><br/>
cwd=<%= cwd %>  SolrHome=<%= solrHome %>
