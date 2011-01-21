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
				 org.apache.solr.response.SolrQueryResponse,
				 org.apache.solr.request.SolrRequestHandler,
                                 java.util.Map"%>
<%@ page import="org.apache.solr.handler.ReplicationHandler" %>
<%
request.setCharacterEncoding("UTF-8");
%>

<html>
<head>

<%@include file="../_info.jsp" %>

<script>
var host_name="<%= hostname %>"
</script>

<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<link rel="stylesheet" type="text/css" href="../solr-admin.css">
<link rel="icon" href="../favicon.ico" type="image/ico" />
<link rel="shortcut icon" href="../favicon.ico" type="image/ico" />
<title>Solr replication admin page</title>
<script type="text/javascript" src="../jquery-1.4.3.min.js"></script>

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
final Map<String,SolrRequestHandler> all = core.getRequestHandlers(ReplicationHandler.class);
  if(all.isEmpty()){
    response.sendError( 404, "No ReplicationHandler registered" );
    return;
  }

// :HACK: we should be more deterministic if multiple instances
final SolrRequestHandler rh = all.values().iterator().next();

NamedList namedlist = executeCommand("details",core,rh);
NamedList detailsMap = (NamedList)namedlist.get("details");
%>
</head>

<body>
<a href=".."><img border="0" align="right" height="78" width="142" src="../solr_small.png" alt="Solr"></a>
<h1>Solr replication (<%= collectionName %>) 

<%
if(detailsMap != null){
  if( "true".equals(detailsMap.get("isMaster")) && "true".equals(detailsMap.get("isSlave")))
    out.println(" Master & Slave");
  else if("true".equals(detailsMap.get("isMaster")))
    out.println(" Master");
  else if("true".equals(detailsMap.get("isSlave")))
    out.println(" Slave");
}
%></h1>

<%= hostname %>:<%= port %><br/>
cwd=<%= cwd %>  SolrHome=<%= solrHome %>
