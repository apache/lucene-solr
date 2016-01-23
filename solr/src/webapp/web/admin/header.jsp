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
<html>
<head>
<%
request.setCharacterEncoding("UTF-8");
%>
<%@include file="_info.jsp" %>
<script>
var host_name="<%= hostname %>"
</script>

<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<link rel="stylesheet" type="text/css" href="solr-admin.css">
<link rel="icon" href="favicon.ico" type="image/ico"></link>
<link rel="shortcut icon" href="favicon.ico" type="image/ico"></link>
<title>Solr admin page</title>
</head>

<body>
<a href="."><img border="0" align="right" height="78" width="142" src="solr_small.png" alt="Solr"></a>
<h1>Solr Admin (<%= collectionName %>)
<%= enabledStatus==null ? "" : (isEnabled ? " - Enabled" : " - Disabled") %> </h1>

<%= hostname %>:<%= port %><br/>
cwd=<%= cwd %>  SolrHome=<%= solrHome %>
