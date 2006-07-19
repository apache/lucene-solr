<%@ page contentType="text/html; charset=utf-8" pageEncoding="UTF-8"%>
<%@include file="_info.jsp" %>
<script>
var host_name="<%= hostname %>"
</script>

<html>
<head>
<link rel="stylesheet" type="text/css" href="solr-admin.css">
<link rel="icon" href="favicon.ico" type="image/ico"></link>
<link rel="shortcut icon" href="favicon.ico" type="image/ico"></link>
<title>Solr admin page</title>
</head>

<body>
<a href="."><img border="0" align="right" height="61" width="142" src="solr-head.gif" alt="Solr"></a>
<h1>Solr Admin (<%= collectionName %>)
<%= enabledStatus==null ? "" : (isEnabled ? " - Enabled" : " - Disabled") %> </h1>

<%= hostname %>:<%= port %><br/>
cwd=<%= cwd %>  SolrHome=<%= solrHome %>
