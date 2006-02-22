<%@include file="_info.jsp" %>

<html>
<head>
<link rel="stylesheet" type="text/css" href="solr-admin.css">
<link rel="icon" href="favicon.ico" type="image/ico"></link>
<link rel="shortcut icon" href="favicon.ico" type="image/ico"></link>
<title>Solr admin page</title>
</head>

<body>
<a href="."><img border="0" align="right" height="88" width="215" src="solr-head.gif" alt="Solr"></a>
<h1>Solr Admin (<%= collectionName %>)
<%= enabledStatus==null ? "" : (isEnabled ? " - Enabled" : " - Disabled") %> </h1>

<%= hostname %>:<%= port %><br/>
<%= cwd %>