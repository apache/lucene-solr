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

<%-- $Id$ --%>
<%-- $Source: /cvs/main/searching/SolrServer/resources/admin/index.jsp,v $ --%>
<%-- $Name:  $ --%>

<%@ page import="java.util.Date" %>

<%-- jsp:include page="header.jsp"/ --%>
<%-- do a verbatim include so we can use the local vars --%>
<%@include file="header.jsp" %>

<br clear="all">
<table>

<tr>
  <td>
	<h3>Solr</h3>
  </td>
  <td>
    [<a href="get-file.jsp?file=schema.xml">Schema</a>]
    [<a href="get-file.jsp?file=solrconfig.xml">Config</a>]
    [<a href="analysis.jsp?highlight=on">Analysis</a>]
    <br>
    [<a href="stats.jsp">Statistics</a>]
    [<a href="registry.jsp">Info</a>]
    [<a href="distributiondump.jsp">Distribution</a>]
    [<a href="ping">Ping</a>]
    [<a href="logging.jsp">Logging</a>]
  </td>
</tr>


<tr>
  <td>
    <strong>App server:</strong><br>
  </td>
  <td>
    [<a href="get-properties.jsp">Java Properties</a>]
    [<a href="threaddump.jsp">Thread Dump</a>]
  <%
    if (enabledFile!=null)
    if (isEnabled) {
  %>
  [<a href="action.jsp?action=Disable">Disable</a>]
  <%
    } else {
  %>
  [<a href="action.jsp?action=Enable">Enable</a>]
  <%
    }
  %>
  </td>
</tr>

<jsp:include page="get-file.jsp?file=admin-extra.html&optional=y" flush="true"/>

</table><P>


<table>
<tr>
  <td>
	<h3>Make a Query</h3>
  </td>
  <td>
[<a href="form.jsp">Full Interface</a>]
  </td>
  
</tr>
<tr>
  <td>
  Query String:
  </td>
  <td colspan=2>
	<form name=queryForm method="GET" action="../select/">
        <textarea class="std" rows="4" cols="40" name="q"><%= defaultSearch %></textarea>
        <input name="version" type="hidden" value="2.2">
	<input name="start" type="hidden" value="0">
	<input name="rows" type="hidden" value="10">
	<input name="indent" type="hidden" value="on">
        <br><input class="stdbutton" type="submit" value="search" 
        	onclick="if (queryForm.q.value.length==0) { alert('no empty queries, please'); return false; } else { queryForm.submit(); } ">
	</form>
  </td>
</tr>
</table><p>

<table>
<tr>
  <td>
	<h3>Assistance</h3>
  </td>
  <td>
	[<a href="http://lucene.apache.org/solr/">Documentation</a>]
	[<a href="http://issues.apache.org/jira/browse/SOLR">Issue Tracker</a>]
	[<a href="mailto:solr-user@lucene.apache.org">Send Email</a>]
	<br>
        [<a href="http://wiki.apache.org/solr/SolrQuerySyntax">Solr Query Syntax</a>]
  </td>
</tr>
<tr>
  <td>
  </td>
  <td>
  Current Time: <%= new Date() %>
  </td>
</tr>
<tr>
  <td>
  </td>
  <td>
  Server Start At: <%= new Date(core.getStartTime()) %>
  </td>
</tr>
</table>
</body>
</html>
