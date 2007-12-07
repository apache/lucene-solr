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
    [<a href="get-file.jsp?core=<%=core.getName()%>&file=<%=core.getSchemaFile()%>">Schema</a>]
    [<a href="get-file.jsp?core=<%=core.getName()%>&file=<%=core.getConfigFile()%>">Config</a>]
    [<a href="analysis.jsp?core=<%=core.getName()%>&highlight=on">Analysis</a>]
    <br>
    [<a href="stats.jsp?core=<%=core.getName()%>">Statistics</a>]
    [<a href="registry.jsp?core=<%=core.getName()%>">Info</a>]
    [<a href="distributiondump.jsp?core=<%=core.getName()%>">Distribution</a>]
    [<a href="ping?core=<%=core.getName()%>">Ping</a>]
    [<a href="logging.jsp?core=<%=core.getName()%>">Logging</a>]
  </td>
</tr>

<%-- List the cores (that arent this one) so we can switch --%>
<% java.util.Collection<SolrCore> cores = org.apache.solr.core.MultiCore.getRegistry().getCores();
if (cores.size() > 1) {%><tr><td><strong>Cores:</strong><br></td><td><%
  java.util.Iterator<SolrCore> icore = cores.iterator();
  while (icore.hasNext()) {
    SolrCore acore = icore.next();
    if (acore == core) continue;
    %>[<a href=".?core=<%=acore.getName()%>"><%=acore.getName()%></a>]<%         
  }%></td></tr><%
}%>

<tr>
  <td>
    <strong>App server:</strong><br>
  </td>
  <td>
    [<a href="get-properties.jsp?core=<%=core.getName()%>">Java Properties</a>]
    [<a href="threaddump.jsp?core=<%=core.getName()%>">Thread Dump</a>]
  <%
    if (enabledFile!=null)
    if (isEnabled) {
  %>
  [<a href="action.jsp?core=<%=core.getName()%>&action=Disable">Disable</a>]
  <%
    } else {
  %>
  [<a href="action.jsp?core=<%=core.getName()%>&action=Enable">Enable</a>]
  <%
    }
  %>
  </td>
</tr>


<jsp:include page="get-file.jsp?core=<%=core.getName()%>&file=admin-extra.html&optional=y" flush="true"/>

</table><P>


<table>
<tr>
  <td>
	<h3>Make a Query</h3>
  </td>
  <td>
[<a href="form.jsp?core=<%=core.getName()%>">Full Interface</a>]
  </td>
  
</tr>
<tr>
  <td>
  Query String:
  </td>
  <td colspan=2>
	<form name=queryForm method="GET" action="../select/">
        <textarea class="std" rows="4" cols="40" name="q"><%= defaultSearch %></textarea>
        <input name="core" type="hidden" value="<%=core.getName()%>">
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
