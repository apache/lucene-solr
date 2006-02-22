
<!-- $Id: index.jsp,v 1.26 2005/09/20 18:23:30 yonik Exp $ -->
<!-- $Source: /cvs/main/searching/SolrServer/resources/admin/index.jsp,v $ -->
<!-- $Name:  $ -->

<!-- jsp:include page="header.jsp"/ -->
<!-- do a verbatim include so we can use the local vars -->
<%@include file="header.jsp" %>

<br clear="all">
<table>

<tr>
  <td>
	<h3>Solr</h3>
  </td>
  <td>
    [<a href="solar-status">Status</a>]
    [<a href="get-file.jsp?file=schema.xml">Schema</a>]
    [<a href="get-file.jsp?file=solrconfig.xml">Config</a>]
    [<a href="analysis.jsp?highlight=on">Analysis</a>]
    <br>
    [<a href="stats.jsp">Statistics</a>]
    [<a href="registry.jsp">Info</a>]
    [<a href="distributiondump.jsp">Distribution</a>]
    [<a href="ping">Ping</a>]
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

<!-- TODO: make it possible to add links to the admin page via solrconfig.xml
<tr>
  <td>
	<strong>Hardware:</strong><br>
  </td>
  <td>
	[<a href="http://playground.cnet.com/db/machines-match.php3?searchterm=<%= hostname %>&searchfield=hostorserial">Status</a>]
	[<a href="http://playground.cnet.com/db/machines-match.php3?searchterm=<%= hostname %>/t&searchfield=hostorserial">Traffic</a>]
	[<a href="http://monitor.cnet.com/orca_mon/?mgroup=prob&hours=48&hostname=<%= hostname %>">Problems</a>]
  </td>
</tr>
-->

</table><P>


<table>
<tr>
  <td>
	<h3>Make a Query</h3>
  </td>
  <td>

  <td>
	[<a href="form.jsp">Full Interface</a>]
  </td>
</tr>
<tr>
  <td>
  StyleSheet:<br>Query:
  </td>
  <td colspan=2>
	<form method="GET" action="../select/">
        <input name="stylesheet" type="text" value=""><br>
        <textarea rows="4" cols="40" name="q"><%= defaultSearch %></textarea>
        <input name="version" type="hidden" value="2.0">
	<input name="start" type="hidden" value="0">
	<input name="rows" type="hidden" value="10">
	<input name="indent" type="hidden" value="on">
        <br><input type="submit" value="search">
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
	[<a href="http://incubator.apache.org/solr/">Documentation</a>]
	[<a href="http://issues.apache.org/jira/browse/SOLR">Issue Tracker</a>]
	[<a href="mailto:solr-user@lucene.apache.org">Send Email</a>]
	<br>
        [<a href="http://lucene.apache.org/java/docs/queryparsersyntax.html">Lucene Query Syntax</a>]
  </td>
<!--
  <td rowspan="3">
	<a href="http://incubator.apache.org/solr/"><img align="right" border=0 height="107" width="148" src="power.png"></a>
  </td>
 -->
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
