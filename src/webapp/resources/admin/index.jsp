<%@ page import="org.apache.solr.core.SolrConfig,
                 org.apache.solr.core.SolrCore,
                 org.apache.solr.schema.IndexSchema,
                 java.io.File"%>
<%@ page import="java.net.InetAddress"%>
<%@ page import="java.net.UnknownHostException"%>
<%@ page import="java.util.Date"%>
<!-- $Id: index.jsp,v 1.26 2005/09/20 18:23:30 yonik Exp $ -->
<!-- $Source: /cvs/main/searching/SolrServer/resources/admin/index.jsp,v $ -->
<!-- $Name:  $ -->

<%
  SolrCore core = SolrCore.getSolrCore();
  Integer port = new Integer(request.getServerPort());
  IndexSchema schema = core.getSchema();

  String rootdir = "/var/opt/resin3/"+port.toString();
  File pidFile = new File(rootdir + "/logs/resin.pid");
  File enableFile = new File(rootdir + "/logs/server-enabled");
  boolean isEnabled = false;
  String enabledStatus = "";
  String enableActionStatus = "";
  String makeEnabled = "";
  String action = request.getParameter("action");
  String startTime = "";

  try {
    startTime = (pidFile.lastModified() > 0)
      ? new Date(pidFile.lastModified()).toString()
      : "No Resin Pid found (logs/resin.pid)";
  } catch (Exception e) {
    out.println("<ERROR>");
    out.println("Couldn't open Solr pid file:" + e.toString());
    out.println("</ERROR>");
  }

  try {
    if (action != null) {
      if ("Enable".compareTo(action) == 0) {
        if (enableFile.createNewFile()) {
          enableActionStatus += "Enable Succeeded";
        } else {
          enableActionStatus += "Already Enabled)";
        }
      }
      if ("Disable".compareTo(action) == 0) {
        if (enableFile.delete()) {
          enableActionStatus = "Disable Succeeded";
        } else {
          enableActionStatus = "Already Disabled";
        }
      }
    }
  } catch (Exception e) {
    out.println("<ERROR>");
    out.println("Couldn't "+action+" server-enabled file:" + e.toString());
    out.println("</ERROR>");
  }

  try {
    isEnabled = (enableFile.lastModified() > 0);
    enabledStatus = (isEnabled)
      ? "Enabled"
      : "Disabled";
    makeEnabled = (isEnabled)
      ? "Disable"
      : "Enable";
  } catch (Exception e) {
    out.println("<ERROR>");
    out.println("Couldn't check server-enabled file:" + e.toString());
    out.println("</ERROR>");
  }

  String collectionName = schema!=null ? schema.getName():"unknown";
  String hostname="localhost";
  String defaultSearch= SolrConfig.config.get("admin/defaultQuery","");
  try {
    InetAddress addr = InetAddress.getLocalHost();
    // Get IP Address
    byte[] ipAddr = addr.getAddress();
    // Get hostname
    // hostname = addr.getHostName();
    hostname = addr.getCanonicalHostName();
  } catch (UnknownHostException e) {}
%>


<html>
<head>
<link rel="stylesheet" type="text/css" href="solr-admin.css">
<link rel="icon" href="favicon.ico" type="image/ico"></link>
  <link rel="shortcut icon" href="favicon.ico" type="image/ico"></link>
<title>SOLR admin page</title>
</head>

<body>
<a href=""><img border="0" align="right" height="88" width="215" src="solr-head.gif" alt="SOLR"></a>
<h1>SOLR Admin (<%= collectionName %>) - <%= enabledStatus %></h1>
<%= hostname %> : <%= port.toString() %>
<br clear="all">
<table>

<tr>
  <td>
	<h3>SOLR</h3>
  </td>
  <td>
    [<a href="solar-status">Status</a>]
    [<a href="get-file.jsp?file=solrconfig.xml">Config</a>]
    [<a href="get-file.jsp?file=conf/solar/WEB-INF/web.external.xml">web.external.xml</a>]
    [<a href="get-properties.jsp">Properties</a>]
    [<a href="raw-schema.jsp">Schema</a>]
    [<a href="analysis.jsp?highlight=on">Analysis</a>]
    <br>
    [<a href="registry.jsp">Info</a>]
    [<a href="stats.jsp">Statistics</a>]
    [<a href="distributiondump.jsp">Distribution</a>]
    [<a href="ping">Ping</a>]
    [<a href="logging.jsp">Logging</a>]
  </td>
</tr>

<tr>
  <td>
    <strong>Resin server:</strong><br>
  </td>
  <td>
    [<a href="/server-status">Status</a>]
    [<a href="get-file.jsp?file=conf/resin.conf">Config</a>]
    [<a href="threaddump.jsp">Thread Dump</a>]
  <%
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
	[<a href="http://pi.cnet.com/solar/">Documentation</a>]
	[<a href="http://bugzilla.cnet.com/enter_bug.cgi?op_sys=All&product=PI-Solr&component=Operational">File a Bugzilla</a>]
	[<a href="mailto:solar@cnet.com">Send Email</a>]
	<br>
        [<a href="http://lucene.apache.org/java/docs/queryparsersyntax.html">Lucene Query Syntax</a>]
  </td>
  <td rowspan="3">
	<a href="http://pi.cnet.com/"><img align="right" border=0 height="107" width="148" src="power.png"></a>
  </td>
</tr>
<tr>
  <td>
  </td>
  <td>
  Current Time: <%= new Date().toString() %>
  </td>
</tr>
<tr>
  <td>
  </td>
  <td>
  Server Start At: <%= startTime %>
  </td>
</tr>
</table>
</body>
</html>
