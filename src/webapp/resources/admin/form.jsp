<%@ page import="org.apache.solr.core.SolrConfig,
                 org.apache.solr.core.SolrCore,
                 org.apache.solr.schema.IndexSchema,
                 java.io.File
"%>
<%@ page import="java.net.InetAddress"%>
<%@ page import="java.net.UnknownHostException"%>
<%@ page import="java.util.Date"%>
<!-- $Id: form.jsp,v 1.6 2005/09/16 21:45:54 yonik Exp $ -->
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
<link rel="stylesheet" type="text/css" href="/admin/solr-admin.css">
<link rel="icon" href="/favicon.ico" type="image/ico">
<link rel="shortcut icon" href="/favicon.ico" type="image/ico">
<title>SOLR Interface</title>
</head>

<body>
<a href="/admin/"><img border="0" align="right" height="88" width="215" src="solr-head.gif" alt="SOLR"></a>
<h1>SOLR Interface (<%= collectionName %>) - <%= enabledStatus %></h1>
<%= hostname %> : <%= port.toString() %>
<br clear="all">


<h2>/select mode</h2>

<form method="GET" action="/select/">
<table>
<tr>
  <td>
	<strong>SOLR/Lucene Statement</strong>
  </td>
  <td>
	<textarea rows="5" cols="60" name="q"></textarea>
  </td>
</tr>
<tr>
  <td>
	<strong>Return Number Found</strong>
  </td>
  <td>
	<input name="getnumfound" type="checkbox" >  <em><font size="-1">(Option ignored by SOLR... the number of matching documents is always returned)</font></em>
  </td>
</tr>
<tr>
  <td>
	<strong>Protocol Version</strong>
  </td>
  <td>
	<input name="version" type="text" value="2.0">
  </td>
</tr>
<tr>
  <td>
	<strong>Start Row</strong>
  </td>
  <td>
	<input name="start" type="text" value="0">
  </td>
</tr>
<tr>
  <td>
	<strong>Maximum Rows Returned</strong>
  </td>
  <td>
	<input name="rows" type="text" value="10">
  </td>
</tr>
<tr>
  <td>
	<strong>Fields to Return</strong>
  </td>
  <td>
	<input name="fl" type="text" value="">
  </td>
</tr>
<tr>
  <td>
	<strong>Query Type</strong>
  </td>
  <td>
	<input name="qt" type="text" value="standard">
  </td>
</tr>
<tr>
  <td>
	<strong>Style Sheet</strong>
  </td>
  <td>
	<input name="stylesheet" type="text" value="">
  </td>
</tr>
<tr>
  <td>
	<strong>Indent XML</strong>
  </td>
  <td>
	<input name="indent" type="checkbox" checked="true">
  </td>
</tr>
<tr>
  <td>
	<strong>Debug: enable</strong>
  </td>
  <td>
	<input name="debugQuery" type="checkbox" >
  <em><font size="-1">  Note: do "view source" in your browser to see explain() correctly indented</font></em>
  </td>
</tr>
<tr>
  <td>
	<strong>Debug: explain others</strong>
  </td>
  <td>
	<input name="explainOther" type="text" >
  <em><font size="-1">  apply original query scoring to matches of this query</font></em>
  </td>
</tr>
<tr>
  <td>
  </td>
  <td>
	<input type="submit" value="search">
  </td>
</tr>
</table>
</form>


</body>
</html>
