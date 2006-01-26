<%@ page import="org.apache.solr.core.SolrCore,
                 org.apache.solr.schema.IndexSchema,
                 java.io.File,
                 java.net.InetAddress,
                 java.net.UnknownHostException"%>
<%@ page import="java.util.Date"%>
<%@ page import="java.util.logging.Level"%>
<%@ page import="java.util.logging.Logger"%>
<%
  SolrCore core = SolrCore.getSolrCore();
  IndexSchema schema = core.getSchema();
  String collectionName = schema!=null ? schema.getName():"unknown";

  String action = request.getParameter("action");
  String logging = request.getParameter("log");
  String enableActionStatus = "";
  boolean isValid = false;
  boolean wasOk = true;

  String rootdir = "/var/opt/resin3/"+request.getServerPort();
  File pidFile = new File(rootdir + "/logs/resin.pid");
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

  File enableFile = new File(rootdir + "/logs/server-enabled");

  if (action != null) {
    // Validate fname
    if ("Enable".compareTo(action) == 0) isValid = true;
    if ("Disable".compareTo(action) == 0) isValid = true;
  }
  if (logging != null) {
    action = "Set Log Level";
    isValid = true;
  }
  if (isValid) {
    if ("Enable".compareTo(action) == 0) {
      try {
        if (enableFile.createNewFile()) {
          enableActionStatus += "Enable Succeeded";
        } else {
          enableActionStatus += "Already Enabled";
        }
      } catch(Exception e) {
          enableActionStatus += "Enable Failed: " + e.toString();
          wasOk = false;
      }
    }
    if ("Disable".compareTo(action) == 0) {
      try {
        if (enableFile.delete()) {
          enableActionStatus = "Disable Succeeded";
        } else {
          enableActionStatus = "Already Disabled";
        }
      } catch(Exception e) {
          enableActionStatus += "Disable Failed: " + e.toString();
          wasOk = false;
      }
    }
    if (logging != null) {
      try {
        Logger log = SolrCore.log;
        Logger parent = log.getParent();
        while (parent != null) {
          log = parent;
          parent = log.getParent();
        }
        log.setLevel(Level.parse(logging));
        enableActionStatus = "Set Log Level (" + logging + ") Succeeded";
      } catch(Exception e) {
          enableActionStatus += "Set Log Level (" + logging + ") Failed: "
                                 + e.toString();
          wasOk = false;
      }
    }
  } else {
    enableActionStatus = "Illegal Action";
  }

  String hostname="localhost";
  try {
    InetAddress addr = InetAddress.getLocalHost();
    // Get IP Address
    byte[] ipAddr = addr.getAddress();
    // Get hostname
    // hostname = addr.getHostName();
    hostname = addr.getCanonicalHostName();
  } catch (UnknownHostException e) {}
%>
<%
  if (wasOk) {
%>
<meta http-equiv="refresh" content="4;url=index.jsp">
<%
  }
%>
<html>
<head>
    <link rel="stylesheet" type="text/css" href="/admin/solr-admin.css">
    <link rel="icon" href="/favicon.ico" type="image/ico">
    <link rel="shortcut icon" href="/favicon.ico" type="image/ico">
</head>
<body>
<a href="/admin/"><img border="0" align="right" height="88" width="215" src="solr-head.gif" alt="SOLR"></a>
<h1>SOLR Action (<%= collectionName %>) - <%= action %></h1>
<%= hostname %> : <%= request.getServerPort() %>
<br clear="all">
<table>
  <tr>
    <td>
      <H3>Action:</H3>
    </td>
    <td>
      <%= action %><br>
    </td>
  </tr>
  <tr>
    <td>
      <H4>Result:</H4>
    </td>
    <td>
      <%= enableActionStatus %><br>
    </td>
  </tr>
</table>
<br>
<table>
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
<br><br>
    <a href="/admin">Return to Admin Page</a>
</body>
</html>
