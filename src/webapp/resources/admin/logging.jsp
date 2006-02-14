<%@ page import="org.apache.solr.core.SolrCore,
                 org.apache.solr.schema.IndexSchema,
                 java.io.File,
                 java.net.InetAddress,
                 java.net.UnknownHostException"%>
<%@ page import="java.util.Date"%>
<%@ page import="java.util.logging.Level"%>
<%@ page import="java.util.logging.LogManager"%>
<%@ page import="java.util.logging.Logger"%>
<%
  SolrCore core = SolrCore.getSolrCore();
  Integer port = new Integer(request.getServerPort());
  IndexSchema schema = core.getSchema();  String collectionName = schema!=null ? schema.getName():"unknown";

  String rootdir = "/var/opt/resin3/"+port.toString();
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

  LogManager mgr = LogManager.getLogManager();
  Logger log = SolrCore.log;

  Logger parent = log.getParent();
  while(parent != null) {
    log = parent;
    parent = log.getParent();
  }
  Level lvl = log.getLevel();
      
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
<html>
<head>
    <link rel="stylesheet" type="text/css" href="solr-admin.css">
    <link rel="icon" href="favicon.ico" type="image/ico">
    <link rel="shortcut icon" href="favicon.ico" type="image/ico">
</head>
<body>
<a href=""><img border="0" align="right" height="88" width="215" src="solr-head.gif" alt="SOLR"></a>
<h1>SOLR Logging (<%= collectionName %>)</h1>
<%= hostname %> : <%= port.toString() %>
<br clear="all">
<table>
  <tr>
    <td>
      <H3>Log Level:</H3>
    </td>
    <td>
      <%= lvl.toString() %><br>
    </td>
  </tr>
  <tr>
    <td>
    Set Level
    </td>
    <td>
    [<a href=action.jsp?log=ALL>ALL</a>]
    [<a href=action.jsp?log=CONFIG>CONFIG</a>]
    [<a href=action.jsp?log=FINE>FINE</a>]
    [<a href=action.jsp?log=FINER>FINER</a>]
    [<a href=action.jsp?log=FINEST>FINEST</a>]
    [<a href=action.jsp?log=INFO>INFO</a>]
    [<a href=action.jsp?log=OFF>OFF</a>]
    [<a href=action.jsp?log=SEVERE>SEVERE</a>]
    [<a href=action.jsp?log=WARNING>WARNING</a>]
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
    <a href="">Return to Admin Page</a>
</body>
</html>
