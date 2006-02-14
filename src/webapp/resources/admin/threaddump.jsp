<%@ page import="org.apache.solr.core.SolrCore,
                 org.apache.solr.schema.IndexSchema,
                 java.io.BufferedReader,
                 java.io.File,
                 java.io.FileReader,
                 java.net.InetAddress,
                 java.net.UnknownHostException,
                 java.util.Date"%>
<%
  SolrCore core = SolrCore.getSolrCore();
  Integer port = new Integer(request.getServerPort());
  IndexSchema schema = core.getSchema();
  String collectionName = schema!=null ? schema.getName():"unknown";

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

  String hostname="localhost";
  try {
    InetAddress addr = InetAddress.getLocalHost();
    // Get IP Address
    byte[] ipAddr = addr.getAddress();
    // Get hostname
    // hostname = addr.getHostName();
    hostname = addr.getCanonicalHostName();
  } catch (UnknownHostException e) {}

  File getinfo = new File(rootdir + "/logs/jvm.log");
%>
<html>
<head>
    <link rel="stylesheet" type="text/css" href="solr-admin.css">
    <link rel="icon" href="favicon.ico" type="image/ico">
    <link rel="shortcut icon" href="favicon.ico" type="image/ico">
</head>
<body>
<a href="."><img border="0" align="right" height="88" width="215" src="solr-head.gif" alt="SOLR"></a>
<h1>SOLR Thread Dump (<%= collectionName %>)</h1>
<%= hostname %> : <%= port.toString() %>
<br clear="all">
<%
  Runtime rt = Runtime.getRuntime();
  Process p = rt.exec(rootdir + "/getinfo");
  p.waitFor();
%>
<table>
  <tr>
    <td>
      <H3>Exit Value:</H3>
    </td>
    <td>
      <%= p.exitValue() %>
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
<%
  BufferedReader in = new BufferedReader(new FileReader(getinfo));
  StringBuffer buf = new StringBuffer();
  String line;

  while((line = in.readLine()) != null) {
    if (line.startsWith("taking thread dump")) {
      buf = new StringBuffer();
    }
    buf.append(line).append("<br>");
  }
%>
<br>
Thread Dumps
<table>
  <tr>
    <td>
    </td>
    <td>
      [<a href=get-file.jsp?file=logs/jvm.log>All Entries</a>]
    </td>
  </tr>
  <tr>
    <td>
      Last Entry
    </td>
    <td>
      <%= buf.toString() %>
    </td>
  </tr>
</table>
<br><br>
    <a href=".">Return to Admin Page</a>
</body>
</html>
