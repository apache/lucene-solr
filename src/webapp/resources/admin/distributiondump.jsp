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

  File slaveinfo = new File(rootdir + "/logs/snappuller.status");

  StringBuffer buffer = new StringBuffer();
  String mode = "";

  if (slaveinfo.canRead()) {
    // Slave instance
    mode = "Slave";
    File slavevers = new File(rootdir + "/logs/snapshot.current");
    BufferedReader inforeader = new BufferedReader(new FileReader(slaveinfo));
    BufferedReader versreader = new BufferedReader(new FileReader(slavevers));
    buffer.append("<tr>\n" +
                    "<td>\n" +
                      "Version:" +
                    "</td>\n" +
                    "<td>\n")
          .append(    versreader.readLine())
          .append(  "<td>\n" +
                    "</td>\n" +
                  "</tr>\n" +
                  "<tr>\n" +
                    "<td>\n" +
                      "Status:" +
                    "</td>\n" +
                    "<td>\n")
          .append(    inforeader.readLine())
          .append(  "</td>\n" +
                  "</tr>\n");
  } else {
    // Master instance
    mode = "Master";
    File masterdir = new File(rootdir + "/logs/clients");
    File[] clients = masterdir.listFiles();
    if (clients == null) {
      buffer.append("<tr>\n" +
                      "<td>\n" +
                      "</td>\n" +
                      "<td>\n" +
                        "No distribution info present" +
                      "</td>\n" +
                    "</tr>\n");
    } else {
      int i = 0;
      while (i < clients.length) {
        BufferedReader reader = new BufferedReader(new FileReader(clients[i]));
        buffer.append("<tr>\n" +
                        "<td>\n" +
                        "Client:" +
                        "</td>\n" +
                        "<td>\n")
              .append(    clients[i].toString())
              .append(  "</td>\n" +
                      "</tr>\n" +
                      "<tr>\n" +
                        "<td>\n" +
                        "</td>\n" +
                        "<td>\n")
              .append(    reader.readLine())
              .append(  "</td>\n" +
                      "</tr>\n" +
                      "<tr>\n" +
                      "</tr>\n");
        i++;
      }
    }
  }
%>
<html>
<head>
    <link rel="stylesheet" type="text/css" href="solr-admin.css">
    <link rel="icon" href="favicon.ico" type="image/ico">
    <link rel="shortcut icon" href="favicon.ico" type="image/ico">
</head>
<body>
<a href=""><img border="0" align="right" height="88" width="215" src="solr-head.gif" alt="SOLR"></a>
<h1>SOLR Distribution Info (<%= collectionName %>)</h1>
<%= hostname %> : <%= port.toString() %>
<br clear="all">
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
<br>
<h3><%= mode %> Status</h3>
<table>
<%= buffer %>
</table>
<br><br>
    <a href="">Return to Admin Page</a>
</body>
</html>
