<%@ page import="org.apache.solr.core.SolrCore,
                 org.apache.solr.schema.IndexSchema,
                 java.io.File,
                 java.net.InetAddress,
                 java.net.UnknownHostException"%>
<%@ page import="java.util.Date"%>
<%--
  Created by IntelliJ IDEA.
  User: yonik
  Date: Oct 14, 2004
  Time: 2:40:56 PM
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/xml;charset=UTF-8" language="java" %>

<?xml-stylesheet type="text/xsl" href="/admin/status.xsl"?>

<%
  SolrCore core = SolrCore.getSolrCore();
  IndexSchema schema = core.getSchema();
  String collectionName = schema!=null ? schema.getName():"unknown";

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
<solr>
  <schema><%= collectionName %></schema>
  <host><%= hostname %> : <%= request.getServerPort() %></host>
  <now><%= new Date().toString() %></now>
  <start><%= startTime %></start>
  <status>
    <cvsId><%= core.cvsId %></cvsId>
    <cvsSource><%= core.cvsSource %></cvsSource>
    <cvsTag><%= core.cvsTag %></cvsTag>
    <state>IN_SERVICE</state>
    <schemaFile>schema.xml</schemaFile>
    <schemaName><%= schema.getName() %></schemaName>
    <indexDir><%= core.getDataDir() %></indexDir>
    <maxDoc><%= core.maxDoc() %></maxDoc>
  </status>
</solr>
