<%@ page import="java.util.Date"%>
<%--
  Created by IntelliJ IDEA.
  User: yonik
  Date: Oct 14, 2004
  Time: 2:40:56 PM
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/xml;charset=UTF-8" language="java" %>

<?xml-stylesheet type="text/xsl" href="status.xsl"?>

<%@include file="_info.jsp" %>

<solr>
  <schema><%= collectionName %></schema>
  <host><%= hostname %> : <%= request.getServerPort() %></host>
  <now><%= new Date().toString() %></now>
  <start><%= new Date(core.getStartTime()) %></start>
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
