<%@ page contentType="text/html; charset=utf-8" pageEncoding="UTF-8"%>
<%@ page import="org.apache.solr.core.SolrCore,
                 org.apache.solr.schema.IndexSchema"%>
<%@ page import="java.io.InputStreamReader"%>
<%@ page import="java.io.Reader"%>
<%@ page contentType="text/plain;charset=UTF-8" language="java" %>
<%
  SolrCore core = SolrCore.getSolrCore();
  IndexSchema schema = core.getSchema();
  Reader input = new InputStreamReader(schema.getInputStream());
  char[] buf = new char[4096];
  while (true) {
    int len = input.read(buf);
    if (len<=0) break;
    out.write(buf,0,len);
  }
%>