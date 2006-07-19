<%@ page contentType="text/html; charset=utf-8" pageEncoding="UTF-8"%>
<%@ page import="org.apache.solr.core.SolrCore,
                 org.apache.solr.schema.IndexSchema,
                 java.io.File,
                 java.net.InetAddress,
                 java.net.UnknownHostException"%>
<%@ page import="java.util.Date"%>
<%@ page import="java.util.logging.Level"%>
<%@ page import="java.util.logging.LogManager"%>
<%@ page import="java.util.logging.Logger"%>
<%@include file="header.jsp" %>
<%

  LogManager mgr = LogManager.getLogManager();
  Logger log = SolrCore.log;

  Logger parent = log.getParent();
  while(parent != null) {
    log = parent;
    parent = log.getParent();
  }
  Level lvl = log.getLevel();
      
%>
<br clear="all">
<h2>Solr Logging</h2>
<table>
  <tr>
    <td>
      <H3>Log Level:</H3>
    </td>
    <td>
<% if (lvl!=null) {%>
      <%= lvl.toString() %><br>
<% } else { %>
      null<br>
<% } %>
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
<br><br>
    <a href=".">Return to Admin Page</a>
</body>
</html>
