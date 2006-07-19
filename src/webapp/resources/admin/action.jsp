<%@ page contentType="text/html; charset=utf-8" pageEncoding="UTF-8"%>
<%@ page import="org.apache.solr.core.SolrCore,
                 org.apache.solr.schema.IndexSchema,
                 java.io.File,
                 java.net.InetAddress,
                 java.net.UnknownHostException"%>
<%@ page import="java.util.Date"%>
<%@ page import="java.util.logging.Level"%>
<%@ page import="java.util.logging.Logger"%>
<%@include file="header.jsp" %>
<%

  String action = request.getParameter("action");
  String logging = request.getParameter("log");
  String enableActionStatus = "";
  boolean isValid = false;

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
        File enableFile = new File(enabledFile);
        if (enableFile.createNewFile()) {
          enableActionStatus += "Enable Succeeded (enable file ";
          enableActionStatus += enabledFile;
          enableActionStatus += " created)";
        } else {
          enableActionStatus += "Already Enabled";
        }
      } catch(Exception e) {
          enableActionStatus += "Enable Failed: " + e.toString();
      }
    }
    if ("Disable".compareTo(action) == 0) {
      try {
        File enableFile = new File(enabledFile);
        if (enableFile.delete()) {
          enableActionStatus = "Disable Succeeded (enable file ";
          enableActionStatus += enabledFile;
          enableActionStatus += " removed)";
        } else {
          enableActionStatus = "Already Disabled";
        }
      } catch(Exception e) {
          enableActionStatus += "Disable Failed: " + e.toString();
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
      }
    }
  } else {
    enableActionStatus = "Illegal Action";
  }
  // :TODO: might want to rework this so any logging change happens *after*
  SolrCore.log.log(Level.INFO, enableActionStatus);
%>
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
<br><br>
    <a href=".">Return to Admin Page</a>
</body>
</html>
