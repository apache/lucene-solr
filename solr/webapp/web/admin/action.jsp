<%@ page contentType="text/html; charset=utf-8" pageEncoding="UTF-8"%>
<%--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
--%>
<%@ page import="org.apache.solr.common.util.XML"%>
<%@ page import="org.apache.solr.core.SolrCore,
                 org.apache.solr.schema.IndexSchema,
                 java.io.File,
                 java.net.InetAddress,
                 java.net.UnknownHostException"%>
<%@ page import="java.util.Date"%>
<%@include file="header.jsp" %>
<%

  String action = request.getParameter("action");
  String enableActionStatus = "";
  boolean isValid = false;

  if (action != null) {
    // Validate fname
    if ("Enable".compareTo(action) == 0) isValid = true;
    if ("Disable".compareTo(action) == 0) isValid = true;
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
  } else {
    enableActionStatus = "Illegal Action";
  }
  // :TODO: might want to rework this so any logging change happens *after*
  SolrCore.log.info(enableActionStatus);
%>
<br clear="all">
<table>
  <tr>
    <td>
      <H3>Action:</H3>
    </td>
    <td>
      <% XML.escapeCharData(action, out); %><br>
    </td>
  </tr>
  <tr>
    <td>
      <H4>Result:</H4>
    </td>
    <td>
      <% XML.escapeCharData(enableActionStatus, out); %><br>
    </td>
  </tr>
</table>
<br><br>
    <a href=".">Return to Admin Page</a>
</body>
</html>
