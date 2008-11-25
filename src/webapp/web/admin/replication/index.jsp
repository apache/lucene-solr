<%@ page contentType="text/html; charset=utf-8" pageEncoding="UTF-8" %>
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
<%@ page import="java.util.Collection" %>
<%@ page import="java.util.Date" %>


<%-- do a verbatim include so we can use the local vars --%>
<%@include file="header.jsp"
        %>

<br clear="all">
<table>

<%

  final SolrCore solrcore = core;

%>
<%
  if ("false".equals(detailsMap.get("isMaster")))
    if (detailsMap != null) {%>
<tr>
  <td>
    <strong>Master</strong>
  </td>
  <td>
    <%
      out.println((String) detailsMap.get("masterUrl"));
    %>
  </td>
</tr>

<tr>
  <%
    NamedList nl = (NamedList) detailsMap.get("masterDetails");
    if (nl != null) {
      long masterVersion = (Long) nl.get("indexversion");
      long masterGeneration = (Long) nl.get("generation");
      long replicatableMasterVer = 0, replicatableMasterGen = 0;
      if (nl.get("replicatableindexversion") != null)
        replicatableMasterVer = (Long) nl.get("replicatableindexversion");
      if (nl.get("replicatablegeneration") != null)
        replicatableMasterGen = (Long) nl.get("replicatablegeneration");
  %>
  <td>
  </td>
  <td>Latest Index Version:<%=masterVersion%>, Generation: <%=masterGeneration%>
  </td>
</tr>

<tr>
  <td></td>
  <td>Replicatable Index Version:<%=replicatableMasterVer%>, Generation: <%=replicatableMasterGen%>
  </td>
</tr>
<%}%>

<tr>
  <td>
    <strong>Poll Interval</strong>
  </td>
  <td>
    <%
      out.println((String) detailsMap.get("pollInterval"));
    %>
  </td>
</tr>
<%}%>

<tr>
  <td>
    <strong>Local Index</strong>
  </td>
  <td>
    <%
      if (detailsMap != null)
        out.println("Index Version: " + detailsMap.get("indexversion") + ", Generation: " + detailsMap.get("generation"));
    %>
  </td>
</tr>

<tr>
  <td>
  </td>
  <td>
    <% if (null != core.getIndexDir()) {
      File dir = new File(core.getIndexDir());
      out.println("Location: " + dir.getCanonicalPath());
    }%>
  </td>
</tr>

<tr>
  <td></td>
  <td><% if (detailsMap != null)
    out.println("Size: " + detailsMap.get("indexSize"));
  %>
  </td>
</tr>

<%
  if ("true".equals(detailsMap.get("isMaster")))
    if (detailsMap != null) {
%>

<tr>
  <td></td>
  <td>
    <%out.println("Config Files To Replicate: " + detailsMap.get("confFiles"));%>
  </td>
</tr>

<tr>
  <td></td>
  <td>
    <%out.println("Trigger Replication On: " + detailsMap.get("replicateAfter")); %>
  </td>
</tr>
<%}%>

<%
  if ("false".equals(detailsMap.get("isMaster")))
    if (detailsMap != null) {%>
<tr>
  <td>
  </td>
  <td>
    <%
      out.println("Times Replicated Since Startup: " + detailsMap.get("timesIndexReplicated"));
    %>
  </td>
</tr>

<tr>
  <td>
  </td>
  <td>
    <%
      out.println("Previous Replication Done At: " + detailsMap.get("indexReplicatedAt"));
    %>
  </td>
</tr>

<tr>
  <td>
  </td>
  <td>
    <%
      out.println("Config Files Replicated At: " + detailsMap.get("confFilesReplicatedAt"));
    %>
  </td>
</tr>

<tr>
  <td>
  </td>
  <td>
    <%
      out.println("Config Files Replicated: " + detailsMap.get("confFilesReplicated"));
    %>
  </td>
</tr>

<tr>
  <td>
  </td>
  <td>
    <%
      out.println("Times Config Files Replicated Since Startup: " + detailsMap.get("timesConfigReplicated"));
    %>
  </td>
</tr>

<tr>
  <td>
  </td>
  <td>
    <%
      if (detailsMap.get("nextExecutionAt") != null)
        if (detailsMap.get("nextExecutionAt") != "")
          out.println("Next Replication Cycle At: " + detailsMap.get("nextExecutionAt"));
        else if ("true".equals(detailsMap.get("isPollingDisabled")))
          out.println("Next Replication Cycle At: Polling disabled.");
        else {
          NamedList nl1 = (NamedList) detailsMap.get("masterDetails");
          out.println("Next Replication Cycle At: After " + nl1.get("replicateAfter") + " on master.");
        }
    %>
  </td>
</tr>

<%
  if ("true".equals(detailsMap.get("isReplicating"))) {
%>
<tr>
  <td><strong>Current Replication Status</strong>

  <td>
    <%out.println("Start Time: " + detailsMap.get("replicationStartTime"));%>
  </td>
</tr>

<tr>
  <td></td>
  <td>
    <%
      out.println("Files Downloaded: " + detailsMap.get("numFilesDownloaded") + " / " + detailsMap.get("numFilesToDownload"));%>
  </td>
</tr>

<tr>
  <td></td>
  <td>
    <%
      out.println("Downloaded: " + detailsMap.get("bytesDownloaded") + " / " + detailsMap.get("bytesToDownload") + " [" + detailsMap.get("totalPercent") + "%]");%>
  </td>
</tr>

<tr>
  <td></td>
  <td>
    <%
      out.println("Downloading File: " + detailsMap.get("currentFile") + ", Downloaded: " + detailsMap.get("currentFileSizeDownloaded") + " / " + detailsMap.get("currentFileSize") + " [" + detailsMap.get("currentFileSizePercent") + "%]");%>
  </td>
</tr>

<tr>
  <td></td>
  <td>
    <%
      out.println("Time Elapsed: " + detailsMap.get("timeElapsed") + ", Estimated Time Remaining: " + detailsMap.get("timeRemaining") + ", Speed: " + detailsMap.get("downloadSpeed") + "/s");%>
  </td>
</tr>
<%}%>

<tr>
  <td><strong>Controls</strong>
  </td>
  <td><%
    String pollVal = request.getParameter("poll");
    if (pollVal != null)
      if (pollVal.equals("disable"))
        executeCommand("disablepoll", core, rh);
      else if (pollVal.equals("enable"))
        executeCommand("enablepoll", core, rh);
    if ("false".equals(detailsMap.get("isPollingDisabled"))) {
  %>

    <form name=polling method="POST" action="./index.jsp" accept-charset="UTF-8">
      <input name="poll" type="hidden" value="disable">
      <input class="stdbutton" type="submit" value="Disable Poll">
    </form>

    <%}%>
    <%
      if ("true".equals(detailsMap.get("isPollingDisabled"))) {
    %>

    <form name=polling method="POST" action="./index.jsp" accept-charset="UTF-8">
      <input name="poll" type="hidden" value="enable">
      <input class="stdbutton" type="submit" value="Enable Poll">
    </form>
    <%
      }
    %>

  </td>
</tr>

<tr>
  <td></td>
  <td>
    <form name=replicate method="POST" action="./index.jsp" accept-charset="UTF-8">
      <input name="replicate" type="hidden" value="now">
      <input name="replicateButton" class="stdbutton" type="submit" value="Replicate Now">
    </form>
    <%
      if ("true".equals(detailsMap.get("isReplicating"))) {
    %>
    <script type="text/javascript">
      document["replicate"].replicateButton.disabled = true;
      document["replicate"].replicateButton.className = 'stdbuttondis';
    </script>
    <form name=abort method="POST" action="./index.jsp" accept-charset="UTF-8">
      <input name="abort" type="hidden" value="stop">
      <input name="abortButton" class="stdbutton" type="submit" value="Abort">
    </form>

    <%} else {%>
    <script type="text/javascript">
      document["replicate"].replicateButton.disabled = false;
      document["replicate"].replicateButton.className = 'stdbutton';
    </script>
    <%
      }
      String replicateParam = request.getParameter("replicate");
      String abortParam = request.getParameter("abort");
      if (replicateParam != null)
        if (replicateParam.equals("now")) {
          executeCommand("snappull", solrcore, rh);
        }
      if (abortParam != null)
        if (abortParam.equals("stop")) {
          executeCommand("abortsnappull", solrcore, rh);
        }
    %>
  </td>

</tr>

<%}%>

<%-- List the cores (that arent this one) so we can switch --%>
<% org.apache.solr.core.CoreContainer cores = (org.apache.solr.core.CoreContainer) request.getAttribute("org.apache.solr.CoreContainer");
  if (cores != null) {
    Collection<String> names = cores.getCoreNames();
    if (names.size() > 1) {%>
<tr>
  <td><strong>Cores:</strong><br></td>
  <td><%
    for (String name : names) {
  %>[<a href="../../<%=name%>/admin/"><%=name%>
  </a>]<%
    }%></td>
</tr>
<%
    }
  }%>


</table>
<P>

<p>

<table>
  <tr>
    <td>
    </td>
    <td>
      Current Time: <%= new Date() %>
    </td>
  </tr>
  <tr>
    <td>
    </td>
    <td>
      Server Start At: <%= new Date(core.getStartTime()) %>
    </td>
  </tr>
</table>

<br>
<a href="..">Return to Admin Page</a>
</body>
</html>
