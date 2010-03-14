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
<%@include file="header.jsp"%>

<br clear="all" />
(<a href="http://wiki.apache.org/solr/SolrReplication">What Is This Page?</a>)
<br clear="all" />
<table>

<%

  final SolrCore solrcore = core;

%>
<%
NamedList slave = null, master = null;
if (detailsMap != null)
   if ("true".equals(detailsMap.get("isSlave")))
       if(detailsMap.get("slave") != null){
           slave = (NamedList)detailsMap.get("slave");%>
<tr>
  <td>
    <strong>Master</strong>
  </td>
  <td>
    <%=slave.get("masterUrl")%>
    <%
    NamedList nl = (NamedList) slave.get("masterDetails");
    if(nl == null)
    	out.print(" - <b>Unreachable</b>");
    %>
  </td>
</tr>
<%
    if (nl != null) {         
      nl = (NamedList) nl.get("master");
      if(nl != null){      
  %>
<tr>  
  <td>
  </td>
  <td>Latest Index Version:<%=nl.get("indexVersion")%>, Generation: <%=nl.get("generation")%>
  </td>
</tr>
<tr>
  <td></td>
  <td>Replicatable Index Version:<%=nl.get("replicatableIndexVersion")%>, Generation: <%=nl.get("replicatableGeneration")%>
  </td>
</tr>
<%
}
}%>

<tr>
  <td>
    <strong>Poll Interval</strong>
  </td>
  <td>
    <%=slave.get("pollInterval")%>
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
        out.println("Index Version: " + detailsMap.get("indexVersion") + ", Generation: " + detailsMap.get("generation"));
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
  if (detailsMap != null)
    if ("true".equals(detailsMap.get("isMaster"))) 
       if(detailsMap.get("master") != null){
           master = (NamedList) detailsMap.get("master");
%>

<tr>
  <td></td>
  <td>
    <%out.println("Config Files To Replicate: " + master.get("confFiles"));%>
  </td>
</tr>

<tr>
  <td></td>
  <td>
    <%out.println("Trigger Replication On: " + master.get("replicateAfter")); %>
  </td>
</tr>
<%}%>

<%
  if ("true".equals(detailsMap.get("isSlave")))
    if (slave != null) {%>
<tr>
  <td>
  </td>
  <td>
    <%
      out.println("Times Replicated Since Startup: " + slave.get("timesIndexReplicated"));
    %>
  </td>
</tr>

<tr>
  <td>
  </td>
  <td>
    <%
      out.println("Previous Replication Done At: " + slave.get("indexReplicatedAt"));
    %>
  </td>
</tr>

<tr>
  <td>
  </td>
  <td>
    <%
      out.println("Config Files Replicated At: " + slave.get("confFilesReplicatedAt"));
    %>
  </td>
</tr>

<tr>
  <td>
  </td>
  <td>
    <%
      out.println("Config Files Replicated: " + slave.get("confFilesReplicated"));
    %>
  </td>
</tr>

<tr>
  <td>
  </td>
  <td>
    <%
      out.println("Times Config Files Replicated Since Startup: " + slave.get("timesConfigReplicated"));
    %>
  </td>
</tr>

<tr>
  <td>
  </td>
  <td>
    <%
      if (slave.get("nextExecutionAt") != null)
        if (slave.get("nextExecutionAt") != "")
          out.println("Next Replication Cycle At: " + slave.get("nextExecutionAt"));
        else if ("true".equals(slave.get("isPollingDisabled")))
          out.println("Next Replication Cycle At: Polling disabled.");
        else {
          NamedList nl1 = (NamedList) slave.get("masterDetails");
          if(nl1 != null){
          	NamedList nl2 = (NamedList) nl1.get("master");
          	if(nl2 != null)
          		out.println("Next Replication Cycle At: After " + nl2.get("replicateAfter") + " on master.");
          }
        }
    %>
  </td>
</tr>

<%
  if ("true".equals(slave.get("isReplicating"))) {
%>
<tr>
  <td><strong>Current Replication Status</strong>

  <td>
    <%out.println("Start Time: " + slave.get("replicationStartTime"));%>
  </td>
</tr>

<tr>
  <td></td>
  <td>
    <%
      out.println("Files Downloaded: " + slave.get("numFilesDownloaded") + " / " + slave.get("numFilesToDownload"));%>
  </td>
</tr>

<tr>
  <td></td>
  <td>
    <%
      out.println("Downloaded: " + slave.get("bytesDownloaded") + " / " + slave.get("bytesToDownload") + " [" + slave.get("totalPercent") + "%]");%>
  </td>
</tr>

<tr>
  <td></td>
  <td>
    <%
      out.println("Downloading File: " + slave.get("currentFile") + ", Downloaded: " + slave.get("currentFileSizeDownloaded") + " / " + slave.get("currentFileSize") + " [" + slave.get("currentFileSizePercent") + "%]");%>
  </td>
</tr>

<tr>
  <td></td>
  <td>
    <%
      out.println("Time Elapsed: " + slave.get("timeElapsed") + ", Estimated Time Remaining: " + slave.get("timeRemaining") + ", Speed: " + slave.get("downloadSpeed") + "/s");%>
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
    if(slave != null)
    	if ("false".equals(slave.get("isPollingDisabled"))) {
  %>

    <form name=polling method="POST" action="./index.jsp" accept-charset="UTF-8">
      <input name="poll" type="hidden" value="disable">
      <input class="stdbutton" type="submit" value="Disable Poll">
    </form>

    <%}%>
    <%
      if(slave != null)
      	if ("true".equals(slave.get("isPollingDisabled"))) {
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
      if(slave != null)
      	if ("true".equals(slave.get("isReplicating"))) {
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
          executeCommand("fetchindex", solrcore, rh);
        }
      if (abortParam != null)
        if (abortParam.equals("stop")) {
          executeCommand("abortfetch", solrcore, rh);
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
  %>[<a href="../../../<%=name%>/admin/index.jsp"><%=name%>
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
