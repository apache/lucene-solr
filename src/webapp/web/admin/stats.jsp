<%@ page contentType="text/xml; charset=utf-8" pageEncoding="UTF-8" language="java" %>
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
<%@ page import="org.apache.solr.core.SolrInfoMBean,
                 org.apache.solr.core.SolrInfoRegistry,
                 org.apache.solr.common.util.NamedList,
                 java.util.Date,
                 java.util.Map"%>
<?xml-stylesheet type="text/xsl" href="stats.xsl"?>
<%@include file="_info.jsp" %>


<solr>
  <%  
  if (core.getName() != null) { %> 
	  <core><%=core.getName()%></core> 
  <% } %>
  <schema><%= collectionName %></schema>
  <host><%= hostname %></host>
  <now><%= new Date().toString() %></now>
  <start><%= new Date(core.getStartTime()) %></start>
  <solr-info>
<%
for (SolrInfoMBean.Category cat : SolrInfoMBean.Category.values()) {
%>
    <<%= cat.toString() %>>
<%
 Map<String,SolrInfoMBean> reg = core.getInfoRegistry();
 synchronized(reg) {
  for (Map.Entry<String,SolrInfoMBean> entry : reg.entrySet()) {
    String key = entry.getKey();
    SolrInfoMBean m = entry.getValue();

    if (m.getCategory() != cat) continue;

    NamedList nl = m.getStatistics();
    if ((nl != null) && (nl.size() != 0)) {
      String na     = "None Provided";
      String name   = (m.getName()!=null ? m.getName() : na);
      String vers   = (m.getVersion()!=null ? m.getVersion() : na);
      String desc   = (m.getDescription()!=null ? m.getDescription() : na);
%>
    <entry>
      <name>
        <%= key %>
      </name>
      <class>
        <%= name %>
      </class>
      <version>
        <%= vers %>
      </version>
      <description>
        <%= desc %>
      </description>
      <stats>
<%
      for (int i = 0; i < nl.size() ; i++) {
%>
        <stat name="<%= nl.getName(i) %>" >
          <%= nl.getVal(i).toString() %>
        </stat>
<%
      }
%>
      </stats>
    </entry>
<%
    }
%>
<%
  }
 }
%>
    </<%= cat.toString() %>>
<%
}
%>
  </solr-info>
</solr>
