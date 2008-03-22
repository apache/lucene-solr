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
                 java.net.URL,
                 java.util.Date,
                 java.util.Map"%>
<?xml-stylesheet type="text/xsl" href="registry.xsl"?>

<%@include file="_info.jsp" %>

<solr>
  <core><%= core.getName()%></core>
  <schema><%= collectionName %></schema>
  <host><%= hostname %></host>
  <now><%= new Date().toString() %></now>
  <start><%= new Date(core.getStartTime()) %></start>
  <solr-spec-version><%= solrSpecVersion %></solr-spec-version>
  <solr-impl-version><%= solrImplVersion %></solr-impl-version>
  <lucene-spec-version><%= luceneSpecVersion %></lucene-spec-version>
  <lucene-impl-version><%= luceneImplVersion %></lucene-impl-version>
  <solr-info>
<%
for (SolrInfoMBean.Category cat : SolrInfoMBean.Category.values()) {
%>
    <<%= cat.toString() %>>
<%
 Map<String, SolrInfoMBean> reg = SolrInfoRegistry.getRegistry();
 synchronized(reg) {
  for (Map.Entry<String,SolrInfoMBean> entry : reg.entrySet()) {
    String key = entry.getKey();
    SolrInfoMBean m = entry.getValue();

    if (m.getCategory() != cat) continue;

    String na     = "None Provided";
    String name   = (m.getName()!=null ? m.getName() : na);
    String vers   = (m.getVersion()!=null ? m.getVersion() : na);
    String desc   = (m.getDescription()!=null ? m.getDescription() : na);
    String srcId  = (m.getSourceId()!=null ? m.getSourceId() : na);
    String src = (m.getSource()!=null ? m.getSource() : na);
    // print
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
        <sourceid>
          <%= srcId %>
        </sourceid>
        <source>
          <%= src %>
        </source>

<%
    URL[] urls = m.getDocs();
    if ((urls != null) && (urls.length != 0)) {
%>
        <urls>
<%
      for (URL u : urls) {
%>
          <url>
            <%= u.toString() %>
          </url>
<%
      }
%>
        </urls>
<%
    }
%>
      </entry>
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
