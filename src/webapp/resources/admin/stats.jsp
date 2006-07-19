<%@ page contentType="text/xml; charset=utf-8" pageEncoding="UTF-8" language="java" %>
<%@ page import="org.apache.solr.core.SolrInfoMBean,
                 org.apache.solr.core.SolrInfoRegistry,
                 org.apache.solr.util.NamedList,
                 java.util.Date,
                 java.util.Map"%>
<?xml-stylesheet type="text/xsl" href="stats.xsl"?>
<%@include file="_info.jsp" %>


<solr>
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
 Map<String,SolrInfoMBean> reg = SolrInfoRegistry.getRegistry();
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
