<%@ page import="org.apache.solr.core.SolrInfoMBean,
                 org.apache.solr.core.SolrInfoRegistry,
                 java.net.URL,
                 java.util.Date,
                 java.util.Map"%>

<%@ page contentType="text/xml; charset=utf-8" pageEncoding="UTF-8" language="java" %>
<?xml-stylesheet type="text/xsl" href="registry.xsl"?>

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
