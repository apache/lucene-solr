<%@ page import="org.apache.solr.core.SolrCore,
                 org.apache.solr.core.SolrInfoMBean,
                 org.apache.solr.core.SolrInfoRegistry,
                 org.apache.solr.schema.IndexSchema,
                 org.apache.solr.util.NamedList,
                 java.io.File"%>
<%@ page import="java.net.InetAddress"%>
<%@ page import="java.net.UnknownHostException"%>
<%@ page import="java.util.Date"%>
<%@ page import="java.util.Map"%>

<%@ page contentType="text/xml;charset=UTF-8" language="java" %>
<?xml-stylesheet type="text/xsl" href="stats.xsl"?>

<%
  SolrCore core = SolrCore.getSolrCore();
  Integer port = new Integer(request.getServerPort());
  IndexSchema schema = core.getSchema();
  String collectionName = schema!=null ? schema.getName():"unknown";
  Map<String, SolrInfoMBean> reg = SolrInfoRegistry.getRegistry();

  String rootdir = "/var/opt/resin3/"+port.toString();
  File pidFile = new File(rootdir + "/logs/resin.pid");
  String startTime = "";

  try {
    startTime = (pidFile.lastModified() > 0)
                  ? new Date(pidFile.lastModified()).toString()
                  : "No Resin Pid found (logs/resin.pid)";
  } catch (Exception e) {
    out.println("<ERROR>");
    out.println("Couldn't open Solr pid file:" + e.toString());
    out.println("</ERROR>");
  }

  String hostname="localhost";
  try {
    InetAddress addr = InetAddress.getLocalHost();
    // Get IP Address
    byte[] ipAddr = addr.getAddress();
    // Get hostname
    // hostname = addr.getHostName();
    hostname = addr.getCanonicalHostName();
  } catch (UnknownHostException e) {}
%>

<solr>
  <schema><%= collectionName %></schema>
  <host><%= hostname %></host>
  <now><%= new Date().toString() %></now>
  <start><%= startTime %></start>
  <solr-info>
<%
for (SolrInfoMBean.Category cat : SolrInfoMBean.Category.values()) {
%>
    <<%= cat.toString() %>>
<%
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
