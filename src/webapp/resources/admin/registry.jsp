<%@ page import="org.apache.solr.core.SolrCore,
                 org.apache.solr.core.SolrInfoMBean,
                 org.apache.solr.core.SolrInfoRegistry,
                 org.apache.solr.schema.IndexSchema,
                 java.io.File,
                 java.net.InetAddress,
                 java.net.URL"%>
<%@ page import="java.net.UnknownHostException"%>
<%@ page import="java.util.Date"%>
<%@ page import="java.util.Map"%>

<%@ page contentType="text/xml;charset=UTF-8" language="java" %>
<?xml-stylesheet type="text/xsl" href="/admin/registry.xsl"?>

<%
  SolrCore core = SolrCore.getSolrCore();
  IndexSchema schema = core.getSchema();
  String collectionName = schema!=null ? schema.getName():"unknown";
  Map<String, SolrInfoMBean> reg = SolrInfoRegistry.getRegistry();

  String rootdir = "/var/opt/resin3/"+request.getServerPort();
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

    String na     = "None Provided";
    String name   = (m.getName()!=null ? m.getName() : na);
    String vers   = (m.getVersion()!=null ? m.getVersion() : na);
    String desc   = (m.getDescription()!=null ? m.getDescription() : na);
    String cvsId  = (m.getCvsId()!=null ? m.getCvsId() : na);
    String cvsSrc = (m.getCvsSource()!=null ? m.getCvsSource() : na);
    String cvsTag = (m.getCvsName()!=null ? m.getCvsName() : na);
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
        <cvsid>
          <%= cvsId %>
        </cvsid>
        <cvssrc>
          <%= cvsSrc %>
        </cvssrc>
        <cvstag>
          <%= cvsTag %>
        </cvstag>
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
