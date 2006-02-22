<%@ page import="org.apache.solr.core.SolrConfig,
                 org.apache.solr.core.SolrCore,
                 org.apache.solr.schema.IndexSchema,
                 java.io.File"%>
<%@ page import="java.net.InetAddress"%>

<%
  SolrCore core = SolrCore.getSolrCore();
  int port = request.getServerPort();
  IndexSchema schema = core.getSchema();

  // enabled/disabled is purely from the point of a load-balancer
  // and has no effect on local server function.  If there is no healthcheck
  // configured, don't put any status on the admin pages.
  String enabledStatus = null;
  String enabledFile = SolrConfig.config.get("admin/healthcheck/text()",null);
  boolean isEnabled = false;
  if (enabledFile!=null) {
    isEnabled = new File(enabledFile).exists();
  }

  String collectionName = schema!=null ? schema.getName():"unknown";
  InetAddress addr = InetAddress.getLocalHost();
  String hostname = addr.getCanonicalHostName();

  String defaultSearch = SolrConfig.config.get("admin/defaultQuery/text()",null);
  String cwd=System.getProperty("user.dir");
%>
