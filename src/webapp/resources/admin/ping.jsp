<%@ page import="org.apache.solr.core.SolrConfig,
                 org.apache.solr.core.SolrCore,
                 org.apache.solr.core.SolrException"%>
<%@ page import="org.apache.solr.request.LocalSolrQueryRequest"%>
<%@ page import="org.apache.solr.request.SolrQueryResponse"%>
<%@ page import="java.util.StringTokenizer"%>
<%
  SolrCore core = SolrCore.getSolrCore();

  String queryArgs = (request.getQueryString() == null) ?
      SolrConfig.config.get("admin/pingQuery","") : request.getQueryString();
  StringTokenizer qtokens = new StringTokenizer(queryArgs,"&");
  String tok;
  String query = null;
  while (qtokens.hasMoreTokens()) {
    tok = qtokens.nextToken();
    String[] split = tok.split("=");
    if (split[0].startsWith("q")) {
      query = split[1];
    }
  }
  LocalSolrQueryRequest req = new LocalSolrQueryRequest(core, query,null,0,1,LocalSolrQueryRequest.emptyArgs);
  SolrQueryResponse resp = new SolrQueryResponse();
  try {
    core.execute(req,resp);
    if (resp.getException() != null) {
      response.sendError(500, SolrException.toStr(resp.getException()));
    }
  } catch (Throwable t) {
      response.sendError(500, SolrException.toStr(t));
  } finally {
      req.close();
  }
%>
