<%@ page contentType="text/plain; charset=utf-8" pageEncoding="UTF-8" %>
<%@ page import="org.apache.solr.core.Config,
                 org.apache.solr.core.SolrConfig,
                 java.io.InputStream,
                 java.io.InputStreamReader,
                 java.io.Reader,
                 java.util.StringTokenizer"%>
<%
  String fname = request.getParameter("file");
  String optional = request.getParameter("optional");
  String gettableFiles = SolrConfig.config.get("admin/gettableFiles","");
  StringTokenizer st = new StringTokenizer(gettableFiles);
  InputStream is;
  boolean isValid = false;
  boolean isOptional = false;
  if (fname != null) {
    // Validate fname
    while(st.hasMoreTokens()) {
      if (st.nextToken().compareTo(fname) == 0) isValid = true;
    }
  }
  if (optional!=null && optional.equalsIgnoreCase("y")) {
    isOptional=true;
  }
  if (isValid) {
    try {
    is= Config.openResource(fname);
    Reader input = new InputStreamReader(is);
    char[] buf = new char[4096];
    while (true) {
      int len = input.read(buf);
      if (len<=0) break;
      out.write(buf,0,len);
    }
    }
    catch (RuntimeException re) {
      if (!isOptional) {
        throw re;
      }
    }
  } else {
    out.println("<ERROR>");
    out.println("Permission denied for file "+ fname);
    out.println("</ERROR>");
  }
%>
