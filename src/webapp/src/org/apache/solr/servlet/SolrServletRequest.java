package org.apache.solr.servlet;

import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.StrUtils;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.Set;

/**
 * @author yonik
 * @version $Id$
 */
class SolrServletRequest extends SolrQueryRequestBase {

  final HttpServletRequest req;

  public SolrServletRequest(SolrCore core, HttpServletRequest req) {
    super(core);
    this.req = req;
  }

  public String getParam(String name) {
    return req.getParameter(name);
  }

  public String[] getParams(String name) {
    return req.getParameterValues(name);
  }


  public String getParamString() {
    StringBuilder sb = new StringBuilder(128);
    try {
      boolean first=true;

      for (Map.Entry<String,String[]> entry : (Set<Map.Entry<String,String[]>>)req.getParameterMap().entrySet()) {
        String key = entry.getKey();
        String[] valarr = entry.getValue();

        for (String val : valarr) {
          if (!first) sb.append('&');
          first=false;
          sb.append(key);
          sb.append('=');
          StrUtils.partialURLEncodeVal(sb, val==null ? "" : val);
        }
      }
    }
    catch (Exception e) {
      // should never happen... we only needed this because
      // partialURLEncodeVal can throw an IOException, but it
      // never will when adding to a StringBuilder.
      throw new RuntimeException(e);
    }

    return sb.toString();
  }

}
