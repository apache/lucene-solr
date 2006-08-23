package org.apache.solr.request;

import javax.servlet.ServletRequest;

/**
 * @author yonik
 * @version $Id$
 */
public class ServletSolrParams extends MultiMapSolrParams {
  public ServletSolrParams(ServletRequest req) {
    super(req.getParameterMap());
  }

  public String get(String name) {
    String[] arr = map.get(name);
    if (arr==null) return null;
    String s = arr[0];
    if (s.length()==0) return null;  // screen out blank parameters
    return s;
  }
}
