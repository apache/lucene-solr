package org.apache.solr.handler.admin;

import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.core.SolrCore;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.response.SolrQueryResponse;

import java.util.Set;
import java.util.Map;
import java.util.HashSet;

/**
 * A request handler that provides info about all 
 * registered SolrInfoMBeans.
 */
public class SolrInfoMBeanHandler extends RequestHandlerBase {

  /**
   * Take an array of any type and generate a Set containing the toString.
   * Set is garunteed to never be null (but may be empty)
   */
  private Set<String> arrayToSet(Object[] arr) {
    HashSet<String> r = new HashSet<String>();
    if (null == arr) return r;
    for (Object o : arr) {
      if (null != o) r.add(o.toString());
    }
    return r;
  }
  

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrCore core = req.getCore();
    
    NamedList<NamedList<NamedList<Object>>> cats = new NamedList<NamedList<NamedList<Object>>>();
    rsp.add("solr-mbeans", cats);
    
    String[] requestedCats = req.getParams().getParams("cat");
    if (null == requestedCats || 0 == requestedCats.length) {
      for (SolrInfoMBean.Category cat : SolrInfoMBean.Category.values()) {
        cats.add(cat.name(), new SimpleOrderedMap<NamedList<Object>>());
      }
    } else {
      for (String catName : requestedCats) {
        cats.add(catName,new SimpleOrderedMap<NamedList<Object>>());
      }
    }
         
    Set<String> requestedKeys = arrayToSet(req.getParams().getParams("key"));
    
    Map<String, SolrInfoMBean> reg = core.getInfoRegistry();
    for (Map.Entry<String, SolrInfoMBean> entry : reg.entrySet()) {
      String key = entry.getKey();
      SolrInfoMBean m = entry.getValue();

      if ( ! ( requestedKeys.isEmpty() || requestedKeys.contains(key) ) ) continue;

      NamedList<NamedList<Object>> catInfo = cats.get(m.getCategory().name());
      if ( null == catInfo ) continue;

      NamedList<Object> mBeanInfo = new SimpleOrderedMap<Object>();
      mBeanInfo.add("class", m.getName());
      mBeanInfo.add("version", m.getVersion());
      mBeanInfo.add("description", m.getDescription());
      mBeanInfo.add("srcId", m.getSourceId());
      mBeanInfo.add("src", m.getSource());
      mBeanInfo.add("docs", m.getDocs());
      
      if (req.getParams().getFieldBool(key, "stats", false))
        mBeanInfo.add("stats", m.getStatistics());
      
      catInfo.add(key, mBeanInfo);
    }
    rsp.setHttpCaching(false); // never cache, no matter what init config looks like
  }

  public String getDescription() {
    return "Get Info (and statistics) about all registered SolrInfoMBeans";
  }

  public String getSourceId() {
    return "$Id$";
  }

  public String getSource() {
    return "$URL$";
  }

  public String getVersion() {
    return "$Revision$";
  }
}
