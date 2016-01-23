package org.apache.solr.handler;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.core.SolrCore;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.response.SolrQueryResponse;

import org.apache.lucene.LucenePackage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;

/**
 * A request handler that provides System Information about the
 * current running instance and all registered SolrMBeans.
 */
public class SystemInfoRequestHandler extends RequestHandlerBase {
  static InetAddress addr = null;
  static String hostname = "unknown";
  static {
    try {
      addr = InetAddress.getLocalHost();
      hostname = addr.getCanonicalHostName();
    } catch (UnknownHostException e) {
      //default to unknown
    }
  }

  /**
   * Take an array of any type and generate a Set containing the toString.
   * Set is garunteed to never be null (but may be empty)
   */
  private Set<String> arrayToSet(Object[] arr) {
    HashSet<String> r = new HashSet<String>();
    if (null == arr) return r;
    for (Object o : arr) {
      r.add(o.toString());
    }
    return r;
  }
  

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrCore core = req.getCore();
    rsp.add("core", core.getName());
    rsp.add("schema", req.getSchema().getSchemaName());
    rsp.add("start", new Date(core.getStartTime()));
    rsp.add("now", new Date().toString());
    rsp.add("host", hostname);
    rsp.add("cwd", System.getProperty("user.dir"));
    rsp.add("instanceDir", core.getSolrConfig().getInstanceDir());

    Package solrP = SolrCore.class.getPackage();
    Package luceneP = LucenePackage.class.getPackage();
    NamedList version = new NamedList();
    version.add("solrSpecVersion", solrP.getSpecificationVersion());
    version.add("solrImplVersion", solrP.getImplementationVersion());
    version.add("luceneSpecVersion", luceneP.getSpecificationVersion());
    version.add("luceneImplVersion", luceneP.getImplementationVersion());

    rsp.add("version", version);
    
    NamedList cats = new NamedList();
    rsp.add("objects", cats);
    
    Set<String> requestedCats = arrayToSet(req.getParams().getParams("cat"));
    if (requestedCats.isEmpty()) {
      for (SolrInfoMBean.Category cat : SolrInfoMBean.Category.values()) {
        requestedCats.add(cat.name());
      }
    }
    for (String catName : requestedCats) {
      cats.add(catName,new SimpleOrderedMap());
    }
         
    Set<String> requestedKeys = arrayToSet(req.getParams().getParams("key"));
    
    Map<String, SolrInfoMBean> reg = core.getInfoRegistry();
    for (Map.Entry<String, SolrInfoMBean> entry : reg.entrySet()) {
      String key = entry.getKey();
      SolrInfoMBean m = entry.getValue();

      if ( ! ( requestedKeys.isEmpty() || requestedKeys.contains(key) ) ) continue;

      NamedList catInfo = (NamedList) cats.get(m.getCategory().name());
      if ( null == catInfo ) continue;

      NamedList mBeanInfo = new SimpleOrderedMap();
      mBeanInfo.add("class", m.getName());
      mBeanInfo.add("version", m.getVersion());
      mBeanInfo.add("description", m.getDescription());
      mBeanInfo.add("srcId", m.getSourceId());
      mBeanInfo.add("src", m.getSource());
      mBeanInfo.add("docs", arrayToSet(m.getDocs()));
      
      if (req.getParams().getFieldBool(key, "stats", false))
        mBeanInfo.add("stats", m.getStatistics());
      
      catInfo.add(key, mBeanInfo);
    }
    rsp.setHttpCaching(false); // never cache, no matter what init config looks like
  }

  public String getDescription() {
    return "Get Solr component statistics";
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
