package org.apache.solr.rest.admin;


import com.google.inject.Inject;
import org.apache.solr.core.JmxMonitoredMap;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.request.SolrQueryRequestDecoder;
import org.apache.solr.rest.APIMBean;
import org.apache.solr.rest.BaseResource;
import org.restlet.resource.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Provides information about the LWMBeans, with the exception of statistics, as that may be more expensive.
 */
public class InfoSR extends BaseResource implements InfoResource {
  private transient static Logger log = LoggerFactory.getLogger(InfoSR.class);

  public Map<String, Object> infoMap;

  @Inject
  public InfoSR(SolrQueryRequestDecoder requestDecoder, JmxMonitoredMap<String, APIMBean> jmx) throws IOException {
    super(requestDecoder);

    File versionFile = new File(System.getProperty("user.dir"), "VERSION.txt");
    log.info("Looking for Information properties in " + versionFile);
    Properties props = null;
    if (versionFile.exists()) {
      props = new Properties();
      props.load(new FileReader(versionFile));
      infoMap = new HashMap<String, Object>(props.size());
      for (Map.Entry<Object, Object> entry : props.entrySet()) {
        infoMap.put(entry.getKey().toString(), entry.getValue());

      }
    } else {
      //a small development easter egg
      infoMap = new HashMap<String, Object>();
      infoMap.put("status", "development");
      for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
        infoMap.put(entry.getKey().toString(), entry.getValue());
      }
      for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
        infoMap.put(entry.getKey(), entry.getValue());
      }
    }
    for (Map.Entry<String, SolrInfoMBean> entry : jmx.entrySet()) {
      Map<String, Object> beanInfoMap = new HashMap<String, Object>();
      infoMap.put(entry.getValue().getName(), beanInfoMap);
      beanInfoMap.put("description", entry.getValue().getDescription());
      if (entry.getValue() instanceof APIMBean) {
        beanInfoMap.put("endpoints", ((APIMBean) entry.getValue()).getEndpoints());
      }
    }
  }



  @Override
  @Get
  public Map<String, Object> info() {
    return infoMap;
  }
}
