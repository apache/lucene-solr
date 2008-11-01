package org.apache.solr.request;

import org.apache.velocity.runtime.resource.loader.ResourceLoader;
import org.apache.velocity.runtime.resource.Resource;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.commons.collections.ExtendedProperties;
import org.apache.solr.core.SolrResourceLoader;

import java.io.InputStream;

// TODO: the name of this class seems ridiculous
public class SolrVelocityResourceLoader extends ResourceLoader {
  private SolrResourceLoader loader;

  public SolrVelocityResourceLoader(SolrResourceLoader loader) {
    super();
    this.loader = loader;
  }

  public void init(ExtendedProperties extendedProperties) {
  }

  public InputStream getResourceStream(String template_name) throws ResourceNotFoundException {
    return loader.openResource(template_name);
  }

  public boolean isSourceModified(Resource resource) {
    return false;
  }

  public long getLastModified(Resource resource) {
    return 0;
  }
}
