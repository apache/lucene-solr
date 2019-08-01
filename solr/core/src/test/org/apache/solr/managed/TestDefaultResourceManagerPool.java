package org.apache.solr.managed;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.TimeSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TestDefaultResourceManagerPool extends SolrTestCaseJ4 {

  private final int SPEED = 50;

  private ResourceManager resourceManager;

  @Before
  public void initManager() {
    resourceManager = new DefaultResourceManager(h.getCoreContainer().getResourceLoader(), TimeSource.get("simTime:" + SPEED));
  }

  @After
  public void destroyManager() {
    if (resourceManager != null) {
      IOUtils.closeQuietly(resourceManager);
      resourceManager = null;
    }
  }

  @Test
  public void testBasicRegistration() throws Exception {

  }
}
