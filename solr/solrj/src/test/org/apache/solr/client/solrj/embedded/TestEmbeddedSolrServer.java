package org.apache.solr.client.solrj.embedded;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.FileUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestEmbeddedSolrServer extends LuceneTestCase {

  protected static Logger log = LoggerFactory.getLogger(TestEmbeddedSolrServer.class);
  
  protected CoreContainer cores = null;
  private File home;
  
  public String getSolrHome() {
    return "solrj/solr/shared";
  }

  public String getOrigSolrXml() {
    return "solr.xml";
  }

  public String getSolrXml() {
    return "test-solr.xml";
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("solr.solr.home", getSolrHome());
    
    home = SolrTestCaseJ4.getFile(getSolrHome());
    System.setProperty("solr.solr.home", home.getAbsolutePath());

    log.info("pwd: " + (new File(".")).getAbsolutePath());
    File origSolrXml = new File(home, getOrigSolrXml());
    File solrXml = new File(home, getSolrXml());
    FileUtils.copyFile(origSolrXml, solrXml);
    cores = new CoreContainer(home.getAbsolutePath(), solrXml);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (cores != null) {
      cores.shutdown();
    }
    File dataDir = new File(home,"data");
    if (!AbstractSolrTestCase.recurseDelete(dataDir)) {
      log.warn("!!!! WARNING: best effort to remove " + dataDir.getAbsolutePath() + " FAILED !!!!!");
    }
    super.tearDown();
  }
  
  protected EmbeddedSolrServer getSolrCore0() {
    return new EmbeddedSolrServer(cores, "core0");
  }

  protected EmbeddedSolrServer getSolrCore1() {
    return new EmbeddedSolrServer(cores, "core1");
  }
  
  public void testGetCoreContainer() {
    Assert.assertEquals(cores, getSolrCore0().getCoreContainer());
    Assert.assertEquals(cores, getSolrCore1().getCoreContainer());
  }
  
  public void testShutdown() {
    
    EmbeddedSolrServer solrServer = getSolrCore0();
    
    Assert.assertEquals(3, cores.getCores().size());
    List<SolrCore> solrCores = new ArrayList<SolrCore>();
    for (SolrCore solrCore : cores.getCores()) {
      Assert.assertEquals(false, solrCore.isClosed());
      solrCores.add(solrCore);
    }
    
    solrServer.shutdown();
    
    Assert.assertEquals(0, cores.getCores().size());
    
    for (SolrCore solrCore : solrCores) {
      Assert.assertEquals(true, solrCore.isClosed());
    }
    
  }

}
