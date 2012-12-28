package org.apache.solr.search;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.handler.component.DebugComponent;
import org.apache.solr.handler.component.FacetComponent;
import org.apache.solr.handler.component.MoreLikeThisComponent;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.handler.component.StatsComponent;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestComponentsName extends SolrTestCaseJ4{
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-components-name.xml","schema.xml");
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    assertU(delQ("*:*"));
    assertU((commit()));
  }
  
  
  @Test
  public void testComponentsName() {
    assertU(adoc("id", "0", "name", "Zapp Brannigan"));
    assertU(adoc("id", "1", "name", "The Zapper"));
    assertU((commit()));
    
    assertQ("match all docs query",
        req("q","*:*")
        ,"//result[@numFound='2']",
        "/response/str[@name='component1'][.='foo']", 
        "/response/str[@name='component2'][.='bar']");
    
    assertQ("use debugQuery",
        req("q","*:*",
            "debugQuery", "true")
        ,"//result[@numFound='2']",
        "/response/str[@name='component1'][.='foo']", 
        "/response/str[@name='component2'][.='bar']",
        "/response/lst[@name='debug']/lst[@name='timing']/lst[@name='prepare']/lst[@name='component1']",
        "/response/lst[@name='debug']/lst[@name='timing']/lst[@name='prepare']/lst[@name='" + QueryComponent.COMPONENT_NAME + "']",
        "/response/lst[@name='debug']/lst[@name='timing']/lst[@name='prepare']/lst[@name='" + FacetComponent.COMPONENT_NAME + "']",
        "/response/lst[@name='debug']/lst[@name='timing']/lst[@name='prepare']/lst[@name='" + MoreLikeThisComponent.COMPONENT_NAME + "']",
        "/response/lst[@name='debug']/lst[@name='timing']/lst[@name='prepare']/lst[@name='" + StatsComponent.COMPONENT_NAME + "']",
        "/response/lst[@name='debug']/lst[@name='timing']/lst[@name='prepare']/lst[@name='" + DebugComponent.COMPONENT_NAME + "']",
        "/response/lst[@name='debug']/lst[@name='timing']/lst[@name='prepare']/lst[@name='component2']");
  }
  
}


