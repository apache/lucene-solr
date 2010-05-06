package org.apache.solr.handler.component;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * Test for SpellCheckComponent's distributed querying
 *
 * @since solr 1.5
 * @version $Id$
 * @see org.apache.solr.handler.component.SpellCheckComponent
 */
public class DistributedSpellCheckComponentTest extends BaseDistributedSearchTestCase {
  
  private String saveProp;
  @Override
  public void setUp() throws Exception {
    // this test requires FSDir
    saveProp = System.getProperty("solr.directoryFactory");
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    super.setUp();
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (saveProp == null)
      System.clearProperty("solr.directoryFactory");
    else
      System.setProperty("solr.directoryFactory", saveProp);
  }
  
  private void q(Object... q) throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();

    for (int i = 0; i < q.length; i += 2) {
      params.add(q[i].toString(), q[i + 1].toString());
    }

    controlClient.query(params);

    // query a random server
    params.set("shards", shards);
    int which = r.nextInt(clients.size());
    SolrServer client = clients.get(which);
    client.query(params);
  }
  
  @Override
  public void doTest() throws Exception {
    index(id, "1", "lowerfilt", "toyota");
    index(id, "2", "lowerfilt", "chevrolet");
    index(id, "3", "lowerfilt", "suzuki");
    index(id, "4", "lowerfilt", "ford");
    index(id, "5", "lowerfilt", "ferrari");
    index(id, "6", "lowerfilt", "jaguar");
    index(id, "7", "lowerfilt", "mclaren");
    index(id, "8", "lowerfilt", "sonata");
    index(id, "9", "lowerfilt", "The quick red fox jumped over the lazy brown dogs.");
    index(id, "10", "lowerfilt", "blue");
    index(id, "12", "lowerfilt", "glue");
    commit();

    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);
    // we care only about the spellcheck results
    handle.put("response", SKIP);
    q("q", "*:*", SpellCheckComponent.SPELLCHECK_BUILD, "true", "qt", "spellCheckCompRH", "shards.qt", "spellCheckCompRH");
    
    query("q", "*:*", "fl", "id,lowerfilt", "spellcheck.q","toyata", "spellcheck", "true", "qt", "spellCheckCompRH", "shards.qt", "spellCheckCompRH");
    query("q", "*:*", "fl", "id,lowerfilt", "spellcheck.q","toyata", "spellcheck", "true", "qt", "spellCheckCompRH", "shards.qt", "spellCheckCompRH", SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true");
    query("q", "*:*", "fl", "id,lowerfilt", "spellcheck.q","bluo", "spellcheck", "true", "qt", "spellCheckCompRH", "shards.qt", "spellCheckCompRH", SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true", SpellCheckComponent.SPELLCHECK_COUNT, "4");
    query("q", "The quick reb fox jumped over the lazy brown dogs", "fl", "id,lowerfilt", "spellcheck", "true", "qt", "spellCheckCompRH", "shards.qt", "spellCheckCompRH", SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true", SpellCheckComponent.SPELLCHECK_COUNT, "4", SpellCheckComponent.SPELLCHECK_COLLATE, "true");
  }
}
