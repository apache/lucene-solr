package org.apache.solr.handler.component;

import java.io.File;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.AbstractSolrTestCase;

public class DistributedSpellCollatorTest extends BaseDistributedSearchTestCase {
  
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
    index(id, "1", "lowerfilt", "The quick red fox jumped over the lazy brown dogs.");
    index(id, "2" , "lowerfilt", "The quack rex fox jumped over the lazy brown dogs.");
    index(id, "3" , "lowerfilt", "The quote rex fox jumped over the lazy brown dogs.");
    index(id, "4" , "lowerfilt", "The quote redo fox jumped over the lazy brown dogs.");
    index(id, "5" , "lowerfilt", "The quote redo fox jumped over the lazy brown dogs.");
    index(id, "6" , "lowerfilt", "The quote redo fox jumped over the lazy brown dogs.");
    index(id, "7" , "lowerfilt", "The quote redo fox jumped over the lazy brown dogs.");
    index(id, "8" , "lowerfilt", "The quote redo fox jumped over the lazy brown dogs.");
    index(id, "9" , "lowerfilt", "The quote redo fox jumped over the lazy brown dogs.");
    index(id, "10", "lowerfilt", "The quote redo fox jumped over the lazy brown dogs.");
    index(id, "11", "lowerfilt", "The quote redo fox jumped over the lazy brown dogs.");
    index(id, "12", "lowerfilt", "The quote redo fox jumped over the lazy brown dogs.");
    index(id, "13", "lowerfilt", "The quote redo fox jumped over the lazy brown dogs.");
    index(id, "14", "lowerfilt", "The quote redo fox jumped over the lazy brown dogs.");
    index(id, "15", "lowerfilt", "The quote redo fox jumped over the lazy brown dogs.");    
    commit();

    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);
    // we care only about the spellcheck results
    handle.put("response", SKIP);
    q("q", "*:*", SpellCheckComponent.SPELLCHECK_BUILD, "true", "qt", "spellCheckCompRH", "shards.qt", "spellCheckCompRH");
        
    query("q", "lowerfilt:(+quock +reb)", "fl", "id,lowerfilt", "spellcheck", "true", "qt", "spellCheckCompRH", "shards.qt", "spellCheckCompRH", SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true", SpellCheckComponent.SPELLCHECK_COUNT, "10", SpellCheckComponent.SPELLCHECK_COLLATE, "true", SpellCheckComponent.SPELLCHECK_MAX_COLLATION_TRIES, "10", SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "10", SpellCheckComponent.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "true");
    query("q", "lowerfilt:(+quock +reb)", "fl", "id,lowerfilt", "spellcheck", "true", "qt", "spellCheckCompRH", "shards.qt", "spellCheckCompRH", SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true", SpellCheckComponent.SPELLCHECK_COUNT, "10", SpellCheckComponent.SPELLCHECK_COLLATE, "true", SpellCheckComponent.SPELLCHECK_MAX_COLLATION_TRIES, "10", SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "10", SpellCheckComponent.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "false");
    query("q", "lowerfilt:(+quock +reb)", "fl", "id,lowerfilt", "spellcheck", "true", "qt", "spellCheckCompRH", "shards.qt", "spellCheckCompRH", SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true", SpellCheckComponent.SPELLCHECK_COUNT, "10", SpellCheckComponent.SPELLCHECK_COLLATE, "true", SpellCheckComponent.SPELLCHECK_MAX_COLLATION_TRIES, "0", SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "1", SpellCheckComponent.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "false");
    
		// Ensure that each iteration of test uses a fresh Jetty data directory.
		// Otherwise we get incorrect # hits
		// This probably should be fixed in BaseDistributedSearch in its own issue,
		// but I needed this test to pass now...
		AbstractSolrTestCase.recurseDelete(testDir);
		testDir = new File(System.getProperty("java.io.tmpdir")
				+ System.getProperty("file.separator") + getClass().getName() + "-"
				+ System.currentTimeMillis());
		testDir.mkdirs();
  }
}
