package org.apache.solr.core;

import java.io.IOException;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.util.AbstractSolrTestCase;

public class AlternateDirectoryTest extends AbstractSolrTestCase {

  public String getSchemaFile() {
    return "schema.xml";
  }

  public String getSolrConfigFile() {
    return "solrconfig-altdirectory.xml";
  }

  /**
   * Simple test to ensure that alternate IndexReaderFactory is being used.
   * 
   * @throws Exception
   */
  public void testAltDirectoryUsed() throws Exception {
    assertTrue(TestFSDirectoryFactory.openCalled);
  }

  static public class TestFSDirectoryFactory extends DirectoryFactory {
    public static boolean openCalled = false;

    public FSDirectory open(String path) throws IOException {
      openCalled = true;
      return FSDirectory.getDirectory(path);
    }

  }

}
