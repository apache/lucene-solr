package org.apache.lucene.index.store;

import junit.framework.TestCase;
import org.apache.lucene.store.FSDirectory;
import java.io.IOException;

abstract public class FSDirectoryTestCase extends TestCase {
  private FSDirectory directory;

  protected final FSDirectory getDirectory() throws IOException {
    return getDirectory(false);
  }

  protected final FSDirectory getDirectory(boolean create) throws IOException {
    if (directory == null) {
      directory = FSDirectory.getDirectory(System.getProperty("test.index.dir"), create);
    }

    return directory;
  }
}
