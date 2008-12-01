package org.apache.solr.core;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Constants;

/**
 * Directory provider which mimics original Solr FSDirectory based behavior.
 * 
 */
public class StandardDirectoryFactory extends DirectoryFactory {

  public Directory open(String path) throws IOException {
    if (!Constants.WINDOWS) {
      return new NIOFSDirectory(new File(path), null);
    }

    return new FSDirectory(new File(path), null);
  }
}
