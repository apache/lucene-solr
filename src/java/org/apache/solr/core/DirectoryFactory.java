package org.apache.solr.core;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * Provides access to a Directory implementation. 
 * 
 */
public abstract class DirectoryFactory implements NamedListInitializedPlugin {

  /**
   * Opens a Lucene directory
   * 
   * @throws IOException
   */
  public abstract Directory open(String path) throws IOException;

  
  public void init(NamedList args) {
  }
}
