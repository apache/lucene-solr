package org.apache.lucene.index;

import java.io.FileNotFoundException;

/**
 * Signals that no index was found in the Directory. Possibly because the
 * directory is empty, however can slso indicate an index corruption.
 */
public final class IndexNotFoundException extends FileNotFoundException {

  public IndexNotFoundException(String msg) {
    super(msg);
  }

}
