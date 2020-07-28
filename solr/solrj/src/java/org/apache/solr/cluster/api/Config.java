package org.apache.solr.cluster.api;

import java.io.InputStream;
import java.util.function.Consumer;

public interface Config {

  String name();

  /** read a file inside the config.
   * The caller should consume the stream before the call returns. This method closes
   * the stream soon after the method returns
   * @param file  name of the file e.g: schema.xml
   */
  void  resource(Consumer<InputStream> file);
}
