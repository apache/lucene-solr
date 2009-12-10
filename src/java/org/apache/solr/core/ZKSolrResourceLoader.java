package org.apache.solr.core;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;

public class ZKSolrResourceLoader extends SolrResourceLoader {

  private ZooKeeperController zooKeeperController;

 
  String collection;

  public ZKSolrResourceLoader(String instanceDir, String collection,
      ZooKeeperController zooKeeperController) {
    super(instanceDir);
    this.zooKeeperController = zooKeeperController;
    this.collection = collection;
  }

  /**
   * <p>
   * This loader will delegate to the context classloader when possible,
   * otherwise it will attempt to resolve resources using any jar files found in
   * the "lib/" directory in the specified instance directory. If the instance
   * directory is not specified (=null), SolrResourceLoader#locateInstanceDir
   * will provide one.
   * <p>
   */
  public ZKSolrResourceLoader(String instanceDir, String collection, ClassLoader parent,
      Properties coreProperties, ZooKeeperController zooKeeperController) {
    super(instanceDir, parent, coreProperties);
    this.zooKeeperController = zooKeeperController;
    this.collection = collection;
  }

  /**
   * Opens any resource by its name. By default, this will look in multiple
   * locations to load the resource: $configDir/$resource (if resource is not
   * absolute) $CWD/$resource otherwise, it will look for it in any jar
   * accessible through the class loader. Override this method to customize
   * loading resources.
   * 
   * @return the stream for the named resource
   */
  public InputStream openResource(String resource) {
    InputStream is = null;
    String file = getConfigDir() + "/" + resource; //nocommit: getConfigDir no longer makes sense here
    //nocommit:
    System.out.println("look for:" + file);
    try {
      if (zooKeeperController.exists(file)) {
        byte[] bytes = zooKeeperController.getFile(getConfigDir(), resource);
        return new ByteArrayInputStream(bytes);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error opening " + file, e);
    }
    try {
      // delegate to the class loader (looking into $INSTANCE_DIR/lib jars)
      is = classLoader.getResourceAsStream(resource);
    } catch (Exception e) {
      throw new RuntimeException("Error opening " + resource, e);
    }
    if (is == null) {
      throw new RuntimeException("Can't find resource '" + resource
          + "' in classpath or '" + getConfigDir() + "', cwd="
          + System.getProperty("user.dir"));
    }
    return is;
  }

  // nocommit: deal with code that uses this call to load the file itself (elevation?)
  public String getConfigDir() {
    return "/configs/" + collection;
  }
}
