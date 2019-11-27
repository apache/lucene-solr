/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.core;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.NoInitialContextException;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.rest.RestManager;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.ManagedIndexSchemaFactory;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since solr 1.3
 */
public class SolrResourceLoader implements ResourceLoader, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String project = "solr";
  static final String base = "org.apache" + "." + project;
  static final String[] packages = {
      "", "analysis.", "schema.", "handler.", "handler.tagger.", "search.", "update.", "core.", "response.", "request.",
      "update.processor.", "util.", "spelling.", "handler.component.", "handler.dataimport.",
      "spelling.suggest.", "spelling.suggest.fst.", "rest.schema.analysis.", "security.", "handler.admin.",
      "cloud.autoscaling."
  };
  private static final java.lang.String SOLR_CORE_NAME = "solr.core.name";
  private static Set<String> loggedOnce = new ConcurrentSkipListSet<>();
  private static final Charset UTF_8 = StandardCharsets.UTF_8;


  private String name = "";
  protected URLClassLoader classLoader;
  private final Path instanceDir;
  private String dataDir;

  private final List<SolrCoreAware> waitingForCore = Collections.synchronizedList(new ArrayList<SolrCoreAware>());
  private final List<SolrInfoBean> infoMBeans = Collections.synchronizedList(new ArrayList<SolrInfoBean>());
  private final List<ResourceLoaderAware> waitingForResources = Collections.synchronizedList(new ArrayList<ResourceLoaderAware>());

  private final Properties coreProperties;

  private volatile boolean live;

  // Provide a registry so that managed resources can register themselves while the XML configuration
  // documents are being parsed ... after all are registered, they are asked by the RestManager to
  // initialize themselves. This two-step process is required because not all resources are available
  // (such as the SolrZkClient) when XML docs are being parsed.    
  private RestManager.Registry managedResourceRegistry;

  public synchronized RestManager.Registry getManagedResourceRegistry() {
    if (managedResourceRegistry == null) {
      managedResourceRegistry = new RestManager.Registry();
    }
    return managedResourceRegistry;
  }

  public SolrResourceLoader() {
    this(SolrResourceLoader.locateSolrHome(), null, null);
  }

  /**
   * <p>
   * This loader will delegate to the context classloader when possible,
   * otherwise it will attempt to resolve resources using any jar files
   * found in the "lib/" directory in the specified instance directory.
   * If the instance directory is not specified (=null), SolrResourceLoader#locateInstanceDir will provide one.
   */
  public SolrResourceLoader(Path instanceDir, ClassLoader parent) {
    this(instanceDir, parent, null);
  }

  public SolrResourceLoader(String name, List<Path> classpath, Path instanceDir, ClassLoader parent) throws MalformedURLException {
    this(instanceDir, parent);
    this.name = name;
    for (Path path : classpath) {
      addToClassLoader(path.toUri().normalize().toURL());
    }

  }


  public SolrResourceLoader(Path instanceDir) {
    this(instanceDir, null, null);
  }

  /**
   * <p>
   * This loader will delegate to Solr's classloader when possible,
   * otherwise it will attempt to resolve resources using any jar files
   * found in the "lib/" directory in the specified instance directory.
   * </p>
   *
   * @param instanceDir - base directory for this resource loader, if null locateSolrHome() will be used.
   * @see #locateSolrHome
   */
  public SolrResourceLoader(Path instanceDir, ClassLoader parent, Properties coreProperties) {
    if (instanceDir == null) {
      this.instanceDir = SolrResourceLoader.locateSolrHome().toAbsolutePath().normalize();
      log.debug("new SolrResourceLoader for deduced Solr Home: '{}'", this.instanceDir);
    } else {
      this.instanceDir = instanceDir.toAbsolutePath().normalize();
      log.debug("new SolrResourceLoader for directory: '{}'", this.instanceDir);
    }

    if (parent == null) {
      parent = getClass().getClassLoader();
    }
    this.classLoader = URLClassLoader.newInstance(new URL[0], parent);

    /*
     * Skip the lib subdirectory when we are loading from the solr home.
     * Otherwise load it, so core lib directories still get loaded.
     * The default sharedLib will pick this up later, and if the user has
     * changed sharedLib, then we don't want to load that location anyway.
     */
    if (!this.instanceDir.equals(SolrResourceLoader.locateSolrHome())) {
      Path libDir = this.instanceDir.resolve("lib");
      if (Files.exists(libDir)) {
        try {
          addToClassLoader(getURLs(libDir));
        } catch (IOException e) {
          log.warn("Couldn't add files from {} to classpath: {}", libDir, e.getMessage());
        }
        reloadLuceneSPI();
      }
    }
    this.coreProperties = coreProperties;
  }

  /**
   * Adds URLs to the ResourceLoader's internal classloader.  This method <b>MUST</b>
   * only be called prior to using this ResourceLoader to get any resources, otherwise
   * its behavior will be non-deterministic. You also have to {link @reloadLuceneSPI}
   * before using this ResourceLoader.
   *
   * @param urls    the URLs of files to add
   */
  void addToClassLoader(List<URL> urls) {
    URLClassLoader newLoader = addURLsToClassLoader(classLoader, urls);
    if (newLoader != classLoader) {
      this.classLoader = newLoader;
    }

    log.info("[{}] Added {} libs to classloader, from paths: {}",
        getCoreName("null"), urls.size(), urls.stream()
        .map(u -> u.getPath().substring(0,u.getPath().lastIndexOf("/")))
        .sorted()
        .distinct()
        .collect(Collectors.toList()));
  }

  private String getCoreName(String defaultVal) {
    if (getCoreProperties() != null) {
      return getCoreProperties().getProperty(SOLR_CORE_NAME, defaultVal);
    } else {
      return defaultVal;
    }
  }

  /**
   * Adds URLs to the ResourceLoader's internal classloader.  This method <b>MUST</b>
   * only be called prior to using this ResourceLoader to get any resources, otherwise
   * its behavior will be non-deterministic. You also have to {link @reloadLuceneSPI}
   * before using this ResourceLoader.
   *
   * @param urls    the URLs of files to add
   */
  void addToClassLoader(URL... urls) {
    addToClassLoader(Arrays.asList(urls));
  }
  
  /**
   * Reloads all Lucene SPI implementations using the new classloader.
   * This method must be called after {@link #addToClassLoader(List)}
   * and before using this ResourceLoader.
   */
  void reloadLuceneSPI() {
    // Codecs:
    PostingsFormat.reloadPostingsFormats(this.classLoader);
    DocValuesFormat.reloadDocValuesFormats(this.classLoader);
    Codec.reloadCodecs(this.classLoader);
    // Analysis:
    CharFilterFactory.reloadCharFilters(this.classLoader);
    TokenFilterFactory.reloadTokenFilters(this.classLoader);
    TokenizerFactory.reloadTokenizers(this.classLoader);
  }

  private static URLClassLoader addURLsToClassLoader(final URLClassLoader oldLoader, List<URL> urls) {
    if (urls.size() == 0) {
      return oldLoader;
    }

    List<URL> allURLs = new ArrayList<>();
    allURLs.addAll(Arrays.asList(oldLoader.getURLs()));
    allURLs.addAll(urls);
    for (URL url : urls) {
      log.debug("Adding '{}' to classloader", url.toString());
    }

    ClassLoader oldParent = oldLoader.getParent();
    IOUtils.closeWhileHandlingException(oldLoader);
    return URLClassLoader.newInstance(allURLs.toArray(new URL[allURLs.size()]), oldParent);
  }

  /**
   * Utility method to get the URLs of all paths under a given directory that match a filter
   *
   * @param libDir the root directory
   * @param filter the filter
   * @return all matching URLs
   * @throws IOException on error
   */
  public static List<URL> getURLs(Path libDir, DirectoryStream.Filter<Path> filter) throws IOException {
    List<URL> urls = new ArrayList<>();
    try (DirectoryStream<Path> directory = Files.newDirectoryStream(libDir, filter)) {
      for (Path element : directory) {
        urls.add(element.toUri().normalize().toURL());
      }
    }
    return urls;
  }

  /**
   * Utility method to get the URLs of all paths under a given directory
   * @param libDir the root directory
   * @return all subdirectories as URLs
   * @throws IOException on error
   */
  public static List<URL> getURLs(Path libDir) throws IOException {
    return getURLs(libDir, new DirectoryStream.Filter<Path>() {
      @Override
      public boolean accept(Path entry) throws IOException {
        return true;
      }
    });
  }

  /**
   * Utility method to get the URLs of all paths under a given directory that match a regex
   *
   * @param libDir the root directory
   * @param regex  the regex as a String
   * @return all matching URLs
   * @throws IOException on error
   */
  public static List<URL> getFilteredURLs(Path libDir, String regex) throws IOException {
    final PathMatcher matcher = libDir.getFileSystem().getPathMatcher("regex:" + regex);
    return getURLs(libDir, new DirectoryStream.Filter<Path>() {
      @Override
      public boolean accept(Path entry) throws IOException {
        return matcher.matches(entry.getFileName());
      }
    });
  }

  /**
   * Ensures a directory name always ends with a '/'.
   */
  public static String normalizeDir(String path) {
    return (path != null && (!(path.endsWith("/") || path.endsWith("\\")))) ? path + File.separator : path;
  }

  public String[] listConfigDir() {
    File configdir = new File(getConfigDir());
    if (configdir.exists() && configdir.isDirectory()) {
      return configdir.list();
    } else {
      return new String[0];
    }
  }

  public String getConfigDir() {
    return instanceDir.resolve("conf").toString();
  }

  public String getDataDir() {
    return dataDir;
  }

  public Properties getCoreProperties() {
    return coreProperties;
  }

  /**
   * EXPERT
   * <p>
   * The underlying class loader.  Most applications will not need to use this.
   *
   * @return The {@link ClassLoader}
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /**
   * Opens a schema resource by its name.
   * Override this method to customize loading schema resources.
   *
   * @return the stream for the named schema
   */
  public InputStream openSchema(String name) throws IOException {
    return openResource(name);
  }

  /**
   * Opens a config resource by its name.
   * Override this method to customize loading config resources.
   *
   * @return the stream for the named configuration
   */
  public InputStream openConfig(String name) throws IOException {
    return openResource(name);
  }

  private Path checkPathIsSafe(Path pathToCheck) throws IOException {
    if (Boolean.getBoolean("solr.allow.unsafe.resourceloading"))
      return pathToCheck;
    pathToCheck = pathToCheck.normalize();
    if (pathToCheck.startsWith(instanceDir))
      return pathToCheck;
    throw new IOException("File " + pathToCheck + " is outside resource loader dir " + instanceDir +
        "; set -Dsolr.allow.unsafe.resourceloading=true to allow unsafe loading");
  }

  /**
   * Opens any resource by its name.
   * By default, this will look in multiple locations to load the resource:
   * $configDir/$resource (if resource is not absolute)
   * $CWD/$resource
   * otherwise, it will look for it in any jar accessible through the class loader.
   * Override this method to customize loading resources.
   *
   * @return the stream for the named resource
   */
  @Override
  public InputStream openResource(String resource) throws IOException {

    Path inConfigDir = getInstancePath().resolve("conf").resolve(resource);
    if (Files.exists(inConfigDir) && Files.isReadable(inConfigDir)) {
      return Files.newInputStream(checkPathIsSafe(inConfigDir));
    }

    Path inInstanceDir = getInstancePath().resolve(resource);
    if (Files.exists(inInstanceDir) && Files.isReadable(inInstanceDir)) {
      return Files.newInputStream(checkPathIsSafe(inInstanceDir));
    }

    // Delegate to the class loader (looking into $INSTANCE_DIR/lib jars).
    // We need a ClassLoader-compatible (forward-slashes) path here!
    InputStream is = classLoader.getResourceAsStream(resource.replace(File.separatorChar, '/'));

    // This is a hack just for tests (it is not done in ZKResourceLoader)!
    // TODO can we nuke this?
    if (is == null && System.getProperty("jetty.testMode") != null) {
      is = classLoader.getResourceAsStream(("conf/" + resource).replace(File.separatorChar, '/'));
    }

    if (is == null) {
      throw new SolrResourceNotFoundException("Can't find resource '" + resource + "' in classpath or '" + instanceDir + "'");
    }
    return is;
  }

  /**
   * Report the location of a resource found by the resource loader
   */
  public String resourceLocation(String resource) {
    Path inConfigDir = getInstancePath().resolve("conf").resolve(resource);
    if (Files.exists(inConfigDir) && Files.isReadable(inConfigDir))
      return inConfigDir.toAbsolutePath().normalize().toString();

    Path inInstanceDir = getInstancePath().resolve(resource);
    if (Files.exists(inInstanceDir) && Files.isReadable(inInstanceDir))
      return inInstanceDir.toAbsolutePath().normalize().toString();

    try (InputStream is = classLoader.getResourceAsStream(resource.replace(File.separatorChar, '/'))) {
      if (is != null)
        return "classpath:" + resource;
    } catch (IOException e) {
      // ignore
    }

    return resource;
  }

  /**
   * Accesses a resource by name and returns the (non comment) lines
   * containing data.
   *
   * <p>
   * A comment line is any line that starts with the character "#"
   * </p>
   *
   * @return a list of non-blank non-comment lines with whitespace trimmed
   * from front and back.
   * @throws IOException If there is a low-level I/O error.
   */
  public List<String> getLines(String resource) throws IOException {
    return getLines(resource, UTF_8);
  }

  /**
   * Accesses a resource by name and returns the (non comment) lines containing
   * data using the given character encoding.
   *
   * <p>
   * A comment line is any line that starts with the character "#"
   * </p>
   *
   * @param resource the file to be read
   * @return a list of non-blank non-comment lines with whitespace trimmed
   * @throws IOException If there is a low-level I/O error.
   */
  public List<String> getLines(String resource,
                               String encoding) throws IOException {
    return getLines(resource, Charset.forName(encoding));
  }


  public List<String> getLines(String resource, Charset charset) throws IOException {
    try {
      return WordlistLoader.getLines(openResource(resource), charset);
    } catch (CharacterCodingException ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error loading resource (wrong encoding?): " + resource, ex);
    }
  }

  /*
   * A static map of short class name to fully qualified class name
   */
  private static final Map<String, String> classNameCache = new ConcurrentHashMap<>();

  @VisibleForTesting
  static void clearCache() {
    classNameCache.clear();
  }

  // Using this pattern, legacy analysis components from previous Solr versions are identified and delegated to SPI loader:
  private static final Pattern legacyAnalysisPattern =
      Pattern.compile("((\\Q" + base + ".analysis.\\E)|(\\Q" + project + ".\\E))([\\p{L}_$][\\p{L}\\p{N}_$]+?)(TokenFilter|Filter|Tokenizer|CharFilter)Factory");

  @Override
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
    return findClass(cname, expectedType, empty);
  }

  /**
   * This method loads a class either with its FQN or a short-name (solr.class-simplename or class-simplename).
   * It tries to load the class with the name that is given first and if it fails, it tries all the known
   * solr packages. This method caches the FQN of a short-name in a static map in-order to make subsequent lookups
   * for the same class faster. The caching is done only if the class is loaded by the webapp classloader and it
   * is loaded using a shortname.
   *
   * @param cname       The name or the short name of the class.
   * @param subpackages the packages to be tried if the cname starts with solr.
   * @return the loaded class. An exception is thrown if it fails
   */
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType, String... subpackages) {
    if (subpackages == null || subpackages.length == 0 || subpackages == packages) {
      subpackages = packages;
      String c = classNameCache.get(cname);
      if (c != null) {
        try {
          return Class.forName(c, true, classLoader).asSubclass(expectedType);
        } catch (ClassNotFoundException | ClassCastException e) {
          // this can happen if the legacyAnalysisPattern below caches the wrong thing
          log.warn( name + " Unable to load cached class, attempting lookup. name={} shortname={} reason={}", c, cname, e);
          classNameCache.remove(cname);
        }
      }
    }

    Class<? extends T> clazz = null;
    try {
      // first try legacy analysis patterns, now replaced by Lucene's Analysis package:
      final Matcher m = legacyAnalysisPattern.matcher(cname);
      if (m.matches()) {
        final String name = m.group(4);
        log.trace("Trying to load class from analysis SPI using name='{}'", name);
        try {
          if (CharFilterFactory.class.isAssignableFrom(expectedType)) {
            return clazz = CharFilterFactory.lookupClass(name).asSubclass(expectedType);
          } else if (TokenizerFactory.class.isAssignableFrom(expectedType)) {
            return clazz = TokenizerFactory.lookupClass(name).asSubclass(expectedType);
          } else if (TokenFilterFactory.class.isAssignableFrom(expectedType)) {
            return clazz = TokenFilterFactory.lookupClass(name).asSubclass(expectedType);
          } else {
            log.warn("'{}' looks like an analysis factory, but caller requested different class type: {}", cname, expectedType.getName());
          }
        } catch (IllegalArgumentException ex) {
          // ok, we fall back to legacy loading
        }
      }

      // first try cname == full name
      try {
        return clazz = Class.forName(cname, true, classLoader).asSubclass(expectedType);
      } catch (ClassNotFoundException e) {
        String newName = cname;
        if (newName.startsWith(project)) {
          newName = cname.substring(project.length() + 1);
        }
        for (String subpackage : subpackages) {
          try {
            String name = base + '.' + subpackage + newName;
            log.trace("Trying class name " + name);
            return clazz = Class.forName(name, true, classLoader).asSubclass(expectedType);
          } catch (ClassNotFoundException e1) {
            // ignore... assume first exception is best.
          }
        }

        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, name +" Error loading class '" + cname + "'", e);
      }

    } finally {
      if (clazz != null) {
        //cache the shortname vs FQN if it is loaded by the webapp classloader  and it is loaded
        // using a shortname
        if (clazz.getClassLoader() == SolrResourceLoader.class.getClassLoader() &&
            !cname.equals(clazz.getName()) &&
            (subpackages.length == 0 || subpackages == packages)) {
          //store in the cache
          classNameCache.put(cname, clazz.getName());
        }

        // print warning if class is deprecated
        if (clazz.isAnnotationPresent(Deprecated.class)) {
          log.warn("Solr loaded a deprecated plugin/analysis class [{}]. Please consult documentation how to replace it accordingly.",
              cname);
        }
      }
    }
  }

  static final String empty[] = new String[0];

  @Override
  public <T> T newInstance(String name, Class<T> expectedType) {
    return newInstance(name, expectedType, empty);
  }

  private static final Class[] NO_CLASSES = new Class[0];
  private static final Object[] NO_OBJECTS = new Object[0];

  public <T> T newInstance(String cname, Class<T> expectedType, String... subpackages) {
    return newInstance(cname, expectedType, subpackages, NO_CLASSES, NO_OBJECTS);
  }

  public CoreAdminHandler newAdminHandlerInstance(final CoreContainer coreContainer, String cname, String... subpackages) {
    Class<? extends CoreAdminHandler> clazz = findClass(cname, CoreAdminHandler.class, subpackages);
    if (clazz == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Can not find class: " + cname + " in " + classLoader);
    }

    CoreAdminHandler obj = null;
    try {
      Constructor<? extends CoreAdminHandler> ctor = clazz.getConstructor(CoreContainer.class);
      obj = ctor.newInstance(coreContainer);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error instantiating class: '" + clazz.getName() + "'", e);
    }

    if (!live) {
      //TODO: Does SolrCoreAware make sense here since in a multi-core context
      // which core are we talking about ?
      if (obj instanceof ResourceLoaderAware) {
        assertAwareCompatibility(ResourceLoaderAware.class, obj);
        waitingForResources.add((ResourceLoaderAware) obj);
      }
    }

    return obj;
  }


  public <T> T newInstance(String cName, Class<T> expectedType, String[] subPackages, Class[] params, Object[] args) {
    Class<? extends T> clazz = findClass(cName, expectedType, subPackages);
    if (clazz == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Can not find class: " + cName + " in " + classLoader);
    }

    T obj = null;
    try {

      Constructor<? extends T> constructor = null;
      try {
        constructor = clazz.getConstructor(params);
        obj = constructor.newInstance(args);
      } catch (NoSuchMethodException e) {
        //look for a zero arg constructor if the constructor args do not match
        try {
          constructor = clazz.getConstructor();
          obj = constructor.newInstance();
        } catch (NoSuchMethodException e1) {
          throw e;
        }
      }

    } catch (Error err) {
      log.error("Loading Class " + cName + " (" + clazz.getName() + ") triggered serious java error: "
          + err.getClass().getName(), err);
      throw err;

    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error instantiating class: '" + clazz.getName() + "'", e);
    }

    if (!live) {
      if (obj instanceof SolrCoreAware) {
        assertAwareCompatibility(SolrCoreAware.class, obj);
        waitingForCore.add((SolrCoreAware) obj);
      }
      if (obj instanceof ResourceLoaderAware) {
        assertAwareCompatibility(ResourceLoaderAware.class, obj);
        waitingForResources.add((ResourceLoaderAware) obj);
      }
      if (obj instanceof SolrInfoBean) {
        //TODO: Assert here?
        infoMBeans.add((SolrInfoBean) obj);
      }
    }

    return obj;
  }


  /**
   * Tell all {@link SolrCoreAware} instances about the SolrCore
   */
  public void inform(SolrCore core) {
    this.dataDir = core.getDataDir();

    // make a copy to avoid potential deadlock of a callback calling newInstance and trying to
    // add something to waitingForCore.
    SolrCoreAware[] arr;

    while (waitingForCore.size() > 0) {
      synchronized (waitingForCore) {
        arr = waitingForCore.toArray(new SolrCoreAware[waitingForCore.size()]);
        waitingForCore.clear();
      }

      for (SolrCoreAware aware : arr) {
        aware.inform(core);
      }
    }

    // this is the last method to be called in SolrCore before the latch is released.
    live = true;
  }

  /**
   * Tell all {@link ResourceLoaderAware} instances about the loader
   */
  public void inform(ResourceLoader loader) throws IOException {

    // make a copy to avoid potential deadlock of a callback adding to the list
    ResourceLoaderAware[] arr;

    while (waitingForResources.size() > 0) {
      synchronized (waitingForResources) {
        arr = waitingForResources.toArray(new ResourceLoaderAware[waitingForResources.size()]);
        waitingForResources.clear();
      }

      for (ResourceLoaderAware aware : arr) {
        aware.inform(loader);
      }
    }
  }

  /**
   * Register any {@link SolrInfoBean}s
   *
   * @param infoRegistry The Info Registry
   */
  public void inform(Map<String, SolrInfoBean> infoRegistry) {
    // this can currently happen concurrently with requests starting and lazy components
    // loading.  Make sure infoMBeans doesn't change.

    SolrInfoBean[] arr;
    synchronized (infoMBeans) {
      arr = infoMBeans.toArray(new SolrInfoBean[infoMBeans.size()]);
      waitingForResources.clear();
    }


    for (SolrInfoBean bean : arr) {
      // Too slow? I suspect not, but we may need
      // to start tracking this in a Set.
      if (!infoRegistry.containsValue(bean)) {
        try {
          infoRegistry.put(bean.getName(), bean);
        } catch (Exception e) {
          log.warn("could not register MBean '" + bean.getName() + "'.", e);
        }
      }
    }
  }

  /**
   * Determines the solrhome from the environment.
   * Tries JNDI (java:comp/env/solr/home) then system property (solr.solr.home);
   * if both fail, defaults to solr/
   * @return the instance directory name
   */
  /**
   * Finds the solrhome based on looking up the value in one of three places:
   * <ol>
   * <li>JNDI: via java:comp/env/solr/home</li>
   * <li>The system property solr.solr.home</li>
   * <li>Look in the current working directory for a solr/ directory</li>
   * </ol>
   * <p>
   * The return value is normalized.  Normalization essentially means it ends in a trailing slash.
   *
   * @return A normalized solrhome
   * @see #normalizeDir(String)
   */
  public static Path locateSolrHome() {

    String home = null;
    // Try JNDI
    try {
      Context c = new InitialContext();
      home = (String) c.lookup("java:comp/env/" + project + "/home");
      logOnceInfo("home_using_jndi", "Using JNDI solr.home: " + home);
    } catch (NoInitialContextException e) {
      log.debug("JNDI not configured for " + project + " (NoInitialContextEx)");
    } catch (NamingException e) {
      log.debug("No /" + project + "/home in JNDI");
    } catch (RuntimeException ex) {
      log.warn("Odd RuntimeException while testing for JNDI: " + ex.getMessage());
    }

    // Now try system property
    if (home == null) {
      String prop = project + ".solr.home";
      home = System.getProperty(prop);
      if (home != null) {
        logOnceInfo("home_using_sysprop", "Using system property " + prop + ": " + home);
      }
    }

    // if all else fails, try 
    if (home == null) {
      home = project + '/';
      logOnceInfo("home_default", project + " home defaulted to '" + home + "' (could not find system property or JNDI)");
    }
    return Paths.get(home);
  }

  /**
   * Solr allows users to store arbitrary files in a special directory located directly under SOLR_HOME.
   * <p>
   * This directory is generally created by each node on startup.  Files located in this directory can then be
   * manipulated using select Solr features (e.g. streaming expressions).
   */
  public static final String USER_FILES_DIRECTORY = "userfiles";

  public static void ensureUserFilesDataDir(Path solrHome) {
    final Path userFilesPath = getUserFilesPath(solrHome);
    final File userFilesDirectory = new File(userFilesPath.toString());
    if (!userFilesDirectory.exists()) {
      try {
        final boolean created = userFilesDirectory.mkdir();
        if (!created) {
          log.warn("Unable to create [{}] directory in SOLR_HOME [{}].  Features requiring this directory may fail.", USER_FILES_DIRECTORY, solrHome);
        }
      } catch (Exception e) {
        log.warn("Unable to create [" + USER_FILES_DIRECTORY + "] directory in SOLR_HOME [" + solrHome + "].  Features requiring this directory may fail.", e);
      }
    }
  }

  public static Path getUserFilesPath(Path solrHome) {
    return Paths.get(solrHome.toAbsolutePath().toString(), USER_FILES_DIRECTORY).toAbsolutePath();
  }

  // Logs a message only once per startup
  private static void logOnceInfo(String key, String msg) {
    if (!loggedOnce.contains(key)) {
      loggedOnce.add(key);
      log.info(msg);
    }
  }

  /**
   * @return the instance path for this resource loader
   */
  public Path getInstancePath() {
    return instanceDir;
  }

  /**
   * Keep a list of classes that are allowed to implement each 'Aware' interface
   */
  private static final Map<Class, Class[]> awareCompatibility;

  static {
    awareCompatibility = new HashMap<>();
    awareCompatibility.put(
        SolrCoreAware.class, new Class[]{
            // DO NOT ADD THINGS TO THIS LIST -- ESPECIALLY THINGS THAT CAN BE CREATED DYNAMICALLY
            // VIA RUNTIME APIS -- UNTILL CAREFULLY CONSIDERING THE ISSUES MENTIONED IN SOLR-8311
            CodecFactory.class,
            DirectoryFactory.class,
            ManagedIndexSchemaFactory.class,
            QueryResponseWriter.class,
            SearchComponent.class,
            ShardHandlerFactory.class,
            SimilarityFactory.class,
            SolrRequestHandler.class,
            UpdateRequestProcessorFactory.class
        }
    );

    awareCompatibility.put(
        ResourceLoaderAware.class, new Class[]{
            // DO NOT ADD THINGS TO THIS LIST -- ESPECIALLY THINGS THAT CAN BE CREATED DYNAMICALLY
            // VIA RUNTIME APIS -- UNTILL CAREFULLY CONSIDERING THE ISSUES MENTIONED IN SOLR-8311
            CharFilterFactory.class,
            TokenFilterFactory.class,
            TokenizerFactory.class,
            QParserPlugin.class,
            FieldType.class
        }
    );
  }

  /**
   * Utility function to throw an exception if the class is invalid
   */
  static void assertAwareCompatibility(Class aware, Object obj) {
    Class[] valid = awareCompatibility.get(aware);
    if (valid == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unknown Aware interface: " + aware);
    }
    for (Class v : valid) {
      if (v.isInstance(obj)) {
        return;
      }
    }
    StringBuilder builder = new StringBuilder();
    builder.append("Invalid 'Aware' object: ").append(obj);
    builder.append(" -- ").append(aware.getName());
    builder.append(" must be an instance of: ");
    for (Class v : valid) {
      builder.append("[").append(v.getName()).append("] ");
    }
    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, builder.toString());
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(classLoader);
  }

  public List<SolrInfoBean> getInfoMBeans() {
    return Collections.unmodifiableList(infoMBeans);
  }


  public static void persistConfLocally(SolrResourceLoader loader, String resourceName, byte[] content) {
    // Persist locally
    File confFile = new File(loader.getConfigDir(), resourceName);
    try {
      File parentDir = confFile.getParentFile();
      if (!parentDir.isDirectory()) {
        if (!parentDir.mkdirs()) {
          final String msg = "Can't create managed schema directory " + parentDir.getAbsolutePath();
          log.error(msg);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg);
        }
      }
      try (OutputStream out = new FileOutputStream(confFile);) {
        out.write(content);
      }
      log.info("Written confile " + resourceName);
    } catch (IOException e) {
      final String msg = "Error persisting conf file " + resourceName;
      log.error(msg, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg, e);
    } finally {
      try {
        IOUtils.fsync(confFile.toPath(), false);
      } catch (IOException e) {
        final String msg = "Error syncing conf file " + resourceName;
        log.error(msg, e);
      }
    }
  }

}
