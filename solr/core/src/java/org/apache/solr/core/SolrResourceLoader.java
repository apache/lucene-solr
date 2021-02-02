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

import java.io.Closeable;
import java.io.File;
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
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import net.sf.saxon.Configuration;
import net.sf.saxon.xpath.XPathFactoryImpl;
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
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.rest.RestManager;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.FieldTypePluginLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchemaFactory;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.SystemIdResolver;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;

/**
 * @since solr 1.3
 */
public class SolrResourceLoader implements ResourceLoader, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String base = "org.apache.solr";
  private static final String[] packages = {
      "", "analysis.", "schema.", "handler.", "handler.tagger.", "search.", "update.", "core.", "response.", "request.",
      "update.processor.", "util.", "spelling.", "handler.component.", "handler.dataimport.",
      "spelling.suggest.", "spelling.suggest.fst.", "rest.schema.analysis.", "security.", "handler.admin."
  };
  private static final Charset UTF_8 = StandardCharsets.UTF_8;
  public static final URL[] EMPTY_URL_ARRAY = new URL[0];
  private static XPathFactoryImpl xpathFactory;
  private final SystemIdResolver sysIdResolver;

  public static XPathFactoryImpl getXpathFactory() {
    return xpathFactory;
  }

  static String luceneMatchVersionPath = "/config/" + IndexSchema.LUCENE_MATCH_VERSION_PARAM;
  static String indexDefaultsPath = "/config/indexDefaults";
  static String mainIndexPath = "/config/mainIndex";
  static String nrtModePath = "/config/indexConfig/nrtmode";
  static String unlockOnStartupPath = "/config/indexConfig/unlockOnStartup";

  static String shardHandlerFactoryPath = "solr/shardHandlerFactory";
  static String counterExpPath = "solr/metrics/suppliers/counter";
  static String meterPath = "solr/metrics/suppliers/meter";
  static String timerPath = "solr/metrics/suppliers/timer";
  static String histoPath = "solr/metrics/suppliers/histogram";
  static String historyPath = "solr/metrics/history";
  static String  transientCoreCacheFactoryPath =  "solr/transientCoreCacheFactory";
  static String  tracerConfigPath = "solr/tracerConfig";

  static String  coreLoadThreadsPath = "solr/@coreLoadThreads";
  static String  persistentPath = "solr/@persistent";
  static String  sharedLibPath = "solr/@sharedLib";
  static String  zkHostPath = "solr/@zkHost";
  static String  coresPath = "solr/cores";

  public static String schemaNamePath = IndexSchema.stepsToPath(IndexSchema.SCHEMA, IndexSchema.AT + IndexSchema.NAME);
  public static String schemaVersionPath = "/schema/@version";

  public static String copyFieldPath = "//" + IndexSchema.COPY_FIELD;

  public static String fieldTypeXPathExpressions = IndexSchema.getFieldTypeXPathExpressions();

  public static String schemaSimPath = IndexSchema.stepsToPath(IndexSchema.SCHEMA, IndexSchema.SIMILARITY); //   /schema/similarity

  public static String defaultSearchFieldPath = IndexSchema.stepsToPath(IndexSchema.SCHEMA, "defaultSearchField", IndexSchema.TEXT_FUNCTION);

  public static String solrQueryParserDefaultOpPath = IndexSchema.stepsToPath(IndexSchema.SCHEMA, "solrQueryParser", IndexSchema.AT + "defaultOperator");

  public static String schemaUniqueKeyPath = IndexSchema.stepsToPath(IndexSchema.SCHEMA, IndexSchema.UNIQUE_KEY, IndexSchema.TEXT_FUNCTION);


  public static XPathExpression shardHandlerFactoryExp;
  public static XPathExpression counterExp;
  public static XPathExpression meterExp;
  public static XPathExpression timerExp;
  public static XPathExpression histoExp;
  public static XPathExpression historyExp;
  public static XPathExpression transientCoreCacheFactoryExp;
  public static XPathExpression tracerConfigExp;

  public static XPathExpression coreLoadThreadsExp;
  public static XPathExpression persistentExp;
  public static XPathExpression sharedLibExp;
  public static XPathExpression zkHostExp;
  public static XPathExpression coresExp;

  public static XPathExpression xpathOrExp;
  public static XPathExpression schemaNameExp;
  public static XPathExpression schemaVersionExp;
  public static XPathExpression schemaSimExp;
  public static XPathExpression defaultSearchFieldExp;
  public static XPathExpression solrQueryParserDefaultOpExp;
  public static XPathExpression schemaUniqueKeyExp;
  public static XPathExpression fieldTypeXPathExpressionsExp;
  public static XPathExpression copyFieldsExp;

  public static XPathExpression luceneMatchVersionExp;
  public static XPathExpression indexDefaultsExp;
  public static XPathExpression mainIndexExp;
  public static XPathExpression nrtModeExp;
  public static XPathExpression unlockOnStartupExp;

  private final Configuration ourConf;

  {
    Configuration conf = Configuration.newConfiguration();
    conf.setNamePool(this.conf.getNamePool());
    conf.setDocumentNumberAllocator(this.conf.getDocumentNumberAllocator());
    conf.setXIncludeAware(true);
    conf.setExpandAttributeDefaults(false);
    conf.setValidation(false);
    ourConf = conf;
  }

  static volatile Configuration conf;
  static {
    refreshConf();
  }

  public static void refreshConf() {
    try {
      if (conf != null) {
        conf.close();
      }
      conf = Configuration.newConfiguration();

      conf.setValidation(false);
      conf.setXIncludeAware(true);
      conf.setExpandAttributeDefaults(true);

      xpathFactory = new XPathFactoryImpl(conf);

      XPath xpath = xpathFactory.newXPath();

      luceneMatchVersionExp = xpath.compile(luceneMatchVersionPath);

      indexDefaultsExp = xpath.compile(indexDefaultsPath);

      mainIndexExp = xpath.compile(mainIndexPath);

      nrtModeExp = xpath.compile(nrtModePath);

      unlockOnStartupExp = xpath.compile(unlockOnStartupPath);

      shardHandlerFactoryExp = xpath.compile(shardHandlerFactoryPath);

      counterExp = xpath.compile(counterExpPath);

      meterExp = xpath.compile(meterPath);

      timerExp = xpath.compile(timerPath);

      histoExp = xpath.compile(histoPath);

      historyExp = xpath.compile(historyPath);

      transientCoreCacheFactoryExp = xpath.compile(transientCoreCacheFactoryPath);

      tracerConfigExp = xpath.compile(tracerConfigPath);

      coreLoadThreadsExp = xpath.compile(coreLoadThreadsPath);

      persistentExp = xpath.compile(persistentPath);

      sharedLibExp = xpath.compile(sharedLibPath);

      zkHostExp = xpath.compile(zkHostPath);

      coresExp = xpath.compile(coresPath);

      String expression =
          IndexSchema.stepsToPath(IndexSchema.SCHEMA, IndexSchema.FIELD) + IndexSchema.XPATH_OR + IndexSchema.stepsToPath(IndexSchema.SCHEMA, IndexSchema.DYNAMIC_FIELD) + IndexSchema.XPATH_OR
              + IndexSchema.stepsToPath(IndexSchema.SCHEMA, IndexSchema.FIELDS, IndexSchema.FIELD) + IndexSchema.XPATH_OR + IndexSchema
              .stepsToPath(IndexSchema.SCHEMA, IndexSchema.FIELDS, IndexSchema.DYNAMIC_FIELD);
      xpathOrExp = xpath.compile(expression);

      schemaNameExp = xpath.compile(schemaNamePath);

      schemaVersionExp = xpath.compile(schemaVersionPath);

      schemaSimExp = xpath.compile(schemaSimPath);

      defaultSearchFieldExp = xpath.compile(defaultSearchFieldPath);

      solrQueryParserDefaultOpExp = xpath.compile(solrQueryParserDefaultOpPath);

      schemaUniqueKeyExp = xpath.compile(schemaUniqueKeyPath);

      fieldTypeXPathExpressionsExp = xpath.compile(fieldTypeXPathExpressions);

      copyFieldsExp = xpath.compile(copyFieldPath);

      FieldTypePluginLoader.refreshConf();

    } catch (Exception e) {
      log.error("", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  private String name = "";
  protected volatile URLClassLoader classLoader;
  protected volatile URLClassLoader resourceClassLoader;
  private final Path instanceDir;

  private final Set<SolrCoreAware> waitingForCore = ConcurrentHashMap.newKeySet(64);
  private final Set<SolrInfoBean> infoMBeans = ConcurrentHashMap.newKeySet(64);
  private final Set<ResourceLoaderAware> waitingForResources = ConcurrentHashMap.newKeySet(64);

  // Provide a registry so that managed resources can register themselves while the XML configuration
  // documents are being parsed ... after all are registered, they are asked by the RestManager to
  // initialize themselves. This two-step process is required because not all resources are available
  // (such as the SolrZkClient) when XML docs are being parsed.    
  private RestManager.Registry managedResourceRegistry;

  /** @see #reloadLuceneSPI() */
  private volatile boolean needToReloadLuceneSPI = false; // requires synchronization


  public RestManager.Registry getManagedResourceRegistry() {
    if (managedResourceRegistry == null) {
      synchronized (this) {
        if (managedResourceRegistry == null) {
          managedResourceRegistry = new RestManager.Registry();
        }
      }
    }
    return managedResourceRegistry;
  }

  public SolrResourceLoader() {
    this(SolrPaths.locateSolrHome(), null);
  }

  /**
   * Creates a loader.
   * Note: we do NOT call {@link #reloadLuceneSPI()}.
   */
  public SolrResourceLoader(String name, List<Path> classpath, Path instanceDir, ClassLoader parent) {
    this(instanceDir, parent);
    this.name = name;



    final List<URL> libUrls = new ArrayList<>(classpath.size());
    try {
      for (Path path : classpath) {
        libUrls.add(path.toUri().normalize().toURL());
      }
    } catch (MalformedURLException e) { // impossible?
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    addToClassLoader(libUrls);
  }


  public SolrResourceLoader(Path instanceDir) {
    this(instanceDir, null);
  }

  /**
   * This loader will delegate to Solr's classloader when possible,
   * otherwise it will attempt to resolve resources using any jar files
   * found in the "lib/" directory in the specified instance directory.
   *
   * @param instanceDir - base directory for this resource loader, if null locateSolrHome() will be used.
   * @see SolrPaths#locateSolrHome()
   */
  public SolrResourceLoader(Path instanceDir, ClassLoader parent) {
    if (instanceDir == null) {
      this.instanceDir = SolrPaths.locateSolrHome().toAbsolutePath().normalize();
      log.debug("new SolrResourceLoader for deduced Solr Home: '{}'", this.instanceDir);
    } else {
      this.instanceDir = instanceDir.toAbsolutePath().normalize();
      log.debug("new SolrResourceLoader for directory: '{}'", this.instanceDir);
    }

    if (parent == null) {
      parent = getClass().getClassLoader();
    }
    this.classLoader = URLClassLoader.newInstance(EMPTY_URL_ARRAY, parent);
    this.resourceClassLoader = URLClassLoader.newInstance(EMPTY_URL_ARRAY, parent);
    this.sysIdResolver = new SystemIdResolver(this);
    // TODO: workout the leak in CollectionsAPIAsyncDistrbutedZkTest
    // assert ObjectReleaseTracker.track(this);
  }

  public SystemIdResolver getSysIdResolver() {
    return sysIdResolver;
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
    URLClassLoader newResourceClassLoader = addURLsToClassLoader(resourceClassLoader, urls);
    if (newLoader == classLoader) {
      return; // short-circuit
    }

    this.classLoader = newLoader;
    this.resourceClassLoader = newResourceClassLoader;
    this.needToReloadLuceneSPI = true;

    if (log.isInfoEnabled()) {
      log.info("Added {} libs to classloader, from paths: {}",
          urls.size(), urls.stream()
              .map(u -> u.getPath().substring(0, u.getPath().lastIndexOf("/")))
              .sorted()
              .distinct()
              .collect(Collectors.toList()));
    }
  }

  public Configuration getConf() {
    return ourConf;
  }

  /**
   * Reloads all Lucene SPI implementations using the new classloader.
   * This method must be called after {@link #addToClassLoader(List)}
   * and before using this ResourceLoader.
   */
  synchronized void reloadLuceneSPI() {
    if (!Boolean.getBoolean("solr.reloadSPI")) {
      return;
    }

    // TODO improve to use a static Set<URL> to check when we need to
    if (!needToReloadLuceneSPI) {
      return;
    }
    needToReloadLuceneSPI = false; // reset
    log.debug("Reloading Lucene SPI");

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
      if (log.isDebugEnabled()) {
        log.debug("Adding '{}' to classloader", url);
      }
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

  public String getConfigDir() {
    return instanceDir.resolve("conf").toString();
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
    if (Files.exists(inConfigDir)) {
      return Files.newInputStream(checkPathIsSafe(inConfigDir));
    }

    Path inInstanceDir = getInstancePath().resolve(resource);
    if (Files.exists(inInstanceDir)) {
      return Files.newInputStream(checkPathIsSafe(inInstanceDir));
    }

    // Delegate to the class loader (looking into $INSTANCE_DIR/lib jars).
    // We need a ClassLoader-compatible (forward-slashes) path here!
    InputStream is = resourceClassLoader.getResourceAsStream(resource.replace(File.separatorChar, '/'));

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
    if (Files.exists(inConfigDir))
      return inConfigDir.toAbsolutePath().normalize().toString();

    Path inInstanceDir = getInstancePath().resolve(resource);
    if (Files.exists(inInstanceDir))
      return inInstanceDir.toAbsolutePath().normalize().toString();

    try (InputStream is = resourceClassLoader.getResourceAsStream(resource.replace(File.separatorChar, '/'))) {
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
  private final Map<String, String> classNameCache = new ConcurrentHashMap<>(256, 0.75f, 24);

  @VisibleForTesting
   void clearCache() {
    classNameCache.clear();
  }

  // Using this pattern, legacy analysis components from previous Solr versions are identified and delegated to SPI loader:
  private static final Pattern legacyAnalysisPattern =
      Pattern.compile("((\\Q" + base + ".analysis.\\E)|(\\Qsolr.\\E))([\\p{L}_$][\\p{L}\\p{N}_$]+?)(TokenFilter|Filter|Tokenizer|CharFilter)Factory");

  @Override
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
    return findClass(cname, expectedType, empty);
  }

  public static final ImmutableMap<String,String> SHORT_NAMES;
  static {
    try (InputStream stream = Utils.class.getClassLoader().getResource("ShortClassNames.properties").openStream()) {
      Properties prop = new Properties();
      prop.load(stream);
      ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();
      for (Map.Entry<Object, Object> e : prop.entrySet()) builder.put(e.getKey().toString(), e.getValue().toString());
      SHORT_NAMES = builder.build();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Resource error: " + e.getMessage(), e);
    }
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
    if (!cname.startsWith("solr.") && cname.contains(".")) {
      //this is the fully qualified class name
      try {
        return Class.forName(cname, true, classLoader).asSubclass(expectedType);
      } catch (ClassNotFoundException e) {

        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, name +" Error loading class '" + cname + "'", e);
      }
    }

    String trans = SHORT_NAMES.get(cname);
    if (trans != null) {
      //A short name is provided
      try {
        return Class.forName(trans, true, classLoader).asSubclass(expectedType);
      } catch (ClassNotFoundException e) {

        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, name +" Error loading class '" + cname + "'", e);
      }
    }

    if (subpackages == null || subpackages.length == 0 || subpackages == packages) {
      subpackages = packages;
      String c = classNameCache.get(cname);
      if (c != null) {
        try {
          return Class.forName(c, true, classLoader).asSubclass(expectedType);
        } catch (ClassNotFoundException | ClassCastException e) {
          // this can happen if the legacyAnalysisPattern below caches the wrong thing
          log.warn("{} Unable to load cached class, attempting lookup. name={} shortname={} reason={}", name , c, cname, e);
          classNameCache.remove(cname);
        }
      }
    }

    Class<? extends T> clazz = null;
    try {
      // first try cname == full name

      String newName = cname;
      if (newName.startsWith("solr")) {
        newName = cname.substring("solr".length() + 1);
      }

      for (String subpackage : subpackages) {
        try {
          String name = base + '.' + subpackage + newName;
          log.trace("Trying class name {}", name);

          clazz = Class.forName(name, true, classLoader).asSubclass(expectedType);
          //assert false : printMapEntry(cname, name);
          return clazz;
        } catch (ClassNotFoundException e1) {
          // ignore... assume first exception is best.
        }

      }

      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, name + " Error loading class '" + cname + "'" + " subpackages=" + Arrays.asList(subpackages));

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

  private String printMapEntry(String cname, String name) {
    return "map.put(\"" + cname + "\", \"" + name + "\");";
  }

  static final String[] empty = new String[0];

  @Override
  public <T> T newInstance(String name, Class<T> expectedType) {
    return newInstance(name, expectedType, empty);
  }

  @SuppressWarnings({"rawtypes"})
  private static final Class[] NO_CLASSES = new Class[0];
  private static final Object[] NO_OBJECTS = new Object[0];

  public <T> T newInstance(String cname, Class<T> expectedType, String... subpackages) {
    return newInstance(cname, expectedType, subpackages, NO_CLASSES, NO_OBJECTS);
  }

  @SuppressWarnings({"rawtypes"})
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
      log.error("Loading Class {} ({}) triggered serious java error: {}", cName, clazz.getName(),
          err.getClass().getName(), err);

      throw err;

    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Error instantiating class: '" + clazz.getName() + "'", e);
    }

    addToCoreAware(obj);
    addToResourceLoaderAware(obj);
    addToInfoBeans(obj);
    return obj;
  }

  public <T> void addToInfoBeans(T obj) {
    if (obj instanceof SolrInfoBean) {
      //TODO: Assert here?
      infoMBeans.add((SolrInfoBean) obj);
    }
  }

  public <T> boolean addToResourceLoaderAware(T obj) {
    if (obj instanceof ResourceLoaderAware) {
      assertAwareCompatibility(ResourceLoaderAware.class, obj);
      waitingForResources.add((ResourceLoaderAware) obj);
    }
    return true;
  }

  /** the inform() callback should be invoked on the listener.
   *
   */
  public <T> boolean addToCoreAware(T obj) {
    if (obj instanceof SolrCoreAware) {
      assertAwareCompatibility(SolrCoreAware.class, obj);
      waitingForCore.add((SolrCoreAware) obj);
    }
    return true;
  }


  /**
   * Tell all {@link SolrCoreAware} instances about the SolrCore
   */
  public void inform(SolrCore core) {
    while (waitingForCore.size() > 0) {
      try (ParWork worker = new ParWork(this, false, true)) {
        waitingForCore.forEach(aware -> {
          worker.collect("informSolrCore", ()-> {
            try {
              aware.inform(core);
            } catch (Exception e) {
              ParWork.propagateInterrupt("Exception informing for SolrCore", e);
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception informing for SolrCore: " + e.getMessage(), e);
            }
            waitingForCore.remove(aware);
          });
        });
      }
    }
  }

  /**
   * Tell all {@link ResourceLoaderAware} instances about the loader
   */
  public void inform(ResourceLoader loader) {
    while (waitingForResources.size() > 0) {
      try (ParWork worker = new ParWork(this, false, true)) {
        waitingForResources.forEach(r -> {
          worker.collect("informResourceLoader", ()-> {
            try {
              r.inform(loader);
            } catch (Exception e) {
              ParWork.propagateInterrupt("Exception informing for ResourceLoader", e);
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception informing for ResourceLoader", e);
            }
            waitingForResources.remove(r);
          });
        });
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
    // loading

    while (infoMBeans.size() > 0) {

      try (ParWork worker = new ParWork(this, false, true)) {
        infoMBeans.forEach(imb -> {
          worker.collect("informInfoRegistry", ()-> {
              try {
                infoRegistry.put(imb.getName(), imb);
                infoMBeans.remove(imb);
              } catch (Exception e) {
                ParWork.propagateInterrupt(e);
                log.warn("could not register MBean '" + imb.getName() + "'.", e);
              }
          });
        });
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
   * @return the instance path for this resource loader
   */
  public Path getInstancePath() {
    return instanceDir;
  }

  /**
   * Keep a list of classes that are allowed to implement each 'Aware' interface
   */
  @SuppressWarnings({"rawtypes"})
  private static final Map<Class, Class[]> awareCompatibility;

  static {
    awareCompatibility = new HashMap<>();
    awareCompatibility.put(
        SolrCoreAware.class, new Class<?>[]{
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
        ResourceLoaderAware.class, new Class<?>[]{
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
  @SuppressWarnings({"rawtypes"})
  public static void assertAwareCompatibility(Class aware, Object obj) {
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
    IOUtils.close(resourceClassLoader);
    assert ObjectReleaseTracker.release(this);
  }

  public Set<SolrInfoBean> getInfoMBeans() {
    return Collections.unmodifiableSet(infoMBeans);
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
      try (OutputStream out = Files.newOutputStream(confFile.toPath(), StandardOpenOption.CREATE)) {
        out.write(content);
      }
      log.info("Written confile {}", resourceName);
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

  public XPath getXPath() {
    return getXpathFactory().newXPath();
  }
}
