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

import com.google.common.annotations.VisibleForTesting;
import java.io.*;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.util.*;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.XMLErrorLogger;
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
import org.apache.solr.util.SystemIdResolver;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.xerces.jaxp.DocumentBuilderFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * @since solr 1.3
 */
public class SolrResourceLoader implements ResourceLoader, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final XMLErrorLogger xmllog = new XMLErrorLogger(log);

  private static final String base = "org.apache.solr";
  private static final String[] packages = {
      "", "analysis.", "schema.", "handler.", "handler.tagger.", "search.", "update.", "core.", "response.", "request.",
      "update.processor.", "util.", "spelling.", "handler.component.", "handler.dataimport.",
      "spelling.suggest.", "spelling.suggest.fst.", "rest.schema.analysis.", "security.", "handler.admin.",
      "cloud.autoscaling."
  };
  private static final Charset UTF_8 = StandardCharsets.UTF_8;

  public static final javax.xml.parsers.DocumentBuilderFactory dbf;

  protected final static ThreadLocal<DocumentBuilder> THREAD_LOCAL_DB= new ThreadLocal<>();
  static {
    dbf = new DocumentBuilderFactoryImpl();
    try {
      dbf.setXIncludeAware(true);
      dbf.setNamespaceAware(true);
      dbf.setValidating(false);
      trySetDOMFeature(dbf, XMLConstants.FEATURE_SECURE_PROCESSING, true);
    } catch(UnsupportedOperationException e) {
      log.warn("XML parser doesn't support XInclude option");
    }
  }

  private final SystemIdResolver sysIdResolver;

  private static void trySetDOMFeature(DocumentBuilderFactory factory, String feature, boolean enabled) {
    try {
      factory.setFeature(feature, enabled);
    } catch (Exception ex) {
      ParWork.propegateInterrupt(ex);
      // ignore
    }
  }

  private String name = "";
  protected URLClassLoader classLoader;
  protected URLClassLoader resourceClassLoader;
  private final Path instanceDir;

  private final Set<SolrCoreAware> waitingForCore = ConcurrentHashMap.newKeySet(5000);
  private final Set<SolrInfoBean> infoMBeans = ConcurrentHashMap.newKeySet(5000);
  private final Set<ResourceLoaderAware> waitingForResources = ConcurrentHashMap.newKeySet(5000);

  private volatile boolean live;

  // Provide a registry so that managed resources can register themselves while the XML configuration
  // documents are being parsed ... after all are registered, they are asked by the RestManager to
  // initialize themselves. This two-step process is required because not all resources are available
  // (such as the SolrZkClient) when XML docs are being parsed.    
  private RestManager.Registry managedResourceRegistry;

  /** @see #reloadLuceneSPI() */
  private boolean needToReloadLuceneSPI = false; // requires synchronization

  public synchronized RestManager.Registry getManagedResourceRegistry() {
    if (managedResourceRegistry == null) {
      managedResourceRegistry = new RestManager.Registry();
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
    this.classLoader = URLClassLoader.newInstance(new URL[0], parent);
    this.resourceClassLoader = URLClassLoader.newInstance(new URL[0], parent);
    this.sysIdResolver = new SystemIdResolver(this);
  }

  public synchronized DocumentBuilder getDocumentBuilder() {
    DocumentBuilder db = THREAD_LOCAL_DB.get();
    if (db == null) {
      try {
        db = dbf.newDocumentBuilder();
      } catch (ParserConfigurationException e) {
        log.error("Error in parser configuration", e);
        throw new RuntimeException(e);
      }
      db.setErrorHandler(xmllog);
      THREAD_LOCAL_DB.set(db);
    }
    db.setEntityResolver(sysIdResolver);
    return db;
  }

  /**
   * Adds URLs to the ResourceLoader's internal classloader.  This method <b>MUST</b>
   * only be called prior to using this ResourceLoader to get any resources, otherwise
   * its behavior will be non-deterministic. You also have to {link @reloadLuceneSPI}
   * before using this ResourceLoader.
   *
   * @param urls    the URLs of files to add
   */
  synchronized void addToClassLoader(List<URL> urls) {
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
    if (Files.exists(inConfigDir) && Files.isReadable(inConfigDir)) {
      return Files.newInputStream(checkPathIsSafe(inConfigDir));
    }

    Path inInstanceDir = getInstancePath().resolve(resource);
    if (Files.exists(inInstanceDir) && Files.isReadable(inInstanceDir)) {
      return Files.newInputStream(checkPathIsSafe(inInstanceDir));
    }

    // Delegate to the class loader (looking into $INSTANCE_DIR/lib jars).
    // We need a ClassLoader-compatible (forward-slashes) path here!
    InputStream is = resourceClassLoader.getResourceAsStream(resource.replace(File.separatorChar, '/'));

    // This is a hack just for tests (it is not done in ZKResourceLoader)!
    // TODO can we nuke this?
    if (is == null && System.getProperty("jetty.testMode") != null) {
      is = resourceClassLoader.getResourceAsStream(("conf/" + resource).replace(File.separatorChar, '/'));
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

  private static final Map<String,String> TRANS_MAP;
  static {
    Map<String,String> map = new HashMap<>(128);
    map.put("solr.WordDelimiterGraphFilterFactory","org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilterFactory");
    map.put("solr.LowerCaseFilterFactory", "org.apache.lucene.analysis.core.LowerCaseFilterFactory");
    map.put("solr.FlattenGraphFilterFactory", "org.apache.lucene.analysis.core.FlattenGraphFilterFactory");
    map.put("solr.StopFilterFactory", "org.apache.lucene.analysis.core.StopFilterFactory");
    map.put("solr.SynonymGraphFilterFactory", "org.apache.lucene.analysis.synonym.SynonymGraphFilterFactory");
    map.put("solr.KeywordMarkerFilterFactory", "org.apache.lucene.analysis.miscellaneous.KeywordMarkerFilterFactory");
    map.put("solr.PorterStemFilterFactory", "org.apache.lucene.analysis.en.PorterStemFilterFactory");
    map.put("solr.DelimitedBoostTokenFilterFactory", "org.apache.lucene.analysis.boost.DelimitedBoostTokenFilterFactory");
    map.put("solr.LetterTokenizerFactory", "org.apache.lucene.analysis.core.LetterTokenizerFactory");
    map.put("solr.StandardTokenizerFactory", "org.apache.lucene.analysis.standard.StandardTokenizerFactory");
    map.put("solr.HTMLStripCharFilterFactory", "org.apache.lucene.analysis.charfilter.HTMLStripCharFilterFactory");
    map.put("solr.MappingCharFilterFactory", "org.apache.lucene.analysis.charfilter.MappingCharFilterFactory");
    map.put("solr.PatternReplaceFilterFactory", "org.apache.lucene.analysis.pattern.PatternReplaceFilterFactory");
    map.put("solr.LengthFilterFactory", "org.apache.lucene.analysis.miscellaneous.LengthFilterFactory");
    map.put("solr.RemoveDuplicatesTokenFilterFactory", "org.apache.lucene.analysis.miscellaneous.RemoveDuplicatesTokenFilterFactory");
    map.put("solr.WhitespaceTokenizerFactory", "org.apache.lucene.analysis.core.WhitespaceTokenizerFactory");
    map.put("solr.ShingleFilterFactory", "org.apache.lucene.analysis.shingle.ShingleFilterFactory");
    map.put("solr.WordDelimiterFilterFactory", "org.apache.lucene.analysis.miscellaneous.WordDelimiterFilterFactory");
    map.put("solr.PatternTokenizerFactory", "org.apache.lucene.analysis.pattern.PatternTokenizerFactory");
    map.put("solr.KeywordTokenizerFactory", "org.apache.lucene.analysis.core.KeywordTokenizerFactory");
    map.put("solr.PathHierarchyTokenizerFactory", "org.apache.lucene.analysis.path.PathHierarchyTokenizerFactory");
    map.put("solr.DelimitedPayloadTokenFilterFactory", "org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilterFactory");
    map.put("solr.EnglishMinimalStemFilterFactory", "org.apache.lucene.analysis.en.EnglishMinimalStemFilterFactory");
    map.put("solr.TrimFilterFactory", "org.apache.lucene.analysis.miscellaneous.TrimFilterFactory");
    map.put("solr.LimitTokenCountFilterFactory", "org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilterFactory");
    map.put("solr.ICUTokenizerFactory", "org.apache.lucene.analysis.icu.segmentation.ICUTokenizerFactory");
    map.put("solr.ICUFoldingFilterFactory", "org.apache.lucene.analysis.icu.ICUFoldingFilterFactory");
    map.put("solr.ProtectedTermFilterFactory", "org.apache.lucene.analysis.miscellaneous.ProtectedTermFilterFactory");
    map.put("solr.ReversedWildcardFilterFactory", "org.apache.solr.analysis.ReversedWildcardFilterFactory");
    map.put("solr.ASCIIFoldingFilterFactory", "org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilterFactory");
    map.put("solr.EnglishPossessiveFilterFactory", "org.apache.lucene.analysis.en.EnglishPossessiveFilterFactory");
    map.put("solr.NGramTokenizerFactory", "org.apache.lucene.analysis.ngram.NGramTokenizerFactory");
    map.put("solr.ManagedStopFilterFactory", "org.apache.solr.rest.schema.analysis.ManagedStopFilterFactory");
    map.put("solr.ManagedSynonymFilterFactory", "org.apache.solr.rest.schema.analysis.ManagedSynonymFilterFactory");
    map.put("solr.ManagedSynonymGraphFilterFactory", "org.apache.solr.rest.schema.analysis.ManagedSynonymGraphFilterFactory");
    map.put("solr.OpenNLPTokenizerFactory", "org.apache.lucene.analysis.opennlp.OpenNLPTokenizerFactory");
    map.put("solr.ICUNormalizer2FilterFactory", "org.apache.lucene.analysis.icu.ICUNormalizer2FilterFactory");
    map.put("solr.DoubleMetaphoneFilterFactory", "org.apache.lucene.analysis.phonetic.DoubleMetaphoneFilterFactory");
    map.put("solr.ClassicTokenizerFactory", "org.apache.lucene.analysis.standard.ClassicTokenizerFactory");
    map.put("solr.GreekLowerCaseFilterFactory", "org.apache.lucene.analysis.el.GreekLowerCaseFilterFactory");
    map.put("solr.ConcatenateGraphFilterFactory", "org.apache.lucene.analysis.miscellaneous.ConcatenateGraphFilterFactory");
    map.put("solr.JapaneseTokenizerFactory", "org.apache.lucene.analysis.ja.JapaneseTokenizerFactory");
    map.put("solr.CJKWidthFilterFactory", "org.apache.lucene.analysis.cjk.CJKWidthFilterFactory");
    map.put("solr.JapaneseReadingFormFilterFactory", "org.apache.lucene.analysis.ja.JapaneseReadingFormFilterFactory");
    map.put("solr.TurkishLowerCaseFilterFactory", "org.apache.lucene.analysis.tr.TurkishLowerCaseFilterFactory");
    map.put("solr.PersianNormalizationFilterFactory", "org.apache.lucene.analysis.fa.PersianNormalizationFilterFactory");
    map.put("solr.ArabicNormalizationFilterFactory", "org.apache.lucene.analysis.ar.ArabicNormalizationFilterFactory");
    map.put("solr.IndicNormalizationFilterFactory", "org.apache.lucene.analysis.in.IndicNormalizationFilterFactory");
    map.put("solr.HindiNormalizationFilterFactory", "org.apache.lucene.analysis.hi.HindiNormalizationFilterFactory");
    map.put("solr.GermanNormalizationFilterFactory", "org.apache.lucene.analysis.de.GermanNormalizationFilterFactory");
    map.put("solr.ElisionFilterFactory", "org.apache.lucene.analysis.util.ElisionFilterFactory");
    map.put("solr.FrenchLightStemFilterFactory", "org.apache.lucene.analysis.fr.FrenchLightStemFilterFactory");
    map.put("solr.ICUTransformFilterFactory", "org.apache.lucene.analysis.icu.ICUTransformFilterFactory");
    map.put("solr.PatternReplaceCharFilterFactory", "org.apache.lucene.analysis.pattern.PatternReplaceCharFilterFactory");
    map.put("solr.ClassicFilterFactory", "org.apache.lucene.analysis.standard.ClassicFilterFactory");
    map.put("solr.KStemFilterFactory", "org.apache.lucene.analysis.en.KStemFilterFactory");
    map.put("solr.CaffeineCache", "org.apache.solr.search.CaffeineCache");
    map.put("ClassicIndexSchemaFactory", "org.apache.solr.schema.ClassicIndexSchemaFactory");
    map.put("solr.FloatPointField", "org.apache.solr.schema.FloatPointField");
    map.put("solr.IntPointField", "org.apache.solr.schema.IntPointField");
    map.put("solr.LongPointField", "org.apache.solr.schema.LongPointField");
    map.put("solr.DoublePointField", "org.apache.solr.schema.DoublePointField");
    map.put("solr.DatePointField", "org.apache.solr.schema.DatePointField");
    map.put("solr.TextField", "org.apache.solr.schema.TextField");
    map.put("solr.MockTokenizerFactory", "org.apache.solr.analysis.MockTokenizerFactory");
    map.put("solr.BoolField", "org.apache.solr.schema.BoolField");
    map.put("solr.StrField", "org.apache.solr.schema.StrField");
    map.put("solr.DateRangeField", "org.apache.solr.schema.DateRangeField");
    map.put("solr.SpatialRecursivePrefixTreeFieldType", "org.apache.solr.schema.SpatialRecursivePrefixTreeFieldType");
    map.put("solr.UUIDField", "org.apache.solr.schema.UUIDField");
    map.put("solr.PointType", "org.apache.solr.schema.PointType");
    map.put("solr.GeoHashField", "org.apache.solr.schema.GeoHashField");
    map.put("solr.LatLonType", "org.apache.solr.schema.LatLonType");
    map.put("solr.CurrencyField", "org.apache.solr.schema.CurrencyField");
    map.put("solr.CurrencyFieldType", "org.apache.solr.schema.CurrencyFieldType");
    map.put("solr.EnumFieldType", "org.apache.solr.schema.EnumFieldType");
    map.put("solr.BinaryField", "org.apache.solr.schema.BinaryField");
    map.put("solr.CollationField", "org.apache.solr.schema.CollationField");
    map.put("solr.ExternalFileField", "org.apache.solr.schema.ExternalFileField");
    map.put("solr.ICUCollationField", "org.apache.solr.schema.ICUCollationField");
    map.put("solr.LatLonPointSpatialField", "org.apache.solr.schema.LatLonPointSpatialField");
    map.put("solr.RandomSortField", "org.apache.solr.schema.RandomSortField");
    map.put("solr.SortableTextField", "org.apache.solr.schema.SortableTextField");
    map.put("solr.NestPathField", "org.apache.solr.schema.NestPathField");
    map.put("solr.FileExchangeRateProvider", "org.apache.solr.schema.FileExchangeRateProvider");
    map.put("solr.MockExchangeRateProvider", "org.apache.solr.schema.MockExchangeRateProvider");
    map.put("solr.OpenExchangeRatesOrgProvider", "org.apache.solr.schema.OpenExchangeRatesOrgProvider");
    map.put("solr.CustomSimilarityFactory", "org.apache.solr.search.similarities.CustomSimilarityFactory");
    map.put("solr.XMLResponseWriter", "org.apache.solr.response.XMLResponseWriter");
    map.put("FooQParserPlugin", "org.apache.solr.search.FooQParserPlugin");
    map.put("solr.RunUpdateProcessorFactory", "org.apache.solr.update.processor.RunUpdateProcessorFactory");
    map.put("solr.RegexReplaceProcessorFactory", "org.apache.solr.update.processor.RegexReplaceProcessorFactory");
    map.put("solr.DistributedUpdateProcessorFactory", "org.apache.solr.update.processor.DistributedUpdateProcessorFactory");
    map.put("solr.DirectUpdateHandler2", "org.apache.solr.update.DirectUpdateHandler2");
    map.put("solr.SearchHandler", "org.apache.solr.handler.component.SearchHandler");
    map.put("solr.SchemaHandler", "org.apache.solr.handler.SchemaHandler");
    map.put("solr.UpdateRequestHandlerApi", "org.apache.solr.handler.UpdateRequestHandlerApi");
    map.put("solr.PropertiesRequestHandler", "org.apache.solr.handler.admin.PropertiesRequestHandler");
    map.put("solr.LukeRequestHandler", "org.apache.solr.handler.admin.LukeRequestHandler");
    map.put("solr.MoreLikeThisHandler", "org.apache.solr.handler.MoreLikeThisHandler");
    map.put("solr.StreamHandler", "org.apache.solr.handler.StreamHandler");
    map.put("solr.GraphHandler", "org.apache.solr.handler.GraphHandler");
    map.put("solr.ExportHandler", "org.apache.solr.handler.ExportHandler");
    map.put("solr.ExportHandler", "org.apache.solr.handler.ExportHandler");
    map.put("solr.ShowFileRequestHandler", "org.apache.solr.handler.admin.ShowFileRequestHandler");
    map.put("solr.HealthCheckHandler", "org.apache.solr.handler.admin.HealthCheckHandler");
    map.put("solr.LoggingHandler", "org.apache.solr.handler.admin.LoggingHandler");
    map.put("solr.ThreadDumpHandler", "org.apache.solr.handler.admin.ThreadDumpHandler");
    map.put("solr.PluginInfoHandler", "org.apache.solr.handler.admin.PluginInfoHandler");
    map.put("solr.SolrInfoMBeanHandler", "org.apache.solr.handler.admin.SolrInfoMBeanHandler");
    map.put("solr.SystemInfoHandler", "org.apache.solr.handler.admin.SystemInfoHandler");
    map.put("solr.SegmentsInfoRequestHandler", "org.apache.solr.handler.admin.SegmentsInfoRequestHandler");
    map.put("solr.PingRequestHandler", "org.apache.solr.handler.PingRequestHandler");
    map.put("solr.RealTimeGetHandler", "org.apache.solr.handler.RealTimeGetHandler");
    map.put("solr.ReplicationHandler", "org.apache.solr.handler.ReplicationHandler");
    map.put("solr.SolrConfigHandler", "org.apache.solr.handler.SolrConfigHandler");
    map.put("solr.UpdateRequestHandler", "org.apache.solr.handler.UpdateRequestHandler");
    map.put("solr.DumpRequestHandler", "org.apache.solr.handler.DumpRequestHandler");
    map.put("solr.SQLHandler", "org.apache.solr.handler.SQLHandler");
    map.put("solr.HighlightComponent", "org.apache.solr.handler.component.HighlightComponent");
    map.put("DirectSolrSpellChecker", "org.apache.solr.spelling.DirectSolrSpellChecker");
    map.put("solr.WordBreakSolrSpellChecker", "org.apache.solr.spelling.WordBreakSolrSpellChecker");
    map.put("solr.FileBasedSpellChecker", "org.apache.solr.spelling.FileBasedSpellChecker");
    TRANS_MAP = Collections.unmodifiableMap(map);
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
      try {
        return Class.forName(cname, true, classLoader).asSubclass(expectedType);
      } catch (ClassNotFoundException e) {

        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, name +" Error loading class '" + cname + "'", e);
      }
    }

    String trans = TRANS_MAP.get(cname);
    if (trans != null) {
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
      ParWork.propegateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Error instantiating class: '" + clazz.getName() + "'", e);
    }

    addToCoreAware(obj);
    addToResourceLoaderAware(obj);
    addToInfoBeans(obj);
    return obj;
  }

  public <T> void addToInfoBeans(T obj) {
    if(!live) {
      if (obj instanceof SolrInfoBean) {
        //TODO: Assert here?
        infoMBeans.add((SolrInfoBean) obj);
      }
    }
  }

  public <T> boolean addToResourceLoaderAware(T obj) {
    if (!live) {
      if (obj instanceof ResourceLoaderAware) {
        assertAwareCompatibility(ResourceLoaderAware.class, obj);
        waitingForResources.add((ResourceLoaderAware) obj);
      }
      return true;
    } else {
      return false;
    }
  }

  /** the inform() callback should be invoked on the listener.
   * If this is 'live', the callback is not called so currently this returns 'false'
   *
   */
  public <T> boolean addToCoreAware(T obj) {
    if (!live) {
      if (obj instanceof SolrCoreAware) {
        assertAwareCompatibility(SolrCoreAware.class, obj);
        waitingForCore.add((SolrCoreAware) obj);
      }
      return true;
    } else {
      return false;
    }
  }


  /**
   * Tell all {@link SolrCoreAware} instances about the SolrCore
   */
  public void inform(SolrCore core) {

    // make a copy to avoid potential deadlock of a callback calling newInstance and trying to
    // add something to waitingForCore.

    while (waitingForCore.size() > 0) {
      for (SolrCoreAware aware : waitingForCore) {
        waitingForCore.remove(aware);
        aware.inform(core);
      }
      try (ParWork worker = new ParWork(this)) {
        waitingForCore.forEach(aware -> {
          worker.collect(()-> {
            try {
              aware.inform(core);
            } catch (Exception e) {
              ParWork.propegateInterrupt(e);
              log.error("Exception informing SolrCore", e);
            }
            waitingForCore.remove(aware);
          });
        });

        worker.addCollect("informResourceLoader");
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

    while (waitingForResources.size() > 0) {
      try (ParWork worker = new ParWork(this)) {
        waitingForResources.forEach(r -> {
          worker.collect(()-> {
            try {
              r.inform(loader);
            } catch (Exception e) {
              ParWork.propegateInterrupt(e);
              log.error("Exception informing ResourceLoader", e);
            }
            waitingForResources.remove(r);
          });
        });

        worker.addCollect("informResourceLoader");
      }
//      if (waitingForResources.size() == 0) {
//        try {
//          Thread.sleep(50); // lttle throttle
//        } catch (Exception e) {
//          SolrZkClient.checkInterrupted(e);
//          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
//        }
//      }
    }
  }

  /**
   * Register any {@link SolrInfoBean}s
   *
   * @param infoRegistry The Info Registry
   */
  public void inform(Map<String, SolrInfoBean> infoRegistry) {
    // this can currently happen concurrently with requests starting and lazy components
    // loading. Make sure infoMBeans doesn't change.

    while (infoMBeans.size() > 0) {

      try (ParWork worker = new ParWork(this)) {
        infoMBeans.forEach(imb -> {
          worker.collect(()-> {
            try {
              try {
                infoRegistry.put(imb.getName(), imb);
              } catch (Exception e) {
                ParWork.propegateInterrupt(e);
                SolrZkClient.checkInterrupted(e);
                log.warn("could not register MBean '" + imb.getName() + "'.", e);
              }
            } catch (Exception e) {
              ParWork.propegateInterrupt(e);
              log.error("Exception informing info registry", e);
            }
            infoMBeans.remove(imb);
          });
        });

        worker.addCollect("informResourceLoader");
      }

//      if (infoMBeans.size() == 0) {
//        try {
//          Thread.sleep(50); // lttle throttle
//        } catch (InterruptedException e) {
//          SolrZkClient.checkInterrupted(e);
//          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
//        }
//      }
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

}
