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
package org.apache.solr.schema;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.Version;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.plugin.AbstractPluginLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static org.apache.solr.common.params.CommonParams.NAME;

public final class FieldTypePluginLoader 
  extends AbstractPluginLoader<FieldType> {

  private static final String LUCENE_MATCH_VERSION_PARAM
    = IndexSchema.LUCENE_MATCH_VERSION_PARAM;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final XPath xpath = XPathFactory.newInstance().newXPath();

  /**
   * @param schema The schema that will be used to initialize the FieldTypes
   * @param fieldTypes All FieldTypes that are instantiated by 
   *        this Plugin Loader will be added to this Map
   * @param schemaAware Any SchemaAware objects that are instantiated by 
   *        this Plugin Loader will be added to this collection.
   */
  public FieldTypePluginLoader(final IndexSchema schema,
                               final Map<String, FieldType> fieldTypes,
                               final Collection<SchemaAware> schemaAware) {
    super("[schema.xml] fieldType", FieldType.class, true, true);
    this.schema = schema;
    this.fieldTypes = fieldTypes;
    this.schemaAware = schemaAware;
  }

  private final IndexSchema schema;
  private final Map<String, FieldType> fieldTypes;
  private final Collection<SchemaAware> schemaAware;


  @Override
  protected FieldType create( SolrResourceLoader loader, 
                              String name, 
                              String className,
                              Node node ) throws Exception {

    FieldType ft = loader.newInstance(className, FieldType.class);
    ft.setTypeName(name);
    
    String expression = "./analyzer[@type='query']";
    Node anode = (Node)xpath.evaluate(expression, node, XPathConstants.NODE);
    Analyzer queryAnalyzer = readAnalyzer(anode);

    expression = "./analyzer[@type='multiterm']";
    anode = (Node)xpath.evaluate(expression, node, XPathConstants.NODE);
    Analyzer multiAnalyzer = readAnalyzer(anode);

    // An analyzer without a type specified, or with type="index"
    expression = "./analyzer[not(@type)] | ./analyzer[@type='index']";
    anode = (Node)xpath.evaluate(expression, node, XPathConstants.NODE);
    Analyzer analyzer = readAnalyzer(anode);

    // a custom similarity[Factory]
    expression = "./similarity";
    anode = (Node)xpath.evaluate(expression, node, XPathConstants.NODE);
    SimilarityFactory simFactory = IndexSchema.readSimilarity(loader, anode);
    if (null != simFactory) {
      ft.setSimilarity(simFactory);
    }

    if (ft instanceof HasImplicitIndexAnalyzer) {
      ft.setIsExplicitAnalyzer(false);
      if (null != queryAnalyzer && null != analyzer) {
        if (log.isWarnEnabled()) {
          log.warn("Ignoring index-time analyzer for field: " + name);
        }
      } else if (null == queryAnalyzer) { // Accept non-query-time analyzer as a query-time analyzer 
        queryAnalyzer = analyzer;
      }
      if (null != queryAnalyzer) {
        ft.setIsExplicitQueryAnalyzer(true);
        ft.setQueryAnalyzer(queryAnalyzer);
      }
    } else {
      if (null == queryAnalyzer) {
        queryAnalyzer = analyzer;
        ft.setIsExplicitQueryAnalyzer(false);
      } else {
        ft.setIsExplicitQueryAnalyzer(true);
      }
      if (null == analyzer) {
        analyzer = queryAnalyzer;
        ft.setIsExplicitAnalyzer(false);
      } else {
        ft.setIsExplicitAnalyzer(true);
      }
  
      if (null != analyzer) {
        ft.setIndexAnalyzer(analyzer);
        ft.setQueryAnalyzer(queryAnalyzer);
        if (ft instanceof TextField) {
          if (null == multiAnalyzer) {
            multiAnalyzer = constructMultiTermAnalyzer(queryAnalyzer);
            ((TextField)ft).setIsExplicitMultiTermAnalyzer(false);
          } else {
            ((TextField)ft).setIsExplicitMultiTermAnalyzer(true);
          }
          ((TextField)ft).setMultiTermAnalyzer(multiAnalyzer);
        }
      }
    }
    if (ft instanceof SchemaAware){
      schemaAware.add((SchemaAware) ft);
    }
    return ft;
  }
  
  @Override
  protected void init(FieldType plugin, Node node) throws Exception {

    Map<String, String> params = DOMUtil.toMapExcept(node.getAttributes(), NAME);
    plugin.setArgs(schema, params);
  }

  @Override
  protected FieldType register(String name, 
                               FieldType plugin) throws Exception {

    log.trace("fieldtype defined: " + plugin );
    return fieldTypes.put( name, plugin );
  }

  // The point here is that, if no multiterm analyzer was specified in the schema file, do one of several things:
  // 1> If legacyMultiTerm == false, assemble a new analyzer composed of all of the charfilters,
  //    lowercase filters and asciifoldingfilter.
  // 2> If legacyMultiTerm == true just construct the analyzer from a KeywordTokenizer. That should mimic current behavior.
  //    Do the same if they've specified that the old behavior is required (legacyMultiTerm="true")

  private Analyzer constructMultiTermAnalyzer(Analyzer queryAnalyzer) {
    if (queryAnalyzer == null) return null;

    if (!(queryAnalyzer instanceof TokenizerChain)) {
      return new KeywordAnalyzer();
    }

    return ((TokenizerChain) queryAnalyzer).getMultiTermAnalyzer();
  }

  //
  // <analyzer><tokenizer class="...."/><tokenizer class="...." arg="....">
  //
  //
  private Analyzer readAnalyzer(Node node) throws XPathExpressionException {
                                
    final SolrResourceLoader loader = schema.getResourceLoader();

    // parent node used to be passed in as "fieldtype"
    // if (!fieldtype.hasChildNodes()) return null;
    // Node node = DOMUtil.getChild(fieldtype,"analyzer");
    
    if (node == null) return null;
    NamedNodeMap attrs = node.getAttributes();
    String analyzerName = DOMUtil.getAttr(attrs,"class");

    // check for all of these up front, so we can error if used in 
    // conjunction with an explicit analyzer class.
    NodeList charFilterNodes = (NodeList)xpath.evaluate
      ("./charFilter",  node, XPathConstants.NODESET);
    NodeList tokenizerNodes = (NodeList)xpath.evaluate
      ("./tokenizer", node, XPathConstants.NODESET);
    NodeList tokenFilterNodes = (NodeList)xpath.evaluate
      ("./filter", node, XPathConstants.NODESET);
      
    if (analyzerName != null) {

      // explicitly check for child analysis factories instead of
      // just any child nodes, because the user might have their
      // own custom nodes (ie: <description> or something like that)
      if (0 != charFilterNodes.getLength() ||
          0 != tokenizerNodes.getLength() ||
          0 != tokenFilterNodes.getLength()) {
        throw new SolrException
        ( SolrException.ErrorCode.SERVER_ERROR,
          "Configuration Error: Analyzer class='" + analyzerName +
          "' can not be combined with nested analysis factories");
      }

      try {
        // No need to be core-aware as Analyzers are not in the core-aware list
        final Class<? extends Analyzer> clazz = loader.findClass(analyzerName, Analyzer.class);
        Analyzer analyzer = clazz.getConstructor().newInstance();

        final String matchVersionStr = DOMUtil.getAttr(attrs, LUCENE_MATCH_VERSION_PARAM);
        final Version luceneMatchVersion = (matchVersionStr == null) ?
          schema.getDefaultLuceneMatchVersion() :
          SolrConfig.parseLuceneVersionString(matchVersionStr);
        if (luceneMatchVersion == null) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Configuration Error: Analyzer '" + clazz.getName() +
                  "' needs a '" + IndexSchema.LUCENE_MATCH_VERSION_PARAM + "' parameter");
        }
        analyzer.setVersion(luceneMatchVersion);
        return analyzer;
      } catch (Exception e) {
        log.error("Cannot load analyzer: "+analyzerName, e);
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
                                 "Cannot load analyzer: "+analyzerName, e );
      }
    }

    // Load the CharFilters

    final ArrayList<CharFilterFactory> charFilters 
      = new ArrayList<>();
    AbstractPluginLoader<CharFilterFactory> charFilterLoader =
      new AbstractPluginLoader<CharFilterFactory>
      ("[schema.xml] analyzer/charFilter", CharFilterFactory.class, false, false) {

      @Override
      protected CharFilterFactory create(SolrResourceLoader loader, String name, String className, Node node) throws Exception {
        final Map<String,String> params = DOMUtil.toMap(node.getAttributes());
        String configuredVersion = params.remove(LUCENE_MATCH_VERSION_PARAM);
        params.put(LUCENE_MATCH_VERSION_PARAM, parseConfiguredVersion(configuredVersion, CharFilterFactory.class.getSimpleName()).toString());
        CharFilterFactory factory;
        if (Objects.nonNull(name)) {
          factory = CharFilterFactory.forName(name, params);
          if (Objects.nonNull(className)) {
            log.error("Both of name: " + name + " and className: " + className + " are specified for charFilter.");
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Cannot create charFilter: Both of name and className are specified.");
          }
        } else if (Objects.nonNull(className)) {
          factory = loader.newInstance(className, CharFilterFactory.class, getDefaultPackages(), new Class[]{Map.class}, new Object[]{params});
        } else {
          log.error("Neither of name or className is specified for charFilter.");
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Cannot create charFilter: Neither of name or className is specified.");
        }
        factory.setExplicitLuceneMatchVersion(null != configuredVersion);
        return factory;
      }

      @Override
      protected void init(CharFilterFactory plugin, Node node) throws Exception {
        if( plugin != null ) {
          charFilters.add( plugin );
        }
      }

      @Override
      protected CharFilterFactory register(String name, 
                                           CharFilterFactory plugin) {
        return null; // used for map registration
      }
    };

    charFilterLoader.load( loader, charFilterNodes );

    // Load the Tokenizer
    // Although an analyzer only allows a single Tokenizer, we load a list to make sure
    // the configuration is ok

    final ArrayList<TokenizerFactory> tokenizers 
      = new ArrayList<>(1);
    AbstractPluginLoader<TokenizerFactory> tokenizerLoader =
      new AbstractPluginLoader<TokenizerFactory>
      ("[schema.xml] analyzer/tokenizer", TokenizerFactory.class, false, false) {
      
      @Override
      protected TokenizerFactory create(SolrResourceLoader loader, String name, String className, Node node) throws Exception {
        final Map<String,String> params = DOMUtil.toMap(node.getAttributes());
        String configuredVersion = params.remove(LUCENE_MATCH_VERSION_PARAM);
        params.put(LUCENE_MATCH_VERSION_PARAM, parseConfiguredVersion(configuredVersion, TokenizerFactory.class.getSimpleName()).toString());
        TokenizerFactory factory;
        if (Objects.nonNull(name)) {
          factory = TokenizerFactory.forName(name, params);
          if (Objects.nonNull(className)) {
            log.error("Both of name: " + name + " and className: " + className + " are specified for tokenizer.");
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Cannot create tokenizer: Both of name and className are specified.");
          }
        } else if (Objects.nonNull(className)) {
          factory = loader.newInstance(className, TokenizerFactory.class, getDefaultPackages(), new Class[]{Map.class}, new Object[]{params});
        } else {
          log.error("Neither of name or className is specified for tokenizer.");
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Cannot create tokenizer: Neither of name or className is specified.");
        }
        factory.setExplicitLuceneMatchVersion(null != configuredVersion);
        return factory;
      }
      
      @Override
      protected void init(TokenizerFactory plugin, Node node) throws Exception {
        if( !tokenizers.isEmpty() ) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
              "The schema defines multiple tokenizers for: "+node );
        }
        tokenizers.add( plugin );
      }

      @Override
      protected TokenizerFactory register(String name, TokenizerFactory plugin) {
        return null; // used for map registration
      }
    };

    tokenizerLoader.load( loader, tokenizerNodes );
    
    // Make sure something was loaded
    if( tokenizers.isEmpty() ) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"analyzer without class or tokenizer");
    }

    // Load the Filters

    final ArrayList<TokenFilterFactory> filters 
      = new ArrayList<>();

    AbstractPluginLoader<TokenFilterFactory> filterLoader = 
      new AbstractPluginLoader<TokenFilterFactory>("[schema.xml] analyzer/filter", TokenFilterFactory.class, false, false)
    {
      @Override
      protected TokenFilterFactory create(SolrResourceLoader loader, String name, String className, Node node) throws Exception {
        final Map<String,String> params = DOMUtil.toMap(node.getAttributes());
        String configuredVersion = params.remove(LUCENE_MATCH_VERSION_PARAM);
        params.put(LUCENE_MATCH_VERSION_PARAM, parseConfiguredVersion(configuredVersion, TokenFilterFactory.class.getSimpleName()).toString());
        TokenFilterFactory factory;
        if (Objects.nonNull(name)) {
          factory = TokenFilterFactory.forName(name, params);
          if (Objects.nonNull(className)) {
            log.error("Both of name: " + name + " and className: " + className + " are specified for tokenFilter.");
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Cannot create tokenFilter: Both of name and className are specified.");
          }
        } else if (Objects.nonNull(className)) {
          factory = loader.newInstance(className, TokenFilterFactory.class, getDefaultPackages(), new Class[]{Map.class}, new Object[]{params});
        } else {
          log.error("Neither of name or className is specified for tokenFilter.");
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Cannot create tokenFilter: Neither of name or className is specified.");
        }
        factory.setExplicitLuceneMatchVersion(null != configuredVersion);
        return factory;
      }
      
      @Override
      protected void init(TokenFilterFactory plugin, Node node) throws Exception {
        if( plugin != null ) {
          filters.add( plugin );
        }
      }

      @Override
      protected TokenFilterFactory register(String name, TokenFilterFactory plugin) throws Exception {
        return null; // used for map registration
      }
    };
    filterLoader.load( loader, tokenFilterNodes );
    
    return new TokenizerChain(charFilters.toArray(new CharFilterFactory[charFilters.size()]),
                              tokenizers.get(0), filters.toArray(new TokenFilterFactory[filters.size()]));
  }

  private Version parseConfiguredVersion(String configuredVersion, String pluginClassName) {
    Version version = (configuredVersion != null) ?
            SolrConfig.parseLuceneVersionString(configuredVersion) : schema.getDefaultLuceneMatchVersion();

    if (!version.onOrAfter(Version.LUCENE_8_0_0)) {
      log.warn(pluginClassName + " is using deprecated " + version +
        " emulation. You should at some point declare and reindex to at least 8.0, because " +
        "7.x emulation is deprecated and will be removed in 9.0");
    }
    return version;
  }
    
}
