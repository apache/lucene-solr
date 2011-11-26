/**
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Version;
import org.apache.solr.analysis.*;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.plugin.AbstractPluginLoader;
import org.w3c.dom.*;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.util.*;
import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FieldTypePluginLoader 
  extends AbstractPluginLoader<FieldType> {

  private static final String LUCENE_MATCH_VERSION_PARAM
    = IndexSchema.LUCENE_MATCH_VERSION_PARAM;

  protected final static Logger log 
    = LoggerFactory.getLogger(FieldTypePluginLoader.class);

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
    super("[schema.xml] fieldType", true, true);
    this.schema = schema;
    this.fieldTypes = fieldTypes;
    this.schemaAware = schemaAware;
  }

  private final IndexSchema schema;
  private final Map<String, FieldType> fieldTypes;
  private final Collection<SchemaAware> schemaAware;


  @Override
  protected FieldType create( ResourceLoader loader, 
                              String name, 
                              String className, 
                              Node node ) throws Exception {

    FieldType ft = (FieldType)loader.newInstance(className);
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
    Similarity similarity = IndexSchema.readSimilarity(loader, anode);
    
    if (queryAnalyzer==null) queryAnalyzer=analyzer;
    if (analyzer==null) analyzer=queryAnalyzer;
    if (multiAnalyzer == null) {
      Boolean legacyMatch = ! schema.getDefaultLuceneMatchVersion().onOrAfter(Version.LUCENE_36);
      legacyMatch = (DOMUtil.getAttr(node, "legacyMultiTerm", null) == null) ? legacyMatch :
          Boolean.parseBoolean(DOMUtil.getAttr(node, "legacyMultiTerm", null));
      multiAnalyzer = constructMultiTermAnalyzer(queryAnalyzer, legacyMatch);
    }
    if (analyzer!=null) {
      ft.setAnalyzer(analyzer);
      ft.setQueryAnalyzer(queryAnalyzer);
      ft.setMultiTermAnalyzer(multiAnalyzer);
    }
    if (similarity!=null) {
      ft.setSimilarity(similarity);
    }
    if (ft instanceof SchemaAware){
      schemaAware.add((SchemaAware) ft);
    }
    return ft;
  }
  
  @Override
  protected void init(FieldType plugin, Node node) throws Exception {

    Map<String,String> params = DOMUtil.toMapExcept( node.getAttributes(), 
                                                     "name","class" );
    plugin.setArgs(schema, params );
  }
  
  @Override
  protected FieldType register(String name, 
                               FieldType plugin) throws Exception {

    log.trace("fieldtype defined: " + plugin );
    return fieldTypes.put( name, plugin );
  }

  // The point here is that, if no multitermanalyzer was specified in the schema file, do one of several things:
  // 1> If legacyMultiTerm == false, assemble a new analyzer composed of all of the charfilters,
  //    lowercase filters and asciifoldingfilter.
  // 2> If letacyMultiTerm == true just construct the analyzer from a KeywordTokenizer. That should mimic current behavior.
  //    Do the same if they've specified that the old behavior is required (legacyMultiTerm="true")

  private Analyzer constructMultiTermAnalyzer(Analyzer queryAnalyzer, Boolean legacyMultiTerm) {
    if (queryAnalyzer == null) return null;

    if (legacyMultiTerm || (!(queryAnalyzer instanceof TokenizerChain))) {
      return new KeywordAnalyzer();
    }

    TokenizerChain tc = (TokenizerChain) queryAnalyzer;

    // we know it'll never be longer than this unless the code below is explicitly changed
    TokenFilterFactory[] filters = new TokenFilterFactory[2];
    int idx = 0;
    for (TokenFilterFactory factory : tc.getTokenFilterFactories()) {
      if (factory instanceof LowerCaseFilterFactory) {
        filters[idx] = new LowerCaseFilterFactory();
        filters[idx++].init(factory.getArgs());
      }
      if (factory instanceof ASCIIFoldingFilterFactory) {
        filters[idx] = new ASCIIFoldingFilterFactory();
        filters[idx++].init(factory.getArgs());
      }
    }
    WhitespaceTokenizerFactory white = new WhitespaceTokenizerFactory();
    white.init(tc.getTokenizerFactory().getArgs());

    return new TokenizerChain(tc.getCharFilterFactories(),
        white,
        Arrays.copyOfRange(filters, 0, idx));
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
    if (analyzerName != null) {
      try {
        // No need to be core-aware as Analyzers are not in the core-aware list
        final Class<? extends Analyzer> clazz = loader.findClass
          (analyzerName).asSubclass(Analyzer.class);
        
        try {
          // first try to use a ctor with version parameter 
          // (needed for many new Analyzers that have no default one anymore)
          Constructor<? extends Analyzer> cnstr 
            = clazz.getConstructor(Version.class);
          final String matchVersionStr 
            = DOMUtil.getAttr(attrs, LUCENE_MATCH_VERSION_PARAM);
          final Version luceneMatchVersion = (matchVersionStr == null) ?
            schema.getDefaultLuceneMatchVersion() : 
            Config.parseLuceneVersionString(matchVersionStr);
          if (luceneMatchVersion == null) {
            throw new SolrException
              ( SolrException.ErrorCode.SERVER_ERROR,
                "Configuration Error: Analyzer '" + clazz.getName() +
                "' needs a 'luceneMatchVersion' parameter");
          }
          return cnstr.newInstance(luceneMatchVersion);
        } catch (NoSuchMethodException nsme) {
          // otherwise use default ctor
          return clazz.newInstance();
        }
      } catch (Exception e) {
        log.error("Cannot load analyzer: "+analyzerName, e);
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
                                 "Cannot load analyzer: "+analyzerName, e );
      }
    }

    // Load the CharFilters

    final ArrayList<CharFilterFactory> charFilters 
      = new ArrayList<CharFilterFactory>();
    AbstractPluginLoader<CharFilterFactory> charFilterLoader =
      new AbstractPluginLoader<CharFilterFactory>
      ( "[schema.xml] analyzer/charFilter", false, false ) {

      @Override
      protected void init(CharFilterFactory plugin, Node node) throws Exception {
        if( plugin != null ) {
          final Map<String,String> params = DOMUtil.toMapExcept(node.getAttributes(),"class");
          // copy the luceneMatchVersion from config, if not set
          if (!params.containsKey(LUCENE_MATCH_VERSION_PARAM))
            params.put(LUCENE_MATCH_VERSION_PARAM, 
                       schema.getDefaultLuceneMatchVersion().toString());
          plugin.init( params );
          charFilters.add( plugin );
        }
      }

      @Override
      protected CharFilterFactory register(String name, 
                                           CharFilterFactory plugin) {
        return null; // used for map registration
      }
    };

    charFilterLoader.load( loader, (NodeList)xpath.evaluate("./charFilter",  node, XPathConstants.NODESET) );
                            

    // Load the Tokenizer
    // Although an analyzer only allows a single Tokenizer, we load a list to make sure
    // the configuration is ok

    final ArrayList<TokenizerFactory> tokenizers 
      = new ArrayList<TokenizerFactory>(1);
    AbstractPluginLoader<TokenizerFactory> tokenizerLoader =
      new AbstractPluginLoader<TokenizerFactory>
      ( "[schema.xml] analyzer/tokenizer", false, false ) {
      @Override
      protected void init(TokenizerFactory plugin, Node node) throws Exception {
        if( !tokenizers.isEmpty() ) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
              "The schema defines multiple tokenizers for: "+node );
        }
        final Map<String,String> params = DOMUtil.toMapExcept(node.getAttributes(),"class");

        // copy the luceneMatchVersion from config, if not set
        if (!params.containsKey(LUCENE_MATCH_VERSION_PARAM))
          params.put(LUCENE_MATCH_VERSION_PARAM, 
                     schema.getDefaultLuceneMatchVersion().toString());
        plugin.init( params );
        tokenizers.add( plugin );
      }

      @Override
      protected TokenizerFactory register(String name, TokenizerFactory plugin) {
        return null; // used for map registration
      }
    };

    tokenizerLoader.load( loader, (NodeList)xpath.evaluate("./tokenizer", node, XPathConstants.NODESET) );
    
    // Make sure something was loaded
    if( tokenizers.isEmpty() ) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"analyzer without class or tokenizer & filter list");
    }
    

    // Load the Filters

    final ArrayList<TokenFilterFactory> filters 
      = new ArrayList<TokenFilterFactory>();

    AbstractPluginLoader<TokenFilterFactory> filterLoader = 
      new AbstractPluginLoader<TokenFilterFactory>( "[schema.xml] analyzer/filter", false, false )
    {
      @Override
      protected void init(TokenFilterFactory plugin, Node node) throws Exception {
        if( plugin != null ) {
          final Map<String,String> params = DOMUtil.toMapExcept(node.getAttributes(),"class");
          // copy the luceneMatchVersion from config, if not set
          if (!params.containsKey(LUCENE_MATCH_VERSION_PARAM))
            params.put(LUCENE_MATCH_VERSION_PARAM, 
                       schema.getDefaultLuceneMatchVersion().toString());
          plugin.init( params );
          filters.add( plugin );
        }
      }

      @Override
      protected TokenFilterFactory register(String name, TokenFilterFactory plugin) throws Exception {
        return null; // used for map registration
      }
    };
    filterLoader.load( loader, (NodeList)xpath.evaluate("./filter", node, XPathConstants.NODESET) );
    
    return new TokenizerChain(charFilters.toArray(new CharFilterFactory[charFilters.size()]),
                              tokenizers.get(0), filters.toArray(new TokenFilterFactory[filters.size()]));
  }
    
}
