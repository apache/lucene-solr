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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.analysis.TokenFilterFactory;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.analysis.TokenizerFactory;
import org.apache.solr.search.SolrQueryParser;
import org.apache.solr.util.plugin.AbstractPluginLoader;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.w3c.dom.*;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.InputStream;
import java.io.Reader;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * <code>IndexSchema</code> contains information about the valid fields in an index
 * and the types of those fields.
 *
 * @version $Id$
 */
public final class IndexSchema {
  public static final String DEFAULT_SCHEMA_FILE = "schema.xml";

  final static Logger log = Logger.getLogger(IndexSchema.class.getName());
  private final SolrConfig solrConfig;
  private final String resourceName;
  private String name;
  private float version;

  /**
   * Constructs a schema using the specified file name using the normal
   * Config path directory searching rules.
   *
   * @see Config#openResource
   */
  @Deprecated
  public IndexSchema(SolrConfig solrConfig, String name) {
    this(solrConfig, name, null);
  }
    /**
   * Constructs a schema using the specified resource name and stream.
   * If the is stream is null, the resource loader will load the schema resource by name.
   * @see SolrResourceLoader#openSchema
   * By default, this follows the normal config path directory searching rules.
   * @see Config#openResource
   */
  public IndexSchema(SolrConfig solrConfig, String name, InputStream is) {
    this.solrConfig = solrConfig;
    if (name == null)
      name = DEFAULT_SCHEMA_FILE;
    this.resourceName = name;
    SolrResourceLoader loader = solrConfig.getResourceLoader();
    InputStream lis = is;
    if (lis == null)
      lis = loader.openSchema(name);
    readSchema(lis);
    if (lis != is) {
      try {
        lis.close();
      }
      catch(IOException xio) {} // ignore
    }
    
    loader.inform( loader );
  }

  public SolrConfig getSolrConfig() {
    return solrConfig;
  }
  
  
  /** Gets the name of the resource used to instantiate this schema. */
  public String getResourceName() {
    return resourceName;
  }
  
  /** Gets the name of the schema as specified in the schema resource. */
  public String getSchemaName() {
    return name;
  }
  
  float getVersion() {
    return version;
  }
  
  /**
   * Direct access to the InputStream for the schemaFile used by this instance.
   * @see Config#openResource
   */
  @Deprecated
  public InputStream getInputStream() {
    return solrConfig.getResourceLoader().openResource(resourceName);
  }

  /** Gets the name of the schema file.
   * @see IndexSchema#getResourceName
   */
  @Deprecated
  public String getSchemaFile() {
    return resourceName;
  }

  /** The Name of this schema (as specified in the schema file)
   * @see IndexSchema#getSchemaName
   */
  @Deprecated
  public String getName() { return name; }

  private final HashMap<String, SchemaField> fields = new HashMap<String,SchemaField>();
  private final HashMap<String, FieldType> fieldTypes = new HashMap<String,FieldType>();
  private final List<SchemaField> fieldsWithDefaultValue = new ArrayList<SchemaField>();
  private final Collection<SchemaField> requiredFields = new HashSet<SchemaField>();

  /**
   * Provides direct access to the Map containing all explicit
   * (ie: non-dynamic) fields in the index, keyed on field name.
   *
   * <p>
   * Modifying this Map (or any item in it) will affect the real schema
   * </p>
   * 
   * <p>
   * NOTE: this function is not thread safe.  However, it is safe to use within the standard
   * <code>inform( SolrCore core )</code> function for <code>SolrCoreAware</code> classes.
   * Outside <code>inform</code>, this could potentially throw a ConcurrentModificationException
   * </p>
   */
  public Map<String,SchemaField> getFields() { return fields; }

  /**
   * Provides direct access to the Map containing all Field Types
   * in the index, keyed on field type name.
   *
   * <p>
   * Modifying this Map (or any item in it) will affect the real schema.  However if you 
   * make any modifications, be sure to call {@link IndexSchema#refreshAnalyzers()} to
   * update the Analyzers for the registered fields.
   * </p>
   * 
   * <p>
   * NOTE: this function is not thread safe.  However, it is safe to use within the standard
   * <code>inform( SolrCore core )</code> function for <code>SolrCoreAware</code> classes.
   * Outside <code>inform</code>, this could potentially throw a ConcurrentModificationException
   * </p>
   */
  public Map<String,FieldType> getFieldTypes() { return fieldTypes; }

  /**
   * Provides direct access to the List containing all fields with a default value
   */
  public List<SchemaField> getFieldsWithDefaultValue() { return fieldsWithDefaultValue; }

  /**
   * Provides direct access to the List containing all required fields.  This
   * list contains all fields with default values.
   */
  public Collection<SchemaField> getRequiredFields() { return requiredFields; }

  private SimilarityFactory similarityFactory;

  /**
   * Returns the Similarity used for this index
   */
  public Similarity getSimilarity() { return similarityFactory.getSimilarity(); }

  /**
   * Returns the SimilarityFactory used for this index
   */
  public SimilarityFactory getSimilarityFactory() { return similarityFactory; }

  private Analyzer analyzer;

  /**
   * Returns the Analyzer used when indexing documents for this index
   *
   * <p>
   * This Analyzer is field (and dynamic field) name aware, and delegates to
   * a field specific Analyzer based on the field type.
   * </p>
   */
  public Analyzer getAnalyzer() { return analyzer; }

  private Analyzer queryAnalyzer;

  /**
   * Returns the Analyzer used when searching this index
   *
   * <p>
   * This Analyzer is field (and dynamic field) name aware, and delegates to
   * a field specific Analyzer based on the field type.
   * </p>
   */
  public Analyzer getQueryAnalyzer() { return queryAnalyzer; }

  private String defaultSearchFieldName=null;
  private String queryParserDefaultOperator = "OR";

  /**
   * A SolrQueryParser linked to this IndexSchema for field datatype
   * information, and populated with default options from the
   * &lt;solrQueryParser&gt; configuration for this IndexSchema.
   *
   * @param defaultField if non-null overrides the schema default
   */
  public SolrQueryParser getSolrQueryParser(String defaultField) {
    SolrQueryParser qp = new SolrQueryParser(this,defaultField);
    String operator = getQueryParserDefaultOperator();
    qp.setDefaultOperator("AND".equals(operator) ?
                          QueryParser.Operator.AND : QueryParser.Operator.OR);
    return qp;
  }
  
  /**
   * Name of the default search field specified in the schema file
   * @deprecated use getSolrQueryParser().getField()
   */
  public String getDefaultSearchFieldName() {
    return defaultSearchFieldName;
  }

  /**
   * default operator ("AND" or "OR") for QueryParser
   * @deprecated use getSolrQueryParser().getDefaultOperator()
   */
  public String getQueryParserDefaultOperator() {
    return queryParserDefaultOperator;
  }

  private SchemaField uniqueKeyField;

  /**
   * Unique Key field specified in the schema file
   * @return null if this schema has no unique key field
   */
  public SchemaField getUniqueKeyField() { return uniqueKeyField; }

  private String uniqueKeyFieldName;
  private FieldType uniqueKeyFieldType;

  /**
   * The raw (field type encoded) value of the Unique Key field for
   * the specified Document
   * @return null if this schema has no unique key field
   * @see #printableUniqueKey
   */
  public Fieldable getUniqueKeyField(org.apache.lucene.document.Document doc) {
    return doc.getFieldable(uniqueKeyFieldName);  // this should return null if name is null
  }

  /**
   * The printable value of the Unique Key field for
   * the specified Document
   * @return null if this schema has no unique key field
   */
  public String printableUniqueKey(org.apache.lucene.document.Document doc) {
     Fieldable f = doc.getFieldable(uniqueKeyFieldName);
     return f==null ? null : uniqueKeyFieldType.toExternal(f);
  }

  private SchemaField getIndexedField(String fname) {
    SchemaField f = getFields().get(fname);
    if (f==null) {
      throw new RuntimeException("unknown field '" + fname + "'");
    }
    if (!f.indexed()) {
      throw new RuntimeException("'"+fname+"' is not an indexed field:" + f);
    }
    return f;
  }
  
  /**
   * This will re-create the Analyzers.  If you make any modifications to
   * the Field map ({@link IndexSchema#getFields()}, this function is required
   * to synch the internally cached field analyzers.
   * 
   * @since solr 1.3
   */
  public void refreshAnalyzers()
  {
    analyzer = new SolrIndexAnalyzer();
    queryAnalyzer = new SolrQueryAnalyzer();
  }

  private class SolrIndexAnalyzer extends Analyzer {
    protected final HashMap<String,Analyzer> analyzers;

    SolrIndexAnalyzer() {
      analyzers = analyzerCache();
    }

    protected HashMap<String,Analyzer> analyzerCache() {
      HashMap<String,Analyzer> cache = new HashMap<String,Analyzer>();
       for (SchemaField f : getFields().values()) {
        Analyzer analyzer = f.getType().getAnalyzer();
        cache.put(f.getName(), analyzer);
      }
      return cache;
    }

    protected Analyzer getAnalyzer(String fieldName)
    {
      Analyzer analyzer = analyzers.get(fieldName);
      return analyzer!=null ? analyzer : getDynamicFieldType(fieldName).getAnalyzer();
    }

    public TokenStream tokenStream(String fieldName, Reader reader)
    {
      return getAnalyzer(fieldName).tokenStream(fieldName,reader);
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
      return getAnalyzer(fieldName).getPositionIncrementGap(fieldName);
    }
  }


  private class SolrQueryAnalyzer extends SolrIndexAnalyzer {
    @Override
    protected HashMap<String,Analyzer> analyzerCache() {
      HashMap<String,Analyzer> cache = new HashMap<String,Analyzer>();
       for (SchemaField f : getFields().values()) {
        Analyzer analyzer = f.getType().getQueryAnalyzer();
        cache.put(f.getName(), analyzer);
      }
      return cache;
    }

    @Override
    protected Analyzer getAnalyzer(String fieldName)
    {
      Analyzer analyzer = analyzers.get(fieldName);
      return analyzer!=null ? analyzer : getDynamicFieldType(fieldName).getQueryAnalyzer();
    }
  }

  private void readSchema(InputStream is) {
    log.info("Reading Solr Schema");

    try {
      // pass the config resource loader to avoid building an empty one for no reason:
      // in the current case though, the stream is valid so we wont load the resource by name
      Config schemaConf = new Config(solrConfig.getResourceLoader(), "schema", is, "/schema/");
      Document document = schemaConf.getDocument();
      final XPath xpath = schemaConf.getXPath();

      Node nd = (Node) xpath.evaluate("/schema/@name", document, XPathConstants.NODE);
      if (nd==null) {
        log.warning("schema has no name!");
      } else {
        name = nd.getNodeValue();
        log.info("Schema name=" + name);
      }

      version = schemaConf.getFloat("/schema/@version", 1.0f);

      final IndexSchema schema = this;
      AbstractPluginLoader<FieldType> loader = new AbstractPluginLoader<FieldType>( "[schema.xml] fieldType", true, true) {

        @Override
        protected FieldType create( ResourceLoader loader, String name, String className, Node node ) throws Exception
        {
          FieldType ft = (FieldType)loader.newInstance(className);
          ft.setTypeName(name);

          String expression = "./analyzer[@type='query']";
          Node anode = (Node)xpath.evaluate(expression, node, XPathConstants.NODE);
          Analyzer queryAnalyzer = readAnalyzer(anode);

          // An analyzer without a type specified, or with type="index"
          expression = "./analyzer[not(@type)] | ./analyzer[@type='index']";
          anode = (Node)xpath.evaluate(expression, node, XPathConstants.NODE);
          Analyzer analyzer = readAnalyzer(anode);

          if (queryAnalyzer==null) queryAnalyzer=analyzer;
          if (analyzer==null) analyzer=queryAnalyzer;
          if (analyzer!=null) {
            ft.setAnalyzer(analyzer);
            ft.setQueryAnalyzer(queryAnalyzer);
          }
          return ft;
        }
        
        @Override
        protected void init(FieldType plugin, Node node) throws Exception {
          Map<String,String> params = DOMUtil.toMapExcept( node.getAttributes(), "name","class" );
          plugin.setArgs(schema, params );
        }

        @Override
        protected FieldType register(String name, FieldType plugin) throws Exception {
          log.finest("fieldtype defined: " + plugin );
          return fieldTypes.put( name, plugin );
        }
      };
      

      String expression = "/schema/types/fieldtype | /schema/types/fieldType";
      NodeList nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);
      loader.load( solrConfig.getResourceLoader(), nodes );

      

      // Hang on to the fields that say if they are required -- this lets us set a reasonable default for the unique key
      Map<String,Boolean> explicitRequiredProp = new HashMap<String, Boolean>();
      ArrayList<DynamicField> dFields = new ArrayList<DynamicField>();
      expression = "/schema/fields/field | /schema/fields/dynamicField";
      nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);

      for (int i=0; i<nodes.getLength(); i++) {
        Node node = nodes.item(i);

        NamedNodeMap attrs = node.getAttributes();

        String name = DOMUtil.getAttr(attrs,"name","field definition");
        log.finest("reading field def "+name);
        String type = DOMUtil.getAttr(attrs,"type","field " + name);

        FieldType ft = fieldTypes.get(type);
        if (ft==null) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Unknown fieldtype '" + type + "' specified on field " + name,false);
        }

        Map<String,String> args = DOMUtil.toMapExcept(attrs, "name", "type");
        if( args.get( "required" ) != null ) {
          explicitRequiredProp.put( name, Boolean.valueOf( args.get( "required" ) ) );
        }

        SchemaField f = SchemaField.create(name,ft,args);

        if (node.getNodeName().equals("field")) {
          SchemaField old = fields.put(f.getName(),f);
          if( old != null ) {
            String msg = "[schema.xml] Duplicate field definition for '"
              + f.getName() + "' ignoring: "+old.toString();
            
            Throwable t = new SolrException( SolrException.ErrorCode.SERVER_ERROR, msg );
            SolrException.logOnce(log,null,t);
            SolrConfig.severeErrors.add( t );
          }
          
          log.fine("field defined: " + f);
          if( f.getDefaultValue() != null ) {
            log.fine(name+" contains default value: " + f.getDefaultValue());
            fieldsWithDefaultValue.add( f );
          }
          if (f.isRequired()) {
            log.fine(name+" is required in this schema");
            requiredFields.add(f);
          }
        } else if (node.getNodeName().equals("dynamicField")) {
          // make sure nothing else has the same path
          boolean dup = false;
          for( DynamicField df : dFields ) {
            if( df.regex.equals( f.name ) ) {
              String msg = "[schema.xml] Duplicate DynamicField definition for '"
                + f.getName() + "' ignoring: "+f.toString();
              
              Throwable t = new SolrException( SolrException.ErrorCode.SERVER_ERROR, msg );
              SolrException.logOnce(log,null,t);
              SolrConfig.severeErrors.add( t );
              dup = true;
              break;
            }
          }
          if( !dup ) {
            dFields.add(new DynamicField(f));
            log.fine("dynamic field defined: " + f);
          }
        } else {
          // we should never get here
          throw new RuntimeException("Unknown field type");
        }
      }
      
    //fields with default values are by definition required
    //add them to required fields, and we only have to loop once
    // in DocumentBuilder.getDoc()
    requiredFields.addAll(getFieldsWithDefaultValue());

    // OK, now sort the dynamic fields largest to smallest size so we don't get
    // any false matches.  We want to act like a compiler tool and try and match
    // the largest string possible.
    Collections.sort(dFields);

    log.finest("Dynamic Field Ordering:" + dFields);

    // stuff it in a normal array for faster access
    dynamicFields = (DynamicField[])dFields.toArray(new DynamicField[dFields.size()]);


    Node node = (Node) xpath.evaluate("/schema/similarity", document, XPathConstants.NODE);
    if (node==null) {
      similarityFactory = new SimilarityFactory() {
        public Similarity getSimilarity() {
          return Similarity.getDefault();
        }
      };
      log.fine("using default similarity");
    } else {
      final Object obj = solrConfig.getResourceLoader().newInstance(((Element) node).getAttribute("class"));
      if (obj instanceof SimilarityFactory) {
        // configure a factory, get a similarity back
        SolrParams params = SolrParams.toSolrParams(DOMUtil.childNodesToNamedList(node));
        similarityFactory = (SimilarityFactory)obj;
        similarityFactory.init(params);
      } else {
        // just like always, assume it's a Similarlity and get an ClassCastException - reasonable error handling
        similarityFactory = new SimilarityFactory() {
          public Similarity getSimilarity() {
            return (Similarity) obj;
          }
        };
      }
      log.fine("using similarity factory" + similarityFactory.getClass().getName());
    }

    node = (Node) xpath.evaluate("/schema/defaultSearchField/text()", document, XPathConstants.NODE);
    if (node==null) {
      log.warning("no default search field specified in schema.");
    } else {
      defaultSearchFieldName=node.getNodeValue().trim();
      // throw exception if specified, but not found or not indexed
      if (defaultSearchFieldName!=null) {
        SchemaField defaultSearchField = getFields().get(defaultSearchFieldName);
        if ((defaultSearchField == null) || !defaultSearchField.indexed()) {
          String msg =  "default search field '" + defaultSearchFieldName + "' not defined or not indexed" ;
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, msg );
        }
      }
      log.info("default search field is "+defaultSearchFieldName);
    }

    node = (Node) xpath.evaluate("/schema/solrQueryParser/@defaultOperator", document, XPathConstants.NODE);
    if (node==null) {
      log.fine("using default query parser operator (OR)");
    } else {
      queryParserDefaultOperator=node.getNodeValue().trim();
      log.info("query parser default operator is "+queryParserDefaultOperator);
    }

    node = (Node) xpath.evaluate("/schema/uniqueKey/text()", document, XPathConstants.NODE);
    if (node==null) {
      log.warning("no uniqueKey specified in schema.");
    } else {
      uniqueKeyField=getIndexedField(node.getNodeValue().trim());
      uniqueKeyFieldName=uniqueKeyField.getName();
      uniqueKeyFieldType=uniqueKeyField.getType();
      log.info("unique key field: "+uniqueKeyFieldName);
      
      // Unless the uniqueKeyField is marked 'required=false' then make sure it exists
      if( Boolean.FALSE != explicitRequiredProp.get( uniqueKeyFieldName ) ) {
        uniqueKeyField.required = true;
        requiredFields.add(uniqueKeyField);
      }
    }

    /////////////// parse out copyField commands ///////////////
    // Map<String,ArrayList<SchemaField>> cfields = new HashMap<String,ArrayList<SchemaField>>();
    // expression = "/schema/copyField";
    
    dynamicCopyFields = new DynamicCopy[] {};
    expression = "//copyField";
    nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);

      for (int i=0; i<nodes.getLength(); i++) {
        node = nodes.item(i);
        NamedNodeMap attrs = node.getAttributes();

        String source = DOMUtil.getAttr(attrs,"source","copyField definition");
        String dest   = DOMUtil.getAttr(attrs,"dest",  "copyField definition");

        registerCopyField(source, dest);
     }
      
      for (Map.Entry<SchemaField, Integer> entry : copyFieldTargetCounts.entrySet())    {
        if (entry.getValue() > 1 && !entry.getKey().multiValued())  {
          log.warning("Field " + entry.getKey().name + " is not multivalued "+
                      "and destination for multiple copyFields ("+
                      entry.getValue()+")");
        }
      }

    } catch (SolrException e) {
      SolrConfig.severeErrors.add( e );
      throw e;
    } catch(Exception e) {
      // unexpected exception...
      SolrConfig.severeErrors.add( e );
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Schema Parsing Failed",e,false);
    }

    // create the field analyzers
    refreshAnalyzers();
  }

  /**
   * <p>
   * NOTE: this function is not thread safe.  However, it is safe to use within the standard
   * <code>inform( SolrCore core )</code> function for <code>SolrCoreAware</code> classes.
   * Outside <code>inform</code>, this could potentially throw a ConcurrentModificationException
   * </p>
   * 
   * @see SolrCoreAware
   */
  public void registerCopyField( String source, String dest )
  {
    boolean sourceIsPattern = isWildCard(source);
    boolean destIsPattern   = isWildCard(dest);

    log.fine("copyField source='"+source+"' dest='"+dest+"'");
    SchemaField d = getField(dest);

    if(sourceIsPattern) {
      if( destIsPattern ) {
        DynamicField df = null;
        for( DynamicField dd : dynamicFields ) {
          if( dd.regex.equals( dest ) ) {
            df = dd;
            break;
          }
        }
        if( df == null ) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "copyField dynamic destination must match a dynamicField." );
        }
        registerDynamicCopyField(new DynamicDestCopy(source, df ));
      }
      else {
        registerDynamicCopyField(new DynamicCopy(source, d));
      }
    } 
    else if( destIsPattern ) {
      String msg =  "copyField only supports a dynamic destination if the source is also dynamic" ;
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, msg );
    }
    else {
      // retrieve the field to force an exception if it doesn't exist
      SchemaField f = getField(source);

      SchemaField[] destArr = copyFields.get(source);
      if (destArr==null) {
        destArr=new SchemaField[]{d};
      } else {
        destArr = (SchemaField[])append(destArr,d);
      }
      copyFields.put(source,destArr);

      copyFieldTargetCounts.put(d, (copyFieldTargetCounts.containsKey(d) ? copyFieldTargetCounts.get(d) + 1 : 1));
    }
  }
  
  private void registerDynamicCopyField( DynamicCopy dcopy )
  {
    if( dynamicCopyFields == null ) {
      dynamicCopyFields = new DynamicCopy[] {dcopy};
    }
    else {
      DynamicCopy[] temp = new DynamicCopy[dynamicCopyFields.length+1];
      System.arraycopy(dynamicCopyFields,0,temp,0,dynamicCopyFields.length);
      temp[temp.length -1] = dcopy;
      dynamicCopyFields = temp;
    }
    log.finest("Dynamic Copy Field:" + dcopy );
  }

  private static Object[] append(Object[] orig, Object item) {
    Object[] newArr = (Object[])java.lang.reflect.Array.newInstance(orig.getClass().getComponentType(), orig.length+1);
    System.arraycopy(orig, 0, newArr, 0, orig.length);
    newArr[orig.length] = item;
    return newArr;
  }

  //
  // <analyzer><tokenizer class="...."/><tokenizer class="...." arg="....">
  //
  //
  private Analyzer readAnalyzer(Node node) throws XPathExpressionException {
    // parent node used to be passed in as "fieldtype"
    // if (!fieldtype.hasChildNodes()) return null;
    // Node node = DOMUtil.getChild(fieldtype,"analyzer");

    if (node == null) return null;
    NamedNodeMap attrs = node.getAttributes();
    String analyzerName = DOMUtil.getAttr(attrs,"class");
    if (analyzerName != null) {
      return (Analyzer)solrConfig.getResourceLoader().newInstance(analyzerName);
    }

    XPath xpath = XPathFactory.newInstance().newXPath();

    // Load the Tokenizer
    // Although an analyzer only allows a single Tokenizer, we load a list to make sure 
    // the configuration is ok
    // --------------------------------------------------------------------------------
    final ArrayList<TokenizerFactory> tokenizers = new ArrayList<TokenizerFactory>(1);
    AbstractPluginLoader<TokenizerFactory> tokenizerLoader = 
      new AbstractPluginLoader<TokenizerFactory>( "[schema.xml] analyzer/tokenizer", false, false )
    {
      @Override
      protected void init(TokenizerFactory plugin, Node node) throws Exception {
        if( !tokenizers.isEmpty() ) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
              "The schema defines multiple tokenizers for: "+node );
        }
        plugin.init( DOMUtil.toMapExcept(node.getAttributes(),"class") );
        tokenizers.add( plugin );
      }

      @Override
      protected TokenizerFactory register(String name, TokenizerFactory plugin) throws Exception {
        return null; // used for map registration
      }
    };
    tokenizerLoader.load( solrConfig.getResourceLoader(), (NodeList)xpath.evaluate("./tokenizer", node, XPathConstants.NODESET) );
    
    // Make sure something was loaded
    if( tokenizers.isEmpty() ) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"analyzer without class or tokenizer & filter list");
    }
    

    // Load the Filters
    // --------------------------------------------------------------------------------
    final ArrayList<TokenFilterFactory> filters = new ArrayList<TokenFilterFactory>();
    AbstractPluginLoader<TokenFilterFactory> filterLoader = 
      new AbstractPluginLoader<TokenFilterFactory>( "[schema.xml] analyzer/filter", false, false )
    {
      @Override
      protected void init(TokenFilterFactory plugin, Node node) throws Exception {
        if( plugin != null ) {
          plugin.init( DOMUtil.toMapExcept(node.getAttributes(),"class") );
          filters.add( plugin );
        }
      }

      @Override
      protected TokenFilterFactory register(String name, TokenFilterFactory plugin) throws Exception {
        return null; // used for map registration
      }
    };
    filterLoader.load( solrConfig.getResourceLoader(), (NodeList)xpath.evaluate("./filter", node, XPathConstants.NODESET) );
    
    return new TokenizerChain(tokenizers.get(0), filters.toArray(new TokenFilterFactory[filters.size()]));
  };


  static abstract class DynamicReplacement implements Comparable<DynamicReplacement> {
    final static int STARTS_WITH=1;
    final static int ENDS_WITH=2;

    final String regex;
    final int type;

    final String str;

    protected DynamicReplacement(String regex) {
      this.regex = regex;
      if (regex.startsWith("*")) {
        type=ENDS_WITH;
        str=regex.substring(1);
      }
      else if (regex.endsWith("*")) {
        type=STARTS_WITH;
        str=regex.substring(0,regex.length()-1);
      }
      else {
        throw new RuntimeException("dynamic field name must start or end with *");
      }
    }

    public boolean matches(String name) {
      if (type==STARTS_WITH && name.startsWith(str)) return true;
      else if (type==ENDS_WITH && name.endsWith(str)) return true;
      else return false;
    }

    /**
     * Sort order is based on length of regex.  Longest comes first.
     * @param other The object to compare to.
     * @return a negative integer, zero, or a positive integer
     * as this object is less than, equal to, or greater than
     * the specified object.
     */
    public int compareTo(DynamicReplacement other) {
      return other.regex.length() - regex.length();
    }
  }


  //
  // Instead of storing a type, this could be implemented as a hierarchy
  // with a virtual matches().
  // Given how often a search will be done, however, speed is the overriding
  // concern and I'm not sure which is faster.
  //
  final static class DynamicField extends DynamicReplacement {
    final SchemaField prototype;

    DynamicField(SchemaField prototype) {
      super(prototype.name);
      this.prototype=prototype;
    }

    SchemaField makeSchemaField(String name) {
      // could have a cache instead of returning a new one each time, but it might
      // not be worth it.
      // Actually, a higher level cache could be worth it to avoid too many
      // .startsWith() and .endsWith() comparisons.  it depends on how many
      // dynamic fields there are.
      return new SchemaField(prototype, name);
    }

    public String toString() {
      return prototype.toString();
    }
  }

  static class DynamicCopy extends DynamicReplacement {
    final SchemaField targetField;
    DynamicCopy(String regex, SchemaField targetField) {
      super(regex);
      this.targetField = targetField;
    }
    
    public SchemaField getTargetField( String sourceField )
    {
      return targetField;
    }

    @Override
    public String toString() {
      return targetField.toString();
    }
  }

  static class DynamicDestCopy extends DynamicCopy 
  {
    final DynamicField dynamic;
    
    final int dtype;
    final String dstr;
    
    DynamicDestCopy(String source, DynamicField dynamic) {
      super(source, dynamic.prototype );
      this.dynamic = dynamic;
      
      String dest = dynamic.regex;
      if (dest.startsWith("*")) {
        dtype=ENDS_WITH;
        dstr=dest.substring(1);
      }
      else if (dest.endsWith("*")) {
        dtype=STARTS_WITH;
        dstr=dest.substring(0,dest.length()-1);
      }
      else {
        throw new RuntimeException("dynamic copyField destination name must start or end with *");
      }
    }
    
    @Override
    public SchemaField getTargetField( String sourceField )
    {
      String dyn = ( type==STARTS_WITH ) 
        ? sourceField.substring( str.length() )
        : sourceField.substring( 0, sourceField.length()-str.length() );
      
      String name = (dtype==STARTS_WITH) ? (dstr+dyn) : (dyn+dstr);
      return dynamic.makeSchemaField( name );
    }

    @Override
    public String toString() {
      return targetField.toString();
    }
  }

  private DynamicField[] dynamicFields;
  public SchemaField[] getDynamicFieldPrototypes() {
    SchemaField[] df = new SchemaField[dynamicFields.length];
    for (int i=0;i<dynamicFields.length;i++) {
      df[i] = dynamicFields[i].prototype;
    }
    return df;
  }

  public String getDynamicPattern(String fieldName) {
   for (DynamicField df : dynamicFields) {
     if (df.matches(fieldName)) return df.regex;
   }
   return  null; 
  }
  
  /**
   * Does the schema have the specified field defined explicitly, i.e.
   * not as a result of a copyField declaration with a wildcard?  We
   * consider it explicitly defined if it matches a field or dynamicField
   * declaration.
   * @param fieldName
   * @return true if explicitly declared in the schema.
   */
  public boolean hasExplicitField(String fieldName) {
    if(fields.containsKey(fieldName)) {
      return true;
    }

    for (DynamicField df : dynamicFields) {
      if (df.matches(fieldName)) return true;
    }

    return false;
  }

  /**
   * Returns the SchemaField that should be used for the specified field name, or
   * null if none exists.
   *
   * @param fieldName may be an explicitly defined field, or a name that
   * matches a dynamic field.
   * @see #getFieldType
   */
  public SchemaField getFieldOrNull(String fieldName) {
     SchemaField f = fields.get(fieldName);
    if (f != null) return f;

    for (DynamicField df : dynamicFields) {
      if (df.matches(fieldName)) return df.makeSchemaField(fieldName);
    }
    
    return f;
  }

  /**
   * Returns the SchemaField that should be used for the specified field name
   *
   * @param fieldName may be an explicitly defined field, or a name that
   * matches a dynamic field.
   * @throws SolrException if no such field exists
   * @see #getFieldType
   */
  public SchemaField getField(String fieldName) {
     SchemaField f = fields.get(fieldName);
    if (f != null) return f;

    for (DynamicField df : dynamicFields) {
      if (df.matches(fieldName)) return df.makeSchemaField(fieldName);
    }

    // Hmmm, default field could also be implemented with a dynamic field of "*".
    // It would have to be special-cased and only used if nothing else matched.
    /***  REMOVED -YCS
    if (defaultFieldType != null) return new SchemaField(fieldName,defaultFieldType);
    ***/
    throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"undefined field "+fieldName);
  }

  /**
   * Returns the FieldType for the specified field name.
   *
   * <p>
   * This method exists because it can be more efficient then
   * {@link #getField} for dynamic fields if a full SchemaField isn't needed.
   * </p>
   *
   * @param fieldName may be an explicitly created field, or a name that
   * excercies a dynamic field.
   * @throws SolrException if no such field exists
   * @see #getField(String)
   * @see #getFieldTypeNoEx
   */
  public FieldType getFieldType(String fieldName) {
    SchemaField f = fields.get(fieldName);
    if (f != null) return f.getType();

    return getDynamicFieldType(fieldName);
  }

  /**
   * Returns the FieldType for the specified field name.
   *
   * <p>
   * This method exists because it can be more efficient then
   * {@link #getField} for dynamic fields if a full SchemaField isn't needed.
   * </p>
   *
   * @param fieldName may be an explicitly created field, or a name that
   * excercies a dynamic field.
   * @return null if field is not defined.
   * @see #getField(String)
   * @see #getFieldTypeNoEx
   */
  public FieldType getFieldTypeNoEx(String fieldName) {
    SchemaField f = fields.get(fieldName);
    if (f != null) return f.getType();
    return dynFieldType(fieldName);
  }


  /**
   * Returns the FieldType of the best matching dynamic field for
   * the specified field name
   *
   * @param fieldName may be an explicitly created field, or a name that
   * excercies a dynamic field.
   * @throws SolrException if no such field exists
   * @see #getField(String)
   * @see #getFieldTypeNoEx
   */
  public FieldType getDynamicFieldType(String fieldName) {
     for (DynamicField df : dynamicFields) {
      if (df.matches(fieldName)) return df.prototype.getType();
    }
    throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"undefined field "+fieldName);
  }

  private FieldType dynFieldType(String fieldName) {
     for (DynamicField df : dynamicFields) {
      if (df.matches(fieldName)) return df.prototype.getType();
    }
    return null;
  };


  private final Map<String, SchemaField[]> copyFields = new HashMap<String,SchemaField[]>();
  private DynamicCopy[] dynamicCopyFields;
  /**
   * keys are all fields copied to, count is num of copyField
   * directives that target them.
   */
  private Map<SchemaField, Integer> copyFieldTargetCounts
    = new HashMap<SchemaField, Integer>();

  /**
   * Get all copy fields, both the static and the dynamic ones.
   * @param destField
   * @return Array of fields copied into this field
   */

  public SchemaField[] getCopySources(String destField) {
    SchemaField f = getField(destField);
    if (!isCopyFieldTarget(f)) {
      return new SchemaField[0];
    }
    List<SchemaField> sf = new ArrayList<SchemaField>();
    for (Map.Entry<String, SchemaField[]> cfs : copyFields.entrySet()) {
      for (SchemaField cf : cfs.getValue()) {
        if (cf.getName().equals(destField)) {
          sf.add(getField(cfs.getKey()));
        }
      }
    }
    return sf.toArray(new SchemaField[1]);
  }
  /**
   * Get all copy fields, both the static and the dynamic ones.
   * @param sourceField
   * @return Array of fields to copy to.
   */
  public SchemaField[] getCopyFields(String sourceField) {
    // Get the dynamic ones into a list.
    List<SchemaField> matchCopyFields = new ArrayList<SchemaField>();

    for(DynamicCopy dynamicCopy : dynamicCopyFields) {
      if(dynamicCopy.matches(sourceField)) {
        matchCopyFields.add(dynamicCopy.getTargetField(sourceField));
      }
    }

    // Get the fixed ones, if there are any.
    SchemaField[] fixedCopyFields = copyFields.get(sourceField);

    boolean appendFixed = copyFields.containsKey(sourceField);

    // Construct the results by concatenating dynamic and fixed into a results array.

    SchemaField[] results = new SchemaField[matchCopyFields.size() + (appendFixed ? fixedCopyFields.length : 0)];

    matchCopyFields.toArray(results);

    if(appendFixed) {
      System.arraycopy(fixedCopyFields, 0, results, matchCopyFields.size(), fixedCopyFields.length);
    }

    return results;
  }
  
  /**
   * Check if a field is used as the destination of a copyField operation 
   * 
   * @since solr 1.3
   */
  public boolean isCopyFieldTarget( SchemaField f )
  {
    return copyFieldTargetCounts.containsKey( f );
  }

  /**
   * Is the given field name a wildcard?  I.e. does it begin or end with *?
   * @param name
   * @return true/false
   */
  private static boolean isWildCard(String name) {
    return  name.startsWith("*") || name.endsWith("*");
  }

}
