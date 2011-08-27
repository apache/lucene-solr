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
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.SimilarityProvider;
import org.apache.lucene.util.Version;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SystemIdResolver;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.search.SolrSimilarityProvider;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.w3c.dom.*;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.Reader;
import java.io.IOException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>IndexSchema</code> contains information about the valid fields in an index
 * and the types of those fields.
 *
 *
 */
public final class IndexSchema {
  public static final String DEFAULT_SCHEMA_FILE = "schema.xml";
  public static final String LUCENE_MATCH_VERSION_PARAM = "luceneMatchVersion";

  final static Logger log = LoggerFactory.getLogger(IndexSchema.class);
  private final SolrConfig solrConfig;
  private final String resourceName;
  private String name;
  private float version;
  private final SolrResourceLoader loader;

  private final HashMap<String, SchemaField> fields = new HashMap<String,SchemaField>();


  private final HashMap<String, FieldType> fieldTypes = new HashMap<String,FieldType>();

  private final List<SchemaField> fieldsWithDefaultValue = new ArrayList<SchemaField>();
  private final Collection<SchemaField> requiredFields = new HashSet<SchemaField>();
  private DynamicField[] dynamicFields;

  private Analyzer analyzer;
  private Analyzer queryAnalyzer;

  private String defaultSearchFieldName=null;
  private String queryParserDefaultOperator = "OR";


  private final Map<String, List<CopyField>> copyFieldsMap = new HashMap<String, List<CopyField>>();
  private DynamicCopy[] dynamicCopyFields;
  /**
   * keys are all fields copied to, count is num of copyField
   * directives that target them.
   */
  private Map<SchemaField, Integer> copyFieldTargetCounts
    = new HashMap<SchemaField, Integer>();

    /**
   * Constructs a schema using the specified resource name and stream.
   * If the is stream is null, the resource loader will load the schema resource by name.
   * @see SolrResourceLoader#openSchema
   * By default, this follows the normal config path directory searching rules.
   * @see SolrResourceLoader#openResource
   */
  public IndexSchema(SolrConfig solrConfig, String name, InputSource is) {
    this.solrConfig = solrConfig;
    if (name == null)
      name = DEFAULT_SCHEMA_FILE;
    this.resourceName = name;
    loader = solrConfig.getResourceLoader();
    if (is == null) {
      is = new InputSource(loader.openSchema(name));
      is.setSystemId(SystemIdResolver.createSystemIdFromResourceName(name));
    }
    readSchema(is);
    loader.inform( loader );
  }
  
  /**
   * @since solr 1.4
   */
  public SolrResourceLoader getResourceLoader()
  {
    return loader;
  }
  
  /** Gets the name of the resource used to instantiate this schema. */
  public String getResourceName() {
    return resourceName;
  }
  
  /** Gets the name of the schema as specified in the schema resource. */
  public String getSchemaName() {
    return name;
  }
  
  /** The Default Lucene Match Version for this IndexSchema */
  public Version getDefaultLuceneMatchVersion() {
    return solrConfig.luceneMatchVersion;
  }

  float getVersion() {
    return version;
  }


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

  private SimilarityProviderFactory similarityProviderFactory;

  /**
   * Returns the SimilarityProvider used for this index
   */
  public SimilarityProvider getSimilarityProvider() { return similarityProviderFactory.getSimilarityProvider(this); }

  /**
   * Returns the SimilarityProviderFactory used for this index
   */
  public SimilarityProviderFactory getSimilarityProviderFactory() { return similarityProviderFactory; }

  private Similarity fallbackSimilarity;
  
  /** fallback similarity, in the case a field doesnt specify */
  public Similarity getFallbackSimilarity() { return fallbackSimilarity; }

  /**
   * Returns the Analyzer used when indexing documents for this index
   *
   * <p>
   * This Analyzer is field (and dynamic field) name aware, and delegates to
   * a field specific Analyzer based on the field type.
   * </p>
   */
  public Analyzer getAnalyzer() { return analyzer; }



  /**
   * Returns the Analyzer used when searching this index
   *
   * <p>
   * This Analyzer is field (and dynamic field) name aware, and delegates to
   * a field specific Analyzer based on the field type.
   * </p>
   */
  public Analyzer getQueryAnalyzer() { return queryAnalyzer; }

  
  /**
   * Name of the default search field specified in the schema file
   */
  public String getDefaultSearchFieldName() {
    return defaultSearchFieldName;
  }

  /**
   * default operator ("AND" or "OR") for QueryParser
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
  public IndexableField getUniqueKeyField(org.apache.lucene.document.Document doc) {
    return doc.getField(uniqueKeyFieldName);  // this should return null if name is null
  }

  /**
   * The printable value of the Unique Key field for
   * the specified Document
   * @return null if this schema has no unique key field
   */
  public String printableUniqueKey(org.apache.lucene.document.Document doc) {
    IndexableField f = doc.getField(uniqueKeyFieldName);
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

    @Override
    public TokenStream tokenStream(String fieldName, Reader reader)
    {
      return getAnalyzer(fieldName).tokenStream(fieldName,reader);
    }

    @Override
    public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
      return getAnalyzer(fieldName).reusableTokenStream(fieldName,reader);
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

  private void readSchema(InputSource is) {
    log.info("Reading Solr Schema");

    try {
      // pass the config resource loader to avoid building an empty one for no reason:
      // in the current case though, the stream is valid so we wont load the resource by name
      Config schemaConf = new Config(loader, "schema", is, "/schema/");
      Document document = schemaConf.getDocument();
      final XPath xpath = schemaConf.getXPath();
      final List<SchemaAware> schemaAware = new ArrayList<SchemaAware>();
      Node nd = (Node) xpath.evaluate("/schema/@name", document, XPathConstants.NODE);
      if (nd==null) {
        log.warn("schema has no name!");
      } else {
        name = nd.getNodeValue();
        log.info("Schema name=" + name);
      }

      version = schemaConf.getFloat("/schema/@version", 1.0f);


      // load the Field Types

      final FieldTypePluginLoader typeLoader 
        = new FieldTypePluginLoader(this, fieldTypes, schemaAware);

      String expression = "/schema/types/fieldtype | /schema/types/fieldType";
      NodeList nodes = (NodeList) xpath.evaluate(expression, document, 
                                                 XPathConstants.NODESET);
      typeLoader.load( loader, nodes );

      // load the Fields

      // Hang on to the fields that say if they are required -- this lets us set a reasonable default for the unique key
      Map<String,Boolean> explicitRequiredProp = new HashMap<String, Boolean>();
      ArrayList<DynamicField> dFields = new ArrayList<DynamicField>();
      expression = "/schema/fields/field | /schema/fields/dynamicField";
      nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);

      for (int i=0; i<nodes.getLength(); i++) {
        Node node = nodes.item(i);

        NamedNodeMap attrs = node.getAttributes();

        String name = DOMUtil.getAttr(attrs,"name","field definition");
        log.trace("reading field def "+name);
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
              + f.getName() + "' [[["+old.toString()+"]]] and [[["+f.toString()+"]]]";
            SolrException t = new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg );
            SolrException.logOnce(log,null,t);
            SolrConfig.severeErrors.add( t );
            throw t;
          }
          log.debug("field defined: " + f);
          if( f.getDefaultValue() != null ) {
            log.debug(name+" contains default value: " + f.getDefaultValue());
            fieldsWithDefaultValue.add( f );
          }
          if (f.isRequired()) {
            log.debug(name+" is required in this schema");
            requiredFields.add(f);
          }
        } else if (node.getNodeName().equals("dynamicField")) {
          // make sure nothing else has the same path
          addDynamicField(dFields, f);
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

    log.trace("Dynamic Field Ordering:" + dFields);

    // stuff it in a normal array for faster access
    dynamicFields = dFields.toArray(new DynamicField[dFields.size()]);

    Node node = (Node) xpath.evaluate("/schema/similarity", document, XPathConstants.NODE);
    Similarity similarity = readSimilarity(loader, node);
    fallbackSimilarity = similarity == null ? new DefaultSimilarity() : similarity;

    node = (Node) xpath.evaluate("/schema/similarityProvider", document, XPathConstants.NODE);
    if (node==null) {
      final SolrSimilarityProvider provider = new SolrSimilarityProvider(this);
      similarityProviderFactory = new SimilarityProviderFactory() {
        @Override
        public SolrSimilarityProvider getSimilarityProvider(IndexSchema schema) {
          return provider;
        }
      };
      log.debug("using default similarityProvider");
    } else {
      final Object obj = loader.newInstance(((Element) node).getAttribute("class"));
      // just like always, assume it's a SimilarityProviderFactory and get a ClassCastException - reasonable error handling
      // configure a factory, get a similarity back
      NamedList<?> args = DOMUtil.childNodesToNamedList(node);
      similarityProviderFactory = (SimilarityProviderFactory)obj;
      similarityProviderFactory.init(args);
      if (similarityProviderFactory instanceof SchemaAware){
        schemaAware.add((SchemaAware) similarityProviderFactory);
      }
      log.debug("using similarityProvider factory" + similarityProviderFactory.getClass().getName());
    }

    node = (Node) xpath.evaluate("/schema/defaultSearchField/text()", document, XPathConstants.NODE);
    if (node==null) {
      log.warn("no default search field specified in schema.");
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
      log.debug("using default query parser operator (OR)");
    } else {
      queryParserDefaultOperator=node.getNodeValue().trim();
      log.info("query parser default operator is "+queryParserDefaultOperator);
    }

    node = (Node) xpath.evaluate("/schema/uniqueKey/text()", document, XPathConstants.NODE);
    if (node==null) {
      log.warn("no uniqueKey specified in schema.");
    } else {
      uniqueKeyField=getIndexedField(node.getNodeValue().trim());
      if (!uniqueKeyField.stored()) {
        log.error("uniqueKey is not stored - distributed search will not work");
      }
      if (uniqueKeyField.multiValued()) {
        log.error("uniqueKey should not be multivalued");
      }
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
        String maxChars = DOMUtil.getAttr(attrs, "maxChars");
        int maxCharsInt = CopyField.UNLIMITED;
        if (maxChars != null) {
          try {
            maxCharsInt = Integer.parseInt(maxChars);
          } catch (NumberFormatException e) {
            log.warn("Couldn't parse maxChars attribute for copyField from "
                    + source + " to " + dest + " as integer. The whole field will be copied.");
          }
        }

        registerCopyField(source, dest, maxCharsInt);
     }
      
      for (Map.Entry<SchemaField, Integer> entry : copyFieldTargetCounts.entrySet())    {
        if (entry.getValue() > 1 && !entry.getKey().multiValued())  {
          log.warn("Field " + entry.getKey().name + " is not multivalued "+
                      "and destination for multiple copyFields ("+
                      entry.getValue()+")");
        }
      }
      //Run the callbacks on SchemaAware now that everything else is done
      for (SchemaAware aware : schemaAware) {
        aware.inform(this);
      }
    } catch (SolrException e) {
      SolrConfig.severeErrors.add( e );
      throw e;
    } catch(Exception e) {
      // unexpected exception...
      SolrConfig.severeErrors.add( e );
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Schema Parsing Failed: " + e.getMessage(), e,false);
    }

    // create the field analyzers
    refreshAnalyzers();

  }

  private void addDynamicField(List<DynamicField> dFields, SchemaField f) {
    boolean dup = isDuplicateDynField(dFields, f);
    if( !dup ) {
      addDynamicFieldNoDupCheck(dFields, f);
    } else {
      String msg = "[schema.xml] Duplicate DynamicField definition for '"
              + f.getName() + "'";

      SolrException t = new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg);
      SolrException.logOnce(log, null, t);
      SolrConfig.severeErrors.add(t);
      throw t;
    }
  }

  /**
   * Register one or more new Dynamic Field with the Schema.
   * @param f The {@link org.apache.solr.schema.SchemaField}
   */
  public void registerDynamicField(SchemaField ... f) {
    List<DynamicField> dynFields = new ArrayList<DynamicField>(Arrays.asList(dynamicFields));
    for (SchemaField field : f) {
      if (isDuplicateDynField(dynFields, field) == false) {
        log.debug("dynamic field creation for schema field: " + field.getName());
        addDynamicFieldNoDupCheck(dynFields, field);
      } else {
        log.debug("dynamic field already exists: dynamic field: [" + field.getName() + "]");
      }
    }
    Collections.sort(dynFields);
    dynamicFields = dynFields.toArray(new DynamicField[dynFields.size()]);
  }

  private void addDynamicFieldNoDupCheck(List<DynamicField> dFields, SchemaField f) {
    dFields.add(new DynamicField(f));
    log.debug("dynamic field defined: " + f);
  }

  private boolean isDuplicateDynField(List<DynamicField> dFields, SchemaField f) {
    for( DynamicField df : dFields ) {
      if( df.regex.equals( f.name ) ) return true;
    }
    return false;
  }

  public void registerCopyField( String source, String dest )
  {
    registerCopyField(source, dest, CopyField.UNLIMITED);
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
  public void registerCopyField( String source, String dest, int maxChars )
  {
    boolean sourceIsPattern = isWildCard(source);
    boolean destIsPattern   = isWildCard(dest);

    log.debug("copyField source='"+source+"' dest='"+dest+"' maxChars='"+maxChars);
    SchemaField d = getFieldOrNull(dest);
    if(d == null){
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "copyField destination :'"+dest+"' does not exist" );
    }

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
        registerDynamicCopyField(new DynamicDestCopy(source, df, maxChars ));
      }
      else {
        registerDynamicCopyField(new DynamicCopy(source, d, maxChars));
      }
    } 
    else if( destIsPattern ) {
      String msg =  "copyField only supports a dynamic destination if the source is also dynamic" ;
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, msg );
    }
    else {
      // retrieve the field to force an exception if it doesn't exist
      SchemaField f = getField(source);

      List<CopyField> copyFieldList = copyFieldsMap.get(source);
      if (copyFieldList == null) {
        copyFieldList = new ArrayList<CopyField>();
        copyFieldsMap.put(source, copyFieldList);
      }
      copyFieldList.add(new CopyField(f, d, maxChars));

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
    log.trace("Dynamic Copy Field:" + dcopy );
  }

  private static Object[] append(Object[] orig, Object item) {
    Object[] newArr = (Object[])java.lang.reflect.Array.newInstance(orig.getClass().getComponentType(), orig.length+1);
    System.arraycopy(orig, 0, newArr, 0, orig.length);
    newArr[orig.length] = item;
    return newArr;
  }

  static Similarity readSimilarity(ResourceLoader loader, Node node) throws XPathExpressionException {
    if (node==null) {
      return null;
    } else {
      SimilarityFactory similarityFactory;
      final Object obj = loader.newInstance(((Element) node).getAttribute("class"));
      if (obj instanceof SimilarityFactory) {
        // configure a factory, get a similarity back
        SolrParams params = SolrParams.toSolrParams(DOMUtil.childNodesToNamedList(node));
        similarityFactory = (SimilarityFactory)obj;
        similarityFactory.init(params);
      } else {
        // just like always, assume it's a Similarity and get a ClassCastException - reasonable error handling
        similarityFactory = new SimilarityFactory() {
          @Override
          public Similarity getSimilarity() {
            return (Similarity) obj;
          }
        };
      }
      return similarityFactory.getSimilarity();
    }
  }


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

    @Override
    public String toString() {
      return prototype.toString();
    }
  }

  static class DynamicCopy extends DynamicReplacement {
    final SchemaField targetField;
    final int maxChars;

    DynamicCopy(String regex, SchemaField targetField) {
      this(regex, targetField, CopyField.UNLIMITED);
    }

    DynamicCopy(String regex, SchemaField targetField, int maxChars) {
      super(regex);
      this.targetField = targetField;
      this.maxChars = maxChars;
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
      this(source, dynamic, CopyField.UNLIMITED);
    }
      
    DynamicDestCopy(String source, DynamicField dynamic, int maxChars) {
      super(source, dynamic.prototype, maxChars);
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
   * Is the specified field dynamic or not.
   * @param fieldName
   * @return true if the specified field is dynamic
   */
  public boolean isDynamicField(String fieldName) {
    if(fields.containsKey(fieldName)) {
      return false;
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
   * @param fieldName may be an explicitly defined field or a name that
   * matches a dynamic field.
   * @see #getFieldType
   * @see #getField(String)
   * @return The {@link org.apache.solr.schema.SchemaField}
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
   * @param fieldName may be an explicitly defined field or a name that
   * matches a dynamic field.
   * @throws SolrException if no such field exists
   * @see #getFieldType
   * @see #getFieldOrNull(String)
   * @return The {@link SchemaField}
   */
  public SchemaField getField(String fieldName) {
    SchemaField f = getFieldOrNull(fieldName);
    if (f != null) return f;


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
   * Given the name of a {@link org.apache.solr.schema.FieldType} (not to be confused with {@link #getFieldType(String)} which
   * takes in the name of a field), return the {@link org.apache.solr.schema.FieldType}.
   * @param fieldTypeName The name of the {@link org.apache.solr.schema.FieldType}
   * @return The {@link org.apache.solr.schema.FieldType} or null.
   */
  public FieldType getFieldTypeByName(String fieldTypeName){
    return fieldTypes.get(fieldTypeName);
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
    for (Map.Entry<String, List<CopyField>> cfs : copyFieldsMap.entrySet()) {
      for (CopyField copyField : cfs.getValue()) {
        if (copyField.getDestination().getName().equals(destField)) {
          sf.add(copyField.getSource());
        }
      }
    }
    return sf.toArray(new SchemaField[sf.size()]);
  }

  /**
   * Get all copy fields for a specified source field, both static
   * and dynamic ones.
   * @param sourceField
   * @return List of CopyFields to copy to.
   * @since solr 1.4
   */
  // This is useful when we need the maxSize param of each CopyField
  public List<CopyField> getCopyFieldsList(final String sourceField){
    final List<CopyField> result = new ArrayList<CopyField>();
    for (DynamicCopy dynamicCopy : dynamicCopyFields) {
      if (dynamicCopy.matches(sourceField)) {
        result.add(new CopyField(getField(sourceField), dynamicCopy.getTargetField(sourceField), dynamicCopy.maxChars));
      }
    }
    List<CopyField> fixedCopyFields = copyFieldsMap.get(sourceField);
    if (fixedCopyFields != null)
    {
      result.addAll(fixedCopyFields);
    }

    return result;
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
