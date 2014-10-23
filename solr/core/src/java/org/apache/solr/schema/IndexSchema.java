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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.lucene.util.Version;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SchemaXmlWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.search.similarities.DefaultSimilarityFactory;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/**
 * <code>IndexSchema</code> contains information about the valid fields in an index
 * and the types of those fields.
 *
 *
 */
public class IndexSchema {
  public static final String COPY_FIELD = "copyField";
  public static final String COPY_FIELDS = COPY_FIELD + "s";
  public static final String DEFAULT_OPERATOR = "defaultOperator";
  public static final String DEFAULT_SCHEMA_FILE = "schema.xml";
  public static final String DEFAULT_SEARCH_FIELD = "defaultSearchField";
  public static final String DESTINATION = "dest";
  public static final String DYNAMIC_FIELD = "dynamicField";
  public static final String DYNAMIC_FIELDS = DYNAMIC_FIELD + "s";
  public static final String FIELD = "field";
  public static final String FIELDS = FIELD + "s";
  public static final String FIELD_TYPE = "fieldType";
  public static final String FIELD_TYPES = FIELD_TYPE + "s";
  public static final String INTERNAL_POLY_FIELD_PREFIX = "*" + FieldType.POLY_FIELD_SEPARATOR;
  public static final String LUCENE_MATCH_VERSION_PARAM = "luceneMatchVersion";
  public static final String NAME = "name";
  public static final String REQUIRED = "required";
  public static final String SCHEMA = "schema";
  public static final String SIMILARITY = "similarity";
  public static final String SLASH = "/";
  public static final String SOLR_QUERY_PARSER = "solrQueryParser";
  public static final String SOURCE = "source";
  public static final String TYPE = "type";
  public static final String TYPES = "types";
  public static final String UNIQUE_KEY = "uniqueKey";
  public static final String VERSION = "version";

  private static final String AT = "@";
  private static final String DESTINATION_DYNAMIC_BASE = "destDynamicBase";
  private static final String MAX_CHARS = "maxChars";
  private static final String SOLR_CORE_NAME = "solr.core.name";
  private static final String SOURCE_DYNAMIC_BASE = "sourceDynamicBase";
  private static final String SOURCE_EXPLICIT_FIELDS = "sourceExplicitFields";
  private static final String TEXT_FUNCTION = "text()";
  private static final String XPATH_OR = " | ";

  final static Logger log = LoggerFactory.getLogger(IndexSchema.class);
  protected final SolrConfig solrConfig;
  protected String resourceName;
  protected String name;
  protected float version;
  protected final SolrResourceLoader loader;

  protected Map<String,SchemaField> fields = new HashMap<>();
  protected Map<String,FieldType> fieldTypes = new HashMap<>();

  protected List<SchemaField> fieldsWithDefaultValue = new ArrayList<>();
  protected Collection<SchemaField> requiredFields = new HashSet<>();
  protected volatile DynamicField[] dynamicFields;
  public DynamicField[] getDynamicFields() { return dynamicFields; }

  private Analyzer indexAnalyzer;
  private Analyzer queryAnalyzer;

  protected List<SchemaAware> schemaAware = new ArrayList<>();

  protected String defaultSearchFieldName=null;
  protected String queryParserDefaultOperator = "OR";
  protected boolean isExplicitQueryParserDefaultOperator = false;


  protected Map<String, List<CopyField>> copyFieldsMap = new HashMap<>();
  public Map<String,List<CopyField>> getCopyFieldsMap() { return Collections.unmodifiableMap(copyFieldsMap); }
  
  protected DynamicCopy[] dynamicCopyFields;
  public DynamicCopy[] getDynamicCopyFields() { return dynamicCopyFields; }

  /**
   * keys are all fields copied to, count is num of copyField
   * directives that target them.
   */
  protected Map<SchemaField, Integer> copyFieldTargetCounts = new HashMap<>();

    /**
   * Constructs a schema using the specified resource name and stream.
   * @see SolrResourceLoader#openSchema
   * By default, this follows the normal config path directory searching rules.
   * @see SolrResourceLoader#openResource
   */
  public IndexSchema(SolrConfig solrConfig, String name, InputSource is) {
    assert null != solrConfig : "SolrConfig should never be null";
    assert null != name : "schema resource name should never be null";
    assert null != is : "schema InputSource should never be null";

    this.solrConfig = solrConfig;
    this.resourceName = name;
    loader = solrConfig.getResourceLoader();
    try {
      readSchema(is);
      loader.inform(loader);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * @since solr 1.4
   */
  public SolrResourceLoader getResourceLoader() {
    return loader;
  }
  
  /** Gets the name of the resource used to instantiate this schema. */
  public String getResourceName() {
    return resourceName;
  }

  /** Sets the name of the resource used to instantiate this schema. */
  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  /** Gets the name of the schema as specified in the schema resource. */
  public String getSchemaName() {
    return name;
  }
  
  /** The Default Lucene Match Version for this IndexSchema */
  public Version getDefaultLuceneMatchVersion() {
    return solrConfig.luceneMatchVersion;
  }

  public float getVersion() {
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

  protected Similarity similarity;

  /**
   * Returns the Similarity used for this index
   */
  public Similarity getSimilarity() {
    if (null == similarity) {
      similarity = similarityFactory.getSimilarity();
    }
    return similarity; 
  }

  protected SimilarityFactory similarityFactory;
  protected boolean isExplicitSimilarity = false;


  /** Returns the SimilarityFactory that constructed the Similarity for this index */
  public SimilarityFactory getSimilarityFactory() { return similarityFactory; }
  
  /**
   * Returns the Analyzer used when indexing documents for this index
   *
   * <p>
   * This Analyzer is field (and dynamic field) name aware, and delegates to
   * a field specific Analyzer based on the field type.
   * </p>
   */
  public Analyzer getIndexAnalyzer() { return indexAnalyzer; }

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
   * Name of the default search field specified in the schema file.
   * <br/><b>Note:</b>Avoid calling this, try to use this method so that the 'df' param is consulted as an override:
   * {@link org.apache.solr.search.QueryParsing#getDefaultField(IndexSchema, String)}
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

  protected SchemaField uniqueKeyField;

  /**
   * Unique Key field specified in the schema file
   * @return null if this schema has no unique key field
   */
  public SchemaField getUniqueKeyField() { return uniqueKeyField; }

  protected String uniqueKeyFieldName;
  protected FieldType uniqueKeyFieldType;

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
  public String printableUniqueKey(StoredDocument doc) {
    StorableField f = doc.getField(uniqueKeyFieldName);
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
  public void refreshAnalyzers() {
    indexAnalyzer = new SolrIndexAnalyzer();
    queryAnalyzer = new SolrQueryAnalyzer();
  }
  
  public Map<String,UninvertingReader.Type> getUninversionMap(IndexReader reader) {
    Map<String,UninvertingReader.Type> map = new HashMap<>();
    for (FieldInfo f : MultiFields.getMergedFieldInfos(reader)) {
      if (f.hasDocValues() == false && f.isIndexed()) {
        SchemaField sf = getFieldOrNull(f.name);
        if (sf != null) {
          UninvertingReader.Type type = sf.getType().getUninversionType(sf);
          if (type != null) {
            map.put(f.name, type);
          }
        }
      }
    }
    return map;
  }

  /**
   * Writes the schema in schema.xml format to the given writer 
   */
  void persist(Writer writer) throws IOException {
    final SolrQueryResponse response = new SolrQueryResponse();
    response.add(IndexSchema.SCHEMA, getNamedPropertyValues());
    final NamedList args = new NamedList(Arrays.<Object>asList("indent", "on"));
    final LocalSolrQueryRequest req = new LocalSolrQueryRequest(null, args);
    final SchemaXmlWriter schemaXmlWriter = new SchemaXmlWriter(writer, req, response);
    schemaXmlWriter.setEmitManagedSchemaDoNotEditWarning(true);
    schemaXmlWriter.writeResponse();
    schemaXmlWriter.close();
  }

  public boolean isMutable() {
    return false;
  }

  private class SolrIndexAnalyzer extends DelegatingAnalyzerWrapper {
    protected final HashMap<String, Analyzer> analyzers;

    SolrIndexAnalyzer() {
      super(PER_FIELD_REUSE_STRATEGY);
      analyzers = analyzerCache();
    }

    protected HashMap<String, Analyzer> analyzerCache() {
      HashMap<String, Analyzer> cache = new HashMap<>();
      for (SchemaField f : getFields().values()) {
        Analyzer analyzer = f.getType().getIndexAnalyzer();
        cache.put(f.getName(), analyzer);
      }
      return cache;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
      Analyzer analyzer = analyzers.get(fieldName);
      return analyzer != null ? analyzer : getDynamicFieldType(fieldName).getIndexAnalyzer();
    }

  }

  private class SolrQueryAnalyzer extends SolrIndexAnalyzer {
    SolrQueryAnalyzer() {}

    @Override
    protected HashMap<String, Analyzer> analyzerCache() {
      HashMap<String, Analyzer> cache = new HashMap<>();
       for (SchemaField f : getFields().values()) {
        Analyzer analyzer = f.getType().getQueryAnalyzer();
        cache.put(f.getName(), analyzer);
      }
      return cache;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
      Analyzer analyzer = analyzers.get(fieldName);
      return analyzer != null ? analyzer : getDynamicFieldType(fieldName).getQueryAnalyzer();
    }
  }

  protected void readSchema(InputSource is) {
    String resourcePath = CloudUtil.unifiedResourcePath(loader) + resourceName;
    log.info("Reading Solr Schema from " + resourcePath);

    try {
      // pass the config resource loader to avoid building an empty one for no reason:
      // in the current case though, the stream is valid so we wont load the resource by name
      Config schemaConf = new Config(loader, SCHEMA, is, SLASH+SCHEMA+SLASH);
      Document document = schemaConf.getDocument();
      final XPath xpath = schemaConf.getXPath();
      String expression = stepsToPath(SCHEMA, AT + NAME);
      Node nd = (Node) xpath.evaluate(expression, document, XPathConstants.NODE);
      StringBuilder sb = new StringBuilder();
      // Another case where the initialization from the test harness is different than the "real world"
      sb.append("[");
      if (loader.getCoreProperties() != null) {
        sb.append(loader.getCoreProperties().getProperty(SOLR_CORE_NAME));
      } else {
        sb.append("null");
      }
      sb.append("] ");
      if (nd==null) {
        sb.append("schema has no name!");
        log.warn(sb.toString());
      } else {
        name = nd.getNodeValue();
        sb.append("Schema ");
        sb.append(NAME);
        sb.append("=");
        sb.append(name);
        log.info(sb.toString());
      }

      //                      /schema/@version
      expression = stepsToPath(SCHEMA, AT + VERSION);
      version = schemaConf.getFloat(expression, 1.0f);

      // load the Field Types
      final FieldTypePluginLoader typeLoader = new FieldTypePluginLoader(this, fieldTypes, schemaAware);
      expression = getFieldTypeXPathExpressions();
      NodeList nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);
      typeLoader.load(loader, nodes);

      // load the fields
      Map<String,Boolean> explicitRequiredProp = loadFields(document, xpath);

      expression = stepsToPath(SCHEMA, SIMILARITY); //   /schema/similarity
      Node node = (Node) xpath.evaluate(expression, document, XPathConstants.NODE);
      similarityFactory = readSimilarity(loader, node);
      if (similarityFactory == null) {
        similarityFactory = new DefaultSimilarityFactory();
        final NamedList similarityParams = new NamedList();
        Version luceneVersion = getDefaultLuceneMatchVersion();
        if (!luceneVersion.onOrAfter(Version.LUCENE_4_7_0)) {
          similarityParams.add(DefaultSimilarityFactory.DISCOUNT_OVERLAPS, false);
        }
        similarityFactory.init(SolrParams.toSolrParams(similarityParams));
      } else {
        isExplicitSimilarity = true;
      }
      if ( ! (similarityFactory instanceof SolrCoreAware)) {
        // if the sim factory isn't SolrCoreAware (and hence schema aware), 
        // then we are responsible for erroring if a field type is trying to specify a sim.
        for (FieldType ft : fieldTypes.values()) {
          if (null != ft.getSimilarity()) {
            String msg = "FieldType '" + ft.getTypeName()
                + "' is configured with a similarity, but the global similarity does not support it: " 
                + similarityFactory.getClass();
            log.error(msg);
            throw new SolrException(ErrorCode.SERVER_ERROR, msg);
          }
        }
      }

      //                      /schema/defaultSearchField/text()
      expression = stepsToPath(SCHEMA, DEFAULT_SEARCH_FIELD, TEXT_FUNCTION);
      node = (Node) xpath.evaluate(expression, document, XPathConstants.NODE);
      if (node==null) {
        log.debug("no default search field specified in schema.");
      } else {
        defaultSearchFieldName=node.getNodeValue().trim();
        // throw exception if specified, but not found or not indexed
        if (defaultSearchFieldName!=null) {
          SchemaField defaultSearchField = getFields().get(defaultSearchFieldName);
          if ((defaultSearchField == null) || !defaultSearchField.indexed()) {
            String msg =  "default search field '" + defaultSearchFieldName + "' not defined or not indexed" ;
            throw new SolrException(ErrorCode.SERVER_ERROR, msg);
          }
        }
        log.info("default search field in schema is "+defaultSearchFieldName);
      }

      //                      /schema/solrQueryParser/@defaultOperator
      expression = stepsToPath(SCHEMA, SOLR_QUERY_PARSER, AT + DEFAULT_OPERATOR);
      node = (Node) xpath.evaluate(expression, document, XPathConstants.NODE);
      if (node==null) {
        log.debug("using default query parser operator (OR)");
      } else {
        isExplicitQueryParserDefaultOperator = true;
        queryParserDefaultOperator=node.getNodeValue().trim();
        log.info("query parser default operator is "+queryParserDefaultOperator);
      }

      //                      /schema/uniqueKey/text()
      expression = stepsToPath(SCHEMA, UNIQUE_KEY, TEXT_FUNCTION);
      node = (Node) xpath.evaluate(expression, document, XPathConstants.NODE);
      if (node==null) {
        log.warn("no " + UNIQUE_KEY + " specified in schema.");
      } else {
        uniqueKeyField=getIndexedField(node.getNodeValue().trim());
        if (null != uniqueKeyField.getDefaultValue()) {
          String msg = UNIQUE_KEY + " field ("+uniqueKeyFieldName+
              ") can not be configured with a default value ("+
              uniqueKeyField.getDefaultValue()+")";
          log.error(msg);
          throw new SolrException(ErrorCode.SERVER_ERROR, msg);
        }

        if (!uniqueKeyField.stored()) {
          log.warn(UNIQUE_KEY + " is not stored - distributed search and MoreLikeThis will not work");
        }
        if (uniqueKeyField.multiValued()) {
          String msg = UNIQUE_KEY + " field ("+uniqueKeyFieldName+
              ") can not be configured to be multivalued";
          log.error(msg);
          throw new SolrException(ErrorCode.SERVER_ERROR, msg);
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
      loadCopyFields(document, xpath);

      //Run the callbacks on SchemaAware now that everything else is done
      for (SchemaAware aware : schemaAware) {
        aware.inform(this);
      }
    } catch (SolrException e) {
      throw new SolrException(ErrorCode.getErrorCode(e.code()), e.getMessage() + ". Schema file is " +
          resourcePath, e);
    } catch(Exception e) {
      // unexpected exception...
      throw new SolrException(ErrorCode.SERVER_ERROR,
          "Schema Parsing Failed: " + e.getMessage() + ". Schema file is " + resourcePath,
          e);
    }

    // create the field analyzers
    refreshAnalyzers();
  }

  /** 
   * Loads fields and dynamic fields.
   * 
   * @return a map from field name to explicit required value  
   */ 
  protected synchronized Map<String,Boolean> loadFields(Document document, XPath xpath) throws XPathExpressionException {
    // Hang on to the fields that say if they are required -- this lets us set a reasonable default for the unique key
    Map<String,Boolean> explicitRequiredProp = new HashMap<>();
    
    ArrayList<DynamicField> dFields = new ArrayList<>();

    //                  /schema/field | /schema/dynamicField | /schema/fields/field | /schema/fields/dynamicField
    String expression = stepsToPath(SCHEMA, FIELD)
        + XPATH_OR + stepsToPath(SCHEMA, DYNAMIC_FIELD)
        + XPATH_OR + stepsToPath(SCHEMA, FIELDS, FIELD)
        + XPATH_OR + stepsToPath(SCHEMA, FIELDS, DYNAMIC_FIELD);

    NodeList nodes = (NodeList)xpath.evaluate(expression, document, XPathConstants.NODESET);

    for (int i=0; i<nodes.getLength(); i++) {
      Node node = nodes.item(i);

      NamedNodeMap attrs = node.getAttributes();

      String name = DOMUtil.getAttr(attrs, NAME, "field definition");
      log.trace("reading field def "+name);
      String type = DOMUtil.getAttr(attrs, TYPE, "field " + name);

      FieldType ft = fieldTypes.get(type);
      if (ft==null) {
        throw new SolrException
            (ErrorCode.BAD_REQUEST, "Unknown " + FIELD_TYPE + " '" + type + "' specified on field " + name);
      }

      Map<String,String> args = DOMUtil.toMapExcept(attrs, NAME, TYPE);
      if (null != args.get(REQUIRED)) {
        explicitRequiredProp.put(name, Boolean.valueOf(args.get(REQUIRED)));
      }

      SchemaField f = SchemaField.create(name,ft,args);

      if (node.getNodeName().equals(FIELD)) {
        SchemaField old = fields.put(f.getName(),f);
        if( old != null ) {
          String msg = "[schema.xml] Duplicate field definition for '"
            + f.getName() + "' [[["+old.toString()+"]]] and [[["+f.toString()+"]]]";
          throw new SolrException(ErrorCode.SERVER_ERROR, msg );
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
      } else if (node.getNodeName().equals(DYNAMIC_FIELD)) {
        if (isValidDynamicField(dFields, f)) {
          addDynamicFieldNoDupCheck(dFields, f);
        }
      } else {
        // we should never get here
        throw new RuntimeException("Unknown field type");
      }
    }

    //fields with default values are by definition required
    //add them to required fields, and we only have to loop once
    // in DocumentBuilder.getDoc()
    requiredFields.addAll(fieldsWithDefaultValue);

    dynamicFields = dynamicFieldListToSortedArray(dFields);
                                                                   
    return explicitRequiredProp;
  }
  
  /**
   * Sort the dynamic fields and stuff them in a normal array for faster access.
   */
  protected static DynamicField[] dynamicFieldListToSortedArray(List<DynamicField> dynamicFieldList) {
    // Avoid creating the array twice by converting to an array first and using Arrays.sort(),
    // rather than Collections.sort() then converting to an array, since Collections.sort()
    // copies to an array first, then sets each collection member from the array. 
    DynamicField[] dFields = dynamicFieldList.toArray(new DynamicField[dynamicFieldList.size()]);
    Arrays.sort(dFields);

    log.trace("Dynamic Field Ordering:" + Arrays.toString(dFields));

    return dFields; 
  }

  /**
   * Loads the copy fields
   */
  protected synchronized void loadCopyFields(Document document, XPath xpath) throws XPathExpressionException {
    String expression = "//" + COPY_FIELD;
    NodeList nodes = (NodeList)xpath.evaluate(expression, document, XPathConstants.NODESET);

    for (int i=0; i<nodes.getLength(); i++) {
      Node node = nodes.item(i);
      NamedNodeMap attrs = node.getAttributes();

      String source = DOMUtil.getAttr(attrs, SOURCE, COPY_FIELD + " definition");
      String dest   = DOMUtil.getAttr(attrs, DESTINATION,  COPY_FIELD + " definition");
      String maxChars = DOMUtil.getAttr(attrs, MAX_CHARS);

      int maxCharsInt = CopyField.UNLIMITED;
      if (maxChars != null) {
        try {
          maxCharsInt = Integer.parseInt(maxChars);
        } catch (NumberFormatException e) {
          log.warn("Couldn't parse " + MAX_CHARS + " attribute for " + COPY_FIELD + " from "
                  + source + " to " + dest + " as integer. The whole field will be copied.");
        }
      }

      if (dest.equals(uniqueKeyFieldName)) {
        String msg = UNIQUE_KEY + " field ("+uniqueKeyFieldName+
          ") can not be the " + DESTINATION + " of a " + COPY_FIELD + "(" + SOURCE + "=" +source+")";
        log.error(msg);
        throw new SolrException(ErrorCode.SERVER_ERROR, msg);
      }
      
      registerCopyField(source, dest, maxCharsInt);
    }
      
    for (Map.Entry<SchemaField, Integer> entry : copyFieldTargetCounts.entrySet()) {
      if (entry.getValue() > 1 && !entry.getKey().multiValued())  {
        log.warn("Field " + entry.getKey().name + " is not multivalued "+
            "and destination for multiple " + COPY_FIELDS + " ("+
            entry.getValue()+")");
      }
    }
  }

  /**
   * Converts a sequence of path steps into a rooted path, by inserting slashes in front of each step.
   * @param steps The steps to join with slashes to form a path
   * @return a rooted path: a leading slash followed by the given steps joined with slashes
   */
  private String stepsToPath(String... steps) {
    StringBuilder builder = new StringBuilder();
    for (String step : steps) { builder.append(SLASH).append(step); }
    return builder.toString();
  }

  /** Returns true if the given name has exactly one asterisk either at the start or end of the name */
  private static boolean isValidFieldGlob(String name) {
    if (name.startsWith("*") || name.endsWith("*")) {
      int count = 0;
      for (int pos = 0 ; pos < name.length() && -1 != (pos = name.indexOf('*', pos)) ; ++pos) ++count;
      if (1 == count) return true;
    }
    return false;
  }
  
  protected boolean isValidDynamicField(List<DynamicField> dFields, SchemaField f) {
    String glob = f.getName();
    if (f.getDefaultValue() != null) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
          DYNAMIC_FIELD + " can not have a default value: " + glob);
    }
    if (f.isRequired()) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
          DYNAMIC_FIELD + " can not be required: " + glob);
    }
    if ( ! isValidFieldGlob(glob)) {
      String msg = "Dynamic field name '" + glob
          + "' should have either a leading or a trailing asterisk, and no others.";
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    if (isDuplicateDynField(dFields, f)) {
      String msg = "[schema.xml] Duplicate DynamicField definition for '" + glob + "'";
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return true;
  }


  /**
   * Register one or more new Dynamic Fields with the Schema.
   * @param fields The sequence of {@link org.apache.solr.schema.SchemaField}
   */
  public void registerDynamicFields(SchemaField... fields) {
    List<DynamicField> dynFields = new ArrayList<>(Arrays.asList(dynamicFields));
    for (SchemaField field : fields) {
      if (isDuplicateDynField(dynFields, field)) {
        log.debug("dynamic field already exists: dynamic field: [" + field.getName() + "]");
      } else {
        log.debug("dynamic field creation for schema field: " + field.getName());
        addDynamicFieldNoDupCheck(dynFields, field);
      }
    }
    dynamicFields = dynamicFieldListToSortedArray(dynFields);
  }

  private void addDynamicFieldNoDupCheck(List<DynamicField> dFields, SchemaField f) {
    dFields.add(new DynamicField(f));
    log.debug("dynamic field defined: " + f);
  }

  protected boolean isDuplicateDynField(List<DynamicField> dFields, SchemaField f) {
    for (DynamicField df : dFields) {
      if (df.getRegex().equals(f.name)) return true;
    }
    return false;
  }

  public void registerCopyField( String source, String dest ) {
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
  public void registerCopyField(String source, String dest, int maxChars) {
    log.debug(COPY_FIELD + " " + SOURCE + "='" + source + "' " + DESTINATION + "='" + dest
              + "' " + MAX_CHARS + "=" + maxChars);

    DynamicField destDynamicField = null;
    SchemaField destSchemaField = fields.get(dest);
    SchemaField sourceSchemaField = fields.get(source);
    
    DynamicField sourceDynamicBase = null;
    DynamicField destDynamicBase = null;
    
    boolean sourceIsDynamicFieldReference = false;
    boolean sourceIsExplicitFieldGlob = false;


    final String invalidGlobMessage = "is an invalid glob: either it contains more than one asterisk,"
                                    + " or the asterisk occurs neither at the start nor at the end.";
    final boolean sourceIsGlob = isValidFieldGlob(source);
    if (source.contains("*") && ! sourceIsGlob) {
      String msg = "copyField source :'" + source + "' " + invalidGlobMessage;
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    if (dest.contains("*") && ! isValidFieldGlob(dest)) {
      String msg = "copyField dest :'" + dest + "' " + invalidGlobMessage;
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }

    if (null == sourceSchemaField && sourceIsGlob) {
      Pattern pattern = Pattern.compile(source.replace("*", ".*")); // glob->regex
      for (String field : fields.keySet()) {
        if (pattern.matcher(field).matches()) {
          sourceIsExplicitFieldGlob = true;
          break;
        }
      }
    }
    
    if (null == destSchemaField || (null == sourceSchemaField && ! sourceIsExplicitFieldGlob)) {
      // Go through dynamicFields array only once, collecting info for both source and dest fields, if needed
      for (DynamicField dynamicField : dynamicFields) {
        if (null == sourceSchemaField && ! sourceIsDynamicFieldReference && ! sourceIsExplicitFieldGlob) {
          if (dynamicField.matches(source)) {
            sourceIsDynamicFieldReference = true;
            if ( ! source.equals(dynamicField.getRegex())) {
              sourceDynamicBase = dynamicField;
            }
          }
        }
        if (null == destSchemaField) {
          if (dest.equals(dynamicField.getRegex())) {
            destDynamicField = dynamicField;
            destSchemaField = dynamicField.prototype;
          } else if (dynamicField.matches(dest)) {
            destSchemaField = dynamicField.makeSchemaField(dest);
            destDynamicField = new DynamicField(destSchemaField);
            destDynamicBase = dynamicField;
          }
        }
        if (null != destSchemaField 
            && (null != sourceSchemaField || sourceIsDynamicFieldReference || sourceIsExplicitFieldGlob)) {
          break;
        }
      }
    }
    if (null == sourceSchemaField && ! sourceIsGlob && ! sourceIsDynamicFieldReference) {
      String msg = "copyField source :'" + source + "' is not a glob and doesn't match any explicit field or dynamicField.";
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    if (null == destSchemaField) {
      String msg = "copyField dest :'" + dest + "' is not an explicit field and doesn't match a dynamicField.";
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    if (sourceIsDynamicFieldReference || sourceIsGlob) {
      if (null != destDynamicField) { // source: glob or no-asterisk dynamic field ref; dest: dynamic field ref
        registerDynamicCopyField(new DynamicCopy(source, destDynamicField, maxChars, sourceDynamicBase, destDynamicBase));
        incrementCopyFieldTargetCount(destSchemaField);
      } else {                        // source: glob or no-asterisk dynamic field ref; dest: explicit field
        destDynamicField = new DynamicField(destSchemaField);
        registerDynamicCopyField(new DynamicCopy(source, destDynamicField, maxChars, sourceDynamicBase, null));
        incrementCopyFieldTargetCount(destSchemaField);
      }
    } else {                          
      if (null != destDynamicField) { // source: explicit field; dest: dynamic field reference
        if (destDynamicField.pattern instanceof DynamicReplacement.DynamicPattern.NameEquals) {
          // Dynamic dest with no asterisk is acceptable
          registerDynamicCopyField(new DynamicCopy(source, destDynamicField, maxChars, sourceDynamicBase, destDynamicBase));
          incrementCopyFieldTargetCount(destSchemaField);
        } else {
          String msg = "copyField only supports a dynamic destination with an asterisk "
                     + "if the source also has an asterisk";
          throw new SolrException(ErrorCode.SERVER_ERROR, msg);
        }
      } else {                        // source & dest: explicit fields 
        List<CopyField> copyFieldList = copyFieldsMap.get(source);
        if (copyFieldList == null) {
          copyFieldList = new ArrayList<>();
          copyFieldsMap.put(source, copyFieldList);
        }
        copyFieldList.add(new CopyField(sourceSchemaField, destSchemaField, maxChars));
        incrementCopyFieldTargetCount(destSchemaField);
      }
    }
  }
  
  private void incrementCopyFieldTargetCount(SchemaField dest) {
    copyFieldTargetCounts.put(dest, copyFieldTargetCounts.containsKey(dest) ? copyFieldTargetCounts.get(dest) + 1 : 1);
  }
  
  private void registerDynamicCopyField( DynamicCopy dcopy ) {
    if( dynamicCopyFields == null ) {
      dynamicCopyFields = new DynamicCopy[] {dcopy};
    }
    else {
      DynamicCopy[] temp = new DynamicCopy[dynamicCopyFields.length+1];
      System.arraycopy(dynamicCopyFields,0,temp,0,dynamicCopyFields.length);
      temp[temp.length -1] = dcopy;
      dynamicCopyFields = temp;
    }
    log.trace("Dynamic Copy Field:" + dcopy);
  }

  static SimilarityFactory readSimilarity(SolrResourceLoader loader, Node node) {
    if (node==null) {
      return null;
    } else {
      SimilarityFactory similarityFactory;
      final String classArg = ((Element) node).getAttribute(SimilarityFactory.CLASS_NAME);
      final Object obj = loader.newInstance(classArg, Object.class, "search.similarities.");
      if (obj instanceof SimilarityFactory) {
        // configure a factory, get a similarity back
        final NamedList<Object> namedList = DOMUtil.childNodesToNamedList(node);
        namedList.add(SimilarityFactory.CLASS_NAME, classArg);
        SolrParams params = SolrParams.toSolrParams(namedList);
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
      return similarityFactory;
    }
  }


  public static abstract class DynamicReplacement implements Comparable<DynamicReplacement> {
    abstract protected static class DynamicPattern {
      protected final String regex;
      protected final String fixedStr;

      protected DynamicPattern(String regex, String fixedStr) { this.regex = regex; this.fixedStr = fixedStr; }

      static DynamicPattern createPattern(String regex) {
        if (regex.startsWith("*")) { return new NameEndsWith(regex); }
        else if (regex.endsWith("*")) { return new NameStartsWith(regex); }
        else { return new NameEquals(regex);
        }
      }
      
      /** Returns true if the given name matches this pattern */
      abstract boolean matches(String name);

      /** Returns the remainder of the given name after removing this pattern's fixed string component */
      abstract String remainder(String name);

      /** Returns the result of combining this pattern's fixed string component with the given replacement */
      abstract String subst(String replacement);
      
      /** Returns the length of the original regex, including the asterisk, if any. */
      public int length() { return regex.length(); }

      private static class NameStartsWith extends DynamicPattern {
        NameStartsWith(String regex) { super(regex, regex.substring(0, regex.length() - 1)); }
        boolean matches(String name) { return name.startsWith(fixedStr); }
        String remainder(String name) { return name.substring(fixedStr.length()); }
        String subst(String replacement) { return fixedStr + replacement; }
      }
      private static class NameEndsWith extends DynamicPattern {
        NameEndsWith(String regex) { super(regex, regex.substring(1)); }
        boolean matches(String name) { return name.endsWith(fixedStr); }
        String remainder(String name) { return name.substring(0, name.length() - fixedStr.length()); }
        String subst(String replacement) { return replacement + fixedStr; }
      }
      private static class NameEquals extends DynamicPattern {
        NameEquals(String regex) { super(regex, regex); }
        boolean matches(String name) { return regex.equals(name); }
        String remainder(String name) { return ""; }
        String subst(String replacement) { return fixedStr; }
      }
    }

    protected DynamicPattern pattern;

    public boolean matches(String name) { return pattern.matches(name); }

    protected DynamicReplacement(String regex) {
      pattern = DynamicPattern.createPattern(regex);
    }

    /**
     * Sort order is based on length of regex.  Longest comes first.
     * @param other The object to compare to.
     * @return a negative integer, zero, or a positive integer
     * as this object is less than, equal to, or greater than
     * the specified object.
     */
    @Override
    public int compareTo(DynamicReplacement other) {
      return other.pattern.length() - pattern.length();
    }
    
    /** Returns the regex used to create this instance's pattern */
    public String getRegex() {
      return pattern.regex;
    }
  }


  public final static class DynamicField extends DynamicReplacement {
    private final SchemaField prototype;
    public SchemaField getPrototype() { return prototype; }

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

  public static class DynamicCopy extends DynamicReplacement {
    private final DynamicField destination;
    
    private final int maxChars;
    public int getMaxChars() { return maxChars; }

    final DynamicField sourceDynamicBase;
    public DynamicField getSourceDynamicBase() { return sourceDynamicBase; }

    final DynamicField destDynamicBase;
    public DynamicField getDestDynamicBase() { return destDynamicBase; }

    DynamicCopy(String sourceRegex, DynamicField destination, int maxChars, 
                DynamicField sourceDynamicBase, DynamicField destDynamicBase) {
      super(sourceRegex);
      this.destination = destination;
      this.maxChars = maxChars;
      this.sourceDynamicBase = sourceDynamicBase;
      this.destDynamicBase = destDynamicBase;
    }

    public String getDestFieldName() { return destination.getRegex(); }

    /**
     *  Generates a destination field name based on this source pattern,
     *  by substituting the remainder of this source pattern into the
     *  the given destination pattern.
     */
    public SchemaField getTargetField(String sourceField) {
      String remainder = pattern.remainder(sourceField);
      String targetFieldName = destination.pattern.subst(remainder);
      return destination.makeSchemaField(targetFieldName);
    }

    
    @Override
    public String toString() {
      return destination.prototype.toString();
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
     if (df.matches(fieldName)) return df.getRegex();
   }
   return  null; 
  }
  
  /**
   * Does the schema explicitly define the specified field, i.e. not as a result
   * of a copyField declaration?  We consider it explicitly defined if it matches
   * a field name or a dynamicField name.
   * @return true if explicitly declared in the schema.
   */
  public boolean hasExplicitField(String fieldName) {
    if (fields.containsKey(fieldName)) {
      return true;
    }

    for (DynamicField df : dynamicFields) {
      if (fieldName.equals(df.getRegex())) return true;
    }

    return false;
  }

  /**
   * Is the specified field dynamic or not.
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
    throw new SolrException(ErrorCode.BAD_REQUEST,"undefined field: \""+fieldName+"\"");
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
   *  excercises a dynamic field.
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
   * exercises a dynamic field.
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
   * exercises a dynamic field.
   * @throws SolrException if no such field exists
   * @see #getField(String)
   * @see #getFieldTypeNoEx
   */
  public FieldType getDynamicFieldType(String fieldName) {
     for (DynamicField df : dynamicFields) {
      if (df.matches(fieldName)) return df.prototype.getType();
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"undefined field "+fieldName);
  }

  private FieldType dynFieldType(String fieldName) {
     for (DynamicField df : dynamicFields) {
      if (df.matches(fieldName)) return df.prototype.getType();
    }
    return null;
  };


  /**
   * Get all copy fields, both the static and the dynamic ones.
   * @return Array of fields copied into this field
   */

  public List<String> getCopySources(String destField) {
    SchemaField f = getField(destField);
    if (!isCopyFieldTarget(f)) {
      return Collections.emptyList();
    }
    List<String> fieldNames = new ArrayList<>();
    for (Map.Entry<String, List<CopyField>> cfs : copyFieldsMap.entrySet()) {
      for (CopyField copyField : cfs.getValue()) {
        if (copyField.getDestination().getName().equals(destField)) {
          fieldNames.add(copyField.getSource().getName());
        }
      }
    }
    for (DynamicCopy dynamicCopy : dynamicCopyFields) {
      if (dynamicCopy.getDestFieldName().equals(destField)) {
        fieldNames.add(dynamicCopy.getRegex());
      }
    }
    return fieldNames;
  }

  /**
   * Get all copy fields for a specified source field, both static
   * and dynamic ones.
   * @return List of CopyFields to copy to.
   * @since solr 1.4
   */
  // This is useful when we need the maxSize param of each CopyField
  public List<CopyField> getCopyFieldsList(final String sourceField){
    final List<CopyField> result = new ArrayList<>();
    for (DynamicCopy dynamicCopy : dynamicCopyFields) {
      if (dynamicCopy.matches(sourceField)) {
        result.add(new CopyField(getField(sourceField), dynamicCopy.getTargetField(sourceField), dynamicCopy.maxChars));
      }
    }
    List<CopyField> fixedCopyFields = copyFieldsMap.get(sourceField);
    if (null != fixedCopyFields) {
      result.addAll(fixedCopyFields);
    }

    return result;
  }
  
  /**
   * Check if a field is used as the destination of a copyField operation 
   * 
   * @since solr 1.3
   */
  public boolean isCopyFieldTarget( SchemaField f ) {
    return copyFieldTargetCounts.containsKey( f );
  }

  /**
   * Get a map of property name -> value for the whole schema.
   */
  public SimpleOrderedMap<Object> getNamedPropertyValues() {
    SimpleOrderedMap<Object> topLevel = new SimpleOrderedMap<>();
    topLevel.add(NAME, getSchemaName());
    topLevel.add(VERSION, getVersion());
    if (null != uniqueKeyFieldName) {
      topLevel.add(UNIQUE_KEY, uniqueKeyFieldName);
    }
    if (null != defaultSearchFieldName) {
      topLevel.add(DEFAULT_SEARCH_FIELD, defaultSearchFieldName);
    }
    if (isExplicitQueryParserDefaultOperator) {
      SimpleOrderedMap<Object> solrQueryParserProperties = new SimpleOrderedMap<>();
      solrQueryParserProperties.add(DEFAULT_OPERATOR, queryParserDefaultOperator);
      topLevel.add(SOLR_QUERY_PARSER, solrQueryParserProperties);
    }
    if (isExplicitSimilarity) {
      topLevel.add(SIMILARITY, similarityFactory.getNamedPropertyValues());
    }
    List<SimpleOrderedMap<Object>> fieldTypeProperties = new ArrayList<>();
    SortedMap<String,FieldType> sortedFieldTypes = new TreeMap<>(fieldTypes);
    for (FieldType fieldType : sortedFieldTypes.values()) {
      fieldTypeProperties.add(fieldType.getNamedPropertyValues(false));
    }
    topLevel.add(FIELD_TYPES, fieldTypeProperties);  
    List<SimpleOrderedMap<Object>> fieldProperties = new ArrayList<>();
    SortedSet<String> fieldNames = new TreeSet<>(fields.keySet());
    for (String fieldName : fieldNames) {
      fieldProperties.add(fields.get(fieldName).getNamedPropertyValues(false));
    }
    topLevel.add(FIELDS, fieldProperties);
    List<SimpleOrderedMap<Object>> dynamicFieldProperties = new ArrayList<>();
    for (IndexSchema.DynamicField dynamicField : dynamicFields) {
      if ( ! dynamicField.getRegex().startsWith(INTERNAL_POLY_FIELD_PREFIX)) { // omit internal polyfields
        dynamicFieldProperties.add(dynamicField.getPrototype().getNamedPropertyValues(false));
      }
    }
    topLevel.add(DYNAMIC_FIELDS, dynamicFieldProperties);
    topLevel.add(COPY_FIELDS, getCopyFieldProperties(false, null, null));
    return topLevel;
  }

  /**
   * Returns a list of copyField directives, with optional details and optionally restricting to those
   * directives that contain the requested source and/or destination field names.
   * 
   * @param showDetails If true, source and destination dynamic bases, and explicit fields matched by source globs,
   *                    will be added to dynamic copyField directives where appropriate
   * @param requestedSourceFields If not null, output is restricted to those copyField directives
   *                              with the requested source field names 
   * @param requestedDestinationFields If not null, output is restricted to those copyField directives
   *                                   with the requested destination field names 
   * @return a list of copyField directives 
   */
  public List<SimpleOrderedMap<Object>> getCopyFieldProperties
      (boolean showDetails, Set<String> requestedSourceFields, Set<String> requestedDestinationFields) {
    List<SimpleOrderedMap<Object>> copyFieldProperties = new ArrayList<>();
    SortedMap<String,List<CopyField>> sortedCopyFields = new TreeMap<>(copyFieldsMap);
    for (List<CopyField> copyFields : sortedCopyFields.values()) {
      Collections.sort(copyFields, new Comparator<CopyField>() {
        @Override
        public int compare(CopyField cf1, CopyField cf2) {
          // sources are all be the same, just sorting by destination here
          return cf1.getDestination().getName().compareTo(cf2.getDestination().getName());
        }
      });
      for (CopyField copyField : copyFields) {
        final String source = copyField.getSource().getName();
        final String destination = copyField.getDestination().getName();
        if (   (null == requestedSourceFields      || requestedSourceFields.contains(source))
            && (null == requestedDestinationFields || requestedDestinationFields.contains(destination))) {
          SimpleOrderedMap<Object> props = new SimpleOrderedMap<>();
          props.add(SOURCE, source);
          props.add(DESTINATION, destination);
            if (0 != copyField.getMaxChars()) {
              props.add(MAX_CHARS, copyField.getMaxChars());
            }
          copyFieldProperties.add(props);
        }
      }
    }
    for (IndexSchema.DynamicCopy dynamicCopy : dynamicCopyFields) {
      final String source = dynamicCopy.getRegex();
      final String destination = dynamicCopy.getDestFieldName();
      if (   (null == requestedSourceFields      || requestedSourceFields.contains(source))
          && (null == requestedDestinationFields || requestedDestinationFields.contains(destination))) {
        SimpleOrderedMap<Object> dynamicCopyProps = new SimpleOrderedMap<>();

        dynamicCopyProps.add(SOURCE, dynamicCopy.getRegex());
        if (showDetails) {
          IndexSchema.DynamicField sourceDynamicBase = dynamicCopy.getSourceDynamicBase();
          if (null != sourceDynamicBase) {
            dynamicCopyProps.add(SOURCE_DYNAMIC_BASE, sourceDynamicBase.getRegex());
          } else if (source.contains("*")) {
            List<String> sourceExplicitFields = new ArrayList<>();
            Pattern pattern = Pattern.compile(source.replace("*", ".*"));   // glob->regex
            for (String field : fields.keySet()) {
              if (pattern.matcher(field).matches()) {
                sourceExplicitFields.add(field);
              }
            }
            if (sourceExplicitFields.size() > 0) {
              Collections.sort(sourceExplicitFields);
              dynamicCopyProps.add(SOURCE_EXPLICIT_FIELDS, sourceExplicitFields);
            }
          }
        }
        
        dynamicCopyProps.add(DESTINATION, dynamicCopy.getDestFieldName());
        if (showDetails) {
          IndexSchema.DynamicField destDynamicBase = dynamicCopy.getDestDynamicBase();
          if (null != destDynamicBase) {
            dynamicCopyProps.add(DESTINATION_DYNAMIC_BASE, destDynamicBase.getRegex());
          }
        }

        if (0 != dynamicCopy.getMaxChars()) {
          dynamicCopyProps.add(MAX_CHARS, dynamicCopy.getMaxChars());
        }

        copyFieldProperties.add(dynamicCopyProps);
      }
    }
    return copyFieldProperties;
  }

  protected IndexSchema(final SolrConfig solrConfig, final SolrResourceLoader loader) {
    this.solrConfig = solrConfig;
    this.loader = loader;
  }

  /**
   * Copies this schema, adds the given field to the copy
   * Requires synchronizing on the object returned by
   * {@link #getSchemaUpdateLock()}.
   *
   * @param newField the SchemaField to add 
   * @param persist to persist the schema or not or not
   * @return a new IndexSchema based on this schema with newField added
   * @see #newField(String, String, Map)
   */
  public IndexSchema addField(SchemaField newField, boolean persist) {
    return addFields(Collections.singletonList(newField),Collections.EMPTY_MAP,persist );
  }

  public IndexSchema addField(SchemaField newField) {
    return addField(newField, true);
  }

  /**
   * Copies this schema, adds the given field to the copy
   *  Requires synchronizing on the object returned by
   * {@link #getSchemaUpdateLock()}.
   *
   * @param newField the SchemaField to add
   * @param copyFieldNames 0 or more names of targets to copy this field to.  The targets must already exist.
   * @return a new IndexSchema based on this schema with newField added
   * @see #newField(String, String, Map)
   */
  public IndexSchema addField(SchemaField newField, Collection<String> copyFieldNames) {
    return addFields(singletonList(newField), singletonMap(newField.getName(), copyFieldNames), true);
  }

  /**
   * Copies this schema, adds the given fields to the copy.
   * Requires synchronizing on the object returned by
   * {@link #getSchemaUpdateLock()}.
   *
   * @param newFields the SchemaFields to add
   * @return a new IndexSchema based on this schema with newFields added
   * @see #newField(String, String, Map)
   */
  public IndexSchema addFields(Collection<SchemaField> newFields) {
    return addFields(newFields, Collections.<String, Collection<String>>emptyMap(), true);
  }

  /**
   * Copies this schema, adds the given fields to the copy
   * Requires synchronizing on the object returned by
   * {@link #getSchemaUpdateLock()}.
   *
   * @param newFields the SchemaFields to add
   * @param copyFieldNames 0 or more names of targets to copy this field to.  The target fields must already exist.
   * @param persist Persist the schema or not
   * @return a new IndexSchema based on this schema with newFields added
   * @see #newField(String, String, Map)
   */
  public IndexSchema addFields(Collection<SchemaField> newFields, Map<String, Collection<String>> copyFieldNames, boolean persist) {
    String msg = "This IndexSchema is not mutable.";
    log.error(msg);
    throw new SolrException(ErrorCode.SERVER_ERROR, msg);
  }


  /**
   * Copies this schema, adds the given dynamic fields to the copy,
   * Requires synchronizing on the object returned by
   * {@link #getSchemaUpdateLock()}.
   *
   * @param newDynamicFields the SchemaFields to add
   * @param copyFieldNames 0 or more names of targets to copy this field to.  The target fields must already exist.
   * @param persist to persist the schema or not or not
   * @return a new IndexSchema based on this schema with newDynamicFields added
   * @see #newDynamicField(String, String, Map)
   */
  public IndexSchema addDynamicFields
      (Collection<SchemaField> newDynamicFields,
       Map<String, Collection<String>> copyFieldNames,
       boolean persist) {
    String msg = "This IndexSchema is not mutable.";
    log.error(msg);
    throw new SolrException(ErrorCode.SERVER_ERROR, msg);
  }

  /**
   * Copies this schema and adds the new copy fields to the copy
   * Requires synchronizing on the object returned by
   * {@link #getSchemaUpdateLock()}.
   *
   * @param copyFields Key is the name of the source field name, value is a collection of target field names.  Fields must exist.
   * @param persist to persist the schema or not or not
   * @return The new Schema with the copy fields added
   */
  public IndexSchema addCopyFields(Map<String, Collection<String>> copyFields, boolean persist){
    String msg = "This IndexSchema is not mutable.";
    log.error(msg);
    throw new SolrException(ErrorCode.SERVER_ERROR, msg);
  }

  /**
   * Returns a SchemaField if the given fieldName does not already 
   * exist in this schema, and does not match any dynamic fields 
   * in this schema.  The resulting SchemaField can be used in a call
   * to {@link #addField(SchemaField)}.
   *
   * @param fieldName the name of the field to add
   * @param fieldType the field type for the new field
   * @param options the options to use when creating the SchemaField
   * @return The created SchemaField
   * @see #addField(SchemaField)
   */
  public SchemaField newField(String fieldName, String fieldType, Map<String,?> options) {
    String msg = "This IndexSchema is not mutable.";
    log.error(msg);
    throw new SolrException(ErrorCode.SERVER_ERROR, msg);
  }

  /**
   * Returns a SchemaField if the given dynamic field glob does not already 
   * exist in this schema, and does not match any dynamic fields 
   * in this schema.  The resulting SchemaField can be used in a call
   * to {@link #addField(SchemaField)}.
   *
   * @param fieldNamePattern the pattern for the dynamic field to add
   * @param fieldType the field type for the new field
   * @param options the options to use when creating the SchemaField
   * @return The created SchemaField
   * @see #addField(SchemaField)
   */
  public SchemaField newDynamicField(String fieldNamePattern, String fieldType, Map<String,?> options) {
    String msg = "This IndexSchema is not mutable.";
    log.error(msg);
    throw new SolrException(ErrorCode.SERVER_ERROR, msg);
  }

  /**
   * Returns the schema update lock that should be synchronzied on
   * to update the schema.  Only applicable to mutable schemas.
   *
   * @return the schema update lock object to synchronize on
   */
  public Object getSchemaUpdateLock() {
    String msg = "This IndexSchema is not mutable.";
    log.error(msg);
    throw new SolrException(ErrorCode.SERVER_ERROR, msg);
  }

  /**
   * Copies this schema, adds the given field type to the copy,
   * Requires synchronizing on the object returned by
   * {@link #getSchemaUpdateLock()}.
   *
   * @param fieldTypeList a list of FieldTypes to add
   * @param persist to persist the schema or not or not
   * @return a new IndexSchema based on this schema with the new types added
   * @see #newFieldType(String, String, Map)
   */
  public IndexSchema addFieldTypes(List<FieldType> fieldTypeList, boolean persist) {
    String msg = "This IndexSchema is not mutable.";
    log.error(msg);
    throw new SolrException(ErrorCode.SERVER_ERROR, msg);
  }

  /**
   * Returns a FieldType if the given typeName does not already
   * exist in this schema. The resulting FieldType can be used in a call
   * to {@link #addFieldTypes(java.util.List, boolean)}.
   *
   * @param typeName the name of the type to add
   * @param className the name of the FieldType class
   * @param options the options to use when creating the FieldType
   * @return The created FieldType
   * @see #addFieldTypes(java.util.List, boolean)
   */
  public FieldType newFieldType(String typeName, String className, Map<String,?> options) {
    String msg = "This IndexSchema is not mutable.";
    log.error(msg);
    throw new SolrException(ErrorCode.SERVER_ERROR, msg);
  }

  protected String getFieldTypeXPathExpressions() {
    //               /schema/fieldtype | /schema/fieldType | /schema/types/fieldtype | /schema/types/fieldType
    String expression = stepsToPath(SCHEMA, FIELD_TYPE.toLowerCase(Locale.ROOT)) // backcompat(?)
        + XPATH_OR + stepsToPath(SCHEMA, FIELD_TYPE)
        + XPATH_OR + stepsToPath(SCHEMA, TYPES, FIELD_TYPE.toLowerCase(Locale.ROOT))
        + XPATH_OR + stepsToPath(SCHEMA, TYPES, FIELD_TYPE);
    return expression;
  }
}
