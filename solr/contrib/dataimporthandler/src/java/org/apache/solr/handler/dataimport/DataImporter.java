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

package org.apache.solr.handler.dataimport;

import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.SystemIdResolver;
import org.apache.solr.common.util.XMLErrorLogger;
import org.apache.solr.handler.dataimport.config.ConfigNameConstants;
import org.apache.solr.handler.dataimport.config.ConfigParseUtil;
import org.apache.solr.handler.dataimport.config.DIHConfiguration;
import org.apache.solr.handler.dataimport.config.Entity;
import org.apache.solr.handler.dataimport.config.Script;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.apache.commons.io.IOUtils;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p> Stores all configuration information for pulling and indexing data. </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.3
 */
public class DataImporter {

  public enum Status {
    IDLE, RUNNING_FULL_DUMP, RUNNING_DELTA_DUMP, JOB_FAILED
  }

  private static final Logger LOG = LoggerFactory.getLogger(DataImporter.class);
  private static final XMLErrorLogger XMLLOG = new XMLErrorLogger(LOG);

  private Status status = Status.IDLE;
  private DIHConfiguration config;
  private Date indexStartTime;
  private Properties store = new Properties();
  private Map<String, Properties> dataSourceProps = new HashMap<String, Properties>();
  private IndexSchema schema;
  public DocBuilder docBuilder;
  public DocBuilder.Statistics cumulativeStatistics = new DocBuilder.Statistics();
  private SolrCore core;  
  private DIHPropertiesWriter propWriter;
  private ReentrantLock importLock = new ReentrantLock();
  private final Map<String , Object> coreScopeSession;
  private boolean isDeltaImportSupported = false;  
  private final String handlerName;  
  private Map<String, SchemaField> lowerNameVsSchemaField = new HashMap<String, SchemaField>();

  /**
   * Only for testing purposes
   */
  DataImporter() {
    coreScopeSession = new HashMap<String, Object>();
    createPropertyWriter();
    propWriter.init(this);
    this.handlerName = "dataimport" ;
  }

  private void createPropertyWriter() {
    if (this.core == null
        || !this.core.getCoreDescriptor().getCoreContainer().isZooKeeperAware()) {
      propWriter = new SimplePropertiesWriter();
    } else {
      propWriter = new ZKPropertiesWriter();
    }
    propWriter.init(this);
  }

  DataImporter(InputSource dataConfig, SolrCore core, Map<String, Properties> ds, Map<String, Object> session, String handlerName) {
    this.handlerName = handlerName;
    if (dataConfig == null) {
      throw new DataImportHandlerException(SEVERE, "Configuration not found");
    }
    this.core = core;
    this.schema = core.getSchema();
    loadSchemaFieldMap();
    createPropertyWriter();
    
    dataSourceProps = ds;
    if (session == null)
      session = new HashMap<String, Object>();
    coreScopeSession = session;
    loadDataConfig(dataConfig);
   
    for (Entity e : config.getEntities()) {
      if (e.getAllAttributes().containsKey(SqlEntityProcessor.DELTA_QUERY)) {
        isDeltaImportSupported = true;
        break;
      }
    }
  }
  
  
  
  private void loadSchemaFieldMap() {
    Map<String, SchemaField> modLnvsf = new HashMap<String, SchemaField>();
    for (Map.Entry<String, SchemaField> entry : schema.getFields().entrySet()) {
      modLnvsf.put(entry.getKey().toLowerCase(Locale.ENGLISH), entry.getValue());
    }
    lowerNameVsSchemaField = Collections.unmodifiableMap(modLnvsf);
  }
  
  public SchemaField getSchemaField(String caseInsensitiveName) {
    SchemaField schemaField = null;
    if(schema!=null) {
      schemaField = schema.getFieldOrNull(caseInsensitiveName);
    }
    if (schemaField == null) {
      schemaField = lowerNameVsSchemaField.get(caseInsensitiveName.toLowerCase(Locale.ENGLISH));
    }
    return schemaField;
  }

   public String getHandlerName() {
        return handlerName;
    }

    

  /**
   * Used by tests
   */
  void loadAndInit(String configStr) {
    loadDataConfig(new InputSource(new StringReader(configStr)));       
  }  

  private void loadDataConfig(InputSource configFile) {

    try {
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      
      // only enable xinclude, if a a SolrCore and SystemId is present (makes no sense otherwise)
      if (core != null && configFile.getSystemId() != null) {
        try {
          dbf.setXIncludeAware(true);
          dbf.setNamespaceAware(true);
        } catch( UnsupportedOperationException e ) {
          LOG.warn( "XML parser doesn't support XInclude option" );
        }
      }
      
      DocumentBuilder builder = dbf.newDocumentBuilder();
      if (core != null)
        builder.setEntityResolver(new SystemIdResolver(core.getResourceLoader()));
      builder.setErrorHandler(XMLLOG);
      Document document;
      try {
        document = builder.parse(configFile);
      } finally {
        // some XML parsers are broken and don't close the byte stream (but they should according to spec)
        IOUtils.closeQuietly(configFile.getByteStream());
      }

      config = readFromXml(document);
      LOG.info("Data Configuration loaded successfully");
    } catch (Exception e) {
      throw new DataImportHandlerException(SEVERE,
              "Exception occurred while initializing context", e);
    }
  }
  
  public DIHConfiguration readFromXml(Document xmlDocument) {
    DIHConfiguration config;
    List<Map<String, String >> functions = new ArrayList<Map<String ,String>>();
    Script script = null;
    Map<String, Properties> dataSources = new HashMap<String, Properties>();
    
    NodeList dataConfigTags = xmlDocument.getElementsByTagName("dataConfig");
    if(dataConfigTags == null || dataConfigTags.getLength() == 0) {
      throw new DataImportHandlerException(SEVERE, "the root node '<dataConfig>' is missing");
    }
    Element e = (Element) dataConfigTags.item(0);
    List<Element> documentTags = ConfigParseUtil.getChildNodes(e, "document");
    if (documentTags.isEmpty()) {
      throw new DataImportHandlerException(SEVERE, "DataImportHandler " +
              "configuration file must have one <document> node.");
    }

    List<Element> scriptTags = ConfigParseUtil.getChildNodes(e, ConfigNameConstants.SCRIPT);
    if (!scriptTags.isEmpty()) {
      script = new Script(scriptTags.get(0));
    }

    // Add the provided evaluators
    List<Element> functionTags = ConfigParseUtil.getChildNodes(e, ConfigNameConstants.FUNCTION);
    if (!functionTags.isEmpty()) {
      for (Element element : functionTags) {
        String func = ConfigParseUtil.getStringAttribute(element, NAME, null);
        String clz = ConfigParseUtil.getStringAttribute(element, ConfigNameConstants.CLASS, null);
        if (func == null || clz == null){
          throw new DataImportHandlerException(
                  SEVERE,
                  "<function> must have a 'name' and 'class' attributes");
        } else {
          functions.add(ConfigParseUtil.getAllAttributes(element));
        }
      }
    }
    List<Element> dataSourceTags = ConfigParseUtil.getChildNodes(e, DATA_SRC);
    if (!dataSourceTags.isEmpty()) {
      for (Element element : dataSourceTags) {
        Properties p = new Properties();
        HashMap<String, String> attrs = ConfigParseUtil.getAllAttributes(element);
        for (Map.Entry<String, String> entry : attrs.entrySet()) {
          p.setProperty(entry.getKey(), entry.getValue());
        }
        dataSources.put(p.getProperty("name"), p);
      }
    }
    if(dataSources.get(null) == null){
      for (Properties properties : dataSources.values()) {
        dataSources.put(null,properties);
        break;        
      } 
    }
    return new DIHConfiguration(documentTags.get(0), this, functions, script, dataSources);
  }

  DIHConfiguration getConfig() {
    return config;
  }

  Date getIndexStartTime() {
    return indexStartTime;
  }

  void setIndexStartTime(Date indextStartTime) {
    this.indexStartTime = indextStartTime;
  }

  void store(Object key, Object value) {
    store.put(key, value);
  }

  Object retrieve(Object key) {
    return store.get(key);
  }

  DataSource getDataSourceInstance(Entity key, String name, Context ctx) {
    Properties p = dataSourceProps.get(name);
    if (p == null)
      p = config.getDataSources().get(name);
    if (p == null)
      p = dataSourceProps.get(null);// for default data source
    if (p == null)
      p = config.getDataSources().get(null);
    if (p == null)  
      throw new DataImportHandlerException(SEVERE,
              "No dataSource :" + name + " available for entity :" + key.getName());
    String type = p.getProperty(TYPE);
    DataSource dataSrc = null;
    if (type == null) {
      dataSrc = new JdbcDataSource();
    } else {
      try {
        dataSrc = (DataSource) DocBuilder.loadClass(type, getCore()).newInstance();
      } catch (Exception e) {
        wrapAndThrow(SEVERE, e, "Invalid type for data source: " + type);
      }
    }
    try {
      Properties copyProps = new Properties();
      copyProps.putAll(p);
      Map<String, Object> map = ctx.getRequestParameters();
      if (map.containsKey("rows")) {
        int rows = Integer.parseInt((String) map.get("rows"));
        if (map.containsKey("start")) {
          rows += Integer.parseInt((String) map.get("start"));
        }
        copyProps.setProperty("maxRows", String.valueOf(rows));
      }
      dataSrc.init(ctx, copyProps);
    } catch (Exception e) {
      wrapAndThrow(SEVERE, e, "Failed to initialize DataSource: " + key.getDataSourceName());
    }
    return dataSrc;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public boolean isBusy() {
    return importLock.isLocked();
  }

  public void doFullImport(SolrWriter writer, RequestInfo requestParams) {
    LOG.info("Starting Full Import");
    setStatus(Status.RUNNING_FULL_DUMP);

    setIndexStartTime(new Date());

    try {
      docBuilder = new DocBuilder(this, writer, propWriter, requestParams);
      checkWritablePersistFile(writer);
      docBuilder.execute();
      if (!requestParams.isDebug())
        cumulativeStatistics.add(docBuilder.importStatistics);
    } catch (Throwable t) {
      SolrException.log(LOG, "Full Import failed", t);
      docBuilder.rollback();
    } finally {
      setStatus(Status.IDLE);
      DocBuilder.INSTANCE.set(null);
    }

  }

  private void checkWritablePersistFile(SolrWriter writer) {
//  	File persistFile = propWriter.getPersistFile();
//    boolean isWritable = persistFile.exists() ? persistFile.canWrite() : persistFile.getParentFile().canWrite();
    if (isDeltaImportSupported && !propWriter.isWritable()) {
      throw new DataImportHandlerException(SEVERE,
          "Properties is not writable. Delta imports are supported by data config but will not work.");
    }
  }

  public void doDeltaImport(SolrWriter writer, RequestInfo requestParams) {
    LOG.info("Starting Delta Import");
    setStatus(Status.RUNNING_DELTA_DUMP);

    try {
      setIndexStartTime(new Date());
      docBuilder = new DocBuilder(this, writer, propWriter, requestParams);
      checkWritablePersistFile(writer);
      docBuilder.execute();
      if (!requestParams.isDebug())
        cumulativeStatistics.add(docBuilder.importStatistics);
    } catch (Throwable t) {
      LOG.error("Delta Import Failed", t);
      docBuilder.rollback();
    } finally {
      setStatus(Status.IDLE);
      DocBuilder.INSTANCE.set(null);
    }

  }

  public void runAsync(final RequestInfo reqParams, final SolrWriter sw) {
    new Thread() {
      @Override
      public void run() {
        runCmd(reqParams, sw);
      }
    }.start();
  }

  void runCmd(RequestInfo reqParams, SolrWriter sw) {
    String command = reqParams.getCommand();
    if (command.equals(ABORT_CMD)) {
      if (docBuilder != null) {
        docBuilder.abort();
      }
      return;
    }
    if (!importLock.tryLock()){
      LOG.warn("Import command failed . another import is running");      
      return;
    }
    try {
      if (FULL_IMPORT_CMD.equals(command) || IMPORT_CMD.equals(command)) {
        doFullImport(sw, reqParams);
      } else if (command.equals(DELTA_IMPORT_CMD)) {
        doDeltaImport(sw, reqParams);
      }
    } finally {
      importLock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  Map<String, String> getStatusMessages() {
    //this map object is a Collections.synchronizedMap(new LinkedHashMap()). if we
    // synchronize on the object it must be safe to iterate through the map
    Map statusMessages = (Map) retrieve(STATUS_MSGS);
    Map<String, String> result = new LinkedHashMap<String, String>();
    if (statusMessages != null) {
      synchronized (statusMessages) {
        for (Object o : statusMessages.entrySet()) {
          Map.Entry e = (Map.Entry) o;
          //the toString is taken because some of the Objects create the data lazily when toString() is called
          result.put((String) e.getKey(), e.getValue().toString());
        }
      }
    }
    return result;

  }

  DocBuilder getDocBuilder() {
    return docBuilder;
  }

  static final ThreadLocal<AtomicLong> QUERY_COUNT = new ThreadLocal<AtomicLong>() {
    @Override
    protected AtomicLong initialValue() {
      return new AtomicLong();
    }
  };

  static final ThreadLocal<SimpleDateFormat> DATE_TIME_FORMAT = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }
  };

  static final class MSG {
    public static final String NO_CONFIG_FOUND = "Configuration not found";

    public static final String NO_INIT = "DataImportHandler started. Not Initialized. No commands can be run";

    public static final String INVALID_CONFIG = "FATAL: Could not create importer. DataImporter config invalid";

    public static final String LOAD_EXP = "Exception while loading DataImporter";

    public static final String JMX_DESC = "Manage data import from databases to Solr";

    public static final String CMD_RUNNING = "A command is still running...";

    public static final String DEBUG_NOT_ENABLED = "Debug not enabled. Add a tag <str name=\"enableDebug\">true</str> in solrconfig.xml";

    public static final String CONFIG_RELOADED = "Configuration Re-loaded sucessfully";

    public static final String TOTAL_DOC_PROCESSED = "Total Documents Processed";

    public static final String TOTAL_FAILED_DOCS = "Total Documents Failed";

    public static final String TOTAL_QUERIES_EXECUTED = "Total Requests made to DataSource";

    public static final String TOTAL_ROWS_EXECUTED = "Total Rows Fetched";

    public static final String TOTAL_DOCS_DELETED = "Total Documents Deleted";

    public static final String TOTAL_DOCS_SKIPPED = "Total Documents Skipped";
  }

  public IndexSchema getSchema() {
    return schema;
  }

  Map<String, Object> getCoreScopeSession() {
    return coreScopeSession;
  }

  SolrCore getCore() {
    return core;
  }

  public static final String COLUMN = "column";

  public static final String TYPE = "type";

  public static final String DATA_SRC = "dataSource";

  public static final String MULTI_VALUED = "multiValued";

  public static final String NAME = "name";

  public static final String STATUS_MSGS = "status-messages";

  public static final String FULL_IMPORT_CMD = "full-import";

  public static final String IMPORT_CMD = "import";

  public static final String DELTA_IMPORT_CMD = "delta-import";

  public static final String ABORT_CMD = "abort";

  public static final String DEBUG_MODE = "debug";

  public static final String RELOAD_CONF_CMD = "reload-config";

  public static final String SHOW_CONF_CMD = "show-config";
}
