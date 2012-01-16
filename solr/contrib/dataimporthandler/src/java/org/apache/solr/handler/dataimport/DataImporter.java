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

package org.apache.solr.handler.dataimport;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SystemIdResolver;
import org.apache.solr.common.util.XMLErrorLogger;

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
import java.util.concurrent.ConcurrentHashMap;

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

  private DataConfig config;

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

  /**
   * Only for testing purposes
   */
  DataImporter() {
    coreScopeSession = new ConcurrentHashMap<String, Object>();
    this.propWriter = new SimplePropertiesWriter();
    propWriter.init(this);
    this.handlerName = "dataimport" ;
  }

  DataImporter(InputSource dataConfig, SolrCore core, Map<String, Properties> ds, Map<String, Object> session, String handlerName) {
      this.handlerName = handlerName;
    if (dataConfig == null)
      throw new DataImportHandlerException(SEVERE,
              "Configuration not found");
    this.core = core;
    this.schema = core.getSchema();
    this.propWriter = new SimplePropertiesWriter();
    propWriter.init(this);
    dataSourceProps = ds;
    if (session == null)
      session = new HashMap<String, Object>();
    coreScopeSession = session;
    loadDataConfig(dataConfig);

    for (Map.Entry<String, SchemaField> entry : schema.getFields().entrySet()) {
      config.lowerNameVsSchemaField.put(entry.getKey().toLowerCase(Locale.ENGLISH), entry.getValue());
    }

    for (DataConfig.Entity e : config.document.entities) {
      Map<String, DataConfig.Field> fields = new HashMap<String, DataConfig.Field>();
      initEntity(e, fields, false);
      verifyWithSchema(fields);
      identifyPk(e);
      if (e.allAttributes.containsKey(SqlEntityProcessor.DELTA_QUERY))
        isDeltaImportSupported = true;
    }
  }

   public String getHandlerName() {
        return handlerName;
    }

    private void verifyWithSchema(Map<String, DataConfig.Field> fields) {
    Map<String, SchemaField> schemaFields = schema.getFields();
    for (Map.Entry<String, SchemaField> entry : schemaFields.entrySet()) {
      SchemaField sf = entry.getValue();
      if (!fields.containsKey(sf.getName())) {
        if (sf.isRequired()) {
          LOG
                  .info(sf.getName()
                          + " is a required field in SolrSchema . But not found in DataConfig");
        }
      }
    }
    for (Map.Entry<String, DataConfig.Field> entry : fields.entrySet()) {
      DataConfig.Field fld = entry.getValue();
      SchemaField field = schema.getFieldOrNull(fld.getName());
      if (field == null) {
        field = config.lowerNameVsSchemaField.get(fld.getName().toLowerCase(Locale.ENGLISH));
        if (field == null) {
          LOG.info("The field :" + fld.getName() + " present in DataConfig does not have a counterpart in Solr Schema");
        }
      }
    }

  }

  /**
   * Used by tests
   */
  void loadAndInit(String configStr) {
    loadDataConfig(new InputSource(new StringReader(configStr)));
    Map<String, DataConfig.Field> fields = new HashMap<String, DataConfig.Field>();
    for (DataConfig.Entity entity : config.document.entities) {
      initEntity(entity, fields, false);
    }
  }

  private void identifyPk(DataConfig.Entity entity) {
    SchemaField uniqueKey = schema.getUniqueKeyField();
    String schemaPk = "";
    if (uniqueKey != null)
      schemaPk = uniqueKey.getName();
    else return;
    //if no fields are mentioned . solr uniqueKey is same as dih 'pk'
    entity.pkMappingFromSchema = schemaPk;
    for (DataConfig.Field field : entity.fields) {
      if(field.getName().equals(schemaPk)) {
        entity.pkMappingFromSchema = field.column;
        //get the corresponding column mapping for the solr uniqueKey
        // But if there are multiple columns mapping to the solr uniqueKey, it will fail
        // so , in one off cases we may need pk
        break;
      }
    } 

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

      config = new DataConfig();
      NodeList elems = document.getElementsByTagName("dataConfig");
      if(elems == null || elems.getLength() == 0) {
        throw new DataImportHandlerException(SEVERE, "the root node '<dataConfig>' is missing");
      }
      config.readFromXml((Element) elems.item(0));
      LOG.info("Data Configuration loaded successfully");
    } catch (Exception e) {
      throw new DataImportHandlerException(SEVERE,
              "Exception occurred while initializing context", e);
    }
  }

  private void initEntity(DataConfig.Entity e,
                          Map<String, DataConfig.Field> fields, boolean docRootFound) {
    e.allAttributes.put(DATA_SRC, e.dataSource);

    if (!docRootFound && !"false".equals(e.docRoot)) {
      // if in this chain no document root is found()
      e.isDocRoot = true;
    }
    if (e.allAttributes.get("threads") != null) {
      if(docRootFound) throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "'threads' not allowed below rootEntity ");
      config.isMultiThreaded = true;      
    }

    if (e.fields != null) {
      for (DataConfig.Field f : e.fields) {
        if (schema != null) {
          if(f.name != null && f.name.contains("${")){
            f.dynamicName = true;
            continue;
          }
          SchemaField schemaField = schema.getFieldOrNull(f.getName());
          if (schemaField == null) {
            schemaField = config.lowerNameVsSchemaField.get(f.getName().toLowerCase(Locale.ENGLISH));
            if (schemaField != null) f.name = schemaField.getName();
          }
          if (schemaField != null) {
            f.multiValued = schemaField.multiValued();
            f.allAttributes.put(MULTI_VALUED, Boolean.toString(schemaField
                    .multiValued()));
            f.allAttributes.put(TYPE, schemaField.getType().getTypeName());
            f.allAttributes.put("indexed", Boolean.toString(schemaField.indexed()));
            f.allAttributes.put("stored", Boolean.toString(schemaField.stored()));
            f.allAttributes.put("defaultValue", schemaField.getDefaultValue());
          } else {
            f.toWrite = false;
          }
        }
        fields.put(f.getName(), f);
        f.entity = e;
        f.allAttributes.put("boost", f.boost.toString());
        f.allAttributes.put("toWrite", Boolean.toString(f.toWrite));
        e.allFieldsList.add(Collections.unmodifiableMap(f.allAttributes));
      }
    }
    e.allFieldsList = Collections.unmodifiableList(e.allFieldsList);
    e.allAttributes = Collections.unmodifiableMap(e.allAttributes);

    if (e.entities == null)
      return;
    for (DataConfig.Entity e1 : e.entities) {
      e1.parentEntity = e;
      initEntity(e1, fields, e.isDocRoot || docRootFound);
    }

  }

  DataConfig getConfig() {
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

  DataSource getDataSourceInstance(DataConfig.Entity key, String name, Context ctx) {
    Properties p = dataSourceProps.get(name);
    if (p == null)
      p = config.dataSources.get(name);
    if (p == null)
      p = dataSourceProps.get(null);// for default data source
    if (p == null)
      p = config.dataSources.get(null);
    if (p == null)  
      throw new DataImportHandlerException(SEVERE,
              "No dataSource :" + name + " available for entity :"
                      + key.name);
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
      wrapAndThrow(SEVERE, e, "Failed to initialize DataSource: " + key.dataSource);
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

  public void doFullImport(SolrWriter writer, RequestParams requestParams) {
    LOG.info("Starting Full Import");
    setStatus(Status.RUNNING_FULL_DUMP);

    setIndexStartTime(new Date());

    try {
      docBuilder = new DocBuilder(this, writer, propWriter, requestParams);
      checkWritablePersistFile(writer);
      docBuilder.execute();
      if (!requestParams.debug)
        cumulativeStatistics.add(docBuilder.importStatistics);
    } catch (Throwable t) {
      SolrException.log(LOG, "Full Import failed", t);
      docBuilder.rollback();
    } finally {
      setStatus(Status.IDLE);
      config.clearCaches();
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

  public void doDeltaImport(SolrWriter writer, RequestParams requestParams) {
    LOG.info("Starting Delta Import");
    setStatus(Status.RUNNING_DELTA_DUMP);

    try {
      setIndexStartTime(new Date());
      docBuilder = new DocBuilder(this, writer, propWriter, requestParams);
      checkWritablePersistFile(writer);
      docBuilder.execute();
      if (!requestParams.debug)
        cumulativeStatistics.add(docBuilder.importStatistics);
    } catch (Throwable t) {
      LOG.error("Delta Import Failed", t);
      docBuilder.rollback();
    } finally {
      setStatus(Status.IDLE);
      config.clearCaches();
      DocBuilder.INSTANCE.set(null);
    }

  }

  public void runAsync(final RequestParams reqParams, final SolrWriter sw) {
    new Thread() {
      @Override
      public void run() {
        runCmd(reqParams, sw);
      }
    }.start();
  }

  void runCmd(RequestParams reqParams, SolrWriter sw) {
    String command = reqParams.command;
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

  static final class RequestParams {
    public String command = null;

    public boolean debug = false;
    
    public boolean verbose = false;

    public boolean syncMode = false;

    public boolean commit = true;

    public boolean optimize = true;

    public int start = 0;

    public long rows = Integer.MAX_VALUE;

    public boolean clean = true;

    public List<String> entities;

    public Map<String, Object> requestParams;

    public String dataConfig;

    public ContentStream contentStream;
    
    public List<SolrInputDocument> debugDocuments = Collections.synchronizedList(new ArrayList<SolrInputDocument>());
    
    public NamedList debugVerboseOutput = null;

    public RequestParams() {
    }

    public RequestParams(Map<String, Object> requestParams) {
      if (requestParams.containsKey("command"))
        command = (String) requestParams.get("command");

      if (StrUtils.parseBool((String)requestParams.get("debug"),false)) {
        debug = true;
        rows = 10;
        // Set default values suitable for debug mode
        commit = false;
        clean = false;
        verbose = StrUtils.parseBool((String)requestParams.get("verbose"),false);
      }
      syncMode = StrUtils.parseBool((String)requestParams.get("synchronous"),false);
      if (DELTA_IMPORT_CMD.equals(command) || IMPORT_CMD.equals(command)) {
        clean = false;
      }
      if (requestParams.containsKey("commit"))
        commit = StrUtils.parseBool((String) requestParams.get("commit"),true);
      if (requestParams.containsKey("start"))
        start = Integer.parseInt((String) requestParams.get("start"));
      if (requestParams.containsKey("rows"))
        rows = Integer.parseInt((String) requestParams.get("rows"));
      if (requestParams.containsKey("clean"))
        clean = StrUtils.parseBool((String) requestParams.get("clean"),true);
      if (requestParams.containsKey("optimize")) {
        optimize = StrUtils.parseBool((String) requestParams.get("optimize"),true);
        if (optimize)
          commit = true;
      }

      Object o = requestParams.get("entity");

      if (o != null && o instanceof String) {
        entities = new ArrayList<String>();
        entities.add((String) o);
      } else if (o != null && o instanceof List) {
        entities = (List<String>) requestParams.get("entity");
      }

      dataConfig = (String) requestParams.get("dataConfig");
      if (dataConfig != null && dataConfig.trim().length() == 0) {
        // Empty data-config param is not valid, change it to null
        dataConfig = null;
      }

      this.requestParams = requestParams;
    }
  }

  IndexSchema getSchema() {
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
