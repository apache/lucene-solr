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

import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * Stores all configuration information for pulling and indexing data.
 * </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class DataImporter {

  public enum Status {
    IDLE, RUNNING_FULL_DUMP, RUNNING_DELTA_DUMP, JOB_FAILED
  }

  private static final Logger LOG = Logger.getLogger(DataImporter.class
          .getName());

  private Status status = Status.IDLE;

  private DataConfig config;

  private Date lastIndexTime;

  private Date indexStartTime;

  private Properties store = new Properties();

  private Map<String, Properties> dataSourceProps;

  private IndexSchema schema;

  public DocBuilder docBuilder;

  public DocBuilder.Statistics cumulativeStatistics = new DocBuilder.Statistics();

  public Map<String, Evaluator> evaluators;

  private SolrCore core;

  /**
   * Only for testing purposes
   */
  DataImporter() {
  }

  public DataImporter(String dataConfig, SolrCore core,
                      Map<String, Properties> ds) {
    if (dataConfig == null)
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "Configuration not found");
    this.core = core;
    this.schema = core.getSchema();
    dataSourceProps = ds;
    loadDataConfig(dataConfig);

    for (DataConfig.Document document : config.documents) {
      for (DataConfig.Entity e : document.entities) {
        Map<String, DataConfig.Field> fields = new HashMap<String, DataConfig.Field>();
        initEntity(e, fields, false);
        e.implicitFields = new ArrayList<DataConfig.Field>();
        String errs = verifyWithSchema(fields, e.implicitFields);
        if (e.implicitFields.isEmpty())
          e.implicitFields = null;
        if (errs != null) {
          throw new DataImportHandlerException(
                  DataImportHandlerException.SEVERE, errs);
        }
      }
    }
  }

  private String verifyWithSchema(Map<String, DataConfig.Field> fields,
                                  List<DataConfig.Field> autoFields) {
    List<String> errors = new ArrayList<String>();
    Map<String, SchemaField> schemaFields = schema.getFields();
    for (Map.Entry<String, SchemaField> entry : schemaFields.entrySet()) {
      SchemaField sf = entry.getValue();
      if (!fields.containsKey(sf.getName())) {
        if (sf.isRequired()) {
          LOG
                  .info(sf.getName()
                          + " is a required field in SolrSchema . But not found in DataConfig");
        }
        autoFields.add(new DataConfig.Field(sf.getName(), sf.multiValued()));
      }
    }
    for (Map.Entry<String, DataConfig.Field> entry : fields.entrySet()) {
      DataConfig.Field fld = entry.getValue();
      FieldType fieldType = null;

      try {
        fieldType = schema.getDynamicFieldType(fld.name);
      } catch (RuntimeException e) {
        // Ignore because it may not be a dynamic field
      }

      if (fld.name != null) {
        if (schema.getFields().get(fld.name) == null && fieldType == null) {
          errors
                  .add("The field :"
                          + fld.name
                          + " present in DataConfig does not have a counterpart in Solr Schema");
        }
      } else if (schema.getFields().get(fld.column) == null
              && fieldType == null) {
        LOG.info("Column : " + fld.column + " is not a schema field");
      }
    }

    if (!errors.isEmpty()) {
      StringBuffer sb = new StringBuffer("There are errors in the Schema\n");
      for (String error : errors) {
        sb.append(error).append("\n");
      }
      return sb.toString();

    }
    return null;

  }

  void loadDataConfig(String configFile) {

    try {
      DocumentBuilder builder = DocumentBuilderFactory.newInstance()
              .newDocumentBuilder();
      Document document = builder.parse(new InputSource(new StringReader(
              configFile)));

      config = new DataConfig();
      config.readFromXml((Element) document.getElementsByTagName("dataConfig")
              .item(0));

      LOG.info("Data Configuration loaded successfully");
    } catch (Exception e) {
      SolrConfig.severeErrors.add(e);
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "Exception occurred while initializing context", e);
    }
  }

  private void initEntity(DataConfig.Entity e,
                          Map<String, DataConfig.Field> fields, boolean docRootFound) {
    if (e.pk != null)
      e.primaryKeys = e.pk.split(",");
    e.allAttributes.put(DATA_SRC, e.dataSource);

    if (!docRootFound && !"false".equals(e.docRoot)) {
      // if in this chain no document root is found()
      e.isDocRoot = true;
    }

    if (e.fields != null) {
      for (DataConfig.Field f : e.fields) {
        f.nameOrColName = f.getName();
        SchemaField schemaField = schema.getFields().get(f.getName());
        if (schemaField != null) {
          f.multiValued = schemaField.multiValued();
          f.allAttributes.put(MULTI_VALUED, Boolean.toString(schemaField
                  .multiValued()));
          f.allAttributes.put(TYPE, schemaField.getType().getTypeName());
          f.allAttributes.put("indexed", Boolean
                  .toString(schemaField.indexed()));
          f.allAttributes.put("stored", Boolean.toString(schemaField.stored()));
          f.allAttributes.put("defaultValue", schemaField.getDefaultValue());
        } else {

          try {
            f.allAttributes.put(TYPE, schema.getDynamicFieldType(f.getName())
                    .getTypeName());
            f.allAttributes.put(MULTI_VALUED, "true");
            f.multiValued = true;
          } catch (RuntimeException e2) {
            LOG.info("Field in data-config.xml - " + f.getName()
                    + " not found in schema.xml");
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

    addDataSource(e);

    if (e.entities == null)
      return;
    for (DataConfig.Entity e1 : e.entities) {
      e1.parentEntity = e;
      initEntity(e1, fields, e.isDocRoot || docRootFound);
    }

  }

  public DataConfig getConfig() {
    return config;
  }

  public Date getIndexStartTime() {
    return indexStartTime;
  }

  public void setIndexStartTime(Date indextStartTime) {
    this.indexStartTime = indextStartTime;
  }

  public Date getLastIndexTime() {
    return lastIndexTime;
  }

  public void setLastIndexTime(Date lastIndexTime) {
    this.lastIndexTime = lastIndexTime;
  }

  public void store(Object key, Object value) {
    store.put(key, value);
  }

  public Object retrieve(Object key) {
    return store.get(key);
  }

  @SuppressWarnings("unchecked")
  public void addDataSource(DataConfig.Entity key) {
    if ("null".equals(key.dataSource)) {
      key.dataSrc = new MockDataSource();
      return;
    }
    key.dataSrc = getDataSourceInstance(key, key.dataSource, null);
  }

  DataSource getDataSourceInstance(DataConfig.Entity key, String name, Context ctx ) {
    Properties p = dataSourceProps.get(name);
    if (p == null)
      p = config.dataSources.get(name);
    if (p == null)
      p = dataSourceProps.get(null);// for default data source
    if (p == null)
      p = config.dataSources.get(null);
    if (p == null)
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "No dataSource :" + name + " available for entity :"
                      + key.name);
    String impl = p.getProperty(TYPE);
    DataSource dataSrc = null;
    if (impl == null) {
      dataSrc = new JdbcDataSource();
    } else {
      try {
        dataSrc = (DataSource) DocBuilder.loadClass(impl).newInstance();
      } catch (Exception e) {
        throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                "Invalid type for data source: " + impl, e);
      }
    }
    try {
      Properties copyProps = new Properties();
      copyProps.putAll(p);
      if(ctx == null)
        ctx = new ContextImpl(key, null, dataSrc, 0,
              Collections.EMPTY_MAP, new HashMap(), null, this);
      dataSrc.init(ctx, copyProps);
    } catch (Exception e) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "Failed to initialize DataSource: " + key.dataSource, e);
    }
    return dataSrc;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public void doFullImport(SolrWriter writer, RequestParams requestParams,
                           Map<String, String> variables) {
    LOG.info("Starting Full Import");
    setStatus(Status.RUNNING_FULL_DUMP);

    if (requestParams.commit)
      setIndexStartTime(new Date());

    try {
      if (requestParams.clean)
        writer.doDeleteAll();
      docBuilder = new DocBuilder(this, writer, requestParams, variables);
      docBuilder.execute(getConfig().documents.get(0).name);
      if (!requestParams.debug)
        cumulativeStatistics.add(docBuilder.importStatistics);
    } catch (RuntimeException e) {
      LOG.log(Level.SEVERE, "Full Import failed", e);
    } finally {
      setStatus(Status.IDLE);
      config.clearCaches();
      DocBuilder.INSTANCE.set(null);
    }

  }

  public void doDeltaImport(SolrWriter writer, RequestParams requestParams,
                            Map<String, String> variables) {
    LOG.info("Starting Delta Import");
    setStatus(Status.RUNNING_DELTA_DUMP);

    try {
      if (requestParams.commit) {
        Date lastModified = writer.loadIndexStartTime();
        setIndexStartTime(new Date());
        setLastIndexTime(lastModified);
      }
      docBuilder = new DocBuilder(this, writer, requestParams, variables);
      docBuilder.execute(config.documents.get(0).name);
      if (!requestParams.debug)
        cumulativeStatistics.add(docBuilder.importStatistics);
    } catch (RuntimeException e) {
      LOG.log(Level.SEVERE, "Delta Import Failed", e);
    } finally {
      setStatus(Status.IDLE);
      config.clearCaches();
      DocBuilder.INSTANCE.set(null);
    }

  }

  public void runAsync(final RequestParams reqParams, final SolrWriter sw,
                       final Map<String, String> variables) {
    new Thread() {
      @Override
      public void run() {
        runCmd(reqParams, sw, variables);
      }
    }.start();
  }

  void runCmd(RequestParams reqParams, SolrWriter sw,
              Map<String, String> variables) {
    String command = reqParams.command;
    if (command.equals("full-import")) {
      doFullImport(sw, reqParams, variables);
    } else if (command.equals(DELTA_IMPORT_CMD)) {
      doDeltaImport(sw, reqParams, variables);
    } else if (command.equals(ABORT_CMD)) {
      if (docBuilder != null)
        docBuilder.abort();
    }
  }

  @SuppressWarnings("unchecked")
  Map<String, String> getStatusMessages() {
    Map statusMessages = (Map) retrieve(STATUS_MSGS);
    Map<String, String> result = new LinkedHashMap<String, String>();
    if (statusMessages != null) {
      for (Object o : statusMessages.entrySet()) {
        Map.Entry e = (Map.Entry) o;
        result.put((String) e.getKey(), e.getValue().toString());
      }
    }
    return result;

  }

  public DocBuilder getDocBuilder() {
    return docBuilder;
  }

  public static final ThreadLocal<AtomicLong> QUERY_COUNT = new ThreadLocal<AtomicLong>() {
    protected AtomicLong initialValue() {
      return new AtomicLong();
    }
  };

  static final SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat(
          "yyyy-MM-dd HH:mm:ss");

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

    public static final String TOTAL_QUERIES_EXECUTED = "Total Requests made to DataSource";

    public static final String TOTAL_ROWS_EXECUTED = "Total Rows Fetched";

    public static final String TOTAL_DOCS_DELETED = "Total Documents Deleted";

    public static final String TOTAL_DOCS_SKIPPED = "Total Documents Skipped";
  }

  static final class RequestParams {
    public String command = null;

    public boolean debug = false;

    public boolean verbose = false;

    public boolean commit = true;

    public boolean optimize = true;

    public int start = 0;

    public int rows = 10;

    public boolean clean = true;

    public List<String> entities;

    public Map<String, Object> requestParams;

    public String dataConfig;

    public RequestParams() {
    }

    public RequestParams(Map<String, Object> requestParams) {
      if (requestParams.containsKey("command"))
        command = (String) requestParams.get("command");

      if ("on".equals(requestParams.get("debug"))) {
        debug = true;
        // Set default values suitable for debug mode
        commit = false;
        clean = false;
        verbose = "true".equals(requestParams.get("verbose"))
                || "on".equals(requestParams.get("verbose"));
      }
      if (requestParams.containsKey("commit"))
        commit = Boolean.parseBoolean((String) requestParams.get("commit"));
      if (requestParams.containsKey("start"))
        start = Integer.parseInt((String) requestParams.get("start"));
      if (requestParams.containsKey("rows"))
        rows = Integer.parseInt((String) requestParams.get("rows"));
      if (requestParams.containsKey("clean"))
        clean = Boolean.parseBoolean((String) requestParams.get("clean"));
      if (requestParams.containsKey("optimize"))
        optimize = Boolean.parseBoolean((String) requestParams.get("optimize"));

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


  public SolrCore getCore() {
    return core;
  }

  public static final String COLUMN = "column";

  public static final String TYPE = "type";

  public static final String DATA_SRC = "dataSource";

  public static final String MULTI_VALUED = "multiValued";

  public static final String NAME = "name";

  public static final String STATUS_MSGS = "status-messages";

  public static final String FULL_IMPORT_CMD = "full-import";

  public static final String DELTA_IMPORT_CMD = "delta-import";

  public static final String ABORT_CMD = "abort";

  public static final String DEBUG_MODE = "debug";

  public static final String RELOAD_CONF_CMD = "reload-config";

  public static final String SHOW_CONF_CMD = "show-config";
}
