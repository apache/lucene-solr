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
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.dataimport.config.ConfigNameConstants;
import org.apache.solr.handler.dataimport.config.DIHConfiguration;
import org.apache.solr.handler.dataimport.config.Entity;
import org.apache.solr.handler.dataimport.config.EntityField;

import static org.apache.solr.handler.dataimport.SolrWriter.LAST_INDEX_KEY;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p> {@link DocBuilder} is responsible for creating Solr documents out of the given configuration. It also maintains
 * statistics information. It depends on the {@link EntityProcessor} implementations to fetch data. </p>
 * <p>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.3
 */
public class DocBuilder {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final AtomicBoolean WARNED_ABOUT_INDEX_TIME_BOOSTS = new AtomicBoolean();

  private static final Date EPOCH = new Date(0);
  public static final String DELETE_DOC_BY_ID = "$deleteDocById";
  public static final String DELETE_DOC_BY_QUERY = "$deleteDocByQuery";
  public static final String DOC_BOOST = "$docBoost";
  public static final String SKIP_DOC = "$skipDoc";
  public static final String SKIP_ROW = "$skipRow";

  DataImporter dataImporter;

  private DIHConfiguration config;

  private EntityProcessorWrapper currentEntityProcessorWrapper;

  @SuppressWarnings({"unchecked", "rawtypes"})
  private Map statusMessages = Collections.synchronizedMap(new LinkedHashMap());

  public Statistics importStatistics = new Statistics();

  DIHWriter writer;

  boolean verboseDebug = false;

  Map<String, Object> session = new HashMap<>();

  static final ThreadLocal<DocBuilder> INSTANCE = new ThreadLocal<>();
  private Map<String, Object> persistedProperties;
  
  private DIHProperties propWriter;
  private DebugLogger debugLogger;
  private final RequestInfo reqParams;
  
  public DocBuilder(DataImporter dataImporter, DIHWriter solrWriter, DIHProperties propWriter, RequestInfo reqParams) {
    INSTANCE.set(this);
    this.dataImporter = dataImporter;
    this.reqParams = reqParams;
    this.propWriter = propWriter;
    DataImporter.QUERY_COUNT.set(importStatistics.queryCount);
    verboseDebug = reqParams.isDebug() && reqParams.getDebugInfo().verbose;
    persistedProperties = propWriter.readIndexerProperties();
     
    writer = solrWriter;
    ContextImpl ctx = new ContextImpl(null, null, null, null, reqParams.getRawParams(), null, this);
    if (writer != null) {
      writer.init(ctx);
    }
  }


  DebugLogger getDebugLogger(){
    if (debugLogger == null) {
      debugLogger = new DebugLogger();
    }
    return debugLogger;
  }

  private VariableResolver getVariableResolver() {
    try {
      VariableResolver resolver = null;
      String epoch = propWriter.convertDateToString(EPOCH);
      if(dataImporter != null && dataImporter.getCore() != null
          && dataImporter.getCore().getCoreDescriptor().getSubstitutableProperties() != null){
        resolver =  new VariableResolver(dataImporter.getCore().getCoreDescriptor().getSubstitutableProperties());
      } else {
        resolver = new VariableResolver();
      }
      resolver.setEvaluators(dataImporter.getEvaluators());
      Map<String, Object> indexerNamespace = new HashMap<>();
      if (persistedProperties.get(LAST_INDEX_TIME) != null) {
        indexerNamespace.put(LAST_INDEX_TIME, persistedProperties.get(LAST_INDEX_TIME));
      } else  {
        // set epoch
        indexerNamespace.put(LAST_INDEX_TIME, epoch);
      }
      indexerNamespace.put(INDEX_START_TIME, dataImporter.getIndexStartTime());
      indexerNamespace.put("request", new HashMap<>(reqParams.getRawParams()));
      indexerNamespace.put("handlerName", dataImporter.getHandlerName());
      for (Entity entity : dataImporter.getConfig().getEntities()) {
        Map<String, Object> entityNamespace = new HashMap<>();
        String key = SolrWriter.LAST_INDEX_KEY;
        Object lastIndex = persistedProperties.get(entity.getName() + "." + key);
        if (lastIndex != null) {
          entityNamespace.put(SolrWriter.LAST_INDEX_KEY, lastIndex);
        } else  {
          entityNamespace.put(SolrWriter.LAST_INDEX_KEY, epoch);
        }
        indexerNamespace.put(entity.getName(), entityNamespace);
      }
      resolver.addNamespace(ConfigNameConstants.IMPORTER_NS_SHORT, indexerNamespace);
      resolver.addNamespace(ConfigNameConstants.IMPORTER_NS, indexerNamespace);
      return resolver;
    } catch (Exception e) {
      wrapAndThrow(SEVERE, e);
      // unreachable statement
      return null;
    }
  }

  private void invokeEventListener(String className) {
    invokeEventListener(className, null);
  }


  private void invokeEventListener(String className, Exception lastException) {
    try {
      @SuppressWarnings({"unchecked"})
      EventListener listener = (EventListener) loadClass(className, dataImporter.getCore()).getConstructor().newInstance();
      notifyListener(listener, lastException);
    } catch (Exception e) {
      wrapAndThrow(SEVERE, e, "Unable to load class : " + className);
    }
  }

  private void notifyListener(EventListener listener, Exception lastException) {
    String currentProcess;
    if (dataImporter.getStatus() == DataImporter.Status.RUNNING_DELTA_DUMP) {
      currentProcess = Context.DELTA_DUMP;
    } else {
      currentProcess = Context.FULL_DUMP;
    }
    ContextImpl ctx = new ContextImpl(null, getVariableResolver(), null, currentProcess, session, null, this);
    ctx.setLastException(lastException);
    listener.onEvent(ctx);
  }

  @SuppressWarnings("unchecked")
  public void execute() {
    List<EntityProcessorWrapper> epwList = null;
    try {
      dataImporter.store(DataImporter.STATUS_MSGS, statusMessages);
      config = dataImporter.getConfig();
      final AtomicLong startTime = new AtomicLong(System.nanoTime());
      statusMessages.put(TIME_ELAPSED, new Object() {
        @Override
        public String toString() {
          return getTimeElapsedSince(startTime.get());
        }
      });

      statusMessages.put(DataImporter.MSG.TOTAL_QUERIES_EXECUTED,
              importStatistics.queryCount);
      statusMessages.put(DataImporter.MSG.TOTAL_ROWS_EXECUTED,
              importStatistics.rowsCount);
      statusMessages.put(DataImporter.MSG.TOTAL_DOC_PROCESSED,
              importStatistics.docCount);
      statusMessages.put(DataImporter.MSG.TOTAL_DOCS_SKIPPED,
              importStatistics.skipDocCount);

      List<String> entities = reqParams.getEntitiesToRun();

      // Trigger onImportStart
      if (config.getOnImportStart() != null) {
        invokeEventListener(config.getOnImportStart());
      }
      AtomicBoolean fullCleanDone = new AtomicBoolean(false);
      //we must not do a delete of *:* multiple times if there are multiple root entities to be run
      Map<String,Object> lastIndexTimeProps = new HashMap<>();
      lastIndexTimeProps.put(LAST_INDEX_KEY, dataImporter.getIndexStartTime());

      epwList = new ArrayList<>(config.getEntities().size());
      for (Entity e : config.getEntities()) {
        epwList.add(getEntityProcessorWrapper(e));
      }
      for (EntityProcessorWrapper epw : epwList) {
        if (entities != null && !entities.contains(epw.getEntity().getName()))
          continue;
        lastIndexTimeProps.put(epw.getEntity().getName() + "." + LAST_INDEX_KEY, propWriter.getCurrentTimestamp());
        currentEntityProcessorWrapper = epw;
        String delQuery = epw.getEntity().getAllAttributes().get("preImportDeleteQuery");
        if (dataImporter.getStatus() == DataImporter.Status.RUNNING_DELTA_DUMP) {
          cleanByQuery(delQuery, fullCleanDone);
          doDelta();
          delQuery = epw.getEntity().getAllAttributes().get("postImportDeleteQuery");
          if (delQuery != null) {
            fullCleanDone.set(false);
            cleanByQuery(delQuery, fullCleanDone);
          }
        } else {
          cleanByQuery(delQuery, fullCleanDone);
          doFullDump();
          delQuery = epw.getEntity().getAllAttributes().get("postImportDeleteQuery");
          if (delQuery != null) {
            fullCleanDone.set(false);
            cleanByQuery(delQuery, fullCleanDone);
          }
        }
      }

      if (stop.get()) {
        // Dont commit if aborted using command=abort
        statusMessages.put("Aborted", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT).format(new Date()));
        handleError("Aborted", null);
      } else {
        // Do not commit unnecessarily if this is a delta-import and no documents were created or deleted
        if (!reqParams.isClean()) {
          if (importStatistics.docCount.get() > 0 || importStatistics.deletedDocCount.get() > 0) {
            finish(lastIndexTimeProps);
          }
        } else {
          // Finished operation normally, commit now
          finish(lastIndexTimeProps);
        }

        if (config.getOnImportEnd() != null) {
          invokeEventListener(config.getOnImportEnd());
        }
      }

      statusMessages.remove(TIME_ELAPSED);
      statusMessages.put(DataImporter.MSG.TOTAL_DOC_PROCESSED, ""+ importStatistics.docCount.get());
      if(importStatistics.failedDocCount.get() > 0)
        statusMessages.put(DataImporter.MSG.TOTAL_FAILED_DOCS, ""+ importStatistics.failedDocCount.get());

      statusMessages.put("Time taken", getTimeElapsedSince(startTime.get()));
      if (log.isInfoEnabled()) {
        log.info("Time taken = {}", getTimeElapsedSince(startTime.get()));
      }
    } catch(Exception e)
    {
      throw new RuntimeException(e);
    } finally {
      // Cannot use IOUtils.closeQuietly since DIH relies on exceptions bubbling out of writer.close() to indicate
      // success/failure of the run.
      RuntimeException raisedDuringClose = null;
      try {
        if (writer != null) {
          writer.close();
        }
      } catch (RuntimeException e) {
        if (log.isWarnEnabled()) {
          log.warn("Exception encountered while closing DIHWriter " + writer + "; temporarily suppressing to ensure other DocBuilder elements are closed", e); // logOk
        }
        raisedDuringClose = e;
      }

      if (epwList != null) {
        closeEntityProcessorWrappers(epwList);
      }
      if(reqParams.isDebug()) {
        reqParams.getDebugInfo().debugVerboseOutput = getDebugLogger().output;
      }

      if (raisedDuringClose != null) {
        throw raisedDuringClose;
      }
    }
  }
  private void closeEntityProcessorWrappers(List<EntityProcessorWrapper> epwList) {
    for(EntityProcessorWrapper epw : epwList) {
      IOUtils.closeQuietly(epw);

      if(epw.getDatasource() != null) {
        IOUtils.closeQuietly(epw.getDatasource());
      }
      closeEntityProcessorWrappers(epw.getChildren());
    }
  }

  @SuppressWarnings("unchecked")
  private void finish(Map<String,Object> lastIndexTimeProps) {
    log.info("Import completed successfully");
    statusMessages.put("", "Indexing completed. Added/Updated: "
            + importStatistics.docCount + " documents. Deleted "
            + importStatistics.deletedDocCount + " documents.");
    if(reqParams.isCommit()) {
      writer.commit(reqParams.isOptimize());
      addStatusMessage("Committed");
      if (reqParams.isOptimize())
        addStatusMessage("Optimized");
    }
    try {
      propWriter.persist(lastIndexTimeProps);
    } catch (Exception e) {
      log.error("Could not write property file", e);
      statusMessages.put("error", "Could not write property file. Delta imports will not work. " +
          "Make sure your conf directory is writable");
    }
  }

  @SuppressWarnings({"unchecked"})
  void handleError(String message, Exception e) {
    if (!dataImporter.getCore().getCoreContainer().isZooKeeperAware()) {
      writer.rollback();
    }

    statusMessages.put(message, "Indexing error");
    addStatusMessage(message);
    if ((config != null) && (config.getOnError() != null)) {
      invokeEventListener(config.getOnError(), e);
    }
  }

  private void doFullDump() {
    addStatusMessage("Full Dump Started");    
    buildDocument(getVariableResolver(), null, null, currentEntityProcessorWrapper, true, null);
  }

  @SuppressWarnings("unchecked")
  private void doDelta() {
    addStatusMessage("Delta Dump started");
    VariableResolver resolver = getVariableResolver();

    if (config.getDeleteQuery() != null) {
      writer.deleteByQuery(config.getDeleteQuery());
    }

    addStatusMessage("Identifying Delta");
    log.info("Starting delta collection.");
    Set<Map<String, Object>> deletedKeys = new HashSet<>();
    Set<Map<String, Object>> allPks = collectDelta(currentEntityProcessorWrapper, resolver, deletedKeys);
    if (stop.get())
      return;
    addStatusMessage("Deltas Obtained");
    addStatusMessage("Building documents");
    if (!deletedKeys.isEmpty()) {
      allPks.removeAll(deletedKeys);
      deleteAll(deletedKeys);
      // Make sure that documents are not re-created
    }
    deletedKeys = null;
    writer.setDeltaKeys(allPks);

    statusMessages.put("Total Changed Documents", allPks.size());
    VariableResolver vri = getVariableResolver();
    Iterator<Map<String, Object>> pkIter = allPks.iterator();
    while (pkIter.hasNext()) {
      Map<String, Object> map = pkIter.next();
      vri.addNamespace(ConfigNameConstants.IMPORTER_NS_SHORT + ".delta", map);
      buildDocument(vri, null, map, currentEntityProcessorWrapper, true, null);
      pkIter.remove();
      // check for abort
      if (stop.get())
        break;
    }

    if (!stop.get()) {
      log.info("Delta Import completed successfully");
    }
  }

  private void deleteAll(Set<Map<String, Object>> deletedKeys) {
    log.info("Deleting stale documents ");
    Iterator<Map<String, Object>> iter = deletedKeys.iterator();
    while (iter.hasNext()) {
      Map<String, Object> map = iter.next();
      String keyName = currentEntityProcessorWrapper.getEntity().isDocRoot() ? currentEntityProcessorWrapper.getEntity().getPk() : currentEntityProcessorWrapper.getEntity().getSchemaPk();
      Object key = map.get(keyName);
      if(key == null) {
        keyName = findMatchingPkColumn(keyName, map);
        key = map.get(keyName);
      }
      if(key == null) {
        log.warn("no key was available for deleted pk query. keyName = {}", keyName);
        continue;
      }
      writer.deleteDoc(key);
      importStatistics.deletedDocCount.incrementAndGet();
      iter.remove();
    }
  }
  
  @SuppressWarnings("unchecked")
  public void addStatusMessage(String msg) {
    statusMessages.put(msg, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT).format(new Date()));
  }

  private void resetEntity(EntityProcessorWrapper epw) {
    epw.setInitialized(false);
    for (EntityProcessorWrapper child : epw.getChildren()) {
      resetEntity(child);
    }
    
  }
  
  private void buildDocument(VariableResolver vr, DocWrapper doc,
      Map<String,Object> pk, EntityProcessorWrapper epw, boolean isRoot,
      ContextImpl parentCtx) {
    List<EntityProcessorWrapper> entitiesToDestroy = new ArrayList<>();
    try {
      buildDocument(vr, doc, pk, epw, isRoot, parentCtx, entitiesToDestroy);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      for (EntityProcessorWrapper entityWrapper : entitiesToDestroy) {
        entityWrapper.destroy();
      }
      resetEntity(epw);
    }
  }

  @SuppressWarnings("unchecked")
  private void buildDocument(VariableResolver vr, DocWrapper doc,
                             Map<String, Object> pk, EntityProcessorWrapper epw, boolean isRoot,
                             ContextImpl parentCtx, List<EntityProcessorWrapper> entitiesToDestroy) {

    ContextImpl ctx = new ContextImpl(epw, vr, null,
            pk == null ? Context.FULL_DUMP : Context.DELTA_DUMP,
            session, parentCtx, this);
    epw.init(ctx);
    if (!epw.isInitialized()) {
      entitiesToDestroy.add(epw);
      epw.setInitialized(true);
    }
    
    if (reqParams.getStart() > 0) {
      getDebugLogger().log(DIHLogLevels.DISABLE_LOGGING, null, null);
    }

    if (verboseDebug) {
      getDebugLogger().log(DIHLogLevels.START_ENTITY, epw.getEntity().getName(), null);
    }

    int seenDocCount = 0;

    try {
      while (true) {
        if (stop.get())
          return;
        if(importStatistics.docCount.get() > (reqParams.getStart() + reqParams.getRows())) break;
        try {
          seenDocCount++;

          if (seenDocCount > reqParams.getStart()) {
            getDebugLogger().log(DIHLogLevels.ENABLE_LOGGING, null, null);
          }

          if (verboseDebug && epw.getEntity().isDocRoot()) {
            getDebugLogger().log(DIHLogLevels.START_DOC, epw.getEntity().getName(), null);
          }
          if (doc == null && epw.getEntity().isDocRoot()) {
            doc = new DocWrapper();
            ctx.setDoc(doc);
            Entity e = epw.getEntity();
            while (e.getParentEntity() != null) {
              addFields(e.getParentEntity(), doc, (Map<String, Object>) vr
                      .resolve(e.getParentEntity().getName()), vr);
              e = e.getParentEntity();
            }
          }

          Map<String, Object> arow = epw.nextRow();
          if (arow == null) {
            break;
          }

          // Support for start parameter in debug mode
          if (epw.getEntity().isDocRoot()) {
            if (seenDocCount <= reqParams.getStart())
              continue;
            if (seenDocCount > reqParams.getStart() + reqParams.getRows()) {
              log.info("Indexing stopped at docCount = {}", importStatistics.docCount);
              break;
            }
          }

          if (verboseDebug) {
            getDebugLogger().log(DIHLogLevels.ENTITY_OUT, epw.getEntity().getName(), arow);
          }
          importStatistics.rowsCount.incrementAndGet();
          
          DocWrapper childDoc = null;
          if (doc != null) {
            if (epw.getEntity().isChild()) {
              childDoc = new DocWrapper();
              handleSpecialCommands(arow, childDoc);
              addFields(epw.getEntity(), childDoc, arow, vr);
              doc.addChildDocument(childDoc);
            } else {
              handleSpecialCommands(arow, doc);
              vr.addNamespace(epw.getEntity().getName(), arow);
              addFields(epw.getEntity(), doc, arow, vr);
              vr.removeNamespace(epw.getEntity().getName());
            }
          }
          if (epw.getEntity().getChildren() != null) {
            vr.addNamespace(epw.getEntity().getName(), arow);
            for (EntityProcessorWrapper child : epw.getChildren()) {
              if (childDoc != null) {
              buildDocument(vr, childDoc,
                  child.getEntity().isDocRoot() ? pk : null, child, false, ctx, entitiesToDestroy);
              } else {
                buildDocument(vr, doc,
                    child.getEntity().isDocRoot() ? pk : null, child, false, ctx, entitiesToDestroy);
              }
            }
            vr.removeNamespace(epw.getEntity().getName());
          }
          if (epw.getEntity().isDocRoot()) {
            if (stop.get())
              return;
            if (!doc.isEmpty()) {
              boolean result = writer.upload(doc);
              if(reqParams.isDebug()) {
                reqParams.getDebugInfo().debugDocuments.add(doc);
              }
              doc = null;
              if (result){
                importStatistics.docCount.incrementAndGet();
              } else {
                importStatistics.failedDocCount.incrementAndGet();
              }
            }
          }
        } catch (DataImportHandlerException e) {
          if (verboseDebug) {
            getDebugLogger().log(DIHLogLevels.ENTITY_EXCEPTION, epw.getEntity().getName(), e);
          }
          if(e.getErrCode() == DataImportHandlerException.SKIP_ROW){
            continue;
          }
          if (isRoot) {
            if (e.getErrCode() == DataImportHandlerException.SKIP) {
              importStatistics.skipDocCount.getAndIncrement();
              doc = null;
            } else {
              SolrException.log(log, "Exception while processing: "
                      + epw.getEntity().getName() + " document : " + doc, e);
            }
            if (e.getErrCode() == DataImportHandlerException.SEVERE)
              throw e;
          } else
            throw e;
        } catch (Exception t) {
          if (verboseDebug) {
            getDebugLogger().log(DIHLogLevels.ENTITY_EXCEPTION, epw.getEntity().getName(), t);
          }
          throw new DataImportHandlerException(DataImportHandlerException.SEVERE, t);
        } finally {
          if (verboseDebug) {
            getDebugLogger().log(DIHLogLevels.ROW_END, epw.getEntity().getName(), null);
            if (epw.getEntity().isDocRoot())
              getDebugLogger().log(DIHLogLevels.END_DOC, null, null);
          }
        }
      }
    } finally {
      if (verboseDebug) {
        getDebugLogger().log(DIHLogLevels.END_ENTITY, null, null);
      }
    }
  }

  static class DocWrapper extends SolrInputDocument {
    //final SolrInputDocument solrDocument = new SolrInputDocument();
    Map<String ,Object> session;

    public void setSessionAttribute(String key, Object val){
      if(session == null) session = new HashMap<>();
      session.put(key, val);
    }

    public Object getSessionAttribute(String key) {
      return session == null ? null : session.get(key);
    }
  }

  private void handleSpecialCommands(Map<String, Object> arow, DocWrapper doc) {
    Object value = arow.get(DELETE_DOC_BY_ID);
    if (value != null) {
      if (value instanceof Collection) {
        @SuppressWarnings({"rawtypes"})
        Collection collection = (Collection) value;
        for (Object o : collection) {
          writer.deleteDoc(o.toString());
          importStatistics.deletedDocCount.incrementAndGet();
        }
      } else {
        writer.deleteDoc(value);
        importStatistics.deletedDocCount.incrementAndGet();
      }
    }    
    value = arow.get(DELETE_DOC_BY_QUERY);
    if (value != null) {
      if (value instanceof Collection) {
        @SuppressWarnings({"rawtypes"})
        Collection collection = (Collection) value;
        for (Object o : collection) {
          writer.deleteByQuery(o.toString());
          importStatistics.deletedDocCount.incrementAndGet();
        }
      } else {
        writer.deleteByQuery(value.toString());
        importStatistics.deletedDocCount.incrementAndGet();
      }
    }
    value = arow.get(DOC_BOOST);
    if (value != null) {
      String message = "Ignoring document boost: " + value + " as index-time boosts are not supported anymore";
      if (WARNED_ABOUT_INDEX_TIME_BOOSTS.compareAndSet(false, true)) {
        log.warn(message);
      } else {
        log.debug(message);
      }
    }

    value = arow.get(SKIP_DOC);
    if (value != null) {
      if (Boolean.parseBoolean(value.toString())) {
        throw new DataImportHandlerException(DataImportHandlerException.SKIP,
                "Document skipped :" + arow);
      }
    }

    value = arow.get(SKIP_ROW);
    if (value != null) {
      if (Boolean.parseBoolean(value.toString())) {
        throw new DataImportHandlerException(DataImportHandlerException.SKIP_ROW);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void addFields(Entity entity, DocWrapper doc,
                         Map<String, Object> arow, VariableResolver vr) {
    for (Map.Entry<String, Object> entry : arow.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      if (value == null)  continue;
      if (key.startsWith("$")) continue;
      Set<EntityField> field = entity.getColNameVsField().get(key);
      IndexSchema schema = null == reqParams.getRequest() ? null : reqParams.getRequest().getSchema();
      if (field == null && schema != null) {
        // This can be a dynamic field or a field which does not have an entry in data-config ( an implicit field)
        SchemaField sf = schema.getFieldOrNull(key);
        if (sf == null) {
          sf = config.getSchemaField(key);
        }
        if (sf != null) {
          addFieldToDoc(entry.getValue(), sf.getName(), sf.multiValued(), doc);
        }
        //else do nothing. if we add it it may fail
      } else {
        if (field != null) {
          for (EntityField f : field) {
            String name = f.getName();
            boolean multiValued = f.isMultiValued();
            boolean toWrite = f.isToWrite();
            if(f.isDynamicName()){
              name =  vr.replaceTokens(name);
              SchemaField schemaField = config.getSchemaField(name);
              if(schemaField == null) {
                toWrite = false;
              } else {
                multiValued = schemaField.multiValued();
                toWrite = true;
              }
            }
            if (toWrite) {
              addFieldToDoc(entry.getValue(), name, multiValued, doc);
            }
          }
        }
      }
    }
  }

  private void addFieldToDoc(Object value, String name, boolean multiValued, DocWrapper doc) {
    if (value instanceof Collection) {
      @SuppressWarnings({"rawtypes"})
      Collection collection = (Collection) value;
      if (multiValued) {
        for (Object o : collection) {
          if (o != null)
            doc.addField(name, o);
        }
      } else {
        if (doc.getField(name) == null)
          for (Object o : collection) {
            if (o != null)  {
              doc.addField(name, o);
              break;
            }
          }
      }
    } else if (multiValued) {
      if (value != null)  {
        doc.addField(name, value);
      }
    } else {
      if (doc.getField(name) == null && value != null)
        doc.addField(name, value);
    }
  }

  @SuppressWarnings({"unchecked"})
  public EntityProcessorWrapper getEntityProcessorWrapper(Entity entity) {
    EntityProcessor entityProcessor = null;
    if (entity.getProcessorName() == null) {
      entityProcessor = new SqlEntityProcessor();
    } else {
      try {
        entityProcessor = (EntityProcessor) loadClass(entity.getProcessorName(), dataImporter.getCore())
                .newInstance();
      } catch (Exception e) {
        wrapAndThrow (SEVERE,e,
                "Unable to load EntityProcessor implementation for entity:" + entity.getName());
      }
    }
    EntityProcessorWrapper epw = new EntityProcessorWrapper(entityProcessor, entity, this);
    for(Entity e1 : entity.getChildren()) {
      epw.getChildren().add(getEntityProcessorWrapper(e1));
    }
      
    return epw;
  }

  private String findMatchingPkColumn(String pk, Map<String, Object> row) {
    if (row.containsKey(pk)) {
      throw new IllegalArgumentException(String.format(Locale.ROOT,
          "deltaQuery returned a row with null for primary key %s", pk));
    }
    String resolvedPk = null;
    for (String columnName : row.keySet()) {
      if (columnName.endsWith("." + pk) || pk.endsWith("." + columnName)) {
        if (resolvedPk != null)
          throw new IllegalArgumentException(
            String.format(Locale.ROOT, 
              "deltaQuery has more than one column (%s and %s) that might resolve to declared primary key pk='%s'",
              resolvedPk, columnName, pk));
        resolvedPk = columnName;
      }
    }
    if (resolvedPk == null) {
      throw new IllegalArgumentException(
          String
              .format(
                  Locale.ROOT,
                  "deltaQuery has no column to resolve to declared primary key pk='%s'",
                  pk));
    }
    if (log.isInfoEnabled()) {
      log.info(String.format(Locale.ROOT,
          "Resolving deltaQuery column '%s' to match entity's declared pk '%s'",
          resolvedPk, pk));
    }
    return resolvedPk;
  }

  /**
   * <p> Collects unique keys of all Solr documents for whom one or more source tables have been changed since the last
   * indexed time. </p> <p> Note: In our definition, unique key of Solr document is the primary key of the top level
   * entity (unless skipped using docRoot=false) in the Solr document in data-config.xml </p>
   *
   * @return an iterator to the list of keys for which Solr documents should be updated.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Set<Map<String, Object>> collectDelta(EntityProcessorWrapper epw, VariableResolver resolver,
                                               Set<Map<String, Object>> deletedRows) {
    //someone called abort
    if (stop.get())
      return new HashSet();

    ContextImpl context1 = new ContextImpl(epw, resolver, null, Context.FIND_DELTA, session, null, this);
    epw.init(context1);

    Set<Map<String, Object>> myModifiedPks = new HashSet<>();

   

    for (EntityProcessorWrapper childEpw : epw.getChildren()) {
      //this ensures that we start from the leaf nodes
      myModifiedPks.addAll(collectDelta(childEpw, resolver, deletedRows));
      //someone called abort
      if (stop.get())
        return new HashSet();
    }
    
    // identifying the modified rows for this entity
    Map<String, Map<String, Object>> deltaSet = new HashMap<>();
    if (log.isInfoEnabled()) {
      log.info("Running ModifiedRowKey() for Entity: {}", epw.getEntity().getName());
    }
    //get the modified rows in this entity
    String pk = epw.getEntity().getPk();
    while (true) {
      Map<String, Object> row = epw.nextModifiedRowKey();

      if (row == null)
        break;

      Object pkValue = row.get(pk);
      if (pkValue == null) {
        pk = findMatchingPkColumn(pk, row);
        pkValue = row.get(pk);
      }

      deltaSet.put(pkValue.toString(), row);
      importStatistics.rowsCount.incrementAndGet();
      // check for abort
      if (stop.get())
        return new HashSet();
    }
    //get the deleted rows for this entity
    Set<Map<String, Object>> deletedSet = new HashSet<>();
    while (true) {
      Map<String, Object> row = epw.nextDeletedRowKey();
      if (row == null)
        break;

      deletedSet.add(row);
      
      Object pkValue = row.get(pk);
      if (pkValue == null) {
        pk = findMatchingPkColumn(pk, row);
        pkValue = row.get(pk);
      }

      // Remove deleted rows from the delta rows
      String deletedRowPk = pkValue.toString();
      if (deltaSet.containsKey(deletedRowPk)) {
        deltaSet.remove(deletedRowPk);
      }

      importStatistics.rowsCount.incrementAndGet();
      // check for abort
      if (stop.get())
        return new HashSet();
    }

    if (log.isInfoEnabled()) {
      log.info("Completed ModifiedRowKey for Entity: {} rows obtained: {}", epw.getEntity().getName(), deltaSet.size());
      log.info("Completed DeletedRowKey for Entity: {} rows obtained : {}", epw.getEntity().getName(), deletedSet.size()); // logOk
    }

    myModifiedPks.addAll(deltaSet.values());
    Set<Map<String, Object>> parentKeyList = new HashSet<>();
    //all that we have captured is useless (in a sub-entity) if no rows in the parent is modified because of these
    //propogate up the changes in the chain
    if (epw.getEntity().getParentEntity() != null) {
      // identifying deleted rows with deltas

      for (Map<String, Object> row : myModifiedPks) {
        resolver.addNamespace(epw.getEntity().getName(), row);
        getModifiedParentRows(resolver, epw.getEntity().getName(), epw, parentKeyList);
        // check for abort
        if (stop.get())
          return new HashSet();
      }
      // running the same for deletedrows
      for (Map<String, Object> row : deletedSet) {
        resolver.addNamespace(epw.getEntity().getName(), row);
        getModifiedParentRows(resolver, epw.getEntity().getName(), epw, parentKeyList);
        // check for abort
        if (stop.get())
          return new HashSet();
      }
    }
    if (log.isInfoEnabled()) {
      log.info("Completed parentDeltaQuery for Entity: {}", epw.getEntity().getName());
    }
    if (epw.getEntity().isDocRoot())
      deletedRows.addAll(deletedSet);

    // Do not use entity.isDocRoot here because one of descendant entities may set rootEntity="true"
    return epw.getEntity().getParentEntity() == null ?
        myModifiedPks : new HashSet<>(parentKeyList);
  }

  private void getModifiedParentRows(VariableResolver resolver,
                                     String entity, EntityProcessor entityProcessor,
                                     Set<Map<String, Object>> parentKeyList) {
    try {
      while (true) {
        Map<String, Object> parentRow = entityProcessor
                .nextModifiedParentRowKey();
        if (parentRow == null)
          break;

        parentKeyList.add(parentRow);
        importStatistics.rowsCount.incrementAndGet();
        // check for abort
        if (stop.get())
          return;
      }

    } finally {
      resolver.removeNamespace(entity);
    }
  }

  public void abort() {
    stop.set(true);
  }

  private AtomicBoolean stop = new AtomicBoolean(false);

  public static final String TIME_ELAPSED = "Time Elapsed";

  static String getTimeElapsedSince(long l) {
    l = TimeUnit.MILLISECONDS.convert(System.nanoTime() - l, TimeUnit.NANOSECONDS);
    return (l / (60000 * 60)) + ":" + (l / 60000) % 60 + ":" + (l / 1000)
            % 60 + "." + l % 1000;
  }

  public RequestInfo getReqParams() {
    return reqParams;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  static Class loadClass(String name, SolrCore core) throws ClassNotFoundException {
    try {
      return core != null ?
              core.getResourceLoader().findClass(name, Object.class) :
              Class.forName(name);
    } catch (Exception e) {
      try {
        String n = DocBuilder.class.getPackage().getName() + "." + name;
        return core != null ?
                core.getResourceLoader().findClass(n, Object.class) :
                Class.forName(n);
      } catch (Exception e1) {
        throw new ClassNotFoundException("Unable to load " + name + " or " + DocBuilder.class.getPackage().getName() + "." + name, e);
      }
    }
  }

  public static class Statistics {
    public AtomicLong docCount = new AtomicLong();

    public AtomicLong deletedDocCount = new AtomicLong();

    public AtomicLong failedDocCount = new AtomicLong();

    public AtomicLong rowsCount = new AtomicLong();

    public AtomicLong queryCount = new AtomicLong();

    public AtomicLong skipDocCount = new AtomicLong();

    public Statistics add(Statistics stats) {
      this.docCount.addAndGet(stats.docCount.get());
      this.deletedDocCount.addAndGet(stats.deletedDocCount.get());
      this.rowsCount.addAndGet(stats.rowsCount.get());
      this.queryCount.addAndGet(stats.queryCount.get());

      return this;
    }

    public Map<String, Object> getStatsSnapshot() {
      Map<String, Object> result = new HashMap<>();
      result.put("docCount", docCount.get());
      result.put("deletedDocCount", deletedDocCount.get());
      result.put("rowCount", rowsCount.get());
      result.put("queryCount", rowsCount.get());
      result.put("skipDocCount", skipDocCount.get());
      return result;
    }

  }

  private void cleanByQuery(String delQuery, AtomicBoolean completeCleanDone) {
    delQuery = getVariableResolver().replaceTokens(delQuery);
    if (reqParams.isClean()) {
      if (delQuery == null && !completeCleanDone.get()) {
        writer.doDeleteAll();
        completeCleanDone.set(true);
      } else if (delQuery != null) {
        writer.deleteByQuery(delQuery);
      }
    }
  }

  public static final String LAST_INDEX_TIME = "last_index_time";
  public static final String INDEX_START_TIME = "index_start_time";
}
