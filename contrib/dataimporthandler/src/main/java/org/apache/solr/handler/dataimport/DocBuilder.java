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

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * DocBuilder is responsible for creating Solr documents out of the given
 * configuration. It also maintains statistics information. It depends on the
 * EntityProcessor implementations to fetch data.
 * </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class DocBuilder {
  public static final String DOC_BOOST = "$docBoost";

  private static final Logger LOG = Logger
          .getLogger(DocBuilder.class.getName());

  private DataImporter dataImporter;

  private DataConfig.Document document;

  private DataConfig.Entity root;

  @SuppressWarnings("unchecked")
  private Map statusMessages = new LinkedHashMap();

  public Statistics importStatistics = new Statistics();

  SolrWriter writer;

  DataImporter.RequestParams requestParameters;

  boolean verboseDebug = false;

  private Map<String, String> defaultVariables;

  private Map<String, Object> session = new HashMap<String, Object>();

  static final ThreadLocal<DocBuilder> INSTANCE = new ThreadLocal<DocBuilder>();

  public DocBuilder(DataImporter context, SolrWriter writer,
                    DataImporter.RequestParams reqParams, Map<String, String> variables) {
    INSTANCE.set(this);
    this.dataImporter = context;
    this.writer = writer;
    DataImporter.QUERY_COUNT.set(importStatistics.queryCount);
    requestParameters = reqParams;
    verboseDebug = requestParameters.debug && requestParameters.verbose;
    defaultVariables = Collections.unmodifiableMap(variables);
  }

  public VariableResolverImpl getVariableResolver(DataImporter context) {
    VariableResolverImpl resolver = new VariableResolverImpl();
    Map<String, Object> indexerNamespace = new HashMap<String, Object>();
    if (context.getLastIndexTime() != null)
      indexerNamespace.put(LAST_INDEX_TIME, DataImporter.DATE_TIME_FORMAT
              .format(context.getLastIndexTime()));
    indexerNamespace.put(INDEX_START_TIME, context.getIndexStartTime());
    indexerNamespace.put("request", requestParameters);
    indexerNamespace.put("defaults", defaultVariables);
    indexerNamespace.put("functions", EvaluatorBag.getFunctionsNamespace(resolver,
            dataImporter.getConfig().evaluators));
    if (context.getConfig().script != null) {
      indexerNamespace
              .put(DataConfig.SCRIPT, context.getConfig().script.script);
      indexerNamespace.put(DataConfig.SCRIPT_LANG,
              context.getConfig().script.language);
    }
    resolver.addNamespace(DataConfig.IMPORTER_NS, indexerNamespace);
    return resolver;
  }

  @SuppressWarnings("unchecked")
  public void execute(String docName) {
    dataImporter.store(DataImporter.STATUS_MSGS, statusMessages);
    document = dataImporter.getConfig().getDocumentByName(docName);
    if (document == null)
      return;
    final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
    statusMessages.put(TIME_ELAPSED, new Object() {
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

    List<String> entities = requestParameters.entities;

    for (DataConfig.Entity e : document.entities) {
      if (entities != null && !entities.contains(e.name))
        continue;

      root = e;
      if (dataImporter.getStatus() == DataImporter.Status.RUNNING_DELTA_DUMP
              && dataImporter.getLastIndexTime() != null) {
        doDelta();

      } else {
        doFullDump();
      }
      statusMessages.remove(DataImporter.MSG.TOTAL_DOC_PROCESSED);
    }

    if (stop.get()) {
      if (DataImporter.ABORT_CMD.equals(requestParameters.command)) {
        // Dont commit if aborted using command=abort
        statusMessages.put("Aborted", DataImporter.DATE_TIME_FORMAT
                .format(new Date()));
      } else if (requestParameters.commit) {
        // Debug mode, commit if commit=true was specified
        commit();
      }
    } else {
      // Finished operation normally, commit now
      commit();
    }

    statusMessages.remove(TIME_ELAPSED);
    statusMessages.put("Time taken ", getTimeElapsedSince(startTime.get()));
    LOG.info("Time taken = " + getTimeElapsedSince(startTime.get()));
  }

  @SuppressWarnings("unchecked")
  private void commit() {
    if (requestParameters.commit)
      writer.persistIndexStartTime(dataImporter.getIndexStartTime());
    LOG.info("Full Import completed successfully");
    statusMessages.put("", "Indexing completed. Added/Updated: "
            + importStatistics.docCount + " documents. Deleted "
            + importStatistics.deletedDocCount + " documents.");
    writer.commit(requestParameters.optimize);
    addStatusMessage("Committed");
    if (requestParameters.optimize)
      addStatusMessage("Optimized");

  }

  @SuppressWarnings("unchecked")
  private void doFullDump() {
    addStatusMessage("Full Dump Started");
    buildDocument(getVariableResolver(dataImporter), null, null, root, true,
            null);
  }

  @SuppressWarnings("unchecked")
  private void doDelta() {
    addStatusMessage("Delta Dump started");
    VariableResolverImpl resolver = getVariableResolver(dataImporter);

    if (document.deleteQuery != null) {
      writer.deleteByQuery(document.deleteQuery);
    }

    addStatusMessage("Identifying Delta");
    LOG.info("Starting delta collection.");
    Set<Map<String, Object>> deletedKeys = new HashSet<Map<String, Object>>();
    Set<Map<String, Object>> allPks = collectDelta(root, null, resolver,
            dataImporter, deletedKeys);
    if (stop.get())
      return;
    addStatusMessage("Deltas Obtained");
    addStatusMessage("Building documents");
    if (!deletedKeys.isEmpty()) {
      deleteAll(deletedKeys);
      importStatistics.deletedDocCount.addAndGet(deletedKeys.size());
      // Make sure that documents are not re-created
      allPks.removeAll(deletedKeys);
    }

    statusMessages.put("Total Changed Documents", allPks.size());
    for (Map<String, Object> pk : allPks) {
      VariableResolverImpl vri = getVariableResolver(dataImporter);
      vri.addNamespace(DataConfig.IMPORTER_NS + ".delta", pk);
      buildDocument(vri, null, pk, root, true, null);
    }

    if (!stop.get()) {
      writer.persistIndexStartTime(dataImporter.getIndexStartTime());
      LOG.info("Delta Import completed successfully");
    }
  }

  private void deleteAll(Set<Map<String, Object>> deletedKeys) {
    LOG.info("Deleting stale documents ");
    for (Map<String, Object> deletedKey : deletedKeys) {
      writer.deleteDoc(deletedKey.get(root.pk));
    }
  }

  @SuppressWarnings("unchecked")
  public void addStatusMessage(String msg) {
    statusMessages.put(msg, DataImporter.DATE_TIME_FORMAT.format(new Date()));
  }

  @SuppressWarnings("unchecked")
  private void buildDocument(VariableResolverImpl vr, SolrWriter.SolrDoc doc,
                             Map<String, Object> pk, DataConfig.Entity entity, boolean isRoot,
                             ContextImpl parentCtx) {

    EntityProcessor entityProcessor = getEntityProcessor(entity);
    DataSource ds = entity.dataSrc;
    if (verboseDebug) {
      ds = DebugLogger.wrapDs(ds);
    }
    ContextImpl ctx = new ContextImpl(entity, vr, ds,
            pk == null ? Context.FULL_DUMP : Context.DELTA_DUMP,
            requestParameters.requestParams, session, parentCtx, dataImporter);
    entityProcessor.init(ctx);

    if (requestParameters.start > 0) {
      writer.log(SolrWriter.DISABLE_LOGGING, null, null);
    }

    if (verboseDebug) {
      writer.log(SolrWriter.START_ENTITY, entity.name, null);
    }

    int seenDocCount = 0;

    try {
      while (true) {
        if (stop.get())
          return;
        try {
          seenDocCount++;

          if (seenDocCount > requestParameters.start) {
            writer.log(SolrWriter.ENABLE_LOGGING, null, null);
          }

          if (verboseDebug && entity.isDocRoot) {
            writer.log(SolrWriter.START_DOC, entity.name, null);
          }
          if (doc == null && entity.isDocRoot) {
            if (ctx.getDocSession() != null)
              ctx.getDocSession().clear();
            else
              ctx.setDocSession(new HashMap<String, Object>());
            doc = writer.getSolrDocInstance();
            DataConfig.Entity e = entity;
            while (e.parentEntity != null) {
              addFields(e.parentEntity, doc, (Map<String, Object>) vr
                      .resolve(e.parentEntity.name));
              e = e.parentEntity;
            }
          }

          Map<String, Object> arow = entityProcessor.nextRow();
          if (arow == null)
            break;

          if (arow.containsKey(DOC_BOOST)) {
            setDocumentBoost(doc, arow);
          }

          // Support for start parameter in debug mode
          if (entity.isDocRoot) {
            if (seenDocCount <= requestParameters.start)
              continue;
          }

          if (verboseDebug) {
            writer.log(SolrWriter.ENTITY_OUT, entity.name, arow);
          }
          importStatistics.rowsCount.incrementAndGet();
          if (entity.fields != null && doc != null) {
            addFields(entity, doc, arow);
          }
          if (isRoot)
            vr.removeNamespace(null);
          if (entity.entities != null) {
            vr.addNamespace(entity.name, arow);
            for (DataConfig.Entity child : entity.entities) {
              buildDocument(vr, doc, null, child, false, ctx);
            }
            vr.removeNamespace(entity.name);
          }

          if (entity.isDocRoot) {
            if (stop.get())
              return;
            boolean result = writer.upload(doc);
            doc = null;
            if (result)
              importStatistics.docCount.incrementAndGet();
          }

        } catch (DataImportHandlerException e) {
          if (verboseDebug) {
            writer.log(SolrWriter.ENTITY_EXCEPTION, entity.name, e);
          }
          if (isRoot) {
            if (e.getErrCode() == DataImportHandlerException.SKIP) {
              importStatistics.skipDocCount.getAndIncrement();
            } else {
              LOG.log(Level.SEVERE, "Exception while processing: "
                      + entity.name + " document : " + doc, e);
            }
            if (e.getErrCode() == DataImportHandlerException.SEVERE)
              throw e;
          } else
            throw e;
        } finally {
          if (verboseDebug) {
            writer.log(SolrWriter.ROW_END, entity.name, null);
            if (entity.isDocRoot)
              writer.log(SolrWriter.END_DOC, null, null);
          }
        }
      }
    } finally {
      if (verboseDebug) {
        writer.log(SolrWriter.END_ENTITY, null, null);
      }
    }
  }

  private void setDocumentBoost(SolrWriter.SolrDoc doc, Map<String, Object> arow) {
    Object v = arow.get(DOC_BOOST);
    float value = 1.0f;
    if (v instanceof Number) {
      value = ((Number) v).floatValue();
    } else {
      value = Float.parseFloat(v.toString());
    }
    doc.setDocumentBoost(value);
  }

  @SuppressWarnings("unchecked")
  private void addFields(DataConfig.Entity entity, SolrWriter.SolrDoc doc,
                         Map<String, Object> arow) {
    DataConfig.Entity parentMost = entity;
    while (parentMost.parentEntity != null)
      parentMost = parentMost.parentEntity;
    for (DataConfig.Field field : entity.fields) {
      addFieldValue(field, arow, null, doc);
    }
    if (parentMost.implicitFields != null) {
      Map<String, Object> lowerCaseMap = new HashMap<String, Object>();
      for (Map.Entry<String, Object> entry : arow.entrySet())
        lowerCaseMap.put(entry.getKey().toLowerCase(), entry.getValue());

      for (DataConfig.Field automaticField : parentMost.implicitFields) {
        addFieldValue(automaticField, arow, lowerCaseMap, doc);
      }
    }
  }

  private void addFieldValue(DataConfig.Field field, Map<String, Object> arow,
                             Map<String, Object> lowerCaseMap, SolrWriter.SolrDoc doc) {
    if (!field.toWrite)
      return;
    Object value = arow.get(field.column);
    if (value == null) {
      if (lowerCaseMap != null) {
        value = lowerCaseMap.get(field.column.toLowerCase());
      }
      if (value == null)
        return;
    }

    if (value instanceof Collection) {
      Collection collection = (Collection) value;
      if (field.multiValued) {
        for (Object o : collection) {
          doc.addField(field.nameOrColName, o, field.boost);
        }
      } else {
        if (doc.getField(field.nameOrColName) == null)
          for (Object o : collection) {
            doc.addField(field.nameOrColName, o, field.boost);
            break;
          }
      }
    } else if (field.multiValued) {
      doc.addField(field.nameOrColName, value, field.boost);
    } else {
      if (doc.getField(field.nameOrColName) == null)
        doc.addField(field.nameOrColName, value, field.boost);
    }
  }

  public static EntityProcessor getEntityProcessor(DataConfig.Entity entity) {
    if (entity.processor != null)
      return entity.processor;
    EntityProcessor entityProcessor;
    if (entity.proc == null) {
      entityProcessor = new SqlEntityProcessor();
    } else {
      try {
        entityProcessor = (EntityProcessor) loadClass(entity.proc)
                .newInstance();
      } catch (Exception e) {
        throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                "Unable to load EntityProcessor implementation for entity:"
                        + entity.name, e);
      }
    }
    return entity.processor = entityProcessor;
  }

  /**
   * <p>
   * Collects unique keys of all Solr documents for whom one or more source
   * tables have been changed since the last indexed time.
   * </p>
   * <p>
   * Note: In our definition, unique key of Solr document is the primary key of
   * the top level entity (unless skipped using docRoot=false) in the Solr
   * document in data-config.xml
   * </p>
   *
   * @return an iterator to the list of keys for which Solr documents should be
   *         updated.
   */
  @SuppressWarnings("unchecked")
  public Set<Map<String, Object>> collectDelta(DataConfig.Entity entity,
                                               DataConfig.Entity parentEntity, VariableResolverImpl resolver,
                                               DataImporter context, Set<Map<String, Object>> deletedRows) {
    if (stop.get())
      return new HashSet();

    Set<Map<String, Object>> myModifiedPks = new HashSet<Map<String, Object>>();

    if (entity.entities != null) {

      for (DataConfig.Entity entity1 : entity.entities) {
        myModifiedPks.addAll(collectDelta(entity1, entity, resolver, context,
                deletedRows));
      }

    }
    // identifying the modified rows for this entities

    Set<Map<String, Object>> deltaSet = new HashSet<Map<String, Object>>();
    resolver.addNamespace(null, (Map) entity.allAttributes);
    EntityProcessor entityProcessor = getEntityProcessor(entity);
    entityProcessor.init(new ContextImpl(entity, resolver, entity.dataSrc,
            Context.FIND_DELTA, requestParameters.requestParams, session, null,
            dataImporter));
    LOG.info("Running ModifiedRowKey() for Entity: " + entity.name);
    int count = 0;
    while (true) {
      Map<String, Object> row = entityProcessor.nextModifiedRowKey();

      if (row == null)
        break;

      deltaSet.add(row);
      count++;
      importStatistics.rowsCount.incrementAndGet();
    }
    LOG.info("Completed ModifiedRowKey for Entity: " + entity.name
            + " rows obtained : " + count);
    count = 0;
    // identifying the deleted rows from this entities
    LOG.info("Running DeletedRowKey() for Entity: " + entity.name);
    Set<Map<String, Object>> deletedSet = new HashSet<Map<String, Object>>();
    while (true) {
      Map<String, Object> row = entityProcessor.nextDeletedRowKey();
      if (row == null)
        break;

      deletedSet.add(row);
      count++;
      importStatistics.rowsCount.incrementAndGet();
    }
    LOG.info("Completed DeletedRowKey for Entity: " + entity.name
            + " rows obtained : " + count);

    myModifiedPks.addAll(deltaSet);
    Set<Map<String, Object>> parentKeyList = new HashSet<Map<String, Object>>();
    if (parentEntity != null && parentEntity.isDocRoot) {
      EntityProcessor parentEntityProcessor = getEntityProcessor(parentEntity);
      parentEntityProcessor.init(new ContextImpl(parentEntity, resolver,
              parentEntity.dataSrc, Context.FIND_DELTA,
              requestParameters.requestParams, session, null, dataImporter));
      // identifying deleted rows with deltas

      for (Map<String, Object> row : myModifiedPks)
        getModifiedParentRows(resolver.addNamespace(entity.name, row),
                entity.name, parentEntityProcessor, parentKeyList);
      // running the same for deletedrows
      for (Map<String, Object> row : deletedSet) {
        getModifiedParentRows(resolver.addNamespace(entity.name, row),
                entity.name, parentEntityProcessor, parentKeyList);
      }
    }
    LOG.info("Completed parentDeltaQuery for Entity: " + entity.name);
    if (entity.isDocRoot)
      deletedRows.addAll(deletedSet);

    return entity.isDocRoot ? myModifiedPks : new HashSet<Map<String, Object>>(
            parentKeyList);
  }

  private void getModifiedParentRows(VariableResolverImpl resolver,
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

  public static void main(String[] args) throws InterruptedException {
    long l = System.currentTimeMillis();
    Thread.sleep(1050);
    System.out.println(getTimeElapsedSince(l));

  }

  static String getTimeElapsedSince(long l) {
    l = System.currentTimeMillis() - l;
    return (l / (60000 * 60)) % 60 + ":" + (l / 60000) % 60 + ":" + (l / 1000)
            % 60 + "." + l % 1000;
  }

  @SuppressWarnings("unchecked")
  static Class loadClass(String name) throws ClassNotFoundException {
    DocBuilder inst = INSTANCE.get();
    try {
      return inst != null ?
              inst.writer.loadClass(name) :
              Class.forName(name);
    } catch (Exception e) {
      try {
        String n = DocBuilder.class.getPackage().getName() + "." + name;
        return inst != null ?
                inst.writer.loadClass(n) :
                Class.forName(n);
      } catch (Exception e1) {
        throw new ClassNotFoundException("Unable to load " + name + " or " + DocBuilder.class.getPackage().getName() + "." + name, e);
      }
    }
  }

  public static class Statistics {
    public AtomicInteger docCount = new AtomicInteger();

    public AtomicInteger deletedDocCount = new AtomicInteger();

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
  }

  public static final String LAST_INDEX_TIME = "last_index_time";
  public static final String INDEX_START_TIME = "index_start_time";
}
