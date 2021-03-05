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

package org.apache.solr.update.processor;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.SolrException.ErrorCode.CONFLICT;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocBasedVersionConstraintsProcessor extends UpdateRequestProcessor {
  private static final String[] EMPTY_STR_ARR = new String[0];
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String[] versionFieldNames;
  private final SchemaField[] userVersionFields;
  private final SchemaField solrVersionField;
  private final boolean ignoreOldUpdates;
  private final boolean supportMissingVersionOnOldDocs;
  private final String[] deleteVersionParamNames;
  private final SolrCore core;
  private final NamedList<Object> tombstoneConfig;

  private final DistributedUpdateProcessor distribProc;  // the distributed update processor following us
  private final DistributedUpdateProcessor.DistribPhase phase;
  private final boolean useFieldCache;

  private long oldSolrVersion;  // current _version_ of the doc in the index/update log

  public DocBasedVersionConstraintsProcessor(List<String> versionFields,
                                             boolean ignoreOldUpdates,
                                             List<String> deleteVersionParamNames,
                                             boolean supportMissingVersionOnOldDocs,
                                             boolean useFieldCache,
                                             NamedList<Object> tombstoneConfig,
                                             SolrQueryRequest req,
                                             UpdateRequestProcessor next ) {
    super(next);
    this.ignoreOldUpdates = ignoreOldUpdates;
    this.deleteVersionParamNames = deleteVersionParamNames.toArray(EMPTY_STR_ARR);
    this.supportMissingVersionOnOldDocs = supportMissingVersionOnOldDocs;
    this.core = req.getCore();
    this.versionFieldNames = versionFields.toArray(EMPTY_STR_ARR);
    IndexSchema schema = core.getLatestSchema();
    userVersionFields = new SchemaField[versionFieldNames.length];
    for (int i = 0; i < versionFieldNames.length; i++) {
      userVersionFields[i] = schema.getField(versionFieldNames[i]);
    }
    this.solrVersionField = schema.getField(CommonParams.VERSION_FIELD);
    this.useFieldCache = useFieldCache;

    this.distribProc = getDistributedUpdateProcessor(next);

    this.phase = DistributedUpdateProcessor.DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));
    this.tombstoneConfig = tombstoneConfig;
  }

  private static DistributedUpdateProcessor getDistributedUpdateProcessor(UpdateRequestProcessor next) {
    for (UpdateRequestProcessor proc = next; proc != null; proc = proc.next) {
      if (proc instanceof DistributedUpdateProcessor) {
        return (DistributedUpdateProcessor)proc;
      }
    }

    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "DistributedUpdateProcessor must follow DocBasedVersionConstraintsProcessor");
  }

  /**
   * Inspects a raw field value (which may come from a doc in the index, or a
   * doc in the UpdateLog that still has String values, or a String sent by
   * the user as a param) and if it is a String, asks the versionField FieldType
   * to convert it to an Object suitable for comparison.
   */
  private static Object convertFieldValueUsingType(final Object rawValue, SchemaField field) {
    if (rawValue instanceof CharSequence) {
      // in theory, the FieldType might still be CharSequence based,
      // but in that case trust it to do an identity conversion...
      FieldType fieldType = field.getType();
      BytesRefBuilder term = new BytesRefBuilder();
      fieldType.readableToIndexed((CharSequence)rawValue, term);
      return fieldType.toObject(field, term.get());
    }
    // else...
    return rawValue;
  }

  private Object[] convertFieldValuesUsingType(Object[] rawValues) {
    Object[] returnArr = new Object[rawValues.length];
    for (int i = 0; i < returnArr.length; i++) {
      returnArr[i] = convertFieldValueUsingType(rawValues[i], userVersionFields[i]);
    }
    return returnArr;
  }

  /**
   * Returns true if the specified new version values are greater the the ones
   * already known to exist for the document, or if the document does not already
   * exist.
   * Returns false if the specified new versions are not high enough but the
   * processor has been configured with ignoreOldUpdates=true
   * Throws a SolrException if the version is not high enough and
   * ignoreOldUpdates=false
   */
  private boolean isVersionNewEnough(BytesRef indexedDocId,
                                     Object[] newUserVersions) throws IOException {
    assert null != indexedDocId;
    assert null != newUserVersions;

    newUserVersions = convertFieldValuesUsingType(newUserVersions);

    final DocFoundAndOldUserAndSolrVersions docFoundAndOldUserVersions;
    if (useFieldCache) {
      docFoundAndOldUserVersions = getOldUserVersionsFromFieldCache(indexedDocId);
    } else {
      docFoundAndOldUserVersions = getOldUserVersionsFromStored(indexedDocId);
    }
    oldSolrVersion = docFoundAndOldUserVersions.oldSolrVersion;

    if (!docFoundAndOldUserVersions.found) {
      return true;
    }
    final Object[] oldUserVersions = docFoundAndOldUserVersions.oldUserVersions;

    validateUserVersions(oldUserVersions, versionFieldNames, "Doc exists in index, but has null versionField: ");

    return versionInUpdateIsAcceptable(newUserVersions, oldUserVersions);
  }

  private void validateUserVersions(Object[] userVersions, String[] fieldNames, String errorMessage) {
    assert userVersions.length == fieldNames.length;
    for (int i = 0; i < userVersions.length; i++) {
      Object userVersion = userVersions[i];
      if (supportMissingVersionOnOldDocs && null == userVersion) {
        userVersions[i] = (Comparable<Object>) o -> -1;
      } else if (null == userVersion) {
        // could happen if they turn this feature on after building an index
        // w/o the versionField, or if validating a new doc, not present.
        throw new SolrException(SERVER_ERROR, errorMessage + fieldNames[i]);
      }
    }
  }

  private DocFoundAndOldUserAndSolrVersions getOldUserVersionsFromFieldCache(BytesRef indexedDocId) {
    SolrInputDocument oldDoc = RealTimeGetComponent.getInputDocumentFromTlog(core, indexedDocId, null, null, true);
    if (oldDoc == RealTimeGetComponent.DELETED) {
      return DocFoundAndOldUserAndSolrVersions.NOT_FOUND;
    }
    if (oldDoc == null) {
      // need to look up in index now...
      RefCounted<SolrIndexSearcher> newestSearcher = core.getRealtimeSearcher();
      try {
        SolrIndexSearcher searcher = newestSearcher.get();
        long lookup = searcher.lookupId(indexedDocId);
        if (lookup < 0) {
          // doc not in index either...
          return DocFoundAndOldUserAndSolrVersions.NOT_FOUND;
        }
        final LeafReaderContext segmentContext = searcher.getTopReaderContext().leaves().get((int)(lookup>>32));
        final int docIdInSegment = (int)lookup;

        long oldSolrVersion = getFunctionValues(segmentContext, solrVersionField, searcher).longVal(docIdInSegment);
        Object[] oldUserVersions = getObjectValues(segmentContext, userVersionFields, searcher, docIdInSegment);
        return new DocFoundAndOldUserAndSolrVersions(oldUserVersions, oldSolrVersion);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error reading version from index", e);
      } finally {
        if (newestSearcher != null) { //TODO can this ever be null?
          newestSearcher.decref();
        }
      }
    } else {
      return getUserVersionAndSolrVersionFromDocument(oldDoc);
    }
  }

  private DocFoundAndOldUserAndSolrVersions getOldUserVersionsFromStored(BytesRef indexedDocId) throws IOException {
    // stored fields only...
    SolrInputDocument oldDoc = RealTimeGetComponent.getInputDocument(core, indexedDocId, RealTimeGetComponent.Resolution.DOC);
    if (null == oldDoc) {
      return DocFoundAndOldUserAndSolrVersions.NOT_FOUND;
    } else {
      return getUserVersionAndSolrVersionFromDocument(oldDoc);
    }
  }

  private static final class DocFoundAndOldUserAndSolrVersions {
    private static final DocFoundAndOldUserAndSolrVersions NOT_FOUND = new DocFoundAndOldUserAndSolrVersions();
    private final boolean found;
    private final Object[] oldUserVersions;
    private final long oldSolrVersion;

    private DocFoundAndOldUserAndSolrVersions() {
      this.found = false;
      this.oldSolrVersion = -1;
      this.oldUserVersions = null;
    }

    private DocFoundAndOldUserAndSolrVersions(Object[] oldUserVersions, long oldSolrVersion) {
      this.found = true;
      this.oldUserVersions = oldUserVersions;
      this.oldSolrVersion = oldSolrVersion;
    }
  }

  private DocFoundAndOldUserAndSolrVersions getUserVersionAndSolrVersionFromDocument(SolrInputDocument oldDoc) {
    Object[] oldUserVersions = getUserVersionsFromDocument(oldDoc);

    Object o = oldDoc.getFieldValue(solrVersionField.getName());
    if (o == null) {
      throw new SolrException(SERVER_ERROR, "No _version_ for document " + oldDoc);
    }
    long solrVersion = o instanceof Number ? ((Number) o).longValue() : Long.parseLong(o.toString());

    return new DocFoundAndOldUserAndSolrVersions(oldUserVersions, solrVersion);
  }

  private Object[] getUserVersionsFromDocument(SolrInputDocument doc) {
    Object[] versions = new Object[versionFieldNames.length];
    for (int i = 0; i < versionFieldNames.length; i++) {
      String fieldName = versionFieldNames[i];
      SchemaField schemaField = userVersionFields[i];
      Object userVersion = doc.getFieldValue(fieldName);
      // Make the FieldType resolve any conversion we need.
      userVersion = convertFieldValueUsingType(userVersion, schemaField);
      versions[i] = userVersion;
    }
    return versions;
  }



  /**
   * Returns whether or not the versions in the command are acceptable to be indexed.
   * If the instance is set to ignoreOldUpdates==false, it will throw a SolrException
   * with CONFLICT in the event the version is not acceptable rather than return false.
   *
   * @param newUserVersions New versions in update request
   * @param oldUserVersions Old versions currently in solr index
   * @return True if acceptable, false if not (or will throw exception)
   */
  protected boolean versionInUpdateIsAcceptable(Object[] newUserVersions,
                                                Object[] oldUserVersions) {

    for (int i = 0; i < oldUserVersions.length; i++) {
      Object oldUserVersion = oldUserVersions[i];
      Object newUserVersion = newUserVersions[i];

      if (!(oldUserVersion instanceof Comparable && newUserVersion instanceof Comparable)) {
        throw new SolrException(BAD_REQUEST,
            "old version and new version are not comparable: " +
                oldUserVersion.getClass() + " vs " + newUserVersion.getClass());
      }
      try {
        if (newUpdateComparePasses((Comparable) newUserVersion, (Comparable) oldUserVersion, versionFieldNames[i])) {
          return true;
        }
      } catch (ClassCastException e) {
        throw new SolrException(BAD_REQUEST,
            "old version and new version are not comparable: " +
                oldUserVersion.getClass() + " vs " + newUserVersion.getClass() +
                ": " + e.getMessage(), e);

      }
    }
    if (ignoreOldUpdates) {
      if (log.isDebugEnabled()) {
        log.debug("Dropping update since user version is not high enough: {}; old user version={}",
            Arrays.toString(newUserVersions), Arrays.toString(oldUserVersions));
      }
      return false;
    } else {
      throw new SolrException(CONFLICT,
          "user version is not high enough: " + Arrays.toString(newUserVersions));
    }
  }

  /**
   * Given two comparable user versions, returns whether the new version is acceptable
   * to replace the old version.
   * @param newUserVersion User-specified version on the new version of the document
   * @param oldUserVersion User-specified version on the old version of the document
   * @param userVersionFieldName Field name of the user versions being compared
   * @return True if acceptable, false if not.
   */
  @SuppressWarnings({"unchecked"})
  protected boolean newUpdateComparePasses(@SuppressWarnings({"rawtypes"})Comparable newUserVersion,
                                           @SuppressWarnings({"rawtypes"})Comparable oldUserVersion, String userVersionFieldName) {
    return oldUserVersion.compareTo(newUserVersion) < 0;
  }

  private static Object[] getObjectValues(LeafReaderContext segmentContext,
                                          SchemaField[] fields,
                                          SolrIndexSearcher searcher,
                                          int docIdInSegment) throws IOException {
    FunctionValues[] functionValues = getManyFunctionValues(segmentContext, fields, searcher);
    Object[] objectValues = new Object[functionValues.length];
    for (int i = 0; i < functionValues.length; i++) {
      objectValues[i] = functionValues[i].objectVal(docIdInSegment);
    }
    return objectValues;
  }

  private static FunctionValues[] getManyFunctionValues(LeafReaderContext segmentContext,
                                                SchemaField[] fields,
                                                SolrIndexSearcher searcher) throws IOException {
    FunctionValues[] values = new FunctionValues[fields.length];
    for (int i = 0; i < fields.length; i++) {
      values[i] = getFunctionValues(segmentContext, fields[i], searcher);
    }
    return values;
  }

  @SuppressWarnings({"unchecked"})
  private static FunctionValues getFunctionValues(LeafReaderContext segmentContext,
                                          SchemaField field,
                                          SolrIndexSearcher searcher) throws IOException {
    ValueSource vs = field.getType().getValueSource(field, null);
    @SuppressWarnings({"rawtypes"})
    Map context = ValueSource.newContext(searcher);
    vs.createWeight(context, searcher);
    return vs.getValues(context, segmentContext);
  }

  private boolean isNotLeader(UpdateCommand cmd) {
    if ((cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0) {
      return true;
    }
    if (phase == DistributedUpdateProcessor.DistribPhase.FROMLEADER) {
      return true;
    }
    // if phase==TOLEADER, we can't just assume we are the leader... let the normal logic check.
    distribProc.setupRequest(cmd);
    return !distribProc.isLeader();
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    if (isNotLeader(cmd)) {
      super.processAdd(cmd);
      return;
    }

    final SolrInputDocument newDoc = cmd.getSolrInputDocument();
    Object[] newVersions = getUserVersionsFromDocument(newDoc);
    validateUserVersions(newVersions, versionFieldNames, "Doc does not have versionField: ");

    for (int i=0; ;i++) {
      logOverlyFailedRetries(i, cmd);

      if (!isVersionNewEnough(cmd.getIndexedId(), newVersions)) {
        // drop older update
        return;
      }

      try {
        cmd.setVersion(oldSolrVersion);  // use optimistic concurrency to ensure that the doc has not changed in the meantime
        super.processAdd(cmd);
        return;
      } catch (SolrException e) {
        if (e.code() == 409) {
          continue;  // if a version conflict, retry
        }
        throw e;  // rethrow
      }

    }
  }

  private static void logOverlyFailedRetries(int i, UpdateCommand cmd) {
    // Log a warning every 256 retries.... even a few retries should normally be very unusual.
    if ((i&0xff) == 0xff) {
      log.warn("Unusual number of optimistic concurrency retries: retries={} cmd={}", i, cmd);
    }
  }

  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    if (deleteVersionParamNames.length == 0) {
      // not suppose to look at deletes at all
      super.processDelete(cmd);
      return;
    }

    if ( ! cmd.isDeleteById() ) {
      // nothing to do
      super.processDelete(cmd);
      return;
    }

    String[] deleteParamValues = getDeleteParamValuesFromRequest(cmd);
    validateDeleteParamValues(deleteParamValues);


    if (isNotLeader(cmd)) {
      // transform delete to add earlier rather than later

      SolrInputDocument newDoc = createTombstoneDocument(core.getLatestSchema(), cmd.getId(), versionFieldNames, deleteParamValues, this.tombstoneConfig);

      AddUpdateCommand newCmd = new AddUpdateCommand(cmd.getReq());
      newCmd.solrDoc = newDoc;
      newCmd.commitWithin = cmd.commitWithin;
      super.processAdd(newCmd);
      return;
    }


    for (int i=0; ;i++) {

      logOverlyFailedRetries(i, cmd);

      if (!isVersionNewEnough(cmd.getIndexedId(), deleteParamValues)) {
        // drop this older update
        return;
      }

      // :TODO: should this logic be split and driven by two params?
      //   - deleteVersionParam to do a version check
      //   - some new boolean param to determine if a stub document gets added in place?
      try {
        // drop the delete, and instead propagate an AddDoc that
        // replaces the doc with a new "empty" one that records the deleted version

        SolrInputDocument newDoc = createTombstoneDocument(core.getLatestSchema(), cmd.getId(), versionFieldNames, deleteParamValues, this.tombstoneConfig);

        AddUpdateCommand newCmd = new AddUpdateCommand(cmd.getReq());
        newCmd.solrDoc = newDoc;
        newCmd.commitWithin = cmd.commitWithin;

        newCmd.setVersion(oldSolrVersion);  // use optimistic concurrency to ensure that the doc has not changed in the meantime
        super.processAdd(newCmd);
        return;
      } catch (SolrException e) {
        if (e.code() == 409) {
          continue;  // if a version conflict, retry
        }
        throw e;  // rethrow
      }

    }
  }
  
  /**
   * Creates a tombstone document, that will be used to update a document that's being deleted by ID. The returned document will contain:
   * <ul>
   * <li>uniqueKey</li>
   * <li>versions (All the fields configured as in the {@code versionField} parameter)</li>
   * <li>All the fields set in the {@code tombstoneConfig} parameter</li>
   * </ul>
   * 
   * Note that if the schema contains required fields (other than version fields or uniqueKey), they should be populated by the {@code tombstoneConfig}, 
   * otherwise this tombstone will fail when indexing (and consequently the delete will fail). Alternatively, required fields must be populated by some
   * other means (like {@code DocBasedVersionConstraintsProcessorFactory} or similar) 
   */
  protected SolrInputDocument createTombstoneDocument(IndexSchema schema, String id, String[] versionFieldNames, String[] deleteParamValues, NamedList<Object> tombstoneConfig) {
    final SolrInputDocument newDoc = new SolrInputDocument();
    
    if (tombstoneConfig != null) {
      tombstoneConfig.forEach((k,v) -> newDoc.addField(k, v));
    }
    newDoc.setField(schema.getUniqueKeyField().getName(), id);
    setDeleteParamValues(newDoc, versionFieldNames, deleteParamValues);
    return newDoc;
  }

  private String[] getDeleteParamValuesFromRequest(DeleteUpdateCommand cmd) {
    SolrParams params = cmd.getReq().getParams();
    String[] returnArr = new String[deleteVersionParamNames.length];
    for (int i = 0; i < deleteVersionParamNames.length; i++) {
      String deleteVersionParamName = deleteVersionParamNames[i];
      String deleteParamValue = params.get(deleteVersionParamName);
      returnArr[i] = deleteParamValue;
    }
    return returnArr;
  }

  private void validateDeleteParamValues(String[] values) {
    for (int i = 0; i < values.length; i++) {
      String deleteParamValue = values[i];
      if (null == deleteParamValue) {
        String deleteVersionParamName = deleteVersionParamNames[i];
        throw new SolrException(BAD_REQUEST,
            "Delete by ID must specify doc version param: " +
                deleteVersionParamName);
      }
    }
  }

  private static void setDeleteParamValues(SolrInputDocument doc, String[] versionFieldNames, String[] values) {
    for (int i = 0; i < values.length; i++) {
      String versionFieldName = versionFieldNames[i];
      String value = values[i];
      doc.setField(versionFieldName, value);
    }
  }


}
