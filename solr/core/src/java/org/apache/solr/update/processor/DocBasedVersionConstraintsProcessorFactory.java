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

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.update.VersionInfo;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.SolrException.ErrorCode.CONFLICT;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

/**
 * <p>
 * This Factory generates an UpdateProcessor that helps to enforce Version 
 * constraints on documents based on per-document version numbers using a configured 
 * name of a <code>versionField</code>.  It should be configured on the "default"
 * update processor somewhere before the DistributedUpdateProcessorFactory.
 * As an example, see the solrconfig.xml that the tests use:
 * solr/core/src/test-files/solr/collection1/conf/solrconfig-externalversionconstraint.xml
 * </p>
 * <p>
 * When documents are added through this processor, if a document with the same 
 * unique key already exists in the collection, then the value of the 
 * <code>versionField</code> in the <i>existing</i> document is not less then the 
 * field value in the <i>new</i> document then the new document is rejected with a 
 * 409 Version Conflict error.
 * </p>
 * <p>
 * In addition to the mandatory <code>versionField</code> init param, two additional 
 * optional init params affect the behavior of this factory:
 * </p>
 * <ul>
 *   <li><code>deleteVersionParam</code> - This string parameter controls whether this 
 *     processor will intercept and inspect Delete By Id commands in addition to adding 
 *     documents.  If specified, then the value will specify the name of a request 
 *     paramater which becomes  mandatory for all Delete By Id commands.  This param 
 *     must then be used to specify the document version associated with the delete.
 *     If the version specified using this param is not greater then the value in the 
 *     <code>versionField</code> for any existing document, then the delete will fail 
 *     with a 409 Version Conflict error.  When using this param, Any Delete By Id 
 *     command with a high enough document version number to succeed will be internally 
 *     converted into an Add Document command that replaces the existing document with 
 *     a new one which is empty except for the Unique Key and <code>versionField</code> 
 *     to keeping a record of the deleted version so future Add Document commands will 
 *     fail if their "new" version is not high enough.</li>
 *
 *   <li><code>ignoreOldUpdates</code> - This boolean parameter defaults to 
 *     <code>false</code>, but if set to <code>true</code> causes any update with a 
 *     document version that is not great enough to be silently ignored (and return 
 *     a status 200 to the client) instead of generating a 409 Version Conflict error.
 *   </li>
 * </ul>
 */
public class DocBasedVersionConstraintsProcessorFactory extends UpdateRequestProcessorFactory implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways {
  public final static Logger log = LoggerFactory.getLogger(DocBasedVersionConstraintsProcessorFactory.class);

  private boolean ignoreOldUpdates = false;
  private String versionField = null;
  private String deleteVersionParamName = null;
  private boolean useFieldCache;

  @Override
  public void init( NamedList args )  {

    Object tmp = args.remove("versionField");
    if (null == tmp) {
      throw new SolrException(SERVER_ERROR, 
                              "'versionField' must be configured");
    }
    if (! (tmp instanceof String) ) {
      throw new SolrException(SERVER_ERROR, 
                              "'versionField' must be configured as a <str>");
    }
    versionField = tmp.toString();

    // optional
    tmp = args.remove("deleteVersionParam");
    if (null != tmp) {
      if (! (tmp instanceof String) ) {
        throw new SolrException(SERVER_ERROR, 
                                "'deleteVersionParam' must be configured as a <str>");
      }
      deleteVersionParamName = tmp.toString();
    }

    // optional - defaults to false
    tmp = args.remove("ignoreOldUpdates");
    if (null != tmp) {
      if (! (tmp instanceof Boolean) ) {
        throw new SolrException(SERVER_ERROR, 
                                "'ignoreOldUpdates' must be configured as a <bool>");
      }
      ignoreOldUpdates = ((Boolean)tmp).booleanValue();
    }
    super.init(args);
  }
  

  public UpdateRequestProcessor getInstance(SolrQueryRequest req, 
                                            SolrQueryResponse rsp, 
                                            UpdateRequestProcessor next ) {
    return new DocBasedVersionConstraintsProcessor(versionField, 
                                                   ignoreOldUpdates,
                                                   deleteVersionParamName,
                                                   useFieldCache,
                                                   req, rsp, next);
  }

  @Override
  public void inform(SolrCore core) {

    if (core.getUpdateHandler().getUpdateLog() == null) {
      throw new SolrException(SERVER_ERROR,
          "updateLog must be enabled.");
    }

    if (core.getLatestSchema().getUniqueKeyField() == null) {
      throw new SolrException(SERVER_ERROR,
          "schema must have uniqueKey defined.");
    }

    SchemaField userVersionField = core.getLatestSchema().getField(versionField);
    if (userVersionField == null || !userVersionField.stored() || userVersionField.multiValued()) {
      throw new SolrException(SERVER_ERROR,
          "field " + versionField + " must be defined in schema, be stored, and be single valued.");
    }

    try {
      ValueSource vs = userVersionField.getType().getValueSource(userVersionField, null);
      useFieldCache = true;
    } catch (Exception e) {
      log.warn("Can't use fieldcache/valuesource: " + e.getMessage());
    }
  }



  private static class DocBasedVersionConstraintsProcessor
    extends UpdateRequestProcessor {

    private final String versionFieldName;
    private final SchemaField userVersionField;
    private final SchemaField solrVersionField;
    private final boolean ignoreOldUpdates;
    private final String deleteVersionParamName;
    private final SolrCore core;

    private long oldSolrVersion;  // current _version_ of the doc in the index/update log
    private DistributedUpdateProcessor distribProc;  // the distributed update processor following us
    private DistributedUpdateProcessor.DistribPhase phase;
    private boolean useFieldCache;

    public DocBasedVersionConstraintsProcessor(String versionField,
                                               boolean ignoreOldUpdates,
                                               String deleteVersionParamName,
                                               boolean useFieldCache,
                                               SolrQueryRequest req, 
                                               SolrQueryResponse rsp, 
                                               UpdateRequestProcessor next ) {
      super(next);
      this.ignoreOldUpdates = ignoreOldUpdates;
      this.deleteVersionParamName = deleteVersionParamName;
      this.core = req.getCore();
      this.versionFieldName = versionField;
      this.userVersionField = core.getLatestSchema().getField(versionField);
      this.solrVersionField = core.getLatestSchema().getField(VersionInfo.VERSION_FIELD);
      this.useFieldCache = useFieldCache;

      for (UpdateRequestProcessor proc = next ;proc != null; proc = proc.next) {
        if (proc instanceof DistributedUpdateProcessor) {
          distribProc = (DistributedUpdateProcessor)proc;
          break;
        }
      }

      if (distribProc == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "DistributedUpdateProcessor must follow DocBasedVersionConstraintsProcessor");
      }

      phase = DistributedUpdateProcessor.DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));

    }
    
    /**
     * Inspects a raw field value (which may come from a doc in the index, or a 
     * doc in the UpdateLog that still has String values, or a String sent by 
     * the user as a param) and if it is a String, asks the versionField FieldType 
     * to convert it to an Object suitable for comparison.
     */
    private Object convertFieldValueUsingType(SchemaField sf, final Object rawValue) {
      if (rawValue instanceof CharSequence) {
        // in theory, the FieldType might still be CharSequence based,
        // but in that case trust it to do an identiy conversion...
        FieldType fieldType = userVersionField.getType();
        BytesRef term = new BytesRef();
        fieldType.readableToIndexed((CharSequence)rawValue, term);
        return fieldType.toObject(userVersionField, term);
      }
      // else...
      return rawValue;
    }


    /**
     * Returns true if the specified new version value is greater the the one
     * already known to exist for the document, or the document does not already
     * exist.
     * Returns false if the specified new version is not high enough but the
     * processor has been configured with ignoreOldUpdates=true
     * Throws a SolrException if the version is not high enough and
     * ignoreOldUpdates=false
     */
    private boolean isVersionNewEnough(BytesRef indexedDocId,
                                       Object newUserVersion) throws IOException {
      assert null != indexedDocId;
      assert null != newUserVersion;

      oldSolrVersion = -1;
      // log.info("!!!!!!!!! isVersionNewEnough being called for " + indexedDocId.utf8ToString() + " newVersion=" + newUserVersion);
      newUserVersion = convertFieldValueUsingType(userVersionField, newUserVersion);
      Object oldUserVersion = null;
      SolrInputDocument oldDoc = null;

      if (useFieldCache) {
        oldDoc = RealTimeGetComponent.getInputDocumentFromTlog(core, indexedDocId);
        if (oldDoc == RealTimeGetComponent.DELETED) {
          return true;
        }
        if (oldDoc == null) {
          // need to look up in index now...
          RefCounted<SolrIndexSearcher> newestSearcher = core.getRealtimeSearcher();
          try {
            SolrIndexSearcher searcher = newestSearcher.get();
            long lookup = searcher.lookupId(indexedDocId);
            if (lookup < 0) {
              // doc not in index either...
              return true;
            }

            ValueSource vs = solrVersionField.getType().getValueSource(solrVersionField, null);
            Map context = ValueSource.newContext(searcher);
            vs.createWeight(context, searcher);
            FunctionValues fv = vs.getValues(context, searcher.getTopReaderContext().leaves().get((int)(lookup>>32)));
            oldSolrVersion = fv.longVal((int)lookup);

            vs = userVersionField.getType().getValueSource(userVersionField, null);
            context = ValueSource.newContext(searcher);
            vs.createWeight(context, searcher);
            fv = vs.getValues(context, searcher.getTopReaderContext().leaves().get((int)(lookup>>32)));
            oldUserVersion = fv.objectVal((int)lookup);

          } catch (IOException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error reading version from index", e);
          } finally {
            if (newestSearcher != null) {
              newestSearcher.decref();
            }
          }
        }
      } else {
        // stored fields only...

        oldDoc = RealTimeGetComponent.getInputDocument(core, indexedDocId);

        if (null == oldDoc) {
          // log.info("VERSION no doc found, returning true");
          return true;
        }
      }


      if (oldDoc != null) {
        oldUserVersion = oldDoc.getFieldValue(versionFieldName);
        // Make the FieldType resolve any conversion we need.
        oldUserVersion = convertFieldValueUsingType(userVersionField, oldUserVersion);

        Object o = oldDoc.getFieldValue(solrVersionField.getName());
        if (o == null) {
          throw new SolrException(SERVER_ERROR, "No _version_ for document "+ oldDoc);
        }
        oldSolrVersion = o instanceof Number ? ((Number) o).longValue() : Long.parseLong(o.toString());
      }

      // log.info("VERSION old=" + oldUserVersion + " new=" +newUserVersion );

      if ( null == oldUserVersion) {
        // could happen if they turn this feature on after building an index
        // w/o the versionField
        throw new SolrException(SERVER_ERROR,
            "Doc exists in index, but has null versionField: "
                + versionFieldName);
      }


      if (! (oldUserVersion instanceof Comparable && newUserVersion instanceof Comparable) ) {
        throw new SolrException(BAD_REQUEST,
            "old version and new version are not comparable: " +
                oldUserVersion.getClass()+" vs "+newUserVersion.getClass());
      }

      try {
        if (0 < ((Comparable)newUserVersion).compareTo((Comparable) oldUserVersion)) {
          // log.info("VERSION returning true (proceed with update)" );
          return true;
        }
        if (ignoreOldUpdates) {
          if (log.isDebugEnabled()) {
            log.debug("Dropping update since user version is not high enough: " + newUserVersion + "; old user version=" + oldUserVersion);
          }
          // log.info("VERSION returning false (dropping update)" );
          return false;
        } else {
          // log.info("VERSION will throw conflict" );
          throw new SolrException(CONFLICT,
              "user version is not high enough: " + newUserVersion);
        }
      } catch (ClassCastException e) {
        throw new SolrException(BAD_REQUEST,
            "old version and new version are not comparable: " +
                oldUserVersion.getClass()+" vs "+newUserVersion.getClass() +
                ": " + e.getMessage(), e);

      }
    }



    public boolean isLeader(UpdateCommand cmd) {
      if ((cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0) {
        return false;
      }
      if (phase == DistributedUpdateProcessor.DistribPhase.FROMLEADER) {
        return false;
      }
      // if phase==TOLEADER, we can't just assume we are the leader... let the normal logic check.
      boolean x = distribProc.isLeader(cmd);
      // log.info("VERSION: checking if we are leader:" + x);
      return x;
    }

    public void processAdd(AddUpdateCommand cmd) throws IOException {
      if (!isLeader(cmd)) {
        super.processAdd(cmd);
        return;
      }

      final SolrInputDocument newDoc = cmd.getSolrInputDocument();

      Object newVersion = newDoc.getFieldValue(versionFieldName);
      if ( null == newVersion ) {
        throw new SolrException(BAD_REQUEST, "Doc does not have versionField: " + versionFieldName);
      }

      for (int i=0; ;i++) {
        // Log a warning every 256 retries.... even a few retries should normally be very unusual.
        if ((i&0xff) == 0xff) {
          log.warn("Unusual number of optimistic concurrency retries: retries=" + i + " cmd=" + cmd);
        }

        if (!isVersionNewEnough(cmd.getIndexedId(), newVersion)) {
          // drop older update
          return;
        }

        try {
          cmd.setVersion(oldSolrVersion);  // use optimistic concurrency to ensure that the doc has not changed in the meantime
          super.processAdd(cmd);
          return;
        } catch (SolrException e) {
          if (e.code() == 409) {
            // log.info ("##################### CONFLICT ADDING newDoc=" + newDoc + " newVersion=" + newVersion );
            continue;  // if a version conflict, retry
          }
          throw e;  // rethrow
        }

      }
    }

    public void processDelete(DeleteUpdateCommand cmd) throws IOException {
      if (null == deleteVersionParamName) {
        // not suppose to look at deletes at all
        super.processDelete(cmd);
        return;
      }

      if ( ! cmd.isDeleteById() ) {
        // nothing to do
        super.processDelete(cmd);
        return;
      }

      String deleteParamValue = cmd.getReq().getParams().get(deleteVersionParamName);
      if (null == deleteParamValue) {
        throw new SolrException(BAD_REQUEST,
            "Delete by ID must specify doc version param: " +
                deleteVersionParamName);
      }


      if (!isLeader(cmd)) {
        // transform delete to add earlier rather than later

        SolrInputDocument newDoc = new SolrInputDocument();
        newDoc.setField(core.getLatestSchema().getUniqueKeyField().getName(),
            cmd.getId());
        newDoc.setField(versionFieldName, deleteParamValue);

        AddUpdateCommand newCmd = new AddUpdateCommand(cmd.getReq());
        newCmd.solrDoc = newDoc;
        newCmd.commitWithin = cmd.commitWithin;
        super.processAdd(newCmd);
        return;
      }


      for (int i=0; ;i++) {
        // Log a warning every 256 retries.... even a few retries should normally be very unusual.
        if ((i&0xff) == 0xff) {
          log.warn("Unusual number of optimistic concurrency retries: retries=" + i + " cmd=" + cmd);
        }

        if (!isVersionNewEnough(cmd.getIndexedId(), deleteParamValue)) {
          // drop this older update
          return;
        }

        // :TODO: should this logic be split and driven by two params?
        //   - deleteVersionParam to do a version check
        //   - some new boolean param to determine if a stub document gets added in place?
        try {
          // drop the delete, and instead propogate an AddDoc that
          // replaces the doc with a new "empty" one that records the deleted version

          SolrInputDocument newDoc = new SolrInputDocument();
          newDoc.setField(core.getLatestSchema().getUniqueKeyField().getName(),
              cmd.getId());
          newDoc.setField(versionFieldName, deleteParamValue);

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

  } // end inner class
  
}
