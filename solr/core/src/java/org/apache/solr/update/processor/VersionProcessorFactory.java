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

package org.apache.solr.update.processor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.DocValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.*;
import org.apache.solr.util.RefCounted;
import org.omg.PortableInterceptor.RequestInfo;


/**
 * Pass the command to the UpdateHandler without any modifications
 * 
 * @since solr 1.3
 */
public class VersionProcessorFactory extends UpdateRequestProcessorFactory
{
  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) 
  {
    // TODO: return null if there is not a unique id defined?
    return new VersionProcessor(req, next);
  }
}
// this is a separate class from DistribUpdateProcessor only to facilitate
// working on that in parallel.  Given the dependencies, it will most likely make more sense for it to be merged.
// For example, if not leader, forward to leader.  if leader, determine version, then send to replicas
class VersionProcessor extends UpdateRequestProcessor
{
  private final SolrQueryRequest req;
  private final UpdateHandler updateHandler;
  private final UpdateLog ulog;
  private final VersionInfo vinfo;
  private final boolean versionsStored;
  private final boolean returnVersions = true; // todo: default to false and make configurable
  private final SolrQueryResponse rsp;

  private NamedList addsResponse = null;
  private NamedList deleteResponse = null;
  private final SchemaField idField;
  private CharsRef scratch;

  public VersionProcessor(SolrQueryRequest req, UpdateRequestProcessor next) {
    super( next );
    this.req = req;
    this.updateHandler = req.getCore().getUpdateHandler();
    this.ulog = updateHandler.getUpdateLog();
    this.vinfo = ulog.getVersionInfo();
    versionsStored = this.vinfo != null && this.vinfo.getVersionField() != null;

    // TODO: better way to get the response, or pass back info to it?
    SolrRequestInfo reqInfo = returnVersions ? SolrRequestInfo.getRequestInfo() : null;
    this.rsp = reqInfo != null ? reqInfo.getRsp() : null;
    this.idField = req.getSchema().getUniqueKeyField();
  }

  // TODO: move this to AddUpdateCommand/DeleteUpdateCommand and cache it? And make the hash pluggable of course.
  // The hash also needs to be pluggable
  private int hash(AddUpdateCommand cmd) {
    BytesRef br = cmd.getIndexedId();
    return Hash.murmurhash3_x86_32(br.bytes, br.offset, br.length, 0);
  }
  private int hash(DeleteUpdateCommand cmd) {
    BytesRef br = cmd.getIndexedId();
    return Hash.murmurhash3_x86_32(br.bytes, br.offset, br.length, 0);
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    if (vinfo == null) {
      super.processAdd(cmd);
      return;
    }

    boolean leaderForUpdate = true; // TODO: figure out if I'm the leader
    leaderForUpdate = req.getParams().getBool("leader",true);  // TODO: we need a better indicator of when an update comes from a leader
    boolean needToForward = false;  // TODO: figure out if I need to forward this to the leader

    if (needToForward) {
      // TODO: forward update to the leader
      return;
    }

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find any existing version in the document
    long versionOnUpdate = 0;
    SolrInputField versionField = cmd.getSolrInputDocument().getField(VersionInfo.VERSION_FIELD);
    if (versionField != null) {
      Object o = versionField.getValue();
      versionOnUpdate = o instanceof Number ? ((Number) o).longValue() : Long.parseLong(o.toString());
    } else {
      // TODO: check for the version in the request params (this will be for user provided versions and optimistic concurrency only)
    }



    VersionBucket bucket = vinfo.bucket(hash(cmd));
    synchronized (bucket) {
      // we obtain the version when synchronized and then do the add so we can ensure that
      // if version1 < version2 then version1 is actually added before version2.

      // even if we don't store the version field, synchronizing on the bucket
      // will enable us to know what version happened first, and thus enable
      // realtime-get to work reliably.
      // TODO: if versions aren't stored, do we need to set on the cmd anyway for some reason?
      // there may be other reasons in the future for a version on the commands
      if (versionsStored) {
        long bucketVersion = bucket.highest;

        if (leaderForUpdate) {
          long version = vinfo.getNewClock();
          cmd.setVersion(version);
          cmd.getSolrInputDocument().setField(VersionInfo.VERSION_FIELD, version);
          bucket.updateHighest(version);
        } else {
          // The leader forwarded us this update.
          cmd.setVersion(versionOnUpdate);

          // if we aren't the leader, then we need to check that updates were not re-ordered
          if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
            // we're OK... this update has a version higher than anything we've seen
            // in this bucket so far, so we know that no reordering has yet occured.
            bucket.updateHighest(versionOnUpdate);
          } else {
            // there have been updates higher than the current update.  we need to check
            // the specific version for this id.
            Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
            if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
              // This update is a repeat, or was reordered.  We need to drop this update.
              // TODO: do we need to add anything to the response?
              return;
            }
          }
        }
      }

      super.processAdd(cmd);
    }

    if (returnVersions && rsp != null) {
      if (addsResponse == null) {
        addsResponse = new NamedList<String>();
        rsp.add("adds",addsResponse);
      }
      if (scratch == null) scratch = new CharsRef();
      idField.getType().indexedToReadable(cmd.getIndexedId(), scratch);
      addsResponse.add(scratch.toString(), cmd.getVersion());
    }

    // TODO: keep track of errors?  needs to be done at a higher level though since
    // an id may fail before it gets to this processor.
    // Given that, it may also make sense to move the version reporting out of this
    // processor too.
  }

  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    if (vinfo == null) {
      super.processDelete(cmd);
      return;
    }

    if (cmd.id == null) {
      // delete-by-query
      // TODO: forward to all nodes in distrib mode?  or just don't bother to support?
      super.processDelete(cmd);
      return;
    }

    boolean leaderForUpdate = true; // TODO: figure out if I'm the leader
    leaderForUpdate = req.getParams().getBool("leader",true);  // TODO: we need a better indicator of when an update comes from a leader
    boolean needToForward = false;  // TODO: figure out if I need to forward this to the leader

    if (needToForward) {
      // TODO: forward update to the leader
      return;
    }

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find the version
    String versionOnUpdateS = req.getParams().get("_version_");
    Long versionOnUpdate = versionOnUpdateS == null ? null : Long.parseLong(versionOnUpdateS);

    VersionBucket bucket = vinfo.bucket(hash(cmd));
    synchronized (bucket) {
      if (versionsStored) {
        long bucketVersion = bucket.highest;

        if (leaderForUpdate) {
          long version = vinfo.getNewClock();
          cmd.setVersion(-version);
          bucket.updateHighest(version);
        } else {
          // The leader forwarded us this update.
          cmd.setVersion(versionOnUpdate);
          // if we aren't the leader, then we need to check that updates were not re-ordered
          if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
            // we're OK... this update has a version higher than anything we've seen
            // in this bucket so far, so we know that no reordering has yet occured.
            bucket.updateHighest(versionOnUpdate);
          } else {
            // there have been updates higher than the current update.  we need to check
            // the specific version for this id.
            Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
            if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
              // This update is a repeat, or was reordered.  We need to drop this update.
              // TODO: do we need to add anything to the response?
              return;
            }
          }
        }
      }

      super.processDelete(cmd);
    }

    if (returnVersions && rsp != null) {
      if (deleteResponse == null) {
        deleteResponse = new NamedList<String>();
        rsp.add("deletes",deleteResponse);
      }
      if (scratch == null) scratch = new CharsRef();
      idField.getType().indexedToReadable(cmd.getIndexedId(), scratch);
      deleteResponse.add(scratch.toString(), cmd.getVersion());  // we're returning the version of the delete.. not the version of the doc we deleted.
    }
  }

  @Override
  public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
    super.processMergeIndexes(cmd);
  }

  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException
  {
    super.processCommit(cmd);
  }

  /**
   * @since Solr 1.4
   */
  @Override
  public void processRollback(RollbackUpdateCommand cmd) throws IOException
  {
    super.processRollback(cmd);
  }


}


