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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.lang.invoke.MethodHandles;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.ToleratedUpdateError;
import org.apache.solr.common.ToleratedUpdateError.CmdType;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.SolrCmdDistributor.Error;
import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * <p> 
 * Suppresses errors for individual add/delete commands within a single request.
 * Instead of failing on the first error, at most <code>maxErrors</code> errors (or unlimited 
 * if <code>-1==maxErrors</code>) are logged and recorded the batch continues. 
 * The client will receive a <code>status==200</code> response, which includes a list of errors 
 * that were tolerated.
 * </p>
 * <p>
 * If more then <code>maxErrors</code> occur, the first exception recorded will be re-thrown, 
 * Solr will respond with <code>status==5xx</code> or <code>status==4xx</code> 
 * (depending on the underlying exceptions) and it won't finish processing any more updates in the request. 
 * (ie: subsequent update commands in the request will not be processed even if they are valid).
 * </p>
 * 
 * <p>
 * NOTE: In cloud based collections, this processor expects to <b>NOT</b> be used on {@link DistribPhase#FROMLEADER} 
 * requests (because any successes that occur locally on the leader are considered successes even if there is some 
 * subsequent error on a replica).  {@link TolerantUpdateProcessorFactory} will short circut it away in those 
 * requests.
 * </p>
 * 
 * @see TolerantUpdateProcessorFactory
 */
public class TolerantUpdateProcessor extends UpdateRequestProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  /**
   * String to be used as document key for errors when a real uniqueKey can't be determined
   */
  private static final String UNKNOWN_ID = "(unknown)"; 

  /**
   * Response Header
   */
  private final NamedList<Object> header;
  
  /**
   * Number of errors this UpdateRequestProcessor will tolerate. If more then this occur, 
   * the original exception will be thrown, interrupting the processing of the document
   * batch
   */
  private final int maxErrors;

  /** The uniqueKey field */
  private SchemaField uniqueKeyField;

  private final SolrQueryRequest req;
  private ZkController zkController;

  /**
   * Known errors that occurred in this batch, in order encountered (may not be the same as the 
   * order the commands were originally executed in due to the async distributed updates).
   */
  private final List<ToleratedUpdateError> knownErrors = new ArrayList<ToleratedUpdateError>();

  // Kludge: Because deleteByQuery updates are forwarded to every leader, we can get identical
  // errors reported by every leader for the same underlying problem.
  //
  // It would be nice if we could cleanly handle the unlikely (but possible) situation of an
  // update stream that includes multiple identical DBQs, with identical failures, and 
  // to report each one once, for example...
  //   add: id#1
  //   dbq: foo:bar
  //   add: id#2
  //   add: id#3
  //   dbq: foo:bar
  //
  // ...but i can't figure out a way to accurately identify & return duplicate 
  // ToleratedUpdateErrors from duplicate identical underlying requests w/o erroneously returning identical 
  // ToleratedUpdateErrors for the *same* underlying request but from diff shards.
  //
  // So as a kludge, we keep track of them for deduping against identical remote failures
  //
  private Set<ToleratedUpdateError> knownDBQErrors = new HashSet<>();
        
  private final FirstErrTracker firstErrTracker = new FirstErrTracker();
  private final DistribPhase distribPhase;

  public TolerantUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next, int maxErrors, DistribPhase distribPhase) {
    super(next);
    assert maxErrors >= -1;
      
    header = rsp.getResponseHeader();
    this.maxErrors = ToleratedUpdateError.getEffectiveMaxErrors(maxErrors);
    this.req = req;
    this.distribPhase = distribPhase;
    assert ! DistribPhase.FROMLEADER.equals(distribPhase);
    
    this.zkController = this.req.getCore().getCoreContainer().getZkController();
    this.uniqueKeyField = this.req.getCore().getLatestSchema().getUniqueKeyField();
    assert null != uniqueKeyField : "Factory didn't enforce uniqueKey field?";
  }
  
  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    BytesRef id = null;
    
    try {
      // force AddUpdateCommand to validate+cache the id before proceeding
      id = cmd.getIndexedId();
      
      super.processAdd(cmd);

    } catch (Throwable t) { 
      firstErrTracker.caught(t);
      knownErrors.add(new ToleratedUpdateError
                      (CmdType.ADD,
                       getPrintableId(id),
                       t.getMessage()));
      
      if (knownErrors.size() > maxErrors) {
        firstErrTracker.throwFirst();
      }
    }
  }

  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    
    try {

      super.processDelete(cmd);

    } catch (Throwable t) {
      firstErrTracker.caught(t);

      ToleratedUpdateError err = new ToleratedUpdateError(cmd.isDeleteById() ? CmdType.DELID : CmdType.DELQ,
                                                          cmd.isDeleteById() ? cmd.id : cmd.query,
                                                          t.getMessage());
      knownErrors.add(err);

      // NOTE: we're not using this to dedup before adding to knownErrors.
      // if we're lucky enough to get an immediate local failure (ie: we're a leader, or some other processor
      // failed) then recording the multiple failures is a good thing -- helps us with an accurate fail
      // fast if we exceed maxErrors
      if (CmdType.DELQ.equals(err.getType())) {
        knownDBQErrors.add(err);
      }

      if (knownErrors.size() > maxErrors) {
        firstErrTracker.throwFirst();
      }
    }
  }

  @Override
  public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
    try {
      super.processMergeIndexes(cmd);
    } catch (Throwable t) {
      // we're not tolerant of errors from this type of command, but we
      // do need to track it so we can annotate it with any other errors we were already tolerant of
      firstErrTracker.caught(t);
      throw t;
    }
  }

  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {
    try {
      super.processCommit(cmd);
    } catch (Throwable t) {
      // we're not tolerant of errors from this type of command, but we
      // do need to track it so we can annotate it with any other errors we were already tolerant of
      firstErrTracker.caught(t);
      throw t;
    }
  }

  @Override
  public void processRollback(RollbackUpdateCommand cmd) throws IOException {
    try {
      super.processRollback(cmd);
    } catch (Throwable t) {
      // we're not tolerant of errors from this type of command, but we
      // do need to track it so we can annotate it with any other errors we were already tolerant of
      firstErrTracker.caught(t);
      throw t;
    }
  }

  @Override
  public void finish() throws IOException {

    // even if processAdd threw an error, this.finish() is still called and we might have additional
    // errors from other remote leaders that we need to check for from the finish method of downstream processors
    // (like DUP)

    try {
      super.finish();
    } catch (DistributedUpdateProcessor.DistributedUpdatesAsyncException duae) {
      firstErrTracker.caught(duae);


      // adjust our stats based on each of the distributed errors
      for (Error error : duae.errors) {
        // we can't trust the req info from the Error, because multiple original requests might have been
        // lumped together
        //
        // instead we trust the metadata that the TolerantUpdateProcessor running on the remote node added
        // to the exception when it failed.
        if ( ! (error.e instanceof SolrException) ) {
          log.error("async update exception is not SolrException, no metadata to process", error.e);
          continue;
        }
        SolrException remoteErr = (SolrException) error.e;
        NamedList<String> remoteErrMetadata = remoteErr.getMetadata();

        if (null == remoteErrMetadata) {
          log.warn("remote error has no metadata to aggregate: ", remoteErr);
          continue;
        }

        for (int i = 0; i < remoteErrMetadata.size(); i++) {
          ToleratedUpdateError err =
            ToleratedUpdateError.parseMetadataIfToleratedUpdateError(remoteErrMetadata.getName(i),
                                                                     remoteErrMetadata.getVal(i));
          if (null == err) {
            // some metadata unrelated to this update processor
            continue;
          }

          if (CmdType.DELQ.equals(err.getType())) {
            if (knownDBQErrors.contains(err)) {
              // we've already seen this identical error, probably a dup from another shard
              continue;
            } else {
              knownDBQErrors.add(err);
            }
          }

          knownErrors.add(err);
        }
      }
    }

    header.add("errors", ToleratedUpdateError.formatForResponseHeader(knownErrors));
    // include in response so client knows what effective value was (may have been server side config)
    header.add("maxErrors", ToleratedUpdateError.getUserFriendlyMaxErrors(maxErrors));

    // annotate any error that might be thrown (or was already thrown)
    firstErrTracker.annotate(knownErrors);

    // decide if we have hit a situation where we know an error needs to be thrown.

    if ((DistribPhase.TOLEADER.equals(distribPhase) ? 0 : maxErrors) < knownErrors.size()) {
      // NOTE: even if maxErrors wasn't exceeded, we need to throw an error when we have any errors if we're
      // a leader that was forwarded to by another node so that the forwarding node knows we encountered some
      // problems and can aggregate the results

      firstErrTracker.throwFirst();
    }
  }

  /**
   * Returns the output of {@link org.apache.solr.schema.FieldType#indexedToReadable(BytesRef, CharsRefBuilder)}
   * of the field type of the uniqueKey on the {@link BytesRef} passed as parameter.
   * <code>ref</code> should be the indexed representation of the id -- if null
   * (possibly because it's missing in the update) this method will return {@link #UNKNOWN_ID}
   */
  private String getPrintableId(BytesRef ref) {
    if (ref == null) {
      return UNKNOWN_ID;
    }
    return uniqueKeyField.getType().indexedToReadable(ref, new CharsRefBuilder()).toString();
  }

  /**
   * Simple helper class for "tracking" any exceptions encountered.
   * 
   * Only remembers the "first" exception encountered, and wraps it in a SolrException if needed, so that 
   * it can later be annotated with the metadata our users expect and re-thrown.
   *
   * NOTE: NOT THREAD SAFE
   */
  private static final class FirstErrTracker {

    
    SolrException first = null;
    boolean thrown = false;

    public FirstErrTracker() {
      /* NOOP */
    }

    /** 
     * Call this method immediately anytime an exception is caught from a down stream method -- 
     * even if you are going to ignore it (for now).  If you plan to rethrow the Exception, use 
     * {@link #throwFirst} instead.
     */
    public void caught(Throwable t) {
      assert null != t;
      if (null == first) {
        if (t instanceof SolrException) {
          first = (SolrException)t;
        } else {
          first = new SolrException(ErrorCode.SERVER_ERROR, "Tolerantly Caught Exception: " + t.getMessage(), t);
        }
      }
    }

    /** 
     * Call this method in place of any situation where you would normally (re)throw an exception 
     * (already passed to the {@link #caught} method because maxErrors was exceeded
     * is exceed.
     *
     * This method will keep a record that this update processor has already thrown the exception, and do 
     * nothing on future calls, so subsequent update processor methods can update the metadata but won't 
     * inadvertently re-throw this (or any other) cascading exception by mistake.
     */
    public void throwFirst() throws SolrException {
      assert null != first : "caught was never called?";
      if (! thrown) {
        thrown = true;
        throw first;
      }
    }
    
    /** 
     * Annotates the first exception (which may already have been thrown, or be thrown in the future) with 
     * the metadata from this update processor.  For use in {@link TolerantUpdateProcessor#finish}
     */
    public void annotate(List<ToleratedUpdateError> errors) {

      if (null == first) {
        return; // no exception to annotate
      }

      assert null != errors : "how do we have an exception to annotate w/o any errors?";

      NamedList<String> firstErrMetadata = first.getMetadata();
      if (null == firstErrMetadata) { // obnoxious
        firstErrMetadata = new NamedList<String>();
        first.setMetadata(firstErrMetadata);
      } else {
        // any existing metadata representing ToleratedUpdateErrors in this single exception needs removed
        // so we can add *all* of the known ToleratedUpdateErrors (from this and other exceptions)
        for (int i = 0; i < firstErrMetadata.size(); i++) {
          if (null != ToleratedUpdateError.parseMetadataIfToleratedUpdateError
              (firstErrMetadata.getName(i), firstErrMetadata.getVal(i))) {
               
            firstErrMetadata.remove(i);
            // NOTE: post decrementing index so we don't miss anything as we remove items
            i--;
          }
        }
      }

      for (ToleratedUpdateError te : errors) {
        firstErrMetadata.add(te.getMetadataKey(), te.getMetadataValue());
      }
    }
    
    
    /** The first exception that was thrown (or may be thrown) whose metadata can be annotated. */
    public SolrException getFirst() {
      return first;
    }
  }
}
