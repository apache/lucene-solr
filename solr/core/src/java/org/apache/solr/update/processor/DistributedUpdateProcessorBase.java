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

import java.lang.invoke.MethodHandles;
import java.util.List;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.SolrCmdDistributor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

public class DistributedUpdateProcessorBase extends UpdateRequestProcessor {

  final static String PARAM_WHITELIST_CTX_KEY = DistributedUpdateProcessor.class + "PARAM_WHITELIST_CTX_KEY";
  public static final String DISTRIB_FROM_SHARD = "distrib.from.shard";
  public static final String DISTRIB_FROM_COLLECTION = "distrib.from.collection";
  public static final String DISTRIB_FROM_PARENT = "distrib.from.parent";
  public static final String DISTRIB_FROM = "distrib.from";
  public static final String DISTRIB_INPLACE_PREVVERSION = "distrib.inplace.prevversion";
  protected static final String TEST_DISTRIB_SKIP_SERVERS = "test.distrib.skip.servers";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Request forwarded to a leader of a different shard will be retried up to this amount of times by default
   */
  static final int MAX_RETRIES_ON_FORWARD_DEAULT = Integer.getInteger("solr.retries.on.forward",  25);

  /**
   * Requests from leader to it's followers will be retried this amount of times by default
   */
  static final int MAX_RETRIES_TO_FOLLOWERS_DEFAULT = Integer.getInteger("solr.retries.to.followers", 3);

  /** Helper method for constructor */
  static String buildMsg(List<SolrCmdDistributor.Error> errors) {
    assert null != errors;
    assert 0 < errors.size();

    if (1 == errors.size()) {
      return "Async exception during distributed update: " + errors.get(0).e.getMessage();
    } else {
      StringBuilder buf = new StringBuilder(errors.size() + " Async exceptions during distributed update: ");
      for (SolrCmdDistributor.Error error : errors) {
        buf.append("\n");
        buf.append(error.e.getMessage());
      }
      return buf.toString();
    }
  }

  /**
   * Values this processor supports for the <code>DISTRIB_UPDATE_PARAM</code>.
   * This is an implementation detail exposed solely for tests.
   *
   * @see DistributingUpdateProcessorFactory#DISTRIB_UPDATE_PARAM
   */
  public static enum DistribPhase {
    NONE, TOLEADER, FROMLEADER;

    public static DistributedUpdateProcessor.DistribPhase parseParam(final String param) {
      if (param == null || param.trim().isEmpty()) {
        return NONE;
      }
      try {
        return valueOf(param);
      } catch (IllegalArgumentException e) {
        throw new SolrException
            (SolrException.ErrorCode.BAD_REQUEST, "Illegal value for " +
                DISTRIB_UPDATE_PARAM + ": " + param, e);
      }
    }
  }

  public static final String COMMIT_END_POINT = "commit_end_point";
  public static final String LOG_REPLAY = "log_replay";

  // TODO: move common DistributedUpdateProcessor logic to this class.

  public DistributedUpdateProcessorBase(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    this(req, rsp, new AtomicUpdateDocumentMerger(req), next);
  }

  /** Specification of AtomicUpdateDocumentMerger is currently experimental.
   * @lucene.experimental
   */
  public DistributedUpdateProcessorBase(SolrQueryRequest req,
                                        SolrQueryResponse rsp, AtomicUpdateDocumentMerger docMerger, UpdateRequestProcessor next) {
    super(next);
  }

  public static final class DistributedUpdatesAsyncException extends SolrException {
    public final List<SolrCmdDistributor.Error> errors;
    public DistributedUpdatesAsyncException(List<SolrCmdDistributor.Error> errors) {
      super(buildCode(errors), buildMsg(errors), null);
      this.errors = errors;

      // create a merged copy of the metadata from all wrapped exceptions
      NamedList<String> metadata = new NamedList<String>();
      for (SolrCmdDistributor.Error error : errors) {
        if (error.e instanceof SolrException) {
          SolrException e = (SolrException) error.e;
          NamedList<String> eMeta = e.getMetadata();
          if (null != eMeta) {
            metadata.addAll(eMeta);
          }
        }
      }
      if (0 < metadata.size()) {
        this.setMetadata(metadata);
      }
    }

    /** Helper method for constructor */
    private static int buildCode(List<SolrCmdDistributor.Error> errors) {
      assert null != errors;
      assert 0 < errors.size();

      int minCode = Integer.MAX_VALUE;
      int maxCode = Integer.MIN_VALUE;
      for (SolrCmdDistributor.Error error : errors) {
        log.trace("REMOTE ERROR: {}", error);
        minCode = Math.min(error.statusCode, minCode);
        maxCode = Math.max(error.statusCode, maxCode);
      }
      if (minCode == maxCode) {
        // all codes are consistent, use that...
        return minCode;
      } else if (400 <= minCode && maxCode < 500) {
        // all codes are 4xx, use 400
        return ErrorCode.BAD_REQUEST.code;
      }
      // ...otherwise use sensible default
      return ErrorCode.SERVER_ERROR.code;
    }

  }

  //    Keeps track of the replication factor achieved for a distributed update request
  //    originated in this distributed update processor. A RollupReplicationTracker is the only tracker that will
  //    persist across sub-requests.
  //
  //   Note that the replica that receives the original request has the only RollupReplicationTracker that exists for the
  //   lifetime of the batch. The leader for each shard keeps track of its own achieved replication for its shard
  //   and attaches that to the response to the originating node (i.e. the one with the RollupReplicationTracker).
  //   Followers in general do not need a tracker of any sort with the sole exception of the RollupReplicationTracker
  //   allocated on the original node that receives the top-level request.
  //
  //   DeleteById is tricky. Since the docs are sent one at a time, there has to be some fancy dancing. In the
  //   deleteById case, here are the rules:
  //
  //   If I'm leader, there are two possibilities:
  //     1> I got the original request. This is the hard one. There are two sub-cases:
  //     a> Some document in the request is deleted from the shard I lead. In this case my computed replication
  //        factor counts.
  //     b> No document in the packet is deleted from my shard. In that case I have nothing to say about the
  //        achieved replication factor.
  //
  //     2> I'm a leader and I got the request from some other replica. In this case I can be certain of a couple of things:
  //       a> The document in the request will be deleted from my shard
  //       b> my replication factor counts.
  //
  //   Even the DeleteById case follows the rules for whether a RollupReplicaitonTracker is allocated.
  //   This doesn't matter when it comes to delete-by-query since all leaders get the sub-request.


  public static class RollupRequestReplicationTracker {

    private int achievedRf = Integer.MAX_VALUE;

    public int getAchievedRf() {
      return achievedRf;
    }

    // We want to report only the minimun _ever_ achieved...
    public void testAndSetAchievedRf(int rf) {
      this.achievedRf = Math.min(this.achievedRf, rf);
    }

    public String toString() {
      StringBuilder sb = new StringBuilder("RollupRequestReplicationTracker")
          .append(" achievedRf: ")
          .append(achievedRf);
      return sb.toString();
    }
  }


  // Allocate a LeaderRequestReplicatinTracker if (and only if) we're a leader. If the request comes in to the leader
  // at first, allocate both one of these and a RollupRequestReplicationTracker.
  //
  // Since these are leader-only, all they really have to do is track the individual update request for this shard
  // and return it to be added to the rollup tracker. Which is kind of simple since we get an onSuccess method in
  // SolrCmdDistributor

  public static class LeaderRequestReplicationTracker {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // Since we only allocate one of these on the leader and, by definition, the leader has been found and is running,
    // we have a replication factor of one by default.
    private int achievedRf = 1;

    private final String myShardId;

    public LeaderRequestReplicationTracker(String shardId) {
      this.myShardId = shardId;
    }

    // gives the replication factor that was achieved for this request
    public int getAchievedRf() {
      return achievedRf;
    }

    public void trackRequestResult(SolrCmdDistributor.Node node, boolean success) {
      if (log.isDebugEnabled()) {
        log.debug("trackRequestResult({}): success? {}, shardId={}", node, success, myShardId);
      }

      if (success) {
        ++achievedRf;
      }
    }

    public String toString() {
      StringBuilder sb = new StringBuilder("LeaderRequestReplicationTracker");
      sb.append(", achievedRf=")
          .append(getAchievedRf())
          .append(" for shard ")
          .append(myShardId);
      return sb.toString();
    }
  }

}
