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
import java.lang.invoke.MethodHandles;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

/**
 * <p>
 * Update Processor Factory for managing automatic "expiration" of documents.
 * </p>
 * 
 * <p>
 * The <code>DocExpirationUpdateProcessorFactory</code> provides two features related 
 * to the "expiration" of documents which can be used individually, or in combination:
 * </p>
 * <ol>
 *  <li>Computing expiration field values for documents from a "time to live" (TTL)</li>
 *  <li>Periodically delete documents from the index based on an expiration field</li>
 * </ol>
 * 
 * <p>
 * Documents with expiration field values computed from a TTL can be be excluded from 
 * searchers using simple date based filters relative to <code>NOW</code>, or completely 
 * removed from the index using the periodic delete function of this factory.  Alternatively, 
 * the periodic delete function of this factory can be used to remove any document with an 
 * expiration value - even if that expiration was explicitly set with-out leveraging the TTL 
 * feature of this factory.
 * </p>
 *
 * <p>
 * The following configuration options are supported:
 * </p>
 *
 * <ul>
 *  <li><code>expirationFieldName</code> - The name of the expiration field to use 
 *      in any operations (mandatory).
 *  </li>
 *  <li><code>ttlFieldName</code> - Name of a field this process should look 
 *      for in each document processed, defaulting to <code>_ttl_</code>.  
 *      If the specified field name exists in a document, the document field value 
 *      will be parsed as a {@linkplain DateMathParser Date Math Expression} relative to 
 *      <code>NOW</code> and the result will be added to the document using the 
 *      <code>expirationFieldName</code>.  Use <code>&lt;null name="ttlFieldName"/&gt;</code>
 *      to disable this feature.
 *  </li>
 *  <li><code>ttlParamName</code> - Name of an update request param this process should 
 *      look for in each request when processing document additions, defaulting to  
 *      <code>_ttl_</code>. If the the specified param name exists in an update request, 
 *      the param value will be parsed as a {@linkplain DateMathParser Date Math Expression}
 *      relative to <code>NOW</code> and the result will be used as a default for any 
 *      document included in that request that does not already have a value in the 
 *      field specified by <code>ttlFieldName</code>.  Use 
 *      <code>&lt;null name="ttlParamName"/&gt;</code> to disable this feature.
 *  </li>
 *  <li><code>autoDeletePeriodSeconds</code> - Optional numeric value indicating how 
 *      often this factory should trigger a delete to remove documents.  If this option is 
 *      used, and specifies a non-negative numeric value, a background thread will be 
 *      created that will execute recurring <code>deleteByQuery</code> commands using the 
 *      specified period.  The delete query will remove all documents with an 
 *      <code>expirationFieldName</code> up to <code>NOW</code>.
 *  </li>
 *  <li><code>autoDeleteChainName</code> - Optional name of an 
 *      <code>updateRequestProcessorChain</code> to use when executing automatic deletes.  
 *      If not specified, or <code>&lt;null/&gt;</code>, the default 
 *      <code>updateRequestProcessorChain</code> for this collection is used.  
 *      This option is ignored unless <code>autoDeletePeriodSeconds</code> is configured 
 *      and is non-negative.
 *  </li>
 * </ul>
 *
 * <p>
 * For example: The configuration below will cause any document with a field named 
 * <code>_ttl_</code> to have a Date field named <code>_expire_at_</code> computed 
 * for it when added -- but no automatic deletion will happen.
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.processor.DocExpirationUpdateProcessorFactory"&gt;
 *   &lt;str name="expirationFieldName"&gt;_expire_at_&lt;/str&gt;
 * &lt;/processor&gt;</pre>
 *
 * <p>
 * Alternatively, in this configuration deletes will occur automatically against the 
 * <code>_expire_at_</code> field every 5 minutes - but this processor will not 
 * automatically populate the <code>_expire_at_</code> using any sort of TTL expression.  
 * Only documents that were added with an explicit <code>_expire_at_</code> field value 
 * will ever be deleted.
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.processor.DocExpirationUpdateProcessorFactory"&gt;
 *   &lt;null name="ttlFieldName"/&gt;
 *   &lt;null name="ttlParamName"/&gt;
 *   &lt;int name="autoDeletePeriodSeconds"&gt;300&lt;/int&gt;
 *   &lt;str name="expirationFieldName"&gt;_expire_at_&lt;/str&gt;
 * &lt;/processor&gt;</pre>
 *
 * <p>
 * This last example shows the combination of both features using a custom 
 * <code>ttlFieldName</code>: Documents with a <code>my_ttl</code> field will 
 * have an <code>_expire_at_</code> field computed, and deletes will be triggered 
 * every 5 minutes to remove documents whose 
 * <code>_expire_at_</code> field value is in the past.
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.processor.DocExpirationUpdateProcessorFactory"&gt;
 *   &lt;int name="autoDeletePeriodSeconds"&gt;300&lt;/int&gt;
 *   &lt;str name="ttlFieldName"&gt;my_ttl&lt;/str&gt;
 *   &lt;null name="ttlParamName"/&gt;
 *   &lt;str name="expirationFieldName"&gt;_expire_at_&lt;/str&gt;
 * &lt;/processor&gt;</pre> 
 * @since 4.8.0
 */
public final class DocExpirationUpdateProcessorFactory 
  extends UpdateRequestProcessorFactory 
  implements SolrCoreAware {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String DEF_TTL_KEY = "_ttl_";
  private static final String EXP_FIELD_NAME_CONF = "expirationFieldName";
  private static final String TTL_FIELD_NAME_CONF = "ttlFieldName";
  private static final String TTL_PARAM_NAME_CONF = "ttlParamName";
  private static final String DEL_CHAIN_NAME_CONF = "autoDeleteChainName";
  private static final String DEL_PERIOD_SEC_CONF = "autoDeletePeriodSeconds";
  
  private SolrCore core;
  private ScheduledThreadPoolExecutor executor;

  private String expireField = null;
  private String ttlField = null;
  private String ttlParam = null;

  private String deleteChainName = null;
  private long deletePeriodSeconds = -1L;

  private SolrException confErr(final String msg) {
    return confErr(msg, null);
  }
  private SolrException confErr(final String msg, SolrException root) {
    return new SolrException(SERVER_ERROR, this.getClass().getSimpleName()+": "+msg, root);
  }
  private String removeArgStr(@SuppressWarnings({"rawtypes"})final NamedList args,
                              final String arg, final String def,
                              final String errMsg) {

    if (args.indexOf(arg,0) < 0) return def;

    Object tmp = args.remove(arg);
    if (null == tmp) return null;

    if (tmp instanceof String) return tmp.toString();

    throw confErr(arg + " " + errMsg);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {

    deleteChainName = removeArgStr(args, DEL_CHAIN_NAME_CONF, null,
                                   "must be a <str> or <null/> for default chain");

    ttlField = removeArgStr(args, TTL_FIELD_NAME_CONF, DEF_TTL_KEY, 
                            "must be a <str> or <null/> to disable");
    ttlParam = removeArgStr(args, TTL_PARAM_NAME_CONF, DEF_TTL_KEY, 
                            "must be a <str> or <null/> to disable");

    expireField = removeArgStr(args, EXP_FIELD_NAME_CONF, null, "must be a <str>");
    if (null == expireField) {
      throw confErr(EXP_FIELD_NAME_CONF + " must be configured");
    }

    Object tmp = args.remove(DEL_PERIOD_SEC_CONF);
    if (null != tmp) {
      if (! (tmp instanceof Number)) {
        throw confErr(DEL_PERIOD_SEC_CONF + " must be an <int> or <long>");
      }
      deletePeriodSeconds = ((Number)tmp).longValue();
    }
    
    super.init(args);
  }

  @Override
  public void inform(SolrCore core) {
    this.core = core;
    
    if (null == core.getLatestSchema().getFieldTypeNoEx(expireField)) {
      // TODO: check for managed schema and auto-add as a date field?
      throw confErr(EXP_FIELD_NAME_CONF + " does not exist in schema: " + expireField);
    }

    if (0 < deletePeriodSeconds) {
      // validate that we have a chain we can work with
      try {
        Object ignored = core.getUpdateProcessingChain(deleteChainName);
      } catch (SolrException e) {
        throw confErr(DEL_CHAIN_NAME_CONF + " does not exist: " + deleteChainName, e);
      }
      // schedule recurring deletion
      initDeleteExpiredDocsScheduler(core);
    }
  }

  private void initDeleteExpiredDocsScheduler(SolrCore core) {
    executor = new ScheduledThreadPoolExecutor
      (1, new SolrNamedThreadFactory("autoExpireDocs"),
       new RejectedExecutionHandler() {
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
          log.warn("Skipping execution of '{}' using '{}'", r, e);
        }
      });

    core.addCloseHook(new CloseHook() {
      public void postClose(SolrCore core) {
        // update handler is gone, terminate anything that's left.

        if (executor.isTerminating()) {
          log.info("Waiting for close of DocExpiration Executor");
          ExecutorUtil.shutdownAndAwaitTermination(executor);
        }
      }
      public void preClose(SolrCore core) {
        log.info("Triggering Graceful close of DocExpiration Executor");
        executor.shutdown();
      }
    });

    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    // we don't want this firing right away, since the core may not be ready
    final long initialDelay = deletePeriodSeconds;
    // TODO: should we make initialDelay configurable
    // TODO: should we make initialDelay some fraction of the period?
    executor.scheduleAtFixedRate(new DeleteExpiredDocsRunnable(this), 
                                 deletePeriodSeconds,
                                 deletePeriodSeconds,
                                 TimeUnit.SECONDS);

  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, 
                                            SolrQueryResponse rsp, 
                                            UpdateRequestProcessor next ) {

    String defaultTtl = (null == ttlParam) ? null : req.getParams().get(ttlParam);

    if (null == ttlField && null == defaultTtl) {
      // nothing to do, shortcircut ourselves out of the chain.
      return next;
    } else {
      return new TTLUpdateProcessor(defaultTtl, expireField, ttlField, next);
    }
  }

  private static final class TTLUpdateProcessor extends UpdateRequestProcessor {

    final String defaultTtl;
    final String expireField;
    final String ttlField;
    public TTLUpdateProcessor(final String defaultTtl,
                              final String expireField,
                              final String ttlField,
                              final UpdateRequestProcessor next) {
      super(next);
      this.defaultTtl = defaultTtl;
      this.expireField = expireField;
      this.ttlField = ttlField;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      final SolrInputDocument doc = cmd.getSolrInputDocument();

      final String math = doc.containsKey(ttlField) 
        ? doc.getFieldValue(ttlField).toString() : defaultTtl;

      if (null != math) {
        try {
          final DateMathParser dmp = new DateMathParser();
          // TODO: should we try to accept things like "1DAY" as well as "+1DAY" ?
          // How? 
          // 'startsWith("+")' is a bad idea because it would cause problems with
          // things like "/DAY+1YEAR"
          // Maybe catch ParseException and retry with "+" prepended?
          doc.addField(expireField, dmp.parseMath(math));
        } catch (ParseException pe) {
          throw new SolrException(BAD_REQUEST, "Can't parse ttl as date math: " + math, pe);
        }
      }

      super.processAdd(cmd);
    }
  }

  /**
   * <p>
   * Runnable that uses the the <code>deleteChainName</code> configured for 
   * this factory to execute a delete by query (using the configured 
   * <code>expireField</code>) followed by a soft commit to re-open searchers (if needed)
   * </p>
   * <p>
   * This logic is all wrapped up in a new SolrRequestInfo context with 
   * some logging to help make it obvious this background activity is happening.
   * </p>
   * <p>
   * In cloud mode, this runner only triggers deletes if 
   * {@link #iAmInChargeOfPeriodicDeletes} is true.
   * (logging is minimal in this situation)
   * </p>
   *
   * @see #iAmInChargeOfPeriodicDeletes
   */
  private static final class DeleteExpiredDocsRunnable implements Runnable {
    final DocExpirationUpdateProcessorFactory factory;
    final SolrCore core;
    final String deleteChainName;
    final String expireField;
    public DeleteExpiredDocsRunnable(final DocExpirationUpdateProcessorFactory factory) {
      this.factory = factory;
      this.core = factory.core; 
      this.deleteChainName = factory.deleteChainName;
      this.expireField = factory.expireField;
   }

    public void run() {
      // setup the request context early so the logging (including any from 
      // shouldWeDoPeriodicDelete() ) includes the core context info
      final LocalSolrQueryRequest req = new LocalSolrQueryRequest
        (factory.core, Collections.<String,String[]>emptyMap());
      try {
        // HACK: to indicate to PKI that this is a server initiated request for the purposes
        // of distributed requet/credential forwarding...
        req.setUserPrincipalName(PKIAuthenticationPlugin.NODE_IS_USER);
        
        final SolrQueryResponse rsp = new SolrQueryResponse();
        rsp.addResponseHeader(new SimpleOrderedMap<>(1));
        SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
        try {
          
          if (! factory.iAmInChargeOfPeriodicDeletes() ) {
            // No-Op
            return;
          }
          log.info("Beginning periodic deletion of expired docs");

          UpdateRequestProcessorChain chain = core.getUpdateProcessingChain(deleteChainName);
          UpdateRequestProcessor proc = chain.createProcessor(req, rsp);
          if (null == proc) {
            log.warn("No active processors, skipping automatic deletion of expired docs using chain: {}"
                     , deleteChainName);
            return;
          }
          try {
            DeleteUpdateCommand del = new DeleteUpdateCommand(req);
            del.setQuery("{!cache=false}" + expireField + ":[* TO " +
                SolrRequestInfo.getRequestInfo().getNOW().toInstant()
                         + "]");
            proc.processDelete(del);
            
            // TODO: should this be more configurable? 
            // TODO: in particular: should hard commit be optional?
            CommitUpdateCommand commit = new CommitUpdateCommand(req, false);
            commit.softCommit = true;
            commit.openSearcher = true;
            proc.processCommit(commit);
            
          } finally {
            try {
              proc.finish();
            } finally {
              proc.close();
            }
          }

          log.info("Finished periodic deletion of expired docs");
        } catch (IOException ioe) {
          log.error("IOException in periodic deletion of expired docs: ", ioe);
          // DO NOT RETHROW: ScheduledExecutor will suppress subsequent executions
        } catch (RuntimeException re) {
          log.error("Runtime error in periodic deletion of expired docs: ", re);
          // DO NOT RETHROW: ScheduledExecutor will suppress subsequent executions
        } finally {
          SolrRequestInfo.clearRequestInfo();
        }
      } finally {
        req.close();
      }
    }
  }

  /**
   * <p>
   * Helper method that returns true if the Runnable managed by this factory 
   * should be responsible of doing periodical deletes.
   * </p>
   * <p>
   * In simple standalone installations this method always returns true, 
   * but in cloud mode it will be true if and only if we are currently the leader 
   * of the (active) slice with the first name (lexicographically).
   * </p>
   * <p>
   * If this method returns false, it may have also logged a message letting the user 
   * know why we aren't attempting period deletion (but it will attempt to not log 
   * this excessively)
   * </p>
   */
  private boolean iAmInChargeOfPeriodicDeletes() {
    ZkController zk = core.getCoreContainer().getZkController();

    if (null == zk) return true;
    
    // This is a lot simpler then doing our own "leader" election across all replicas 
    // of all shards since:
    //   a) we already have a per shard leader
    //   b) shard names must be unique
    //   c) ClusterState is already being "watched" by ZkController, no additional zk hits
    //   d) there might be multiple instances of this factory (in multiple chains) per 
    //      collection, so picking an ephemeral node name for our election would be tricky

    CloudDescriptor desc = core.getCoreDescriptor().getCloudDescriptor();
    String col = desc.getCollectionName();

    DocCollection docCollection = zk.getClusterState().getCollection(col);
    if (docCollection.getActiveSlicesArr().length == 0) {
      log.error("Collection {} has no active Slices?", col);
      return false;
    }
    List<Slice> slices = new ArrayList<>(Arrays.asList(docCollection.getActiveSlicesArr()));
    Collections.sort(slices, COMPARE_SLICES_BY_NAME);
    Replica firstSliceLeader = slices.get(0).getLeader();
    if (null == firstSliceLeader) {
      log.warn("Slice in charge of periodic deletes for {} does not currently have a leader",
               col);
      return false;
    }
    String leaderInCharge = firstSliceLeader.getName();
    String myCoreNodeName = desc.getCoreNodeName();
    
    boolean inChargeOfDeletesRightNow = leaderInCharge.equals(myCoreNodeName);

    if (previouslyInChargeOfDeletes && ! inChargeOfDeletesRightNow) {
      // don't spam the logs constantly, just log when we know that we're not the guy
      // (the first time -- or anytime we were, but no longer are)
      log.info("Not currently in charge of periodic deletes for this collection, {}",
               "will not trigger delete or log again until this changes");
    }

    previouslyInChargeOfDeletes = inChargeOfDeletesRightNow;
    return inChargeOfDeletesRightNow;
  }

  /** @see #iAmInChargeOfPeriodicDeletes */
  private volatile boolean previouslyInChargeOfDeletes = true;

  private static final Comparator<Slice> COMPARE_SLICES_BY_NAME = (a, b) -> a.getName().compareTo(b.getName());

}



