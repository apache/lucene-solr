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

package org.apache.solr.cloud.autoscaling;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.IdUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This listener saves events to the {@link CollectionAdminParams#SYSTEM_COLL} collection.
 * <p>Configuration properties:</p>
 * <ul>
 *   <li>collection - optional string, specifies what collection should be used for storing events. Default value
 *   is {@link CollectionAdminParams#SYSTEM_COLL}.</li>
 * </ul>
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class SystemLogListener extends TriggerListenerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String SOURCE_FIELD = "source_s";
  public static final String EVENT_SOURCE_FIELD = "event.source_s";
  public static final String EVENT_TYPE_FIELD = "event.type_s";
  public static final String STAGE_FIELD = "stage_s";
  public static final String ACTION_FIELD = "action_s";
  public static final String MESSAGE_FIELD = "message_t";
  public static final String BEFORE_ACTIONS_FIELD = "before.actions_ss";
  public static final String AFTER_ACTIONS_FIELD = "after.actions_ss";
  public static final String COLLECTIONS_FIELD = "collections_ss";
  public static final String SOURCE = SystemLogListener.class.getSimpleName();
  public static final String DOC_TYPE = "autoscaling_event";

  private String collection = CollectionAdminParams.SYSTEM_COLL;

  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) throws TriggerValidationException {
    super.configure(loader, cloudManager, config);
    collection = (String)config.properties.getOrDefault(CollectionAdminParams.COLLECTION, CollectionAdminParams.SYSTEM_COLL);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName, ActionContext context,
               Throwable error, String message) throws Exception {
    try {
      ClusterState clusterState = cloudManager.getClusterStateProvider().getClusterState();
      DocCollection coll = clusterState.getCollectionOrNull(collection);
      if (coll == null) {
        log.debug("Collection {} missing, skip sending event {}", collection, event);
        return;
      }
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(CommonParams.TYPE, DOC_TYPE);
      doc.addField(SOURCE_FIELD, SOURCE);
      doc.addField("id", IdUtils.timeRandomId());
      doc.addField("event.id_s", event.getId());
      doc.addField(EVENT_TYPE_FIELD, event.getEventType().toString());
      doc.addField(EVENT_SOURCE_FIELD, event.getSource());
      doc.addField("event.time_l", event.getEventTime());
      doc.addField("timestamp", new Date());
      addMap("event.property.", doc, event.getProperties());
      doc.addField(STAGE_FIELD, stage.toString());
      if (actionName != null) {
        doc.addField(ACTION_FIELD, actionName);
      }
      if (message != null) {
        doc.addField(MESSAGE_FIELD, message);
      }
      addError(doc, error);
      // add JSON versions of event and context
      String eventJson = Utils.toJSONString(event);
      doc.addField("event_str", eventJson);
      if (context != null) {
        // capture specifics of operations after compute_plan action
        addOperations(doc, (List<SolrRequest>)context.getProperties().get("operations"));
        // capture specifics of responses after execute_plan action
        addResponses(doc, (List<NamedList<Object>>)context.getProperties().get("responses"));
        addActions(BEFORE_ACTIONS_FIELD, doc, (List<String>)context.getProperties().get(TriggerEventProcessorStage.BEFORE_ACTION.toString()));
        addActions(AFTER_ACTIONS_FIELD, doc, (List<String>)context.getProperties().get(TriggerEventProcessorStage.AFTER_ACTION.toString()));
        String contextJson = Utils.toJSONString(context);
        doc.addField("context_str", contextJson);
      }
      UpdateRequest req = new UpdateRequest();
      req.add(doc);
      req.setParam(CollectionAdminParams.COLLECTION, collection);
      cloudManager.request(req);
    } catch (Exception e) {
      if ((e instanceof SolrException) && e.getMessage().contains("Collection not found")) {
        // relatively benign but log this - collection still existed when we started
        log.info("Collection {} missing, skip sending event {}", collection, event);
      } else {
        log.warn("Exception sending event. Collection: {}, event: {}, exception: {}", collection, event, e);
      }
    }
  }

  private void addActions(String field, SolrInputDocument doc, List<String> actions) {
    if (actions == null) {
      return;
    }
    actions.forEach(a -> doc.addField(field, a));
  }

  private void addMap(String prefix, SolrInputDocument doc, Map<String, Object> map) {
    map.forEach((k, v) -> {
      if (v instanceof Collection) {
        for (Object o : (Collection)v) {
          doc.addField(prefix + k + "_ss", String.valueOf(o));
        }
      } else {
        doc.addField(prefix + k + "_ss", String.valueOf(v));
      }
    });
  }

  @SuppressWarnings({"rawtypes"})
  private void addOperations(SolrInputDocument doc, List<SolrRequest> operations) {
    if (operations == null || operations.isEmpty()) {
      return;
    }
    Set<String> collections = new HashSet<>();
    for (SolrRequest req : operations) {
      SolrParams params = req.getParams();
      if (params == null) {
        continue;
      }
      if (params.get(CollectionAdminParams.COLLECTION) != null) {
        collections.add(params.get(CollectionAdminParams.COLLECTION));
      }
      // build a whitespace-separated param string
      StringJoiner paramJoiner = new StringJoiner(" ");
      paramJoiner.setEmptyValue("");
      for (Iterator<String> it = params.getParameterNamesIterator(); it.hasNext(); ) {
        final String name = it.next();
        final String [] values = params.getParams(name);
        for (String value : values) {
          paramJoiner.add(name + "=" + value);
        }
      }
      String paramString = paramJoiner.toString();
      if (!paramString.isEmpty()) {
        doc.addField("operations.params_ts", paramString);
      }
    }
    if (!collections.isEmpty()) {
      doc.addField(COLLECTIONS_FIELD, collections);
    }
  }

  private void addResponses(SolrInputDocument doc, List<NamedList<Object>> responses) {
    if (responses == null || responses.isEmpty()) {
      return;
    }
    for (NamedList<Object> rsp : responses) {
      Object o = rsp.get("success");
      if (o != null) {
        doc.addField("responses_ts", "success " + o);
      } else {
        o = rsp.get("failure");
        if (o != null) {
          doc.addField("responses_ts", "failure " + o);
        } else { // something else
          doc.addField("responses_ts", Utils.toJSONString(rsp));
        }
      }
    }
  }

  private void addError(SolrInputDocument doc, Throwable error) {
    if (error == null) {
      return;
    }
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    error.printStackTrace(pw);
    pw.flush(); pw.close();
    doc.addField("error.message_t", error.getMessage());
    doc.addField("error.details_t", sw.toString());
  }
}
