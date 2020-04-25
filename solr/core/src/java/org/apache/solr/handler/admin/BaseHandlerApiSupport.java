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

package org.apache.solr.handler.admin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiSupport;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionApiMapping.CommandMeta;
import org.apache.solr.client.solrj.request.CollectionApiMapping.V2EndPoint;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.util.StrUtils.splitSmart;

/**
 * This is a utility class to provide an easy mapping of request handlers which support multiple commands
 * to the V2 API format (core admin api, collections api). This helps in automatically mapping paths
 * to actions and old parameter names to new parameter names
 */
public abstract class BaseHandlerApiSupport implements ApiSupport {
  protected final Map<SolrRequest.METHOD, Map<V2EndPoint, List<ApiCommand>>> commandsMapping;

  protected BaseHandlerApiSupport() {
    commandsMapping = new HashMap<>();
    for (ApiCommand cmd : getCommands()) {
      Map<V2EndPoint, List<ApiCommand>> m = commandsMapping.get(cmd.meta().getHttpMethod());
      if (m == null) commandsMapping.put(cmd.meta().getHttpMethod(), m = new HashMap<>());
      List<ApiCommand> list = m.get(cmd.meta().getEndPoint());
      if (list == null) m.put(cmd.meta().getEndPoint(), list = new ArrayList<>());
      list.add(cmd);
    }
  }

  @Override
  public synchronized Collection<Api> getApis() {
    ImmutableList.Builder<Api> l = ImmutableList.builder();
    for (V2EndPoint op : getEndPoints()) l.add(getApi(op));
    return l.build();
  }


  private Api getApi(final V2EndPoint op) {
    final BaseHandlerApiSupport apiHandler = this;
    return new Api(Utils.getSpec(op.getSpecName())) {
      @Override
      public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
        SolrParams params = req.getParams();
        SolrRequest.METHOD method = SolrRequest.METHOD.valueOf(req.getHttpMethod());
        List<ApiCommand> commands = commandsMapping.get(method).get(op);
        try {
          if (method == POST) {
            List<CommandOperation> cmds = req.getCommands(true);
            if (cmds.size() > 1)
              throw new SolrException(BAD_REQUEST, "Only one command is allowed");
            CommandOperation c = cmds.size() == 0 ? null : cmds.get(0);
            ApiCommand command = null;
            String commandName = c == null ? null : c.name;
            for (ApiCommand cmd : commands) {
              if (Objects.equals(cmd.meta().getName(), commandName)) {
                command = cmd;
                break;
              }
            }

            if (command == null) {
              throw new SolrException(BAD_REQUEST, " no such command " + c);
            }
            wrapParams(req, c, command, false);
            command.invoke(req, rsp, apiHandler);

          } else {
            if (commands == null || commands.isEmpty()) {
              rsp.add("error", "No support for : " + method + " at :" + req.getPath());
              return;
            }
            if (commands.size() > 1) {
              for (ApiCommand command : commands) {
                if (command.meta().getName().equals(req.getPath())) {
                  commands = Collections.singletonList(command);
                  break;
                }
              }
            }
            wrapParams(req, new CommandOperation("", Collections.EMPTY_MAP), commands.get(0), true);
            commands.get(0).invoke(req, rsp, apiHandler);
          }

        } catch (SolrException e) {
          throw e;
        } catch (Exception e) {
          throw new SolrException(BAD_REQUEST, e); //TODO BAD_REQUEST is a wild guess; should we flip the default?  fail here to investigate how this happens in tests
        } finally {
          req.setParams(params);
        }

      }
    };

  }

  /**
   * Wrapper for SolrParams that wraps V2 params and exposes them as V1 params.
   */
  private static void wrapParams(final SolrQueryRequest req, final CommandOperation co, final ApiCommand cmd, final boolean useRequestParams) {
    final Map<String, String> pathValues = req.getPathTemplateValues();
    final Map<String, Object> map = co == null || !(co.getCommandData() instanceof Map) ?
        Collections.singletonMap("", co.getCommandData()) : co.getDataMap();
    final SolrParams origParams = req.getParams();

    req.setParams(
        new SolrParams() {
          @Override
          public String get(String param) {
            Object vals = getParams0(param);
            if (vals == null) return null;
            if (vals instanceof String) return (String) vals;
            if (vals instanceof Boolean || vals instanceof Number) return String.valueOf(vals);
            if (vals instanceof String[] && ((String[]) vals).length > 0) return ((String[]) vals)[0];
            return null;
          }

          private Object getParams0(String param) {
            param = cmd.meta().getParamSubstitute(param); // v1 -> v2, possibly dotted path
            Object o = param.indexOf('.') > 0 ?
                Utils.getObjectByPath(map, true, splitSmart(param, '.')) :
                map.get(param);
            if (o == null) o = pathValues.get(param);
            if (o == null && useRequestParams) o = origParams.getParams(param);
            if (o instanceof List) {
              List l = (List) o;
              return l.toArray(new String[l.size()]);
            }

            return o;
          }

          @Override
          public String[] getParams(String param) {
            Object vals = getParams0(param);
            return vals == null || vals instanceof String[] ?
                (String[]) vals :
                new String[]{vals.toString()};
          }

          @Override
          public Iterator<String> getParameterNamesIterator() {
            return cmd.meta().getParamNamesIterator(co);
          }

          @Override
          public Map toMap(Map<String, Object> suppliedMap) {
            for(Iterator<String> it=getParameterNamesIterator(); it.hasNext(); ) {
              final String param = it.next();
              String key = cmd.meta().getParamSubstitute(param);
              Object o = key.indexOf('.') > 0 ?
                  Utils.getObjectByPath(map, true, splitSmart(key, '.')) :
                  map.get(key);
              if (o == null) o = pathValues.get(key);
              if (o == null && useRequestParams) o = origParams.getParams(key);
              // make strings out of as many things as we can now to minimize differences from
              // the standard impls that pass through a NamedList/SimpleOrderedMap...
              Class<?> oClass = o.getClass();
              if (oClass.isPrimitive() ||
                  Number.class.isAssignableFrom(oClass) ||
                  Character.class.isAssignableFrom(oClass) ||
                  Boolean.class.isAssignableFrom(oClass)) {
                suppliedMap.put(param,String.valueOf(o));
              } else if (List.class.isAssignableFrom(oClass) && ((List)o).get(0) instanceof String ) {
                List<String> l = (List<String>) o;
                suppliedMap.put( param, l.toArray(new String[0]));
              } else {
                // Lists pass through but will require special handling downstream
                // if they contain non-string elements.
                suppliedMap.put(param, o);
              }
            }
            return suppliedMap;
          }
        });

  }

  protected abstract Collection<ApiCommand> getCommands();

  protected abstract Collection<V2EndPoint> getEndPoints();


  public interface ApiCommand  {
    CommandMeta meta();

    void invoke(SolrQueryRequest req, SolrQueryResponse rsp, BaseHandlerApiSupport apiHandler) throws Exception;
  }

}
