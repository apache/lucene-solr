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

package org.apache.solr.api;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SpecProvider;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.JsonSchemaValidator;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.PathTrie;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.SolrRequest.SUPPORTED_METHODS;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.StrUtils.formatString;
import static org.apache.solr.common.util.ValidatingJsonMap.ENUM_OF;
import static org.apache.solr.common.util.ValidatingJsonMap.NOT_NULL;

public class ApiBag {
  private final boolean isCoreSpecific;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, PathTrie<Api>> apis = new ConcurrentHashMap<>();

  public ApiBag(boolean isCoreSpecific) {
    this.isCoreSpecific = isCoreSpecific;
  }

  /**Register a POJO annotated with {@link EndPoint}
   * @param o the instance to be used for invocations
   */
  @SuppressWarnings({"unchecked"})
  public synchronized List<Api> registerObject(Object o) {
    List<Api> l = AnnotatedApi.getApis(o);
    for (Api api : l) {
      register(api, Collections.EMPTY_MAP);
    }
    return l;
  }
  @SuppressWarnings({"unchecked"})
  public synchronized void register(Api api) {
    register(api, Collections.EMPTY_MAP);
  }
  public synchronized void register(Api api, Map<String, String> nameSubstitutes) {
    try {
      validateAndRegister(api, nameSubstitutes);
    } catch (Exception e) {
      log.error("Unable to register plugin: {} with spec {} :", api.getClass().getName(), Utils.toJSONString(api.getSpec()), e);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }

    }
  }

  @SuppressWarnings({"unchecked"})
  private void validateAndRegister(Api api, Map<String, String> nameSubstitutes) {
    ValidatingJsonMap spec = api.getSpec();
    Api introspect = new IntrospectApi(api, isCoreSpecific);
    List<String> methods = spec.getList("methods", ENUM_OF, SUPPORTED_METHODS);
    for (String method : methods) {
      PathTrie<Api> registry = apis.get(method);

      if (registry == null) apis.put(method, registry = new PathTrie<>(ImmutableSet.of("_introspect")));
      ValidatingJsonMap url = spec.getMap("url", NOT_NULL);
      ValidatingJsonMap params = url.getMap("params", null);
      if (params != null) {
        for (Object o : params.keySet()) {
          ValidatingJsonMap param = params.getMap(o.toString(), NOT_NULL);
          param.get("type", ENUM_OF, KNOWN_TYPES);
        }
      }
      List<String> paths = url.getList("paths", NOT_NULL);
      ValidatingJsonMap parts = url.getMap("parts", null);
      if (parts != null) {
        Set<String> wildCardNames = getWildCardNames(paths);
        for (Object o : parts.keySet()) {
          if (!wildCardNames.contains(o.toString()))
            throw new RuntimeException("" + o + " is not a valid part name");
          ValidatingJsonMap pathMeta = parts.getMap(o.toString(), NOT_NULL);
          pathMeta.get("type", ENUM_OF, ImmutableSet.of("enum", "string", "int", "number", "boolean"));
        }
      }
      verifyCommands(api.getSpec());
      for (String path : paths) {
        registry.insert(path, nameSubstitutes, api);
        registerIntrospect(nameSubstitutes, registry, path, introspect);
      }
    }
  }

  public static void registerIntrospect(Map<String, String> nameSubstitutes, PathTrie<Api> registry, String path, Api introspect) {
    List<String> l = PathTrie.getPathSegments(path);
    registerIntrospect(l, registry, nameSubstitutes, introspect);
    int lastIdx = l.size() - 1;
    for (int i = lastIdx; i >= 0; i--) {
      String itemAt = l.get(i);
      if (PathTrie.templateName(itemAt) == null) break;
      l.remove(i);
      if (registry.lookup(l, new HashMap<>()) != null) break;
      registerIntrospect(l, registry, nameSubstitutes, introspect);
    }
  }

  static void registerIntrospect(List<String> l, PathTrie<Api> registry, Map<String, String> substitutes, Api introspect) {
    ArrayList<String> copy = new ArrayList<>(l);
    copy.add("_introspect");
    registry.insert(copy, substitutes, introspect);
  }

  public synchronized Api unregister(SolrRequest.METHOD method, String path) {
    List<String> l = PathTrie.getPathSegments(path);
    List<String> introspectPath = new ArrayList<>(l);
    introspectPath.add("_introspect");
    getRegistry(method.toString()).remove(introspectPath);
    return getRegistry(method.toString()).remove(l);
  }

  public static class IntrospectApi extends Api {
    Api baseApi;
    final boolean isCoreSpecific;

    public IntrospectApi(Api base, boolean isCoreSpecific) {
      super(EMPTY_SPEC);
      this.baseApi = base;
      this.isCoreSpecific = isCoreSpecific;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void call(SolrQueryRequest req, SolrQueryResponse rsp) {

      String cmd = req.getParams().get("command");
      ValidatingJsonMap result = null;
      if (cmd == null) {
        result = isCoreSpecific ? ValidatingJsonMap.getDeepCopy(baseApi.getSpec(), 5, true) : baseApi.getSpec();
      } else {
        ValidatingJsonMap specCopy = ValidatingJsonMap.getDeepCopy(baseApi.getSpec(), 5, true);
        ValidatingJsonMap commands = specCopy.getMap("commands", null);
        if (commands != null) {
          ValidatingJsonMap m = commands.getMap(cmd, null);
          if (m == null) {
            specCopy.put("commands", Collections.singletonMap(cmd, "Command not found!"));
          } else {
            specCopy.put("commands", Collections.singletonMap(cmd, m));
          }

        }
        result = specCopy;
      }
      if (isCoreSpecific) {
        List<String> pieces = req.getHttpSolrCall() == null ? null : ((V2HttpCall) req.getHttpSolrCall()).pieces;
        if (pieces != null) {
          String prefix = "/" + pieces.get(0) + "/" + pieces.get(1);
          List<String> paths = result.getMap("url", NOT_NULL).getList("paths", NOT_NULL);
          result.getMap("url", NOT_NULL).put("paths",
              paths.stream()
                  .map(s -> prefix + s)
                  .collect(Collectors.toList()));
        }
      }
      List l = (List) rsp.getValues().get("spec");
      if (l == null) rsp.getValues().add("spec", l = new ArrayList());
      l.add(result);
    }
  }

  public static Map<String, JsonSchemaValidator> getParsedSchema(ValidatingJsonMap commands) {
    Map<String, JsonSchemaValidator> validators = new HashMap<>();
    for (Object o : commands.entrySet()) {
      @SuppressWarnings({"rawtypes"})
      Map.Entry cmd = (Map.Entry) o;
      try {
        validators.put((String) cmd.getKey(), new JsonSchemaValidator((Map) cmd.getValue()));
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error in api spec", e);
      }
    }
    return validators;
  }


  private void verifyCommands(ValidatingJsonMap spec) {
    ValidatingJsonMap commands = spec.getMap("commands", null);
    if (commands == null) return;
    getParsedSchema(commands);

  }

  private Set<String> getWildCardNames(List<String> paths) {
    Set<String> wildCardNames = new HashSet<>();
    for (String path : paths) {
      List<String> p = PathTrie.getPathSegments(path);
      for (String s : p) {
        String wildCard = PathTrie.templateName(s);
        if (wildCard != null) wildCardNames.add(wildCard);
      }
    }
    return wildCardNames;
  }


  public Api lookup(String path, String httpMethod, Map<String, String> parts) {
    if (httpMethod == null) {
      for (PathTrie<Api> trie : apis.values()) {
        Api api = trie.lookup(path, parts);
        if (api != null) return api;
      }
      return null;
    } else {
      PathTrie<Api> registry = apis.get(httpMethod);
      if (registry == null) return null;
      return registry.lookup(path, parts);
    }
  }

  public static class ReqHandlerToApi extends Api implements PermissionNameProvider {
    SolrRequestHandler rh;

    public ReqHandlerToApi(SolrRequestHandler rh, SpecProvider spec) {
      super(spec);
      this.rh = rh;
    }

    @Override
    public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
      rh.handleRequest(req, rsp);
    }

    @Override
    public Name getPermissionName(AuthorizationContext ctx) {
      if (rh instanceof PermissionNameProvider) {
        return ((PermissionNameProvider) rh).getPermissionName(ctx);
      }
      return null;
    }
  }

  public static List<Api> wrapRequestHandlers(final SolrRequestHandler rh, String... specs) {
    ImmutableList.Builder<Api> b = ImmutableList.builder();
    for (String spec : specs) b.add(new ReqHandlerToApi(rh, Utils.getSpec(spec)));
    return b.build();
  }


  public static final SpecProvider EMPTY_SPEC = () -> ValidatingJsonMap.EMPTY;
  public static final String HANDLER_NAME = "handlerName";
  public static final Set<String> KNOWN_TYPES = ImmutableSet.of("string", "boolean", "list", "int", "double", "object");

  public PathTrie<Api> getRegistry(String method) {
    return apis.get(method);
  }

  public void registerLazy(PluginBag.PluginHolder<SolrRequestHandler> holder, PluginInfo info) {
    String specName = info.attributes.get("spec");
    if (specName == null) specName = "emptySpec";
    register(new LazyLoadedApi(Utils.getSpec(specName), holder), Collections.singletonMap(HANDLER_NAME, info.attributes.get(NAME)));
  }

  public static SpecProvider constructSpec(PluginInfo info) {
    Object specObj = info == null ? null : info.attributes.get("spec");
    if (specObj == null) specObj = "emptySpec";
    if (specObj instanceof Map) {
      @SuppressWarnings({"rawtypes"})
      Map map = (Map) specObj;
      return () -> ValidatingJsonMap.getDeepCopy(map, 4, false);
    } else {
      return Utils.getSpec((String) specObj);
    }
  }

  @SuppressWarnings({"rawtypes"})
  public static List<CommandOperation> getCommandOperations(ContentStream stream, Map<String, JsonSchemaValidator> validators, boolean validate) {
    List<CommandOperation> parsedCommands = null;
    try {
      parsedCommands = CommandOperation.readCommands(Collections.singleton(stream), new NamedList());
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unable to parse commands",e);
    }

    if (validators == null || !validate) {    // no validation possible because we do not have a spec
      return parsedCommands;
    }

    List<CommandOperation> commandsCopy = CommandOperation.clone(parsedCommands);

    for (CommandOperation cmd : commandsCopy) {
      JsonSchemaValidator validator = validators.get(cmd.name);
      if (validator == null) {
        cmd.addError(formatString("Unknown operation ''{0}'' available ops are ''{1}''", cmd.name,
            validators.keySet()));
        continue;
      } else {
        List<String> errs = validator.validateJson(cmd.getCommandData());
        if (errs != null){
          // otherwise swallowed in solrj tests, and just get "Error in command payload" in test log
          // which is quite unhelpful.
          log.error("Command errors for {}:{}", cmd.name, errs );
          for (String err : errs) cmd.addError(err);
        }
      }

    }
    @SuppressWarnings({"rawtypes"})
    List<Map> errs = CommandOperation.captureErrors(commandsCopy);
    if (!errs.isEmpty()) {
      throw new ExceptionWithErrObject(SolrException.ErrorCode.BAD_REQUEST, "Error in command payload", errs);
    }
    return commandsCopy;
  }

  public static class ExceptionWithErrObject extends SolrException {
    @SuppressWarnings({"rawtypes"})
    private List<Map> errs;

    public ExceptionWithErrObject(ErrorCode code, String msg, @SuppressWarnings({"rawtypes"})List<Map> errs) {
      super(code, msg);
      this.errs = errs;
    }

    @SuppressWarnings({"rawtypes"})
    public List<Map> getErrs() {
      return errs;
    }

    @Override
    public String getMessage() {
      return super.getMessage() + ", errors: " + getErrs() + ", ";
    }
  }

  public static class LazyLoadedApi extends Api {

    private final PluginBag.PluginHolder<SolrRequestHandler> holder;
    private Api delegate;

    protected LazyLoadedApi(SpecProvider specProvider, PluginBag.PluginHolder<SolrRequestHandler> lazyPluginHolder) {
      super(specProvider);
      this.holder = lazyPluginHolder;
    }

    @Override
    public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
      if (!holder.isLoaded()) {
        delegate = new ReqHandlerToApi(holder.get(), ApiBag.EMPTY_SPEC);
      }
      delegate.call(req, rsp);
    }
  }

}
