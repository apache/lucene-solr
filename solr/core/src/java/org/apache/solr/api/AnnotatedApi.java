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


import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SpecProvider;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.JsonSchemaCreator;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.SolrJacksonAnnotationInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements an Api just from  an annotated java class
 * The class must have an annotation {@link EndPoint}
 * Each method must have an annotation {@link Command}
 * The methods that implement a command should have the first 2 parameters
 * {@link SolrQueryRequest} and {@link SolrQueryResponse} or it may optionally
 * have a third parameter which could be a java class annotated with jackson annotations.
 * The third parameter is only valid if it is using a json command payload
 */

public class AnnotatedApi extends Api implements PermissionNameProvider , Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ERR = "Error executing commands :";
  private EndPoint endPoint;
  private final Map<String, Cmd> commands ;
  private final Cmd singletonCommand;
  private final Api fallback;

  @Override
  public void close() throws IOException {
    for (Cmd value : commands.values()) {
      if (value.obj instanceof Closeable) {
        ((Closeable) value.obj).close();
      }
      break;// all objects are same so close only one
    }

  }

  public EndPoint getEndPoint() {
    return endPoint;
  }

  public static List<Api> getApis(Object obj) {
    return getApis(obj.getClass(), obj);
  }
  public static List<Api> getApis(final Class<? extends Object> klas , Object obj) {
    if (!Modifier.isPublic(klas.getModifiers())) {
      throw new RuntimeException(klas.getName() + " is not public");
    }
    if (klas.getAnnotation(EndPoint.class) != null) {
      EndPoint endPoint = klas.getAnnotation(EndPoint.class);
      List<Method> methods = new ArrayList<>();
      Map<String, Cmd> commands = new HashMap<>();
      for (Method m : klas.getMethods()) {
        Command command = m.getAnnotation(Command.class);
        if (command != null) {
          methods.add(m);
          if (commands.containsKey(command.name())) {
            throw new RuntimeException("Duplicate commands " + command.name());
          }
          commands.put(command.name(), new Cmd(command.name(), obj, m));
        }
      }
      if (commands.isEmpty()) {
        throw new RuntimeException("No method with @Command in class: " + klas.getName());
      }
      SpecProvider specProvider = readSpec(endPoint, methods);
      return Collections.singletonList(new AnnotatedApi(specProvider, endPoint, commands, null));
    } else {
      List<Api> apis = new ArrayList<>();
      for (Method m : klas.getMethods()) {
        EndPoint endPoint = m.getAnnotation(EndPoint.class);
        if (endPoint == null) continue;
        Cmd cmd = new Cmd("", obj, m);
        SpecProvider specProvider = readSpec(endPoint, Collections.singletonList(m));
        apis.add(new AnnotatedApi(specProvider, endPoint, Collections.singletonMap("", cmd), null));
      }
      if (apis.isEmpty()) {
        throw new RuntimeException("Invalid Class : " + klas.getName() + " No @EndPoints");
      }

      return apis;
    }
  }


  private AnnotatedApi(SpecProvider specProvider, EndPoint endPoint, Map<String, Cmd> commands, Api fallback) {
    super(specProvider);
    this.endPoint = endPoint;
    this.fallback = fallback;
    this.commands = commands;
    this.singletonCommand = commands.get("");
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return endPoint.permission();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static SpecProvider readSpec(EndPoint endPoint, List<Method> m) {
    return () -> {
      Map map = new LinkedHashMap();
      List<String> methods = new ArrayList<>();
      for (SolrRequest.METHOD method : endPoint.method()) {
        methods.add(method.name());
      }
      map.put("methods", methods);
      map.put("url", new ValidatingJsonMap(Collections.singletonMap("paths", Arrays.asList(endPoint.path()))));
      Map<String, Object> cmds = new HashMap<>();

      for (Method method : m) {
        Command command = method.getAnnotation(Command.class);
        if (command != null && !command.name().isEmpty()) {
          cmds.put(command.name(), AnnotatedApi.createSchema(method));
        }
      }
      if (!cmds.isEmpty()) {
        map.put("commands", cmds);
      }
      return new ValidatingJsonMap(map);
    };

  }


  @Override
  public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (singletonCommand != null) {
      singletonCommand.invoke(req, rsp, null);
      return;
    }

    List<CommandOperation> cmds = req.getCommands(true);
    boolean allExists = true;
    for (CommandOperation cmd : cmds) {
      if (!commands.containsKey(cmd.name)) {
        cmd.addError("No such command supported: " + cmd.name);
        allExists = false;
      }
    }
    if (!allExists) {
      if (fallback != null) {
        fallback.call(req, rsp);
        return;
      } else {
        throw new ApiBag.ExceptionWithErrObject(SolrException.ErrorCode.BAD_REQUEST, "Error processing commands",
            CommandOperation.captureErrors(cmds));
      }
    }

    for (CommandOperation cmd : cmds) {
      commands.get(cmd.name).invoke(req, rsp, cmd);
    }

    @SuppressWarnings({"rawtypes"})
    List<Map> errs = CommandOperation.captureErrors(cmds);
    if (!errs.isEmpty()) {
      log.error("{}{}", ERR, Utils.toJSONString(errs));
      throw new ApiBag.ExceptionWithErrObject(SolrException.ErrorCode.BAD_REQUEST, ERR, errs);
    }

  }

  static class Cmd {
    final String command;
    final MethodHandle method;
    final Object obj;
    ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper();
    int paramsCount;
    @SuppressWarnings({"rawtypes"})
    Class parameterClass;
    boolean isWrappedInPayloadObj = false;


    Cmd(String command, Object obj, Method method) {
      this.command = command;
      this.obj = obj;
      try {
        this.method = MethodHandles.publicLookup().unreflect(method);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Unable to access method, may be not public or accessible ", e);
      }
      Class<?>[] parameterTypes = method.getParameterTypes();
      paramsCount = parameterTypes.length;
      if (parameterTypes.length == 1) {
        readPayloadType(method.getGenericParameterTypes()[0]);
      } else if (parameterTypes.length == 3) {
        if (parameterTypes[0] != SolrQueryRequest.class || parameterTypes[1] != SolrQueryResponse.class) {
          throw new RuntimeException("Invalid params for method " + method);
        }
        Type t = method.getGenericParameterTypes()[2];
        readPayloadType(t);
      }
      if (parameterTypes.length > 3) {
        throw new RuntimeException("Invalid params count for method " + method);
      }
    }

    @SuppressWarnings("rawtypes")
    private void readPayloadType(Type t) {
      if (t instanceof ParameterizedType) {
        ParameterizedType typ = (ParameterizedType) t;
        if (typ.getRawType() == PayloadObj.class) {
          isWrappedInPayloadObj = true;
          if(typ.getActualTypeArguments().length == 0){
            //this is a raw type
            parameterClass = Map.class;
            return;
          }
          Type t1 = typ.getActualTypeArguments()[0];
          if (t1 instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) t1;
            parameterClass = (Class) parameterizedType.getRawType();
          } else {
            parameterClass = (Class) typ.getActualTypeArguments()[0];
          }
        }
      } else {
        parameterClass = (Class) t;
      }
    }


    @SuppressWarnings({"unchecked"})
    void invoke(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation cmd) {
      Object original = null;
      try {
        Object o = null;
        String commandName = null;
        if(paramsCount == 1) {
          if(cmd == null) {
            if(parameterClass != null) {
              try {
                ContentStream stream = req.getContentStreams().iterator().next();
                o = mapper.readValue(stream.getStream(), parameterClass);
              } catch (IOException e) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "invalid payload", e);
              }
            }
          } else {
            commandName = cmd.name;
            original = cmd.getCommandData();
            o = original;
            if (o instanceof Map && parameterClass != null && parameterClass != Map.class) {
              o = mapper.readValue(Utils.toJSONString(o), parameterClass);
            }
          }
          PayloadObj<Object> payloadObj = new PayloadObj<>(commandName, original, o, req, rsp);
          cmd = payloadObj;
          method.invoke(obj, payloadObj);
          checkForErrorInPayload(cmd);
        } else if (paramsCount == 2) {
          method.invoke(obj, req, rsp);
        } else {
          o = cmd.getCommandData();
          if (o instanceof Map && parameterClass != null) {
            o = mapper.readValue(Utils.toJSONString(o), parameterClass);
          }
          if (isWrappedInPayloadObj) {
            PayloadObj<Object> payloadObj = new PayloadObj<>(cmd.name, cmd.getCommandData(), o, req, rsp);
            cmd = payloadObj;
            method.invoke(obj, req, rsp, payloadObj);
          } else {
            method.invoke(obj, req, rsp, o);
          }
          checkForErrorInPayload(cmd);
        }
      } catch (RuntimeException se) {
        log.error("Error executing command  ", se);
        throw se;
      } catch (Throwable e) {
        log.error("Error executing command : ", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }

    }

    private void checkForErrorInPayload(CommandOperation cmd) {
      if (cmd.hasError()) {
        throw new ApiBag.ExceptionWithErrObject(SolrException.ErrorCode.BAD_REQUEST, "Error executing command",
            CommandOperation.captureErrors(Collections.singletonList(cmd)));
      }
    }
  }

  public static Map<String, Object> createSchema(Method m) {
    Type[] types = m.getGenericParameterTypes();
    Type t = null;
    if (types.length == 3) t = types[2]; // (SolrQueryRequest req, SolrQueryResponse rsp, PayloadObj<PluginMeta>)
    if(types.length == 1) t = types[0];// (PayloadObj<PluginMeta>)
    if (t != null) {
      if (t instanceof ParameterizedType) {
        ParameterizedType typ = (ParameterizedType) t;
        if (typ.getRawType() == PayloadObj.class) {
          t = typ.getActualTypeArguments()[0];
        }
      }
      return JsonSchemaCreator.getSchema(t);

    }
    return null;
  }

}
