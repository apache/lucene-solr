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


import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
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

public class AnnotatedApi extends Api implements PermissionNameProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ERR = "Error executing commands :";
  private EndPoint endPoint;
  private Map<String, Cmd> commands = new HashMap<>();
  private final Api fallback;

  public AnnotatedApi(Object obj) {
    this(obj, null);
  }

  public AnnotatedApi(Object obj, Api fallback) {
    super(readSpec(obj.getClass()));
    this.fallback = fallback;
    Class<?> klas = obj.getClass();
    if (!Modifier.isPublic(klas.getModifiers())) {
      throw new RuntimeException(obj.getClass().getName() + " is not public");
    }

    endPoint = klas.getAnnotation(EndPoint.class);

    for (Method m : klas.getDeclaredMethods()) {
      Command command = m.getAnnotation(Command.class);
      if (command == null) continue;

      if (commands.containsKey(command.name())) {
        throw new RuntimeException("Duplicate commands " + command.name());
      }
      commands.put(command.name(), new Cmd(command, obj, m));
    }

  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return endPoint.permission();
  }

  private static SpecProvider readSpec(Class klas) {
    EndPoint endPoint = (EndPoint) klas.getAnnotation(EndPoint.class);
    if (endPoint == null)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid class :  " + klas.getName());
    return () -> {
      Map map = new LinkedHashMap();
      List<String> methods = new ArrayList<>();
      for (SolrRequest.METHOD method : endPoint.method()) {
        methods.add(method.name());
      }
      map.put("methods", methods);
      map.put("url", new ValidatingJsonMap(Collections.singletonMap("paths", Arrays.asList(endPoint.path()))));
      Map<String, Object> cmds = new HashMap<>();

      for (Method method : klas.getMethods()) {
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
    if (commands.size() == 1) {
      Cmd cmd = commands.get("");
      if (cmd != null) {
        cmd.invoke(req, rsp, null);
        return;
      }
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

    List<Map> errs = CommandOperation.captureErrors(cmds);
    if (!errs.isEmpty()) {
      log.error(ERR + Utils.toJSONString(errs));
      throw new ApiBag.ExceptionWithErrObject(SolrException.ErrorCode.BAD_REQUEST, ERR, errs);
    }

  }

  class Cmd {
    final Command command;
    final Method method;
    final Object obj;
    ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper();
    int paramsCount;
    Class c;
    boolean isWrappedInPayloadObj = false;


    Cmd(Command command, Object obj, Method method) {
      if (Modifier.isPublic(method.getModifiers())) {
        this.command = command;
        this.obj = obj;
        this.method = method;
        Class<?>[] parameterTypes = method.getParameterTypes();
        paramsCount = parameterTypes.length;
        if (parameterTypes[0] != SolrQueryRequest.class || parameterTypes[1] != SolrQueryResponse.class) {
          throw new RuntimeException("Invalid params for method " + method);
        }
        if (parameterTypes.length == 3) {
          Type t = method.getGenericParameterTypes()[2];
          if (t instanceof ParameterizedType) {
            ParameterizedType typ = (ParameterizedType) t;
            if (typ.getRawType() == PayloadObj.class) {
              isWrappedInPayloadObj = true;
              Type t1 = typ.getActualTypeArguments()[0];
              if (t1 instanceof ParameterizedType) {
                ParameterizedType parameterizedType = (ParameterizedType) t1;
                c = (Class) parameterizedType.getRawType();
              } else {
                c = (Class) typ.getActualTypeArguments()[0];
              }
            }
          } else {
            c = (Class) t;
          }

        }
        if (parameterTypes.length > 3) {
          throw new RuntimeException("Invalid params count for method " + method);

        }
      } else {
        throw new RuntimeException(method.toString() + " is not a public static method");
      }

    }

    void invoke(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation cmd) {
      try {
        if (paramsCount == 2) {
          method.invoke(obj, req, rsp);
        } else {
          Object o = cmd.getCommandData();
          if (o instanceof Map && c != null) {
            o = mapper.readValue(Utils.toJSONString(o), c);
          }
          if (isWrappedInPayloadObj) {
            PayloadObj<Object> payloadObj = new PayloadObj<>(cmd.name, cmd.getCommandData(), o);
            cmd = payloadObj;
            method.invoke(obj, req, rsp, payloadObj);
          } else {
            method.invoke(obj, req, rsp, o);
          }
          if (cmd.hasError()) {
            throw new ApiBag.ExceptionWithErrObject(SolrException.ErrorCode.BAD_REQUEST, "Error executing command",
                CommandOperation.captureErrors(Collections.singletonList(cmd)));
          }
        }


      } catch (SolrException se) {
        log.error("Error executing command  ", se);
        throw se;
      } catch (InvocationTargetException ite) {
        log.error("Error executing command ", ite);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, ite.getCause());
      } catch (Exception e) {
        log.error("Error executing command : ", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }

    }
  }

  public static Map<String, Object> createSchema(Method m) {
    Type[] types = m.getGenericParameterTypes();
    if (types.length == 3) {
      Type t = types[2];
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
