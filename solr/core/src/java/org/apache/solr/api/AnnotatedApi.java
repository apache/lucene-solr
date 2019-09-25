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


import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;

public class AnnotatedApi extends Api implements PermissionNameProvider {
  private EndPoint endPoint;
  private Map<String, Cmd> commands = new HashMap<>();


  public AnnotatedApi(Object obj) {
    super(Utils.getSpec(readSpec(obj.getClass())));
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

  private static String readSpec(Class klas) {
    EndPoint endPoint = (EndPoint) klas.getAnnotation(EndPoint.class);
    return endPoint.spec();

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
        cmd.addError("No such command supported :" + cmd.name);
        allExists = false;
      }
    }
    if (!allExists) {
      throw new ApiBag.ExceptionWithErrObject(SolrException.ErrorCode.BAD_REQUEST, "error processing commands",
          CommandOperation.captureErrors(cmds));
    }

    for (CommandOperation cmd : cmds) {
      commands.get(cmd.name).invoke(req, rsp, cmd);
    }

    List<Map> errs = CommandOperation.captureErrors(cmds);
    if (!errs.isEmpty()) {
      throw new ApiBag.ExceptionWithErrObject(SolrException.ErrorCode.BAD_REQUEST, "Error in executing commands", errs);
    }

  }

  class Cmd {
    final Command command;
    final Method method;
    final Object obj;

    Cmd(Command command, Object obj, Method method) {
      if (Modifier.isPublic(method.getModifiers())) {
        this.command = command;
        this.obj = obj;
        this.method = method;
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length > 1 || parameterTypes[0] != CallInfo.class) {
          throw new RuntimeException("Invalid params for method " + method);
        }
      } else {
        throw new RuntimeException(method.toString() + " is not a public static method");
      }

    }

    void invoke(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation cmd) {
      try {
        method.invoke(obj, new CallInfo(req, rsp, cmd));
      } catch (SolrException se) {
        throw se;
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }

    }
  }

}
