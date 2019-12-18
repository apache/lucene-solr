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
package org.apache.solr.handler.dataimport;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.ProtectionDomain;
import java.util.Map;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * <p>
 * A {@link Transformer} instance capable of executing functions written in scripting
 * languages as a {@link Transformer} instance.
 * </p>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 1.3
 */
public class ScriptTransformer extends Transformer {
 private Invocable engine;
 private String functionName;

  @Override
  public Object transformRow(Map<String,Object> row, Context context) {
    return AccessController.doPrivileged(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        return transformRowUnsafe(row, context);
      }
    }, SCRIPT_SANDBOX);
  }

  public Object transformRowUnsafe(Map<String, Object> row, Context context) {
    try {
      if (engine == null)
        initEngine(context);
      if (engine == null)
        return row;
      return engine.invokeFunction(functionName, new Object[]{row, context});      
    } catch (DataImportHandlerException e) {
      throw e;
    } catch (Exception e) {
      wrapAndThrow(SEVERE,e, "Error invoking script for entity " + context.getEntityAttribute("name"));
    }
    //will not reach here
    return null;
  }

  private void initEngine(Context context) {
    String scriptText = context.getScript();
    String scriptLang = context.getScriptLanguage();
    if (scriptText == null) {
      throw new DataImportHandlerException(SEVERE,
          "<script> tag is not present under <dataConfig>");
    }
    ScriptEngineManager scriptEngineMgr = new ScriptEngineManager();
    ScriptEngine scriptEngine = scriptEngineMgr.getEngineByName(scriptLang);
    if (scriptEngine == null) {
      throw new DataImportHandlerException(SEVERE,
          "Cannot load Script Engine for language: " + scriptLang);
    }
    if (scriptEngine instanceof Invocable) {
      engine = (Invocable) scriptEngine;
    } else {
      throw new DataImportHandlerException(SEVERE,
          "The installed ScriptEngine for: " + scriptLang
              + " does not implement Invocable.  Class is "
              + scriptEngine.getClass().getName());
    }
    try {
      try {
        AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws ScriptException  {
            scriptEngine.eval(scriptText);
            return null;
          }
        }, SCRIPT_SANDBOX);
      } catch (PrivilegedActionException e) {
        throw (ScriptException) e.getException();
      }
    } catch (ScriptException e) {
      wrapAndThrow(SEVERE, e, "'eval' failed with language: " + scriptLang
          + " and script: \n" + scriptText);
    }
  }

  public void setFunctionName(String methodName) {
    this.functionName = methodName;
  }

  public String getFunctionName() {
    return functionName;
  }

  // sandbox for script code: zero permissions
  private static final AccessControlContext SCRIPT_SANDBOX =
      new AccessControlContext(new ProtectionDomain[] { new ProtectionDomain(null, null) });

}
