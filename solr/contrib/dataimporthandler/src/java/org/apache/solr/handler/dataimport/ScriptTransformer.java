/**
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * <p>
 * A {@link Transformer} instance capable of executing functions written in scripting
 * languages as a {@link Transformer} instance.
 * </p>
 * <p/>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p/>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class ScriptTransformer extends Transformer {
  private Object engine;

  private Method invokeFunctionMethod;

  private String functionName;

  @Override
  public Object transformRow(Map<String, Object> row, Context context) {
    try {
      if (engine == null)
        initEngine(context);
      if (engine == null)
        return row;
      return invokeFunctionMethod.invoke(engine, functionName, new Object[]{
              row, context});
    } catch (DataImportHandlerException e) {
      throw e;
    } catch (InvocationTargetException e) {
      wrapAndThrow(SEVERE,e,
              "Could not invoke method :"
                      + functionName
                      + "\n <script>\n"
                      + context.getScript()
                      + "</script>");
    } catch (Exception e) {
      wrapAndThrow(SEVERE,e, "Error invoking script for entity " + context.getEntityAttribute("name"));
    }
    //will not reach here
    return null;
  }

  private void initEngine(Context context) {
    try {
      String scriptText = context.getScript();
      String scriptLang = context.getScriptLanguage();
      if(scriptText == null ){
        throw new DataImportHandlerException(SEVERE,
              "<script> tag is not present under <dataConfig>");
      }
      Object scriptEngineMgr = Class
              .forName("javax.script.ScriptEngineManager").newInstance();
      // create a Script engine
      Method getEngineMethod = scriptEngineMgr.getClass().getMethod(
              "getEngineByName", String.class);
      engine = getEngineMethod.invoke(scriptEngineMgr, scriptLang);
      Method evalMethod = engine.getClass().getMethod("eval", String.class);
      invokeFunctionMethod = engine.getClass().getMethod("invokeFunction",
              String.class, Object[].class);
      evalMethod.invoke(engine, scriptText);
    } catch (Exception e) {
      wrapAndThrow(SEVERE,e, "<script> can be used only in java 6 or above");
    }
  }

  public void setFunctionName(String methodName) {
    this.functionName = methodName;
  }

  public String getFunctionName() {
    return functionName;
  }

}
