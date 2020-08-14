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
import org.apache.solr.common.util.NamedList;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;

/**
 * <p>
 * Implements most of the interactive development functionality
 * </p>
 * <p/>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.3
 */
class DebugLogger {
  private Stack<DebugInfo> debugStack;

  @SuppressWarnings({"rawtypes"})
  NamedList output;
//  private final SolrWriter writer1;

  private static final String LINE = "---------------------------------------------";

  private MessageFormat fmt = new MessageFormat(
          "----------- row #{0}-------------", Locale.ROOT);

  boolean enabled = true;

  @SuppressWarnings({"rawtypes"})
  public DebugLogger() {
//    writer = solrWriter;
    output = new NamedList();
    debugStack = new Stack<DebugInfo>() {

      @Override
      public DebugInfo pop() {
        if (size() == 1)
          throw new DataImportHandlerException(
                  DataImportHandlerException.SEVERE, "Stack is becoming empty");
        return super.pop();
      }
    };
    debugStack.push(new DebugInfo(null, DIHLogLevels.NONE, null));
    output = debugStack.peek().lst;
  }

    private DebugInfo peekStack() {
    return debugStack.isEmpty() ? null : debugStack.peek();
  }

  @SuppressWarnings({"unchecked"})
  public void log(DIHLogLevels event, String name, Object row) {
    if (event == DIHLogLevels.DISABLE_LOGGING) {
      enabled = false;
      return;
    } else if (event == DIHLogLevels.ENABLE_LOGGING) {
      enabled = true;
      return;
    }

    if (!enabled && event != DIHLogLevels.START_ENTITY
            && event != DIHLogLevels.END_ENTITY) {
      return;
    }

    if (event == DIHLogLevels.START_DOC) {
      debugStack.push(new DebugInfo(null, DIHLogLevels.START_DOC, peekStack()));
    } else if (DIHLogLevels.START_ENTITY == event) {
      debugStack
              .push(new DebugInfo(name, DIHLogLevels.START_ENTITY, peekStack()));
    } else if (DIHLogLevels.ENTITY_OUT == event
            || DIHLogLevels.PRE_TRANSFORMER_ROW == event) {
      if (debugStack.peek().type == DIHLogLevels.START_ENTITY
              || debugStack.peek().type == DIHLogLevels.START_DOC) {
        debugStack.peek().lst.add(null, fmt.format(new Object[]{++debugStack
                .peek().rowCount}));
        addToNamedList(debugStack.peek().lst, row);
        debugStack.peek().lst.add(null, LINE);
      }
    } else if (event == DIHLogLevels.ROW_END) {
      popAllTransformers();
    } else if (DIHLogLevels.END_ENTITY == event) {
      while (debugStack.pop().type != DIHLogLevels.START_ENTITY)
        ;
    } else if (DIHLogLevels.END_DOC == event) {
      while (debugStack.pop().type != DIHLogLevels.START_DOC)
        ;
    } else if (event == DIHLogLevels.TRANSFORMER_EXCEPTION) {
      debugStack.push(new DebugInfo(name, event, peekStack()));
      debugStack.peek().lst.add("EXCEPTION",
              getStacktraceString((Exception) row));
    } else if (DIHLogLevels.TRANSFORMED_ROW == event) {
      debugStack.push(new DebugInfo(name, event, peekStack()));
      debugStack.peek().lst.add(null, LINE);
      addToNamedList(debugStack.peek().lst, row);
      debugStack.peek().lst.add(null, LINE);
      if (row instanceof DataImportHandlerException) {
        DataImportHandlerException dataImportHandlerException = (DataImportHandlerException) row;
        dataImportHandlerException.debugged = true;
      }
    } else if (DIHLogLevels.ENTITY_META == event) {
      popAllTransformers();
      debugStack.peek().lst.add(name, row);
    } else if (DIHLogLevels.ENTITY_EXCEPTION == event) {
      if (row instanceof DataImportHandlerException) {
        DataImportHandlerException dihe = (DataImportHandlerException) row;
        if (dihe.debugged)
          return;
        dihe.debugged = true;
      }

      popAllTransformers();
      debugStack.peek().lst.add("EXCEPTION",
              getStacktraceString((Exception) row));
    }
  }

  private void popAllTransformers() {
    while (true) {
      DIHLogLevels type = debugStack.peek().type;
      if (type == DIHLogLevels.START_DOC || type == DIHLogLevels.START_ENTITY)
        break;
      debugStack.pop();
    }
  }

  @SuppressWarnings({"unchecked"})
  private void addToNamedList(@SuppressWarnings({"rawtypes"})NamedList nl, Object row) {
    if (row instanceof List) {
      @SuppressWarnings({"rawtypes"})
      List list = (List) row;
      @SuppressWarnings({"rawtypes"})
      NamedList l = new NamedList();
      nl.add(null, l);
      for (Object o : list) {
        Map<String, Object> map = (Map<String, Object>) o;
        for (Map.Entry<String, Object> entry : map.entrySet())
          nl.add(entry.getKey(), entry.getValue());
      }
    } else if (row instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) row;
      for (Map.Entry<String, Object> entry : map.entrySet())
        nl.add(entry.getKey(), entry.getValue());
    }
  }

  @SuppressWarnings({"rawtypes"})
  DataSource wrapDs(final DataSource ds) {
    return new DataSource() {
      @Override
      public void init(Context context, Properties initProps) {
        ds.init(context, initProps);
      }

      @Override
      public void close() {
        ds.close();
      }

      @Override
      public Object getData(String query) {
        log(DIHLogLevels.ENTITY_META, "query", query);
        long start = System.nanoTime();
        try {
          return ds.getData(query);
        } catch (DataImportHandlerException de) {
          log(DIHLogLevels.ENTITY_EXCEPTION,
                  null, de);
          throw de;
        } catch (Exception e) {
          log(DIHLogLevels.ENTITY_EXCEPTION,
                  null, e);
          DataImportHandlerException de = new DataImportHandlerException(
                  DataImportHandlerException.SEVERE, "", e);
          de.debugged = true;
          throw de;
        } finally {
          log(DIHLogLevels.ENTITY_META, "time-taken", DocBuilder
                  .getTimeElapsedSince(start));
        }
      }
    };
  }

  Transformer wrapTransformer(final Transformer t) {
    return new Transformer() {
      @Override
      public Object transformRow(Map<String, Object> row, Context context) {
        log(DIHLogLevels.PRE_TRANSFORMER_ROW, null, row);
        String tName = getTransformerName(t);
        Object result = null;
        try {
          result = t.transformRow(row, context);
          log(DIHLogLevels.TRANSFORMED_ROW, tName, result);
        } catch (DataImportHandlerException de) {
          log(DIHLogLevels.TRANSFORMER_EXCEPTION, tName, de);
          de.debugged = true;
          throw de;
        } catch (Exception e) {
          log(DIHLogLevels.TRANSFORMER_EXCEPTION, tName, e);
          DataImportHandlerException de = new DataImportHandlerException(DataImportHandlerException.SEVERE, "", e);
          de.debugged = true;
          throw de;
        }
        return result;
      }
    };
  }

  public static String getStacktraceString(Exception e) {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }

  static String getTransformerName(Transformer t) {
    @SuppressWarnings({"rawtypes"})
    Class transClass = t.getClass();
    if (t instanceof EntityProcessorWrapper.ReflectionTransformer) {
      return ((EntityProcessorWrapper.ReflectionTransformer) t).trans;
    }
    if (t instanceof ScriptTransformer) {
      ScriptTransformer scriptTransformer = (ScriptTransformer) t;
      return "script:" + scriptTransformer.getFunctionName();
    }
    if (transClass.getPackage().equals(DebugLogger.class.getPackage())) {
      return transClass.getSimpleName();
    } else {
      return transClass.getName();
    }
  }

  private static class DebugInfo {
    String name;

    int tCount, rowCount;

    @SuppressWarnings({"rawtypes"})
    NamedList lst;

    DIHLogLevels type;

    DebugInfo parent;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public DebugInfo(String name, DIHLogLevels type, DebugInfo parent) {
      this.name = name;
      this.type = type;
      this.parent = parent;
      lst = new NamedList();
      if (parent != null) {
        String displayName = null;
        if (type == DIHLogLevels.START_ENTITY) {
          displayName = "entity:" + name;
        } else if (type == DIHLogLevels.TRANSFORMED_ROW
                || type == DIHLogLevels.TRANSFORMER_EXCEPTION) {
          displayName = "transformer:" + name;
        } else if (type == DIHLogLevels.START_DOC) {
          this.name = displayName = "document#" + SolrWriter.getDocCount();
        }
        parent.lst.add(displayName, lst);
      }
    }
  }

}
