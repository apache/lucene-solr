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

package org.apache.lucene.luke.app.desktop.util;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import javax.swing.JTextArea;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Log appender for text areas */
@Plugin(name = "TextAreaAppender", category = "Core", elementType = "appender", printObject = true)
public final class TextAreaAppender extends AbstractAppender {

  private static JTextArea textArea;

  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final Lock readLock = rwLock.readLock();

  protected TextAreaAppender(String name, Filter filter,
                             org.apache.logging.log4j.core.Layout<? extends Serializable> layout, final boolean ignoreExceptions) {
    super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
  }

  public static void setTextArea(JTextArea ta) {
    if (textArea != null) {
      throw new IllegalStateException("TextArea already set.");
    }
    textArea = ta;
  }

  @Override
  public void append(LogEvent event) {
    if (textArea == null) {
      throw new IllegalStateException();
    }

    readLock.lock();
    try {
      String message = new String(getLayout().toByteArray(event), StandardCharsets.UTF_8);
      textArea.append(message);
    } finally {
      readLock.unlock();
    }
  }

  @PluginFactory
  public static TextAreaAppender createAppender(
      @PluginAttribute("name") String name,
      @PluginElement("Layout") Layout<? extends Serializable> layout,
      @PluginElement("Filter") final Filter filter,
      @PluginAttribute("otherAttribute") String otherAttribute
  ) {
    if (name == null) {
      LOGGER.error("No name provided for MyCustomAppenderImpl");
      return null;
    }
    if (layout == null) {
      layout = PatternLayout.createDefaultLayout();
    }

    return new TextAreaAppender(name, filter, layout, true);
  }

}
