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

import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import java.io.Serializable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.StringLayout;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AbstractOutputStreamAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;

/** Log appender for text areas */
@Plugin(name = "TextArea", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public final class TextAreaAppender extends AbstractAppender {

  private static JTextArea textArea;

  private static final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private static final Lock readLock = rwLock.readLock();
  private static final Lock writeLock = rwLock.writeLock();

  protected TextAreaAppender(String name, Filter filter,
                             org.apache.logging.log4j.core.Layout<? extends Serializable> layout, final boolean ignoreExceptions) {
    super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
  }

  public static void setTextArea(JTextArea ta) {
    writeLock.lock();
    try {
      if (textArea != null) {
        throw new IllegalStateException("TextArea already set.");
      }
      textArea = ta;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void append(LogEvent event) {
    readLock.lock();
    try {
      if (textArea == null) {
        // just ignore any events logged before the area is available
        return;
      }
  
      final String message = ((StringLayout) getLayout()).toSerializable(event);
      SwingUtilities.invokeLater(() -> {
        textArea.append(message);
      });
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Builds TextAreaAppender instances.
   *
   * @param <B> The type to build
   */
  public static class Builder<B extends Builder<B>> extends AbstractOutputStreamAppender.Builder<B>
      implements org.apache.logging.log4j.core.util.Builder<TextAreaAppender> {

    @Override
    public TextAreaAppender build() {
      return new TextAreaAppender(getName(), getFilter(), getOrCreateLayout(), true);
    }
  }

  @PluginBuilderFactory
  public static <B extends Builder<B>> B newBuilder() {
    return new Builder<B>().asBuilder();
  }

}
