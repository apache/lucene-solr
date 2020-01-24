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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

/**
 * Utilities for accessing message resources.
 */
public class MessageUtils {

  public static final String MESSAGE_BUNDLE_BASENAME = "org/apache/lucene/luke/app/desktop/messages/messages";

  public static String getLocalizedMessage(String key) {
    return bundle.getString(key);
  }

  public static String getLocalizedMessage(String key, Object... args) {
    String pattern = bundle.getString(key);
    return new MessageFormat(pattern, Locale.ENGLISH).format(args);
  }

  // https://stackoverflow.com/questions/4659929/how-to-use-utf-8-in-resource-properties-with-resourcebundle
  private static ResourceBundle.Control UTF8_RESOURCEBUNDLE_CONTROL = new ResourceBundle.Control() {
    @Override
    public ResourceBundle newBundle(String baseName, Locale locale, String format, ClassLoader loader, boolean reload) throws IllegalAccessException, InstantiationException, IOException {
      String bundleName = toBundleName(baseName, locale);
      String resourceName = toResourceName(bundleName, "properties");
      try (InputStream is = loader.getResourceAsStream(resourceName)) {
        return new PropertyResourceBundle(new InputStreamReader(is, StandardCharsets.UTF_8));
      }
    }
  };

  private static ResourceBundle bundle = ResourceBundle.getBundle(MESSAGE_BUNDLE_BASENAME, Locale.ENGLISH, UTF8_RESOURCEBUNDLE_CONTROL);

  private MessageUtils() {
  }
}
