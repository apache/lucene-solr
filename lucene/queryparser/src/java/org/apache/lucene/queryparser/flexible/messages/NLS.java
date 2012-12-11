package org.apache.lucene.queryparser.flexible.messages;

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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * MessageBundles classes extend this class, to implement a bundle.
 * 
 * For Native Language Support (NLS), system of software internationalization.
 * 
 * This interface is similar to the NLS class in eclipse.osgi.util.NLS class -
 * initializeMessages() method resets the values of all static strings, should
 * only be called by classes that extend from NLS (see TestMessages.java for
 * reference) - performs validation of all message in a bundle, at class load
 * time - performs per message validation at runtime - see NLSTest.java for
 * usage reference
 * 
 * MessageBundle classes may subclass this type.
 */
public class NLS {

  private static Map<String, Class<? extends NLS>> bundles = 
    new HashMap<String, Class<? extends NLS>>(0);

  protected NLS() {
    // Do not instantiate
  }

  public static String getLocalizedMessage(String key) {
    return getLocalizedMessage(key, Locale.getDefault());
  }

  public static String getLocalizedMessage(String key, Locale locale) {
    Object message = getResourceBundleObject(key, locale);
    if (message == null) {
      return "Message with key:" + key + " and locale: " + locale
          + " not found.";
    }
    return message.toString();
  }

  public static String getLocalizedMessage(String key, Locale locale,
      Object... args) {
    String str = getLocalizedMessage(key, locale);

    if (args.length > 0) {
      str = MessageFormat.format(str, args);
    }

    return str;
  }

  public static String getLocalizedMessage(String key, Object... args) {
    return getLocalizedMessage(key, Locale.getDefault(), args);
  }

  /**
   * Initialize a given class with the message bundle Keys Should be called from
   * a class that extends NLS in a static block at class load time.
   * 
   * @param bundleName
   *          Property file with that contains the message bundle
   * @param clazz
   *          where constants will reside
   */
  protected static void initializeMessages(String bundleName, Class<? extends NLS> clazz) {
    try {
      load(clazz);
      if (!bundles.containsKey(bundleName))
        bundles.put(bundleName, clazz);
    } catch (Throwable e) {
      // ignore all errors and exceptions
      // because this function is supposed to be called at class load time.
    }
  }

  private static Object getResourceBundleObject(String messageKey, Locale locale) {

    // slow resource checking
    // need to loop thru all registered resource bundles
    for (Iterator<String> it = bundles.keySet().iterator(); it.hasNext();) {
      Class<? extends NLS> clazz = bundles.get(it.next());
      ResourceBundle resourceBundle = ResourceBundle.getBundle(clazz.getName(),
          locale);
      if (resourceBundle != null) {
        try {
          Object obj = resourceBundle.getObject(messageKey);
          if (obj != null)
            return obj;
        } catch (MissingResourceException e) {
          // just continue it might be on the next resource bundle
        }
      }
    }
    // if resource is not found
    return null;
  }

  private static void load(Class<? extends NLS> clazz) {
    final Field[] fieldArray = clazz.getDeclaredFields();

    boolean isFieldAccessible = (clazz.getModifiers() & Modifier.PUBLIC) != 0;

    // build a map of field names to Field objects
    final int len = fieldArray.length;
    Map<String, Field> fields = new HashMap<String, Field>(len * 2);
    for (int i = 0; i < len; i++) {
      fields.put(fieldArray[i].getName(), fieldArray[i]);
      loadfieldValue(fieldArray[i], isFieldAccessible, clazz);
    }
  }

  private static void loadfieldValue(Field field, boolean isFieldAccessible,
      Class<? extends NLS> clazz) {
    int MOD_EXPECTED = Modifier.PUBLIC | Modifier.STATIC;
    int MOD_MASK = MOD_EXPECTED | Modifier.FINAL;
    if ((field.getModifiers() & MOD_MASK) != MOD_EXPECTED)
      return;

    // Set a value for this empty field.
    if (!isFieldAccessible)
      makeAccessible(field);
    try {
      field.set(null, field.getName());
      validateMessage(field.getName(), clazz);
    } catch (IllegalArgumentException e) {
      // should not happen
    } catch (IllegalAccessException e) {
      // should not happen
    }
  }

  /**
   * @param key
   *          - Message Key
   */
  private static void validateMessage(String key, Class<? extends NLS> clazz) {
    // Test if the message is present in the resource bundle
    try {
      ResourceBundle resourceBundle = ResourceBundle.getBundle(clazz.getName(),
          Locale.getDefault());
      if (resourceBundle != null) {
        Object obj = resourceBundle.getObject(key);
        //if (obj == null)
        //  System.err.println("WARN: Message with key:" + key + " and locale: "
        //      + Locale.getDefault() + " not found.");
      }
    } catch (MissingResourceException e) {
      //System.err.println("WARN: Message with key:" + key + " and locale: "
      //    + Locale.getDefault() + " not found.");
    } catch (Throwable e) {
      // ignore all other errors and exceptions
      // since this code is just a test to see if the message is present on the
      // system
    }
  }

  /*
   * Make a class field accessible
   */
  private static void makeAccessible(final Field field) {
    if (System.getSecurityManager() == null) {
      field.setAccessible(true);
    } else {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          field.setAccessible(true);
          return null;
        }
      });
    }
  }
}
