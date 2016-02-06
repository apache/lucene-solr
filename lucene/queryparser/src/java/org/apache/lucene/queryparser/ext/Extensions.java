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
package org.apache.lucene.queryparser.ext;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;

import java.util.HashMap;
import java.util.Map;


/**
 * The {@link Extensions} class represents an extension mapping to associate
 * {@link ParserExtension} instances with extension keys. An extension key is a
 * string encoded into a Lucene standard query parser field symbol recognized by
 * {@link ExtendableQueryParser}. The query parser passes each extension field
 * token to {@link #splitExtensionField(String, String)} to separate the
 * extension key from the field identifier.
 * <p>
 * In addition to the key to extension mapping this class also defines the field
 * name overloading scheme. {@link ExtendableQueryParser} uses the given
 * extension to split the actual field name and extension key by calling
 * {@link #splitExtensionField(String, String)}. To change the order or the key
 * / field name encoding scheme users can subclass {@link Extensions} to
 * implement their own.
 * 
 * @see ExtendableQueryParser
 * @see ParserExtension
 */
public class Extensions {
  private final Map<String,ParserExtension> extensions = new HashMap<>();
  private final char extensionFieldDelimiter;
  /**
   * The default extension field delimiter character. This constant is set to
   * ':'
   */
  public static final char DEFAULT_EXTENSION_FIELD_DELIMITER = ':';

  /**
   * Creates a new {@link Extensions} instance with the
   * {@link #DEFAULT_EXTENSION_FIELD_DELIMITER} as a delimiter character.
   */
  public Extensions() {
    this(DEFAULT_EXTENSION_FIELD_DELIMITER);
  }

  /**
   * Creates a new {@link Extensions} instance
   * 
   * @param extensionFieldDelimiter
   *          the extensions field delimiter character
   */
  public Extensions(char extensionFieldDelimiter) {
    this.extensionFieldDelimiter = extensionFieldDelimiter;
  }

  /**
   * Adds a new {@link ParserExtension} instance associated with the given key.
   * 
   * @param key
   *          the parser extension key
   * @param extension
   *          the parser extension
   */
  public void add(String key, ParserExtension extension) {
    this.extensions.put(key, extension);
  }

  /**
   * Returns the {@link ParserExtension} instance for the given key or
   * <code>null</code> if no extension can be found for the key.
   * 
   * @param key
   *          the extension key
   * @return the {@link ParserExtension} instance for the given key or
   *         <code>null</code> if no extension can be found for the key.
   */
  public final ParserExtension getExtension(String key) {
    return this.extensions.get(key);
  }

  /**
   * Returns the extension field delimiter
   * 
   * @return the extension field delimiter
   */
  public char getExtensionFieldDelimiter() {
    return extensionFieldDelimiter;
  }

  /**
   * Splits a extension field and returns the field / extension part as a
   * {@link Pair}. This method tries to split on the first occurrence of the
   * extension field delimiter, if the delimiter is not present in the string
   * the result will contain a <code>null</code> value for the extension key and
   * the given field string as the field value. If the given extension field
   * string contains no field identifier the result pair will carry the given
   * default field as the field value.
   * 
   * @param defaultField
   *          the default query field
   * @param field
   *          the extension field string
   * @return a {@link Pair} with the field name as the {@link Pair#cur} and the
   *         extension key as the {@link Pair#cud}
   */
  public Pair<String,String> splitExtensionField(String defaultField,
      String field) {
    int indexOf = field.indexOf(this.extensionFieldDelimiter);
    if (indexOf < 0)
      return new Pair<>(field, null);
    final String indexField = indexOf == 0 ? defaultField : field.substring(0,
        indexOf);
    final String extensionKey = field.substring(indexOf + 1);
    return new Pair<>(indexField, extensionKey);

  }

  /**
   * Escapes an extension field. The default implementation is equivalent to
   * {@link QueryParser#escape(String)}.
   * 
   * @param extfield
   *          the extension field identifier
   * @return the extension field identifier with all special chars escaped with
   *         a backslash character.
   */
  public String escapeExtensionField(String extfield) {
    return QueryParserBase.escape(extfield);
  }

  /**
   * Builds an extension field string from a given extension key and the default
   * query field. The default field and the key are delimited with the extension
   * field delimiter character. This method makes no assumption about the order
   * of the extension key and the field. By default the extension key is
   * appended to the end of the returned string while the field is added to the
   * beginning. Special Query characters are escaped in the result.
   * <p>
   * Note: {@link Extensions} subclasses must maintain the contract between
   * {@link #buildExtensionField(String)} and
   * {@link #splitExtensionField(String, String)} where the latter inverts the
   * former.
   * </p>
   */
  public String buildExtensionField(String extensionKey) {
    return buildExtensionField(extensionKey, "");
  }

  /**
   * Builds an extension field string from a given extension key and the
   * extensions field. The field and the key are delimited with the extension
   * field delimiter character. This method makes no assumption about the order
   * of the extension key and the field. By default the extension key is
   * appended to the end of the returned string while the field is added to the
   * beginning. Special Query characters are escaped in the result.
   * <p>
   * Note: {@link Extensions} subclasses must maintain the contract between
   * {@link #buildExtensionField(String, String)} and
   * {@link #splitExtensionField(String, String)} where the latter inverts the
   * former.
   * </p>
   * 
   * @param extensionKey
   *          the extension key
   * @param field
   *          the field to apply the extension on.
   * @return escaped extension field identifier
   * @see #buildExtensionField(String) to use the default query field
   */
  public String buildExtensionField(String extensionKey, String field) {
    StringBuilder builder = new StringBuilder(field);
    builder.append(this.extensionFieldDelimiter);
    builder.append(extensionKey);
    return escapeExtensionField(builder.toString());
  }

  /**
   * This class represents a generic pair.
   * 
   * @param <Cur>
   *          the pairs first element
   * @param <Cud>
   *          the pairs last element of the pair.
   */
  public static class Pair<Cur,Cud> {

    public final Cur cur;
    public final Cud cud;

    /**
     * Creates a new Pair
     * 
     * @param cur
     *          the pairs first element
     * @param cud
     *          the pairs last element
     */
    public Pair(Cur cur, Cud cud) {
      this.cur = cur;
      this.cud = cud;
    }
  }

}
