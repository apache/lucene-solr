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
package org.apache.solr.handler.extraction;


/**
 * The various Solr Parameters names to use when extracting content.
 *
 **/
public interface ExtractingParams {

  /**
   * Map all generated attribute names to field names with lowercase and underscores.
   */
  public static final String LOWERNAMES = "lowernames";

  /**
   * if true, ignore TikaException (give up to extract text but index meta data)
   */
  public static final String IGNORE_TIKA_EXCEPTION = "ignoreTikaException";


  /**
   * The param prefix for mapping Tika metadata to Solr fields.
   * <p>
   * To map a field, add a name like:
   * <pre>fmap.title=solr.title</pre>
   *
   * In this example, the tika "title" metadata value will be added to a Solr field named "solr.title"
   *
   *
   */
  public static final String MAP_PREFIX = "fmap.";

  /**
   * Pass in literal values to be added to the document, as in
   * <pre>
   *  literal.myField=Foo 
   * </pre>
   *
   */
  public static final String LITERALS_PREFIX = "literal.";


  /**
   * Restrict the extracted parts of a document to be indexed
   *  by passing in an XPath expression.  All content that satisfies the XPath expr.
   * will be passed to the {@link SolrContentHandler}.
   * <p>
   * See Tika's docs for what the extracted document looks like.
   * @see #CAPTURE_ELEMENTS
   */
  public static final String XPATH_EXPRESSION = "xpath";


  /**
   * Only extract and return the content, do not index it.
   */
  public static final String EXTRACT_ONLY = "extractOnly";

  /**
   * Content output format if extractOnly is true. Default is "xml", alternative is "text".
   */
  public static final String EXTRACT_FORMAT = "extractFormat";

  /**
   * Capture attributes separately according to the name of the element, instead of just adding them to the string buffer
   */
  public static final String CAPTURE_ATTRIBUTES = "captureAttr";

  /**
   * Literal field values will by default override other values such as metadata and content. Set this to false to revert to pre-4.0 behaviour
   */
  public static final String LITERALS_OVERRIDE = "literalsOverride";

  /**
   * Capture the specified fields (and everything included below it that isn't capture by some other capture field) separately from the default.  This is different
   * then the case of passing in an XPath expression.
   * <p>
   * The Capture field is based on the localName returned to the {@link SolrContentHandler}
   * by Tika, not to be confused by the mapped field.  The field name can then
   * be mapped into the index schema.
   * <p>
   * For instance, a Tika document may look like:
   * <pre>
   *  &lt;html&gt;
   *    ...
   *    &lt;body&gt;
   *      &lt;p&gt;some text here.  &lt;div&gt;more text&lt;/div&gt;&lt;/p&gt;
   *      Some more text
   *    &lt;/body&gt;
   * </pre>
   * By passing in the p tag, you could capture all P tags separately from the rest of the t
   * Thus, in the example, the capture of the P tag would be: "some text here.  more text"
   *
   */
  public static final String CAPTURE_ELEMENTS = "capture";

  /**
   * The type of the stream.  If not specified, Tika will use mime type detection.
   */
  public static final String STREAM_TYPE = "stream.type";


  /**
   * Optional.  The file name. If specified, Tika can take this into account while
   * guessing the MIME type.
   */
  public static final String RESOURCE_NAME = "resource.name";

  /**
   * Optional. The password for this resource. Will be used instead of the rule based password lookup mechanisms 
   */
  public static final String RESOURCE_PASSWORD = "resource.password";

  /**
   * Optional.  If specified, the prefix will be prepended to all Metadata, such that it would be possible
   * to setup a dynamic field to automatically capture it
   */
  public static final String UNKNOWN_FIELD_PREFIX = "uprefix";

  /**
   * Optional.  If specified and the name of a potential field cannot be determined, the default Field specified
   * will be used instead.
   */
  public static final String DEFAULT_FIELD = "defaultField";

  /**
   * Optional. If specified, loads the file as a source for password lookups for Tika encrypted documents.
   * <p>
   * File format is Java properties format with one key=value per line.
   * The key is evaluated as a regex against the file name, and the value is the password
   * The rules are evaluated top-bottom, i.e. the first match will be used
   * If you want a fallback password to be always used, supply a .*=&lt;defaultmypassword&gt; at the end  
   */
  public static final String PASSWORD_MAP_FILE = "passwordsFile";
}
