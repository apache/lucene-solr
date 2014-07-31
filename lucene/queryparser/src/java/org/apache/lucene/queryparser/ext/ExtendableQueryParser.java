package org.apache.lucene.queryparser.ext;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.ext.Extensions.Pair;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

/**
 * The {@link ExtendableQueryParser} enables arbitrary query parser extension
 * based on a customizable field naming scheme. The lucene query syntax allows
 * implicit and explicit field definitions as query prefix followed by a colon
 * (':') character. The {@link ExtendableQueryParser} allows to encode extension
 * keys into the field symbol associated with a registered instance of
 * {@link ParserExtension}. A customizable separation character separates the
 * extension key from the actual field symbol. The {@link ExtendableQueryParser}
 * splits (@see {@link Extensions#splitExtensionField(String, String)}) the
 * extension key from the field symbol and tries to resolve the associated
 * {@link ParserExtension}. If the parser can't resolve the key or the field
 * token does not contain a separation character, {@link ExtendableQueryParser}
 * yields the same behavior as its super class {@link QueryParser}. Otherwise,
 * if the key is associated with a {@link ParserExtension} instance, the parser
 * builds an instance of {@link ExtensionQuery} to be processed by
 * {@link ParserExtension#parse(ExtensionQuery)}.If a extension field does not
 * contain a field part the default field for the query will be used.
 * <p>
 * To guarantee that an extension field is processed with its associated
 * extension, the extension query part must escape any special characters like
 * '*' or '['. If the extension query contains any whitespace characters, the
 * extension query part must be enclosed in quotes.
 * Example ('_' used as separation character):
 * <pre>
 *   title_customExt:"Apache Lucene\?" OR content_customExt:prefix\*
 * </pre>
 * 
 * Search on the default field:
 * <pre>
 *   _customExt:"Apache Lucene\?" OR _customExt:prefix\*
 * </pre>
 * </p>
 * <p>
 * The {@link ExtendableQueryParser} itself does not implement the logic how
 * field and extension key are separated or ordered. All logic regarding the
 * extension key and field symbol parsing is located in {@link Extensions}.
 * Customized extension schemes should be implemented by sub-classing
 * {@link Extensions}.
 * </p>
 * <p>
 * For details about the default encoding scheme see {@link Extensions}.
 * </p>
 * 
 * @see Extensions
 * @see ParserExtension
 * @see ExtensionQuery
 */
public class ExtendableQueryParser extends QueryParser {

  private final String defaultField;
  private final Extensions extensions;

  /**
   * Default empty extensions instance
   */
  private static final Extensions DEFAULT_EXTENSION = new Extensions();

  /**
   * Creates a new {@link ExtendableQueryParser} instance
   * 
   * @param matchVersion
   *          the lucene version to use.
   * @param f
   *          the default query field
   * @param a
   *          the analyzer used to find terms in a query string
   */
  public ExtendableQueryParser(final Version matchVersion, final String f,
      final Analyzer a) {
    this(matchVersion, f, a, DEFAULT_EXTENSION);

  }

  /**
   * Creates a new {@link ExtendableQueryParser} instance
   * 
   * @param matchVersion
   *          the lucene version to use.
   * @param f
   *          the default query field
   * @param a
   *          the analyzer used to find terms in a query string
   * @param ext
   *          the query parser extensions
   */
  public ExtendableQueryParser(final Version matchVersion, final String f,
      final Analyzer a, final Extensions ext) {
    super(matchVersion, f, a);
    this.defaultField = f;
    this.extensions = ext;
  }

  /**
   * Returns the extension field delimiter character.
   * 
   * @return the extension field delimiter character.
   */
  public char getExtensionFieldDelimiter() {
    return extensions.getExtensionFieldDelimiter();
  }

  @Override
  protected Query getFieldQuery(final String field, final String queryText, boolean quoted)
      throws ParseException {
    final Pair<String,String> splitExtensionField = this.extensions
        .splitExtensionField(defaultField, field);
    final ParserExtension extension = this.extensions
        .getExtension(splitExtensionField.cud);
    if (extension != null) {
      return extension.parse(new ExtensionQuery(this, splitExtensionField.cur,
          queryText));
    }
    return super.getFieldQuery(field, queryText, quoted);
  }

}
