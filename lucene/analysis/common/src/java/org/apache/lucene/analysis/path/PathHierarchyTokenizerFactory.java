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
package org.apache.lucene.analysis.path;

import java.util.Map;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;

/**
 * Factory for {@link PathHierarchyTokenizer}.
 *
 * <p>This factory is typically configured for use only in the <code>index</code> Analyzer (or only
 * in the <code>query</code> Analyzer, but never both).
 *
 * <p>For example, in the configuration below a query for <code>Books/NonFic</code> will match
 * documents indexed with values like <code>Books/NonFic</code>, <code>Books/NonFic/Law</code>,
 * <code>Books/NonFic/Science/Physics</code>, etc. But it will not match documents indexed with
 * values like <code>Books</code>, or <code>Books/Fic</code>...
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="descendent_path" class="solr.TextField"&gt;
 *   &lt;analyzer type="index"&gt;
 *     &lt;tokenizer class="solr.PathHierarchyTokenizerFactory" delimiter="/" /&gt;
 *   &lt;/analyzer&gt;
 *   &lt;analyzer type="query"&gt;
 *     &lt;tokenizer class="solr.KeywordTokenizerFactory" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;
 * </pre>
 *
 * <p>In this example however we see the oposite configuration, so that a query for <code>
 * Books/NonFic/Science/Physics</code> would match documents containing <code>Books/NonFic</code>,
 * <code>Books/NonFic/Science</code>, or <code>Books/NonFic/Science/Physics</code>, but not <code>
 * Books/NonFic/Science/Physics/Theory</code> or <code>Books/NonFic/Law</code>.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="descendent_path" class="solr.TextField"&gt;
 *   &lt;analyzer type="index"&gt;
 *     &lt;tokenizer class="solr.KeywordTokenizerFactory" /&gt;
 *   &lt;/analyzer&gt;
 *   &lt;analyzer type="query"&gt;
 *     &lt;tokenizer class="solr.PathHierarchyTokenizerFactory" delimiter="/" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;
 * </pre>
 *
 * @since 3.1
 * @lucene.spi {@value #NAME}
 */
public class PathHierarchyTokenizerFactory extends TokenizerFactory {

  /** SPI name */
  public static final String NAME = "pathHierarchy";

  private final char delimiter;
  private final char replacement;
  private final boolean reverse;
  private final int skip;

  /** Creates a new PathHierarchyTokenizerFactory */
  public PathHierarchyTokenizerFactory(Map<String, String> args) {
    super(args);
    delimiter = getChar(args, "delimiter", PathHierarchyTokenizer.DEFAULT_DELIMITER);
    replacement = getChar(args, "replace", delimiter);
    reverse = getBoolean(args, "reverse", false);
    skip = getInt(args, "skip", PathHierarchyTokenizer.DEFAULT_SKIP);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public PathHierarchyTokenizerFactory() {
    throw defaultCtorException();
  }

  @Override
  public Tokenizer create(AttributeFactory factory) {
    if (reverse) {
      return new ReversePathHierarchyTokenizer(factory, delimiter, replacement, skip);
    }
    return new PathHierarchyTokenizer(factory, delimiter, replacement, skip);
  }
}
