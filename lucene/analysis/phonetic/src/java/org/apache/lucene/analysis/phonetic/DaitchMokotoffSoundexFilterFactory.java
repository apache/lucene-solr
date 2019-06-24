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
package org.apache.lucene.analysis.phonetic;

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link DaitchMokotoffSoundexFilter}.
 *
 * Create tokens based on Daitch–Mokotoff Soundex phonetic filter.
 * <p>
 * This takes one optional argument:
 * <dl>
 *  <dt>inject</dt><dd> (default=true) add tokens to the stream with the offset=0</dd>
 * </dl>
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_phonetic" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.DaitchMokotoffSoundexFilterFactory" inject="true"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @see DaitchMokotoffSoundexFilter
 *
 * @lucene.experimental
 * @since 5.0.0
 * @lucene.spi {@value #NAME}
 */
public class DaitchMokotoffSoundexFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "daitchMokotoffSoundex";

  /** parameter name: true if encoded tokens should be added as synonyms */
  public static final String INJECT = "inject"; // boolean

  final boolean inject; //accessed by the test

  /** Creates a new PhoneticFilterFactory */
  public DaitchMokotoffSoundexFilterFactory(Map<String,String> args) {
    super(args);
    inject = getBoolean(args, INJECT, true);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public DaitchMokotoffSoundexFilter create(TokenStream input) {
    return new DaitchMokotoffSoundexFilter(input, inject);
  }

}
