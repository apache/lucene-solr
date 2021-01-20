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
package org.apache.lucene.analysis.payloads;

import java.util.Map;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;

/**
 * Factory for {@link DelimitedPayloadTokenFilter}.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_dlmtd" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.DelimitedPayloadTokenFilterFactory" encoder="float" delimiter="|"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @since 3.1
 * @lucene.spi {@value #NAME}
 */
public class DelimitedPayloadTokenFilterFactory extends TokenFilterFactory
    implements ResourceLoaderAware {

  /** SPI name */
  public static final String NAME = "delimitedPayload";

  public static final String ENCODER_ATTR = "encoder";
  public static final String DELIMITER_ATTR = "delimiter";

  private final String encoderClass;
  private final char delimiter;

  private PayloadEncoder encoder;

  /** Creates a new DelimitedPayloadTokenFilterFactory */
  public DelimitedPayloadTokenFilterFactory(Map<String, String> args) {
    super(args);
    encoderClass = require(args, ENCODER_ATTR);
    delimiter = getChar(args, DELIMITER_ATTR, '|');
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public DelimitedPayloadTokenFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public DelimitedPayloadTokenFilter create(TokenStream input) {
    return new DelimitedPayloadTokenFilter(input, delimiter, encoder);
  }

  @Override
  public void inform(ResourceLoader loader) {
    if (encoderClass.equals("float")) {
      encoder = new FloatEncoder();
    } else if (encoderClass.equals("integer")) {
      encoder = new IntegerEncoder();
    } else if (encoderClass.equals("identity")) {
      encoder = new IdentityEncoder();
    } else {
      encoder = loader.newInstance(encoderClass, PayloadEncoder.class);
    }
  }
}
