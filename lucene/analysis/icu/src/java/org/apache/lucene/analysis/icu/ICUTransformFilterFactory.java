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
package org.apache.lucene.analysis.icu;

import com.ibm.icu.text.Transliterator;
import java.util.Arrays;
import java.util.Map;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;

/**
 * Factory for {@link ICUTransformFilter}.
 *
 * <p>Supports the following attributes:
 *
 * <ul>
 *   <li>id (mandatory): A Transliterator ID, one from {@link Transliterator#getAvailableIDs()}
 *   <li>direction (optional): Either 'forward' or 'reverse'. Default is forward.
 * </ul>
 *
 * @see Transliterator
 * @since 3.1.0
 * @lucene.spi {@value #NAME}
 */
public class ICUTransformFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "icuTransform";

  private final Transliterator transliterator;

  // TODO: add support for custom rules
  /** Creates a new ICUTransformFilterFactory */
  public ICUTransformFilterFactory(Map<String, String> args) {
    super(args);
    String id = require(args, "id");
    String direction =
        get(args, "direction", Arrays.asList("forward", "reverse"), "forward", false);
    int dir = "forward".equals(direction) ? Transliterator.FORWARD : Transliterator.REVERSE;
    transliterator = Transliterator.getInstance(id, dir);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public ICUTransformFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new ICUTransformFilter(input, transliterator);
  }

  @Override
  public TokenStream normalize(TokenStream input) {
    return create(input);
  }
}
