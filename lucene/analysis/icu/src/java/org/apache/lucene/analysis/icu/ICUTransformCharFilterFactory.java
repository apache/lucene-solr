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

import java.util.Arrays;
import java.util.Map;
import java.io.Reader;
import org.apache.lucene.analysis.util.CharFilterFactory;

import com.ibm.icu.text.Transliterator;

/**
 * Factory for {@link ICUTransformCharFilter}.
 * <p>
 * Supports the following attributes:
 * <ul>
 * <li>id (mandatory): A Transliterator ID, one from {@link Transliterator#getAvailableIDs()}
 * <li>direction (optional): Either 'forward' or 'reverse'. Default is forward.
 * </ul>
 * 
 * @see Transliterator
 * @since 8.3.0
 * @lucene.spi {@value #NAME}
 */
public class ICUTransformCharFilterFactory extends CharFilterFactory {

  /** SPI name */
  public static final String NAME = "icuTransform";

  private final Transliterator transliterator;
  private final int maxRollbackBufferCapacity;
  private final boolean failOnRollbackBufferOverflow;

  // TODO: add support for custom rules
  /** Creates a new ICUTransformFilterFactory */
  public ICUTransformCharFilterFactory(Map<String,String> args) {
    super(args);
    String id = require(args, "id");
    String direction = get(args, "direction", Arrays.asList("forward", "reverse"), "forward", false);
    int dir = "forward".equals(direction) ? Transliterator.FORWARD : Transliterator.REVERSE;
    int tmpCapacityHint = getInt(args, "maxRollbackBufferCapacity", ICUTransformCharFilter.DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY);
    this.maxRollbackBufferCapacity = tmpCapacityHint == -1 ? Integer.MAX_VALUE : tmpCapacityHint;
    this.failOnRollbackBufferOverflow = getBoolean(args, "failOnRollbackBufferOverflow", ICUTransformCharFilter.DEFAULT_FAIL_ON_ROLLBACK_BUFFER_OVERFLOW);
    boolean assumeExternalUnicodeNormalization = getBoolean(args, "assumeExternalUnicodeNormalization", false);
    Transliterator stockTransliterator = Transliterator.getInstance(id, dir);
    if (assumeExternalUnicodeNormalization) {
      this.transliterator = ICUTransformCharFilter.withoutUnicodeNormalization(stockTransliterator);
    } else {
      this.transliterator = stockTransliterator;
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public Reader create(Reader input) {
    return new ICUTransformCharFilter(input, transliterator, maxRollbackBufferCapacity, failOnRollbackBufferOverflow);
  }

}
