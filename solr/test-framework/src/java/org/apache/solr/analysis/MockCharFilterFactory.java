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
package org.apache.solr.analysis;

import java.io.Reader;
import java.util.Map;

import org.apache.lucene.analysis.MockCharFilter;
import org.apache.lucene.analysis.util.CharFilterFactory;

/**
 * Factory for {@link MockCharFilter} for testing purposes.
 */
public class MockCharFilterFactory extends CharFilterFactory {

  /** SPI name */
  public static final String NAME = "mock";

  final int remainder;

  /** Creates a new MockCharFilterFactory */
  public MockCharFilterFactory(Map<String,String> args) {
    super(args);
    remainder = requireInt(args, "remainder");
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public MockCharFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public MockCharFilter create(Reader input) {
    return new MockCharFilter(input, remainder);
  }
}
