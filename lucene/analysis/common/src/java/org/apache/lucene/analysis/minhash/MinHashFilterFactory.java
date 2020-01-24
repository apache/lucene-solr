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

package org.apache.lucene.analysis.minhash;

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * {@link TokenFilterFactory} for {@link MinHashFilter}.
 * @since 6.2.0
 * @lucene.spi {@value #NAME}
 */
public class MinHashFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "minHash";

  private int hashCount = MinHashFilter.DEFAULT_HASH_COUNT;
  
  private int bucketCount = MinHashFilter.DEFAULT_BUCKET_COUNT;

  private int hashSetSize = MinHashFilter.DEFAULT_HASH_SET_SIZE;
  
  private boolean withRotation;

  /**
   * Create a {@link MinHashFilterFactory}.
   */
  public MinHashFilterFactory(Map<String,String> args) {
    super(args);
    hashCount = getInt(args, "hashCount", MinHashFilter.DEFAULT_HASH_COUNT);
    bucketCount = getInt(args, "bucketCount", MinHashFilter.DEFAULT_BUCKET_COUNT);
    hashSetSize = getInt(args, "hashSetSize", MinHashFilter.DEFAULT_HASH_SET_SIZE);
    withRotation = getBoolean(args, "withRotation", bucketCount > 1);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.analysis.util.TokenFilterFactory#create(org.apache.lucene.analysis.TokenStream)
   */
  @Override
  public TokenStream create(TokenStream input) {
    return new MinHashFilter(input, hashCount, bucketCount, hashSetSize, withRotation);
  }

}
