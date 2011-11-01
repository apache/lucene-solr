package org.apache.lucene.index.codecs.spi;

/**
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

import java.util.Set;

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.PostingsFormat;

/**
 * This interface is used by SPI to make all codecs of a JAR
 * file available through SPI via {@link java.util.ServiceLoader}.
 * Implementations usually subclass {@link SimpleCodecProvider}.
 * @lucene.internal
 */
public interface CodecProvider {

  /** Looks up a codec by name, must return {@code null} if this provider does not support this name. **/
  Codec lookupCodec(String name);

  /** Returns all codecs, this provider supports. **/
  Set<String> availableCodecs();

  /** Looks up a postings format by name, must return {@code null} if this provider does not support this name. **/
  PostingsFormat lookupPostingsFormat(String name);

  /** Returns all postings formats, this provider supports. **/
  Set<String> availablePostingsFormats();

}
