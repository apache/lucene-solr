package org.apache.lucene.codecs;
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
import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.DocValues;

/**
 * Abstract API that provides access to one or more per-document storage
 * features. The concrete implementations provide access to the underlying
 * storage on a per-document basis corresponding to their actual
 * {@link PerDocConsumer} counterpart.
 * <p>
 * The {@link PerDocProducer} API is accessible through the
 * {@link PostingsFormat} - API providing per field consumers and producers for inverted
 * data (terms, postings) as well as per-document data.
 * 
 * @lucene.experimental
 */
public abstract class PerDocProducer implements Closeable {
  /**
   * Returns {@link DocValues} for the current field.
   * 
   * @param field
   *          the field name
   * @return the {@link DocValues} for this field or <code>null</code> if not
   *         applicable.
   * @throws IOException
   */
  public abstract DocValues docValues(String field) throws IOException;
}
