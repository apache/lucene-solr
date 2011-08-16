package org.apache.lucene.search.cache;

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

import org.apache.lucene.index.DocTermOrds;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;

/**
 * Creates {@link DocTermOrds} instances.
 */
public class DocTermOrdsCreator extends EntryCreatorWithOptions<DocTermOrds> {

  private final String field;

  public DocTermOrdsCreator(String field, int flag) {
    super(flag);
    this.field = field;
  }

  @Override
  public DocTermOrds create(IndexReader reader) throws IOException {
    return new DocTermOrds(reader, field);
  }

  @Override
  public DocTermOrds validate(DocTermOrds entry, IndexReader reader) throws IOException {
    return entry;
  }

  @Override
  public EntryKey getCacheKey() {
    return new SimpleEntryKey(DocTermOrdsCreator.class, field);
  }
}
