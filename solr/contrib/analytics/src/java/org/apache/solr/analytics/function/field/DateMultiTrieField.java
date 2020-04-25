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
package org.apache.solr.analytics.function.field;

import java.time.Instant;
import java.util.Date;
import java.util.function.Consumer;

import org.apache.solr.analytics.value.DateValueStream.CastingDateValueStream;
import org.apache.solr.schema.TrieDateField;

/**
 * An analytics wrapper for a multi-valued {@link TrieDateField} with DocValues enabled.
 * @deprecated Trie fields are deprecated as of Solr 7.0
 */
@Deprecated
public class DateMultiTrieField extends LongMultiTrieField implements CastingDateValueStream {

  public DateMultiTrieField(String fieldName) {
    super(fieldName);
  }

  @Override
  public void streamDates(Consumer<Date> cons) {
    streamLongs(value -> cons.accept(new Date(value)));
  }
  @Override
  public void streamStrings(Consumer<String> cons) {
    streamLongs(value -> cons.accept(Instant.ofEpochMilli(value).toString()));
  }
  @Override
  public void streamObjects(Consumer<Object> cons) {
    streamLongs(value -> cons.accept(new Date(value)));
  }
}
