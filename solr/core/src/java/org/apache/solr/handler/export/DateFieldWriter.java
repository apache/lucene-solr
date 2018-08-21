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

package org.apache.solr.handler.export;

import java.io.IOException;
import java.util.Date;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.solr.common.MapWriter;

class DateFieldWriter extends FieldWriter {
  private String field;

  public DateFieldWriter(String field) {
    this.field = field;
  }

  public boolean write(int docId, LeafReader reader, MapWriter.EntryWriter ew, int fieldIndex) throws IOException {
    NumericDocValues vals = DocValues.getNumeric(reader, this.field);
    long val;
    if (vals.advance(docId) == docId) {
      val = vals.longValue();
    } else {
      return false;
    }
    ew.put(this.field, new Date(val));
    return true;
  }
}
