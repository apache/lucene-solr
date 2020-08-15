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
package org.apache.solr.handler.dataimport;

import static org.apache.solr.handler.dataimport.HTMLStripTransformer.TRUE;

import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@link Transformer} instance which converts a {@link Clob} to a {@link String}.
 * <p>
 * Refer to <a href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * <p>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.4
 */
public class ClobTransformer extends Transformer {
  @Override
  public Object transformRow(Map<String, Object> aRow, Context context) {
    for (Map<String, String> map : context.getAllEntityFields()) {
      if (!TRUE.equals(map.get(CLOB))) continue;
      String column = map.get(DataImporter.COLUMN);
      String srcCol = map.get(RegexTransformer.SRC_COL_NAME);
      if (srcCol == null)
        srcCol = column;
      Object o = aRow.get(srcCol);
      if (o instanceof List) {
        @SuppressWarnings({"unchecked"})
        List<Clob> inputs = (List<Clob>) o;
        List<String> results = new ArrayList<>();
        for (Object input : inputs) {
          if (input instanceof Clob) {
            Clob clob = (Clob) input;
            results.add(readFromClob(clob));
          }
        }
        aRow.put(column, results);
      } else {
        if (o instanceof Clob) {
          Clob clob = (Clob) o;
          aRow.put(column, readFromClob(clob));
        }
      }
    }
    return aRow;
  }

  private String readFromClob(Clob clob) {
    Reader reader = FieldReaderDataSource.readCharStream(clob);
    StringBuilder sb = new StringBuilder();
    char[] buf = new char[1024];
    int len;
    try {
      while ((len = reader.read(buf)) != -1) {
        sb.append(buf, 0, len);
      }
    } catch (IOException e) {
      DataImportHandlerException.wrapAndThrow(DataImportHandlerException.SEVERE, e);
    }
    return sb.toString();
  }

  public static final String CLOB = "clob";
}
