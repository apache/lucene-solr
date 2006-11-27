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

package org.apache.solr.schema;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;

import org.apache.solr.request.*;

import java.util.Map;
import java.io.IOException;

/** <code>CompressableField</code> is an abstract field type which enables a
 * field to be compressed (by specifying <code>compressed="true"</code> at the
 * field definition level) and provides optional support for specifying a
 * threshold at which compression is enabled.
 *
 * Optional settings:
 * <ul>
 *  <li><code>compressThreshold</code>: length, in characters, at which point the 
 *      field contents should be compressed [default: 0]</li>
 * </ul></p>
 * 
 * TODO: Enable compression level specification (not yet in lucene)
 * 
 * @author klaas
 * @version $Id$
 */
public abstract class CompressableField extends FieldType {
  /* if field size (in characters) is greater than this threshold, the field 
     will be stored compressed */
  public static int DEFAULT_COMPRESS_THRESHOLD = 0;

  int compressThreshold;

  private static String CT = "compressThreshold";

  protected void init(IndexSchema schema, Map<String,String> args) {
    SolrParams p = new MapSolrParams(args);
    compressThreshold = p.getInt(CT, DEFAULT_COMPRESS_THRESHOLD);
    args.remove(CT);
    super.init(schema, args);    
  }

    /* Helpers for field construction */
  protected Field.Store getFieldStore(SchemaField field,
                                      String internalVal) {
    /* compress field if length exceeds threshold */
    if(field.isCompressed()) {
      return internalVal.length() >= compressThreshold ? 
        Field.Store.COMPRESS : Field.Store.YES;
    } else
      return super.getFieldStore(field, internalVal);
  } 
}
