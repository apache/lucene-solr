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
package org.apache.solr.schema;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.function.FileFloatSource;
import org.apache.solr.uninverting.UninvertingReader.Type;

/** Get values from an external file instead of the index.
 *
 * <p><code>keyField</code> will normally be the unique key field, but it doesn't have to be.
 * <ul><li> It's OK to have a keyField value that can't be found in the index</li>
 * <li>It's OK to have some documents without a keyField in the file (defVal is used as the default)</li>
 * <li>It's OK for a keyField value to point to multiple documents (no uniqueness requirement)</li>
 * </ul>
 *
 * The format of the external file is simply newline separated keyFieldValue=floatValue.
 * <br>Example:
 * <br><code>doc33=1.414</code>
 * <br><code>doc34=3.14159</code>
 * <br><code>doc40=42</code>
 *
 * <p>Solr looks for the external file in the index directory under the name of
 * external_&lt;fieldname&gt; or external_&lt;fieldname&gt;.*
 *
 * <p>If any files of the latter pattern appear, the last (after being sorted by name) will be used and previous versions will be deleted.
 * This is to help support systems where one may not be able to overwrite a file (like Windows, if the file is in use).
 * <p>If the external file has already been loaded, and it is changed, those changes will not be visible until a commit has been done.
 * <p>The external file may be sorted or unsorted by the key field, but it will be substantially slower (untested) if it isn't sorted.
 * <p>Fields of this type may currently only be used as a ValueSource in a FunctionQuery.
 * <p>You can return the value as a field in the document by wrapping it like so: <code>fl=id,field(inventory_count)</code>.
 *
 * @see ExternalFileFieldReloader
 */
public class ExternalFileField extends FieldType implements SchemaAware {
  private String keyFieldName;
  private IndexSchema schema;
  private float defVal;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    restrictProps(SORT_MISSING_FIRST | SORT_MISSING_LAST);
    keyFieldName = args.remove("keyField");
    String defValS = args.remove("defVal");
    defVal = defValS == null ? 0 : Float.parseFloat(defValS);
    this.schema = schema;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortField getSortField(SchemaField field,boolean reverse) {
    FileFloatSource source = getFileFloatSource(field);
    return source.getSortField(reverse);
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    return null;
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    return getFileFloatSource(field);
  }

  /**
   * Get a FileFloatSource for the given field, using the datadir from the
   * IndexSchema
   * @param field the field to get a source for
   * @return a FileFloatSource
   */
  public FileFloatSource getFileFloatSource(SchemaField field) {
    return getFileFloatSource(field, SolrRequestInfo.getRequestInfo().getReq().getCore().getDataDir());
  }

  /**
   * Get a FileFloatSource for the given field.  Call this in preference to
   * getFileFloatSource(SchemaField) if this may be called before the Core is
   * fully initialised (eg in SolrEventListener calls).
   * @param field the field to get a source for
   * @param datadir the data directory in which to look for the external file
   * @return a FileFloatSource
   */
  public FileFloatSource getFileFloatSource(SchemaField field, String datadir) {
    // Because the float source uses a static cache, all source objects will
    // refer to the same data.
    return new FileFloatSource(field, getKeyField(), defVal, datadir);
  }

  // If no key field is defined, we use the unique key field
  private SchemaField getKeyField() {
    return keyFieldName == null ?
        schema.getUniqueKeyField() :
        schema.getField(keyFieldName);
  }

  @Override
  public void inform(IndexSchema schema) {
    this.schema = schema;

    if (keyFieldName != null && schema.getFieldType(keyFieldName).isPointField()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "keyField '" + keyFieldName + "' has a Point field type, which is not supported.");
    }
  }
}
