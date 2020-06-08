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
package org.apache.solr.update.processor;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.JsonPreAnalyzedParser;
import org.apache.solr.schema.PreAnalyzedField;
import org.apache.solr.schema.PreAnalyzedField.PreAnalyzedParser;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.SimplePreAnalyzedParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>An update processor that parses configured fields of any document being added
 * using {@link PreAnalyzedField} with the configured format parser.</p>
 * 
 * <p>Fields are specified using the same patterns as in {@link FieldMutatingUpdateProcessorFactory}.
 * They are then checked whether they follow a pre-analyzed format defined by <code>parser</code>.
 * Valid fields are then parsed. The original {@link SchemaField} is used for the initial
 * creation of {@link IndexableField}, which is then modified to add the results from
 * parsing (token stream value and/or string value) and then it will be directly added to
 * the final Lucene {@link Document} to be indexed.</p>
 * <p>Fields that are declared in the patterns list but are not present
 * in the current schema will be removed from the input document.</p>
 * <h3>Implementation details</h3>
 * <p>This update processor uses {@link PreAnalyzedParser}
 * to parse the original field content (interpreted as a string value), and thus
 * obtain the stored part and the token stream part. Then it creates the "template"
 * {@link Field}-s using the original {@link SchemaField#createFields(Object)}
 * as declared in the current schema. Finally it sets the pre-analyzed parts if
 * available (string value and the token
 * stream value) on the first field of these "template" fields. If the declared
 * field type does not support stored or indexed parts then such parts are silently
 * discarded. Finally the updated "template" {@link Field}-s are added to the resulting
 * {@link SolrInputField}, and the original value of that field is removed.</p>
 * <h3>Example configuration</h3>
 * <p>In the example configuration below there are two update chains, one that
 * uses the "simple" parser ({@link SimplePreAnalyzedParser}) and one that uses
 * the "json" parser ({@link JsonPreAnalyzedParser}). Field "nonexistent" will be
 * removed from input documents if not present in the schema. Other fields will be
 * analyzed and if valid they will be converted to {@link IndexableField}-s or if
 * they are not in a valid format that can be parsed with the selected parser they
 * will be passed as-is. Assuming that <code>ssto</code> field is stored but not
 * indexed, and <code>sind</code> field is indexed but not stored: if
 * <code>ssto</code> input value contains the indexed part then this part will
 * be discarded and only the stored value part will be retained. Similarly,
 * if <code>sind</code> input value contains the stored part then it
 * will be discarded and only the token stream part will be retained.</p>
 * 
 *  <pre class="prettyprint">
 *   &lt;updateRequestProcessorChain name="pre-analyzed-simple"&gt;
 *    &lt;processor class="solr.PreAnalyzedUpdateProcessorFactory"&gt;
 *      &lt;str name="fieldName"&gt;title&lt;/str&gt;
 *      &lt;str name="fieldName"&gt;nonexistent&lt;/str&gt;
 *      &lt;str name="fieldName"&gt;ssto&lt;/str&gt;
 *      &lt;str name="fieldName"&gt;sind&lt;/str&gt;
 *      &lt;str name="parser"&gt;simple&lt;/str&gt;
 *    &lt;/processor&gt;
 *    &lt;processor class="solr.RunUpdateProcessorFactory" /&gt;
 *  &lt;/updateRequestProcessorChain&gt;
 *
 *  &lt;updateRequestProcessorChain name="pre-analyzed-json"&gt;
 *    &lt;processor class="solr.PreAnalyzedUpdateProcessorFactory"&gt;
 *      &lt;str name="fieldName"&gt;title&lt;/str&gt;
 *      &lt;str name="fieldName"&gt;nonexistent&lt;/str&gt;
 *      &lt;str name="fieldName"&gt;ssto&lt;/str&gt;
 *      &lt;str name="fieldName"&gt;sind&lt;/str&gt;
 *      &lt;str name="parser"&gt;json&lt;/str&gt;
 *    &lt;/processor&gt;
 *    &lt;processor class="solr.RunUpdateProcessorFactory" /&gt;
 *  &lt;/updateRequestProcessorChain&gt;
 *  </pre>
 *
 * @since 4.3.0
 */
public class PreAnalyzedUpdateProcessorFactory extends FieldMutatingUpdateProcessorFactory {
  
  private PreAnalyzedField parser;
  private String parserImpl;

  @Override
  public void init(@SuppressWarnings({"rawtypes"})final NamedList args) {
    parserImpl = (String)args.get("parser");
    args.remove("parser");
    // initialize inclusion / exclusion patterns
    super.init(args);
  }
  
  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new PreAnalyzedUpdateProcessor(getSelector(), next, req.getSchema(), parser);
  }

  @Override
  public void inform(SolrCore core) {
    super.inform(core);
    parser = new PreAnalyzedField();
    Map<String,String> args = new HashMap<>();
    if (parserImpl != null) {
      args.put(PreAnalyzedField.PARSER_IMPL, parserImpl);
    }
    parser.init(core.getLatestSchema(), args);
  }  
}

class PreAnalyzedUpdateProcessor extends FieldMutatingUpdateProcessor {
  
  private PreAnalyzedField parser;
  private IndexSchema schema;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public PreAnalyzedUpdateProcessor(FieldNameSelector sel, UpdateRequestProcessor next, IndexSchema schema, PreAnalyzedField parser) {
    super(sel, next);
    this.schema = schema;
    this.parser = parser;
  }

  @Override
  protected SolrInputField mutate(SolrInputField src) {
    SchemaField sf = schema.getFieldOrNull(src.getName());
    if (sf == null) { // remove this field
      return null;
    }
    FieldType type = PreAnalyzedField.createFieldType(sf);
    if (type == null) { // neither indexed nor stored - skip
      return null;
    }
    SolrInputField res = new SolrInputField(src.getName());
    for (Object o : src) {
      if (o == null) {
        continue;
      }
      Field pre = (Field)parser.createField(sf, o);
      if (pre != null) {
        res.addValue(pre);
      } else { // restore the original value
        log.warn("Could not parse field {} - using original value as is: {}", src.getName(), o);
        res.addValue(o);
      }
    }
    return res;
  }  
}
