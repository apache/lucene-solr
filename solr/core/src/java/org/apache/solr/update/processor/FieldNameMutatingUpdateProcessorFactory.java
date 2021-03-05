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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;

/**
 * <p>
 * In the FieldNameMutatingUpdateProcessorFactory configured below,
 * fields names will be mutated if the name contains space.
 * Use multiple instances of this processor for multiple replacements
 * </p>
 * <pre class="prettyprint">
 * &lt;processor class="solr.FieldNameMutatingUpdateProcessorFactory"&gt;
 *   &lt;str name="pattern "&gt;\s&lt;/str&gt;
 *   &lt;str name="replacement"&gt;_&lt;/str&gt;
 * &lt;/processor&gt;</pre>
 * @since 5.0.0
 */

public class FieldNameMutatingUpdateProcessorFactory  extends UpdateRequestProcessorFactory{

  private String sourcePattern, replacement;
  private Pattern pattern;


  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new UpdateRequestProcessor(next) {
      @Override
      public void processAdd(AddUpdateCommand cmd) throws IOException {
        final SolrInputDocument doc = cmd.getSolrInputDocument();
        final Collection<String> fieldNames
            = new ArrayList<>(doc.getFieldNames());

        for (final String fname : fieldNames) {
          Matcher matcher = pattern.matcher(fname);
          if(matcher.find() ){
            String newFieldName = matcher.replaceAll(replacement);
            if(!newFieldName.equals(fname)){
              SolrInputField old = doc.remove(fname);
              old.setName(newFieldName);
              doc.put(newFieldName, old);
            }
          }
        }

        super.processAdd(cmd);
      }

      @Override
      public void processDelete(DeleteUpdateCommand cmd) throws IOException {
        super.processDelete(cmd);
      }
    };
  }

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    sourcePattern = (String) args.get("pattern");
    replacement = (String) args.get("replacement");
    if(sourcePattern ==null || replacement == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"'pattern' and 'replacement' are required values");
    }
    try {
      pattern = Pattern.compile(sourcePattern);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"invalid pattern "+ sourcePattern );
    }
    super.init(args);
  }
}
