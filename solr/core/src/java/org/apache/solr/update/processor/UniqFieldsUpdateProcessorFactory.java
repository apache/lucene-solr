package org.apache.solr.update.processor;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;

/**
 * A non-duplicate processor. Removes duplicates in the specified fields.
 * 
 * <pre class="prettyprint" >
 * &lt;updateRequestProcessorChain name="uniq-fields"&gt;
 *   &lt;processor class="org.apache.solr.update.processor.UniqFieldsUpdateProcessorFactory"&gt;
 *     &lt;lst name="fields"&gt;
 *       &lt;str&gt;uniq&lt;/str&gt;
 *       &lt;str&gt;uniq2&lt;/str&gt;
 *       &lt;str&gt;uniq3&lt;/str&gt;
 *     &lt;/lst&gt;      
 *   &lt;/processor&gt;
 *   &lt;processor class="solr.RunUpdateProcessorFactory" /&gt;
 * &lt;/updateRequestProcessorChain&gt;</pre>
 * 
 */
public class UniqFieldsUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  private Set<String> fields;

  @SuppressWarnings("unchecked")
  @Override
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
    NamedList<String> flst = (NamedList<String>)args.get("fields");
    if(flst != null){
      fields = new HashSet<String>();
      for(int i = 0; i < flst.size(); i++){
        fields.add(flst.getVal(i));
      }
    }
  }
  
  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                            SolrQueryResponse rsp,
                                            UpdateRequestProcessor next) {
    return new UniqFieldsUpdateProcessor(next, fields);
  }
  
  public class UniqFieldsUpdateProcessor extends UpdateRequestProcessor {
    
    private final Set<String> fields;

    public UniqFieldsUpdateProcessor(UpdateRequestProcessor next, 
                                              Set<String> fields) {
      super(next);
      this.fields = fields;
    }
    
    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      if(fields != null){
        SolrInputDocument solrInputDocument = cmd.getSolrInputDocument();
        List<Object> uniqList = new ArrayList<Object>();
        for (String field : fields) {
          uniqList.clear();
          Collection<Object> col = solrInputDocument.getFieldValues(field);
          if (col != null) {
            for (Object o : col) {
              if(!uniqList.contains(o))
                uniqList.add(o);
            }
            solrInputDocument.remove(field);
            for (Object o : uniqList) {
              solrInputDocument.addField(field, o);
            }
          }    
        }
      }
      super.processAdd(cmd);
    }
  }
}



