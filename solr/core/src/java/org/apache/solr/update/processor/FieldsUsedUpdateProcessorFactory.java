package org.apache.solr.update.processor;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.plugin.SolrCoreAware;

public class FieldsUsedUpdateProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware {
  private FieldMutatingUpdateProcessorFactory.SelectorParams inclusions = new FieldMutatingUpdateProcessorFactory.SelectorParams();
  private Collection<FieldMutatingUpdateProcessorFactory.SelectorParams> exclusions
      = new ArrayList<>();
  private FieldMutatingUpdateProcessor.FieldNameSelector selector = null;
  private String fieldsUsedFieldName;

  @Override
  public void init(NamedList args) {
    inclusions = FieldMutatingUpdateProcessorFactory.parseSelectorParams(args);
    exclusions = FieldMutatingUpdateProcessorFactory.parseSelectorExclusionParams(args);

    fieldsUsedFieldName = args.get("fieldsUsedFieldName").toString();

    if (0 < args.size()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unexpected init param(s): '" + args.getName(0) + "'");
    }

    super.init(args);
  }

  @Override
  public void inform(SolrCore core) {
    selector =
        FieldMutatingUpdateProcessor.createFieldNameSelector
            (core.getResourceLoader(), core, inclusions, FieldMutatingUpdateProcessor.SELECT_ALL_FIELDS);

    for (FieldMutatingUpdateProcessorFactory.SelectorParams exc : exclusions) {
      selector = FieldMutatingUpdateProcessor.wrap
          (selector,
              FieldMutatingUpdateProcessor.createFieldNameSelector
                  (core.getResourceLoader(), core, exc, FieldMutatingUpdateProcessor.SELECT_NO_FIELDS));
    }
  }


  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new FieldsUsedUpdateProcessor(selector, fieldsUsedFieldName, next);
  }

  private class FieldsUsedUpdateProcessor extends UpdateRequestProcessor {
    private final FieldMutatingUpdateProcessor.FieldNameSelector selector;
    private final String fieldsUsedFieldName;

    public FieldsUsedUpdateProcessor(FieldMutatingUpdateProcessor.FieldNameSelector selector,
                                     String fieldsUsedFieldName, UpdateRequestProcessor next) {
      super(next);
      this.selector = selector;
      this.fieldsUsedFieldName = fieldsUsedFieldName;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      final SolrInputDocument doc = cmd.getSolrInputDocument();

      Collection<String> fieldsUsed = new ArrayList<String>();

      for (final String fname : doc.getFieldNames()) {

        if (selector.shouldMutate(fname)) {
          fieldsUsed.add(fname);
        }
      }

      doc.addField(fieldsUsedFieldName, fieldsUsed.toArray());

      super.processAdd(cmd);
    }
  }
}
