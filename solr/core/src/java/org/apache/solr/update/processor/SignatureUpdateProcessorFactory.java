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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.plugin.SolrCoreAware;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 3.1
 **/
public class SignatureUpdateProcessorFactory 
  extends UpdateRequestProcessorFactory 
  implements SolrCoreAware {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private List<String> sigFields;
  private String signatureField;

  private Term signatureTerm;
  private boolean enabled = true;
  private String signatureClass;
  private boolean overwriteDupes;
  private SolrParams params;

  @Override
  public void init(@SuppressWarnings({"rawtypes"})final NamedList args) {
    if (args != null) {
      SolrParams params = args.toSolrParams();
      boolean enabled = params.getBool("enabled", true);
      this.enabled = enabled;

      overwriteDupes = params.getBool("overwriteDupes", true);

      signatureField = params.get("signatureField", "signatureField");

      signatureClass = params.get("signatureClass",
          "org.apache.solr.update.processor.Lookup3Signature");
      this.params = params;

      Object fields = args.get("fields");
      sigFields = fields == null ? null: StrUtils.splitSmart((String)fields, ",", true); 
      if (sigFields != null) {
        Collections.sort(sigFields);
      }
    }
  }

  @Override
  public void inform(SolrCore core) {
    final IndexSchema schema = core.getLatestSchema();
    final SchemaField field = schema.getFieldOrNull(getSignatureField());
    
    if (null == field) {
      throw new SolrException
        (ErrorCode.SERVER_ERROR,
         "Can't use signatureField which does not exist in schema: "
         + getSignatureField());
    }

    if (getOverwriteDupes() && ( ! field.indexed() ) ) {
      throw new SolrException
        (ErrorCode.SERVER_ERROR,
         "Can't set overwriteDupes when signatureField is not indexed: "
         + getSignatureField());
    }

    if (getOverwriteDupes() && (null != core.getCoreDescriptor().getCloudDescriptor()) ) {
      // Not Safe, see SOLR-3473 + SOLR-15290
      if ( ! field.equals(schema.getUniqueKeyField()) ) {
        // TODO: throw new SolrException(ErrorCode.SERVER_ERROR, ...
        log.error("Can't use overwriteDupes safely in SolrCloud when signatureField is not the uniqueKeyField: {}",
                  schema.getUniqueKeyField().getName());
      }
    }
  }

  public List<String> getSigFields() {
    return sigFields;
  }

  public String getSignatureField() {
    return signatureField;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public String getSignatureClass() {
    return signatureClass;
  }

  public boolean getOverwriteDupes() {
    return overwriteDupes;
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {

    return new SignatureUpdateProcessor(req, rsp, this, next);

  }

  class SignatureUpdateProcessor extends UpdateRequestProcessor {
    private final SolrQueryRequest req;

    public SignatureUpdateProcessor(SolrQueryRequest req,
        SolrQueryResponse rsp, SignatureUpdateProcessorFactory factory,
        UpdateRequestProcessor next) {
      super(next);
      this.req = req;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      if (enabled) {
        SolrInputDocument doc = cmd.getSolrInputDocument();
        List<String> currDocSigFields = null;
        boolean isPartialUpdate = AtomicUpdateDocumentMerger.isAtomicUpdate(cmd);
        if (sigFields == null || sigFields.size() == 0) {
          if (isPartialUpdate)  {
            throw new SolrException
                (ErrorCode.SERVER_ERROR,
                    "Can't use SignatureUpdateProcessor with partial updates on signature fields");
          }
          Collection<String> docFields = doc.getFieldNames();
          currDocSigFields = new ArrayList<>(docFields.size());
          currDocSigFields.addAll(docFields);
          Collections.sort(currDocSigFields);
        } else {
          currDocSigFields = sigFields;
        }

        Signature sig = req.getCore().getResourceLoader().newInstance(signatureClass, Signature.class);
        sig.init(params);

        for (String field : currDocSigFields) {
          SolrInputField f = doc.getField(field);
          if (f != null) {
            if (isPartialUpdate)  {
              throw new SolrException
                  (ErrorCode.SERVER_ERROR,
                      "Can't use SignatureUpdateProcessor with partial update request " +
                          "containing signature field: " + field);
            }
            sig.add(field);
            Object o = f.getValue();
            if (o instanceof Collection) {
              for (Object oo : (Collection)o) {
                sig.add(String.valueOf(oo));
              }
            } else {
              sig.add(String.valueOf(o));
            }
          }
        }

        byte[] signature = sig.getSignature();
        char[] arr = new char[signature.length<<1];
        for (int i=0; i<signature.length; i++) {
          int b = signature[i];
          int idx = i<<1;
          arr[idx]= StrUtils.HEX_DIGITS[(b >> 4) & 0xf];
          arr[idx+1]= StrUtils.HEX_DIGITS[b & 0xf];
        }
        String sigString = new String(arr);
        doc.addField(signatureField, sigString);

        if (overwriteDupes) {
          cmd.updateTerm = new Term(signatureField, sigString);
        }

      }

      if (next != null)
        next.processAdd(cmd);
    }

  }

  // for testing
  void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }


}
