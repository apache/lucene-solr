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

package org.apache.solr.handler.designer;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrInputDocument;

import static org.apache.solr.common.params.CommonParams.JSON_MIME;

public class SampleDocuments {
  public static final SampleDocuments NONE = new SampleDocuments(null, null, null);
  public final String contentType;
  public final String fileSource;
  public List<SolrInputDocument> parsed;

  public SampleDocuments(List<SolrInputDocument> parsed, String contentType, String fileSource) {
    this.parsed = parsed != null ? parsed : new LinkedList<>(); // needs to be mutable
    this.contentType = contentType;
    this.fileSource = fileSource;
  }

  public String getSource() {
    return fileSource != null ? fileSource : "paste";
  }

  private boolean isTextContentType() {
    if (contentType == null) {
      return false;
    }
    return contentType.contains(JSON_MIME) || contentType.startsWith("text/") || contentType.contains("application/xml");
  }

  public List<SolrInputDocument> appendDocs(String idFieldName, List<SolrInputDocument> add, int maxDocsToLoad) {
    if (add != null && !add.isEmpty()) {
      final Set<Object> ids =
          parsed.stream().map(doc -> doc.getFieldValue(idFieldName)).filter(Objects::nonNull).collect(Collectors.toSet());
      final List<SolrInputDocument> toAdd = add.stream().filter(doc -> {
        Object id = doc.getFieldValue(idFieldName);
        return id != null && !ids.contains(id); // doc has ID and it's not already in the set
      }).collect(Collectors.toList());
      parsed.addAll(toAdd);
      if (maxDocsToLoad > 0 && parsed.size() > maxDocsToLoad) {
        parsed = parsed.subList(0, maxDocsToLoad);
      }
    }
    return parsed;
  }
}
