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

package org.apache.solr.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.StreamingBinaryResponseParser;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.FastWriter;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrJSONWriter;

import static org.apache.solr.common.params.CommonParams.JAVABIN;

public class ExportTool extends SolrCLI.ToolBase {
  @Override
  public String getName() {
    return "export";
  }

  @Override
  public Option[] getOptions() {
    return OPTIONS;
  }

  @Override
  protected void runImpl(CommandLine cli) throws Exception {
    String url = cli.getOptionValue("url");
    String format = cli.getOptionValue("format", JAVABIN);
    int idx = url.lastIndexOf('/');
    String baseurl = url.substring(0, idx);
    String coll = url.substring(idx + 1);
    String file = cli.getOptionValue("out",
        JAVABIN.equals(format) ? coll + ".javabin" : coll + ".json");
    String maxDocsStr = cli.getOptionValue("limit", String.valueOf(Long.MAX_VALUE));
    long maxDocs = Long.parseLong(maxDocsStr);
    if (JAVABIN.equals(format)) {
      writeJavabinDocs(baseurl, coll, file, maxDocs);
    } else {
      writeJsonLDocs(baseurl, coll, file, maxDocs);
    }
  }

  public static void writeJsonLDocs(String baseurl, String coll, String file, long maxDocs) throws IOException, SolrServerException {
    SolrClient solrClient = new CloudSolrClient.Builder(Collections.singletonList(baseurl)).build();
    FileOutputStream fos = new FileOutputStream(file);
    Writer writer = FastWriter.wrap(new OutputStreamWriter(fos));

    SolrJSONWriter jsonw = new SolrJSONWriter(writer);
    jsonw.setIndent(false);
    Consumer<SolrInputDocument> consumer = doc -> {
      try {
        Map m = new LinkedHashMap(doc.size());
        doc.forEach((s, field) -> {
          Object value = field.getValue();
          if (value instanceof List) {
            if(((List) value).size() ==1){
              value = ((List) value).get(0);
            }
          }
          m.put(s, value);
        });
        jsonw.writeObj(m);
        writer.append('\n');
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };

    try {
      streamDocsWithCursorMark(coll, maxDocs, solrClient, consumer);
    } finally {
      solrClient.close();
      fos.close();
    }
  }

  public static void writeJavabinDocs(String baseurl, String coll, String file, long maxDocs) throws IOException, SolrServerException {
    SolrClient solrClient = new CloudSolrClient.Builder(Collections.singletonList(baseurl)).build();
    FileOutputStream fos = new FileOutputStream(file);
    JavaBinCodec codec = new JavaBinCodec(fos, null);
    codec.writeTag(JavaBinCodec.NAMED_LST, 2);
    codec.writeStr("params");
    codec.writeNamedList(new NamedList<>());
    codec.writeStr("docs");
    codec.writeTag(JavaBinCodec.ITERATOR);
    long[] docsWritten = new long[1];
    docsWritten[0] = 0;
    Consumer<SolrInputDocument> consumer = d -> {
      try {
        codec.writeSolrInputDocument(d);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
    try {
      streamDocsWithCursorMark(coll, maxDocs, solrClient, consumer);
    } finally {
      codec.writeTag(JavaBinCodec.END);
      solrClient.close();
      codec.close();
      fos.close();
    }
  }

  private static void streamDocsWithCursorMark(String coll, long maxDocs, SolrClient solrClient,
                                               Consumer<SolrInputDocument> consumer) throws SolrServerException, IOException {
    long[] docsWritten = new long[]{0};
    NamedList<Object> rsp1 = solrClient.request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/schema/uniquekey",
        new MapSolrParams(Collections.singletonMap("collection", coll))));
    String uniqueKey = (String) rsp1.get("uniqueKey");
    NamedList<Object> rsp;
    SolrQuery q = (new SolrQuery("*:*"))
        .setParam("collection", coll)
        .setRows(100)
        .setSort(SolrQuery.SortClause.asc(uniqueKey));

    String cursorMark = CursorMarkParams.CURSOR_MARK_START;
    boolean done = false;
    while (!done) {
      if (docsWritten[0] >= maxDocs) break;
      QueryRequest request = new QueryRequest(q);

      request.setResponseParser(new StreamingBinaryResponseParser(new StreamingResponseCallback() {
        @Override
        public void streamSolrDocument(SolrDocument doc) {
          SolrInputDocument document = new SolrInputDocument();
          doc.forEach((s, o) -> {
            if (s.equals("_version_")) return;
            if (o instanceof List) {
              if(((List) o).size() ==1) o = ((List) o).get(0);
            }
            document.addField(s, o);
          });

          consumer.accept(document);
          docsWritten[0]++;
        }

        @Override
        public void streamDocListInfo(long numFound, long start, Float maxScore) {

        }
      }));
      q.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
      rsp = solrClient.request(request);
      String nextCursorMark = (String) rsp.get(CursorMarkParams.CURSOR_MARK_NEXT);
      if (nextCursorMark == null || Objects.equals(cursorMark, nextCursorMark)) {
        break;
      }
      cursorMark = nextCursorMark;
    }
  }

  private static final Option[] OPTIONS = {
      OptionBuilder
          .hasArg()
          .isRequired(true)
          .withDescription("Address of the collection, example http://localhost:8983/solr/gettingstarted")
          .create("url"),
      OptionBuilder
          .hasArg()
          .isRequired(false)
          .withDescription("file name . defaults to collection-name.<format>")
          .create("out"),
      OptionBuilder
          .hasArg()
          .isRequired(false)
          .withDescription("format either json or javabin, default to javabin. file extension would be .javabin")
          .create("format"),
      OptionBuilder
          .hasArg()
          .isRequired(false)
          .withDescription("Max number of docs to download. by default it goes on till all docs are downloaded")
          .create("limit"),
  };
}
