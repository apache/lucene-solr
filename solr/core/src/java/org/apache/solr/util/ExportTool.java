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

import static org.apache.solr.common.params.CommonParams.FL;
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

  public static class Info {
    String baseurl;
    String format;
    String query;
    String coll;
    String out;
    String fields;
    long limit;

  }

  @Override
  protected void runImpl(CommandLine cli) throws Exception {
    Info info = new Info();
    String url = cli.getOptionValue("url");
    info.format = cli.getOptionValue("format", "jsonl");
    info.query = cli.getOptionValue("query", "*:*");
    info.fields = cli.getOptionValue("fields");
    int idx = url.lastIndexOf('/');
    info.baseurl = url.substring(0, idx);
    info.coll = url.substring(idx + 1);
    info.out = cli.getOptionValue("out",
        JAVABIN.equals(info.format) ? info.coll + ".javabin" : info.coll + ".json");
    String maxDocsStr = cli.getOptionValue("limit", "100");
    info.limit = Long.parseLong(maxDocsStr);
    if (info.limit == -1) info.limit = Long.MAX_VALUE;
    if (JAVABIN.equals(info.format)) {
      writeJavabinDocs(info);
    } else {
      writeJsonLDocs(info);
    }
  }

  public static void writeJsonLDocs(Info info) throws IOException, SolrServerException {
    SolrClient solrClient = new CloudSolrClient.Builder(Collections.singletonList(info.baseurl)).build();
    FileOutputStream fos = new FileOutputStream(info.out);
    Writer writer = FastWriter.wrap(new OutputStreamWriter(fos));

    SolrJSONWriter jsonw = new SolrJSONWriter(writer);
    jsonw.setIndent(false);
    Consumer<SolrDocument> consumer = doc -> {
      try {
        Map m = new LinkedHashMap(doc.size());
        doc.forEach((s, field) -> {
          if (s.equals("_version_")) return;
          if (field instanceof List) {
            if (((List) field).size() == 1) {
              field = ((List) field).get(0);
            }
          }
          m.put(s, field);
        });
        jsonw.writeObj(m);
        writer.flush();
        writer.append('\n');
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };

    try {
      streamDocsWithCursorMark(info, solrClient, consumer);
    } finally {
      jsonw.close();
      solrClient.close();
      fos.close();
    }
  }

  public static void writeJavabinDocs(Info info) throws IOException, SolrServerException {
    SolrClient solrClient = new CloudSolrClient.Builder(Collections.singletonList(info.baseurl)).build();
    FileOutputStream fos = new FileOutputStream(info.out);
    JavaBinCodec codec = new JavaBinCodec(fos, null);
    codec.writeTag(JavaBinCodec.NAMED_LST, 2);
    codec.writeStr("params");
    codec.writeNamedList(new NamedList<>());
    codec.writeStr("docs");
    codec.writeTag(JavaBinCodec.ITERATOR);
    long[] docsWritten = new long[1];
    docsWritten[0] = 0;
    Consumer<SolrDocument> consumer = doc -> {
      try {
        SolrInputDocument document = new SolrInputDocument();
        doc.forEach((s, o) -> {
          if (s.equals("_version_")) return;
          if (o instanceof List) {
            if (((List) o).size() == 1) o = ((List) o).get(0);
          }
          document.addField(s, o);
        });

        codec.writeSolrInputDocument(document);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
    try {
      streamDocsWithCursorMark(info, solrClient, consumer);
    } finally {
      codec.writeTag(JavaBinCodec.END);
      solrClient.close();
      codec.close();
      fos.close();
    }
  }

  private static void streamDocsWithCursorMark(Info info, SolrClient solrClient,
                                               Consumer<SolrDocument> consumer) throws SolrServerException, IOException {
    long[] docsWritten = new long[]{0};
    NamedList<Object> rsp1 = solrClient.request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/schema/uniquekey",
        new MapSolrParams(Collections.singletonMap("collection", info.coll))));
    String uniqueKey = (String) rsp1.get("uniqueKey");
    NamedList<Object> rsp;
    SolrQuery q = (new SolrQuery(info.query))
        .setParam("collection", info.coll)
        .setRows(100)
        .setSort(SolrQuery.SortClause.asc(uniqueKey));
    if (info.fields != null) {
      q.setParam(FL, info.fields);
    }

    String cursorMark = CursorMarkParams.CURSOR_MARK_START;
    boolean done = false;
    while (!done) {
      if (docsWritten[0] >= info.limit) break;
      QueryRequest request = new QueryRequest(q);

      request.setResponseParser(new StreamingBinaryResponseParser(new StreamingResponseCallback() {
        @Override
        public void streamSolrDocument(SolrDocument doc) {
          consumer.accept(doc);
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
          .withDescription("format  json/javabin, default to json. file extension would be .json")
          .create("format"),
      OptionBuilder
          .hasArg()
          .isRequired(false)
          .withDescription("Max number of docs to download. default = 100, use -1 for all docs")
          .create("limit"),
      OptionBuilder
          .hasArg()
          .isRequired(false)
          .withDescription("A custom query, default is *:*")
          .create("query"),
      OptionBuilder
          .hasArg()
          .isRequired(false)
          .withDescription("Comma separated fields. By default all fields are fetched")
          .create("fields")
  };
}
