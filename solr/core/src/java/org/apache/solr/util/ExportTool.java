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
import java.io.PrintStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
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
    long limit = 100;
    long docsWritten = 0;
    PrintStream output;
    //for testing purposes only
    public SolrClient solrClient;


    public Info(String url) {
      setUrl(url);
      setOutFormat(null, "jsonl");

    }

    public void setUrl(String url) {
      int idx = url.lastIndexOf('/');
      baseurl = url.substring(0, idx);
      coll = url.substring(idx + 1);
      query = "*:*";
    }

    public void setLimit(String maxDocsStr) {
      limit = Long.parseLong(maxDocsStr);
      if (limit == -1) limit = Long.MAX_VALUE;
    }

    public void setOutFormat(String out, String format) {
      this.format = format;
      if (format == null) format = "jsonl";
      if (!formats.contains(format)) {
        throw new IllegalArgumentException("format must be one of :" + formats);
      }

      this.out = out;
      if (this.out == null) {
        this.out = JAVABIN.equals(format) ?
            coll + ".javabin" :
            coll + ".json";
      }

    }

    DocsSink getSink() {
      return JAVABIN.equals(format) ? new JavabinSink(this) : new JsonSink(this);
    }

    void exportDocsWithCursorMark() throws SolrServerException, IOException {
      DocsSink sink = getSink();
      solrClient = new CloudSolrClient.Builder(Collections.singletonList(baseurl)).build();
      NamedList<Object> rsp1 = solrClient.request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/schema/uniquekey",
          new MapSolrParams(Collections.singletonMap("collection", coll))));
      String uniqueKey = (String) rsp1.get("uniqueKey");

      sink.start();
      try {
        NamedList<Object> rsp;
        SolrQuery q = (new SolrQuery(query))
            .setParam("collection", coll)
            .setRows(100)
            .setSort(SolrQuery.SortClause.asc(uniqueKey));
        if (fields != null) {
          q.setParam(FL, fields);
        }

        String cursorMark = CursorMarkParams.CURSOR_MARK_START;
        boolean done = false;
        StreamingResponseCallback streamer = getStreamer(sink);

        if(output!= null) output.println("Exporting data to : "+ out);
        while (!done) {
          if (docsWritten >= limit) break;
          QueryRequest request = new QueryRequest(q);
          request.setResponseParser(new StreamingBinaryResponseParser(streamer));
          q.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
          rsp = solrClient.request(request);
          String nextCursorMark = (String) rsp.get(CursorMarkParams.CURSOR_MARK_NEXT);
          if (nextCursorMark == null || Objects.equals(cursorMark, nextCursorMark)) {
            break;
          }
          cursorMark = nextCursorMark;
          if(output!= null) output.print(".");
        }
        if(output!= null) output.println("\n DONE!");
      } finally {
        sink.end();
        solrClient.close();

      }
    }

    private StreamingResponseCallback getStreamer(DocsSink sink) {
      return new StreamingResponseCallback() {
        @Override
        public void streamSolrDocument(SolrDocument doc) {
          try {
            sink.accept(doc);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          docsWritten++;
        }

        @Override
        public void streamDocListInfo(long numFound, long start, Float maxScore) {

        }
      };
    }

  }

  static Set<String> formats = ImmutableSet.of(JAVABIN, "jsonl");

  @Override
  protected void runImpl(CommandLine cli) throws Exception {
    Info info = new Info(cli.getOptionValue("url"));
    info.query = cli.getOptionValue("query", "*:*");
    info.setOutFormat(cli.getOptionValue("out"), cli.getOptionValue("format"));
    info.fields = cli.getOptionValue("fields");
    info.setLimit(cli.getOptionValue("limit", "100"));
    info.output = super.stdout;
    info.exportDocsWithCursorMark();
  }

  interface DocsSink {
    void start() throws IOException;

    void accept(SolrDocument document) throws IOException;

    void end() throws IOException;
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

  private static class JsonSink implements DocsSink {
    private final Info info;
    private SolrJSONWriter jsonw;
    private FastWriter writer;
    private FileOutputStream fos;

    public JsonSink(Info info) {
      this.info = info;
    }

    @Override
    public void start() throws IOException {
      fos = new FileOutputStream(info.out);
      writer = FastWriter.wrap(new OutputStreamWriter(fos));
      jsonw = new SolrJSONWriter(writer);
      jsonw.setIndent(false);

    }

    @Override
    public void end() throws IOException {
      jsonw.close();
      fos.close();

    }

    @Override
    public void accept(SolrDocument doc) throws IOException {
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

    }
  }

  private static class JavabinSink implements DocsSink {
    private final Info info;
    JavaBinCodec codec;
    FileOutputStream fos;

    public JavabinSink(Info info) {
      this.info = info;
    }

    @Override
    public void start() throws IOException {
      fos = new FileOutputStream(info.out);
      codec = new JavaBinCodec(fos, null);
      codec.writeTag(JavaBinCodec.NAMED_LST, 2);
      codec.writeStr("params");
      codec.writeNamedList(new NamedList<>());
      codec.writeStr("docs");
      codec.writeTag(JavaBinCodec.ITERATOR);

    }

    @Override
    public void end() throws IOException {
      codec.writeTag(JavaBinCodec.END);
      codec.close();
      fos.close();

    }

    @Override
    public void accept(SolrDocument doc) throws IOException {
      SolrInputDocument document = new SolrInputDocument();
      doc.forEach((s, o) -> {
        if (s.equals("_version_")) return;
        if (o instanceof List) {
          if (((List) o).size() == 1) o = ((List) o).get(0);
        }
        document.addField(s, o);
      });

      codec.writeSolrInputDocument(document);

    }
  }
}
