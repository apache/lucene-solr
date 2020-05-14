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

import java.io.*;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.net.URLDecoder;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.handler.component.ShardRequest;



/**
*  A command line tool for indexing Solr logs in the out-of-the-box log format.
**/

public class SolrLogPostTool {

  public static void main(String[] args) throws Exception {

    if(args.length != 2) {
      CLIO.out("");
      CLIO.out("postlogs is a simple tool for indexing Solr logs.");
      CLIO.out("");
      CLIO.out("parameters:");
      CLIO.out("");
      CLIO.out("-- baseUrl: Example http://localhost:8983/solr/collection1");
      CLIO.out("-- rootDir: All files found at or below the root will be indexed.");
      CLIO.out("");
      CLIO.out("Sample syntax 1: ./bin/postlogs http://localhost:8983/solr/collection1 /user/foo/logs/solr.log");
      CLIO.out("Sample syntax 2: ./bin/postlogs http://localhost:8983/solr/collection1 /user/foo/logs");
      CLIO.out("");
      return;
    }

    String baseUrl = args[0];
    String root = args[1];

    HttpSolrClient.Builder builder = new HttpSolrClient.Builder();
    SolrClient client = null;
    try {
      client = builder.withBaseSolrUrl(baseUrl).build();
      File rf = new File(root);
      List<File> files = new ArrayList();
      gatherFiles(rf, files);
      int rec = 0;
      UpdateRequest request = new UpdateRequest();

      for (File file : files) {

        LineNumberReader bufferedReader = null;

        try {
          bufferedReader = new LineNumberReader(new InputStreamReader(new FileInputStream(file), Charset.defaultCharset()));
          LogRecordReader recordReader = new LogRecordReader(bufferedReader);
          SolrInputDocument doc = null;
          String fileName = file.getName();
          while (true) {
            try {
              doc = recordReader.readRecord();
            } catch (Throwable t) {
              CLIO.err("Error reading log record:"+ bufferedReader.getLineNumber() +" from file:"+ fileName);
              CLIO.err(t.getMessage());
              continue;
            }

            if(doc == null) {
              break;
            }

            rec++;
            UUID id = UUID.randomUUID();
            doc.addField("id", id.toString());
            doc.addField("file_s", fileName);
            request.add(doc);
            if (rec == 300) {
              CLIO.out("Sending batch of 300 log records...");
              request.process(client);
              CLIO.out("Batch sent");
              request = new UpdateRequest();
              rec = 0;
            }
          }
        } finally {
          bufferedReader.close();
        }
      }

      if (rec > 0) {
        //Process last batch
        CLIO.out("Sending last batch ...");
        request.process(client);
        client.commit();
        CLIO.out("Committed");
      }
    } finally {
      client.close();
    }
  }

  static void gatherFiles(File rootFile, List<File> files) {

    if(rootFile.isFile()) {
      files.add(rootFile);
    } else {
      File[] subFiles = rootFile.listFiles();
      for(File f : subFiles) {
        if(f.isFile()) {
          files.add(f);
        } else {
          gatherFiles(f, files);
        }
      }
    }
  }

  public static class LogRecordReader {

    private BufferedReader bufferedReader;
    private String pushedBack = null;
    private boolean finished = false;
    private String cause;
    private Pattern p = Pattern.compile("^(\\d\\d\\d\\d\\-\\d\\d\\-\\d\\d[\\s|T]\\d\\d:\\d\\d\\:\\d\\d.\\d\\d\\d)");

    public LogRecordReader(BufferedReader bufferedReader) throws IOException {
      this.bufferedReader = bufferedReader;
    }

    public SolrInputDocument readRecord() throws IOException {
      while(true) {
        String line = null;

        if(finished) {
          return null;
        }

        if(pushedBack != null) {
          line = pushedBack;
          pushedBack = null;
        } else {
          line = bufferedReader.readLine();
        }

        if (line != null) {
          if (line.contains("Registered new searcher")) {
            return parseNewSearch(line);
          } else if (line.contains("path=/update")) {
            return parseUpdate(line);
          } else if (line.contains(" ERROR ")) {
            this.cause = null;
            return parseError(line, readTrace());
          } else if (line.contains("start commit")) {
            return parseCommit(line);
          } else if(line.contains("QTime=")) {
            return parseQueryRecord(line);
          } else {
            continue;
          }
        } else {
          return null;
        }
      }
    }

    private String readTrace() throws IOException {
      StringBuilder buf = new StringBuilder();
      buf.append("%html ");

      while(true) {
        String line = bufferedReader.readLine();
        if (line == null) {
          finished = true;
          return buf.toString();
        } else {
          //look for a date at the beginning of the line
          //If it's not there then read into the stack trace buffer
          Matcher m = p.matcher(line);

          if (!m.find() && buf.length() < 10000) {
            //Line does not start with a timestamp so append to the stack trace
            buf.append(line.replace("\t", "    ") + "<br/>");
            if(line.startsWith("Caused by:")) {
              this.cause = line;
            }
          } else {
            pushedBack = line;
            break;
          }
        }
      }

      return buf.toString();
    }

    private String parseDate(String line) {
      Matcher m = p.matcher(line);
      if(m.find()) {
        String date = m.group(1);
        return date.replace(" ", "T");
      }

      return null;
    }

    private SolrInputDocument parseError(String line, String trace) throws IOException {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("date_dt", parseDate(line));
      doc.addField("type_s", "error");
      doc.addField("line_t", line);

      //Don't include traces that have only the %html header.
      if(trace != null && trace.length() > 6) {
        doc.addField("stack_t", trace);
      }

      if(this.cause != null) {
        doc.addField("root_cause_t", cause.replace("Caused by:", "").trim());
      }

      doc.addField("collection_s", parseCollection(line));
      doc.addField("core_s", parseCore(line));
      doc.addField("shard_s", parseShard(line));
      doc.addField("replica_s", parseReplica(line));

      return doc;
    }

    private SolrInputDocument parseCommit(String line) throws IOException {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("date_dt", parseDate(line));
      doc.addField("type_s", "commit");
      doc.addField("line_t", line);
      if(line.contains("softCommit=true")) {
        doc.addField("soft_commit_s", "true");
      } else {
        doc.addField("soft_commit_s", "false");
      }

      if(line.contains("openSearcher=true")) {
        doc.addField("open_searcher_s", "true");
      } else {
        doc.addField("open_searcher_s", "false");
      }

      doc.addField("collection_s", parseCollection(line));
      doc.addField("core_s", parseCore(line));
      doc.addField("shard_s", parseShard(line));
      doc.addField("replica_s", parseReplica(line));

      return doc;
    }

    private SolrInputDocument parseQueryRecord(String line) {

      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("date_dt", parseDate(line));
      doc.addField("qtime_i", parseQTime(line));
      doc.addField("status_s", parseStatus(line));

      String path = parsePath(line);
      doc.addField("path_s", path);

      if(line.contains("hits=")) {
        doc.addField("hits_l", parseHits(line));
      }

      String params = parseParams(line);
      doc.addField("params_t", params);
      addParams(doc, params);

      doc.addField("collection_s", parseCollection(line));
      doc.addField("core_s", parseCore(line));
      doc.addField("node_s", parseNode(line));
      doc.addField("shard_s", parseShard(line));
      doc.addField("replica_s", parseReplica(line));


      if(path != null && path.contains("/admin")) {
        doc.addField("type_s", "admin");
      } else if(path != null && params.contains("/replication")) {
        doc.addField("type_s", "replication");
      } else if (path != null && path.contains("/get")) {
        doc.addField("type_s", "get");
      } else {
        doc.addField("type_s", "query");
      }

      return doc;
    }


    private SolrInputDocument parseNewSearch(String line) {

      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("date_dt", parseDate(line));
      doc.addField("core_s", parseNewSearcherCore(line));
      doc.addField("type_s", "newSearcher");
      doc.addField("line_t", line);

      return doc;
    }

    private String parseCollection(String line) {
      char[] ca = {' ', ']', ','};
      String parts[] = line.split("c:");
      if(parts.length >= 2) {
        return readUntil(parts[1], ca);
      } else {
        return null;
      }
    }

    private SolrInputDocument parseUpdate(String line) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("date_dt", parseDate(line));

      if(line.contains("deleteByQuery=")) {
        doc.addField("type_s", "deleteByQuery");
      } else if(line.contains("delete=")) {
        doc.addField("type_s", "delete");
      } else {
        doc.addField("type_s", "update");
      }

      doc.addField("collection_s", parseCollection(line));
      doc.addField("core_s", parseCore(line));
      doc.addField("shard_s", parseShard(line));
      doc.addField("replica_s", parseReplica(line));
      doc.addField("line_t", line);

      return doc;
    }

    private String parseNewSearcherCore(String line) {
      char[] ca = {']'};
      String parts[] = line.split("\\[");
      if(parts.length > 3) {
        return readUntil(parts[2], ca);
      } else {
        return null;
      }
    }

    private String parseCore(String line) {
      char[] ca = {' ', ']', '}', ','};
      String parts[] = line.split("x:");
      if(parts.length >= 2) {
        return readUntil(parts[1], ca);
      } else {
        return null;
      }
    }

    private String parseShard(String line) {
      char[] ca = {' ', ']', '}', ','};
      String parts[] = line.split("s:");
      if(parts.length >= 2) {
        return readUntil(parts[1], ca);
      } else {
        return null;
      }
    }

    private String parseReplica(String line) {
      char[] ca = {' ', ']', '}',','};
      String parts[] = line.split("r:");
      if(parts.length >= 2) {
        return readUntil(parts[1], ca);
      } else {
        return null;
      }
    }


    private String parsePath(String line) {
      char[] ca = {' '};
      String parts[] = line.split(" path=");
      if(parts.length == 2) {
        return readUntil(parts[1], ca);
      } else {
        return null;
      }
    }

    private String parseQTime(String line) {
      char[] ca = {'\n', '\r'};
      String parts[] = line.split(" QTime=");
      if(parts.length == 2) {
        return readUntil(parts[1], ca);
      } else {
        return null;
      }
    }

    private String parseNode(String line) {
      char[] ca = {' ', ']', '}', ','};
      String parts[] = line.split("node_name=n:");
      if(parts.length >= 2) {
        return readUntil(parts[1], ca);
      } else {
        return null;
      }
    }

    private String parseStatus(String line) {
      char[] ca = {' ', '\n', '\r'};
      String parts[] = line.split(" status=");
      if(parts.length == 2) {
        return readUntil(parts[1], ca);
      } else {
        return null;
      }
    }

    private String parseHits(String line) {
      char[] ca = {' '};
      String parts[] = line.split(" hits=");
      if(parts.length == 2) {
        return readUntil(parts[1], ca);
      } else {
        return null;
      }
    }

    private String parseParams(String line) {
      char[] ca = {' '};
      String parts[] = line.split(" params=");
      if(parts.length == 2) {
        String p = readUntil(parts[1].substring(1), ca);
        return p.substring(0, p.length()-1);
      } else {
        return null;
      }
    }

    private String readUntil(String s, char[] chars) {
      StringBuilder builder = new StringBuilder();
      for(int i=0; i<s.length(); i++) {
        char a = s.charAt(i);
        for(char c : chars) {
          if(a == c) {
            return builder.toString();
          }
        }
        builder.append(a);
      }

      return builder.toString();
    }

    private void addParams(SolrInputDocument doc,  String params) {
      String[] pairs = params.split("&");
      for(String pair : pairs) {
        String[] parts = pair.split("=");
        if(parts.length == 2 && parts[0].equals("q")) {
          String dq = URLDecoder.decode(parts[1], Charset.defaultCharset());
          doc.addField("q_s", dq);
          doc.addField("q_t", dq);
        }

        if(parts[0].equals("rows")) {
          String dr = URLDecoder.decode(parts[1], Charset.defaultCharset());
          doc.addField("rows_i", dr);
        }

        if(parts[0].equals("distrib")) {
          String dr = URLDecoder.decode(parts[1], Charset.defaultCharset());
          doc.addField("distrib_s", dr);
        }

        if(parts[0].equals("shards")) {
          doc.addField("shards_s", "true");
        }

        if(parts[0].equals("ids") && !isRTGRequest(doc)) {
          doc.addField("ids_s", "true");
        }

        if(parts[0].equals("isShard")) {
          String dr = URLDecoder.decode(parts[1], Charset.defaultCharset());
          doc.addField("isShard_s", dr);
        }

        if(parts[0].equals("wt")) {
          String dr = URLDecoder.decode(parts[1], Charset.defaultCharset());
          doc.addField("wt_s", dr);
        }

        if(parts[0].equals("facet")) {
          String dr = URLDecoder.decode(parts[1], Charset.defaultCharset());
          doc.addField("facet_s", dr);
        }

        if(parts[0].equals("shards.purpose")) {
          try {
            int purpose = Integer.parseInt(parts[1]);
            String[] purposes = getRequestPurposeNames(purpose);
            for (String p : purposes) {
              doc.addField("purpose_ss", p);
            }
          } catch(Throwable e) {
            //We'll just sit on this for now and not interrupt the load for this one field.
          }
        }
      }

      //Special params used to determine what stage a query is.
      //So we populate with defaults.
      //The absence of the distrib params means its a distributed query.


      if(doc.getField("distrib_s") == null) {
        doc.addField("distrib_s", "true");
      }

      if(doc.getField("shards_s") == null) {
        doc.addField("shards_s", "false");
      }

      if(doc.getField("ids_s") == null) {
        doc.addField("ids_s", "false");
      }
    }

    private boolean isRTGRequest(SolrInputDocument doc) {
      final SolrInputField path = doc.getField("path_s");
      if (path == null) return false;


      return "/get".equals(path.getValue());
    }
  }

  private static final Map<Integer, String> purposes;
  protected static final String UNKNOWN_VALUE = "Unknown";
  private static final String[] purposeUnknown = new String[] { UNKNOWN_VALUE };

  public static String[] getRequestPurposeNames(Integer reqPurpose) {
    if (reqPurpose != null) {
      int valid = 0;
      for (Map.Entry<Integer, String>entry : purposes.entrySet()) {
        if ((reqPurpose & entry.getKey()) != 0) {
          valid++;
        }
      }
      if (valid == 0) {
        return purposeUnknown;
      } else {
        String[] result = new String[valid];
        int i = 0;
        for (Map.Entry<Integer, String>entry : purposes.entrySet()) {
          if ((reqPurpose & entry.getKey()) != 0) {
            result[i] = entry.getValue();
            i++;
          }
        }
        return result;
      }
    }
    return purposeUnknown;
  }

  static {
    Map<Integer, String> map = new TreeMap<>();
    map.put(ShardRequest.PURPOSE_PRIVATE, "PRIVATE");
    map.put(ShardRequest.PURPOSE_GET_TOP_IDS, "GET_TOP_IDS");
    map.put(ShardRequest.PURPOSE_REFINE_TOP_IDS, "REFINE_TOP_IDS");
    map.put(ShardRequest.PURPOSE_GET_FACETS, "GET_FACETS");
    map.put(ShardRequest.PURPOSE_REFINE_FACETS, "REFINE_FACETS");
    map.put(ShardRequest.PURPOSE_GET_FIELDS, "GET_FIELDS");
    map.put(ShardRequest.PURPOSE_GET_HIGHLIGHTS, "GET_HIGHLIGHTS");
    map.put(ShardRequest.PURPOSE_GET_DEBUG, "GET_DEBUG");
    map.put(ShardRequest.PURPOSE_GET_STATS, "GET_STATS");
    map.put(ShardRequest.PURPOSE_GET_TERMS, "GET_TERMS");
    map.put(ShardRequest.PURPOSE_GET_TOP_GROUPS, "GET_TOP_GROUPS");
    map.put(ShardRequest.PURPOSE_GET_MLT_RESULTS, "GET_MLT_RESULTS");
    map.put(ShardRequest.PURPOSE_REFINE_PIVOT_FACETS, "REFINE_PIVOT_FACETS");
    map.put(ShardRequest.PURPOSE_SET_TERM_STATS, "SET_TERM_STATS");
    map.put(ShardRequest.PURPOSE_GET_TERM_STATS, "GET_TERM_STATS");
    purposes = Collections.unmodifiableMap(map);
  }
}