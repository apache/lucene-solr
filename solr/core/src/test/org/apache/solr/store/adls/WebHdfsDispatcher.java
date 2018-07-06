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

package org.apache.solr.store.adls;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import com.microsoft.azure.datalake.store.DirectoryEntryType;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.QueueDispatcher;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebHdfsDispatcher extends QueueDispatcher {
  private static final MockResponse DEAD_LETTER = (new MockResponse()).setStatus("HTTP/1.1 503 shutting down");
  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());;
  protected final BlockingQueue<MockResponse> responseQueue = new LinkedBlockingQueue();
  private MockResponse failFastResponse;
  private String path;


  public WebHdfsDispatcher(String path) {
    this.path=path;
  }

  public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
    String requestLine = request.getRequestLine();

    Map<String,String> parms =
      Arrays.stream(requestLine.substring(requestLine.indexOf('?')+1).split("&"))
      .map((item)->StringUtils.split(item,"="))
      .collect(Collectors.toMap(((item)->item[0]),(item)->item[1]));

    String file = StringUtils.substringBetween(requestLine,"/v1","?");
    LOGGER.debug("file is "+file+", parms are "+ parms);

    String op = parms.get("op");

    LOGGER.debug("op is "+op);

    assert op != null;
    MockResponse result = null;

    try {
      if (op.equalsIgnoreCase("CREATE")) {
        File foo = new File(path, file);
        foo.getParentFile().mkdirs();
        if (!foo.createNewFile()){
          result = new MockResponse();
          result.setResponseCode(500);
          return result;
        }
        result = new MockResponse();
        result.setResponseCode(201);
        return result;
      } else if (op.equalsIgnoreCase("GETFILESTATUS")) {
        File foo = new File(path, file);
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("\"length\":"+foo.length()+",");
        builder.append("\"accessTime\":12345678,");

        DirectoryEntryType type = (foo.isDirectory()? DirectoryEntryType.DIRECTORY:DirectoryEntryType.FILE);

        builder.append("\"type\":\""+type.toString()+"\",");
        builder.append("\"modificationTime\":"+foo.lastModified()+",");
        builder.append("\"permission\":"+"\"777\""+",");
        builder.append("\"owner\":"+"\"yomomma\""+",");
        builder.append("\"group\":"+"\"yomomma\""+",");
        builder.append("\"blockSize\":"+"1"+",");
        builder.append("\"replication\":"+"1");
        builder.append("}");

        LOGGER.debug("send back "+builder);

        result = new MockResponse();
        result.setResponseCode(200);
        result.setBody(builder.toString());
        return result;

      } else if (op.equalsIgnoreCase("APPEND")) {
        File writeMe = new File(path, file);
        LOGGER.debug("Write to "+writeMe+" position "+writeMe.length());

        try (FileOutputStream fout = new FileOutputStream(writeMe,true)){
          FileChannel channel = fout.getChannel();
          if (writeMe.length()!=0){
            channel.position(writeMe.length());
          }
          fout.getChannel().write(ByteBuffer.wrap(request.getBody().readByteArray()));
        }
        result = new MockResponse();
        result.setResponseCode(200);
        return result;
      } else if (op.equalsIgnoreCase("OPEN")) {
        File daFile = new File(path, file);
        int offset=0;
        int buffSize=0;
        if (parms.containsKey("offset")){
          offset=Integer.parseInt(parms.get("offset"));
        }
        if (parms.containsKey("length")){
          buffSize=Integer.parseInt(parms.get("length"));
        }

        try (FileInputStream data = new FileInputStream(daFile)) {
          data.getChannel().position((long)offset);

          result = new MockResponse();
          Buffer buff = new Buffer();
          if (buffSize!=0){
            byte bu[]=new byte[buffSize];
            IOUtils.read(data,bu);
            ByteArrayInputStream byis = new ByteArrayInputStream(bu);
            IOUtils.copy(byis,buff.outputStream());
          } else {
            IOUtils.copy(data,buff.outputStream());
          }
          result.setBody(buff);
          result.setResponseCode(200);
        }
        return result;
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    assert result != null;
    return result;
  }

  public MockResponse peek() {
    MockResponse peek = (MockResponse)this.responseQueue.peek();
    if (peek != null) {
      return peek;
    } else {
      return this.failFastResponse != null ? this.failFastResponse : super.peek();
    }
  }

  public void enqueueResponse(MockResponse response) {
    this.responseQueue.add(response);
  }

  public void shutdown() {
    this.responseQueue.add(DEAD_LETTER);
  }

  public void setFailFast(boolean failFast) {
    MockResponse failFastResponse = failFast ? (new MockResponse()).setResponseCode(404) : null;
    this.setFailFast(failFastResponse);
  }

  public void setFailFast(MockResponse failFastResponse) {
    this.failFastResponse = failFastResponse;
  }
}
