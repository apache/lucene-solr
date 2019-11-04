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
package org.apache.solr.packagemanager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;

import com.github.zafarkhaja.semver.Version;
import com.google.common.base.Strings;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

public class PackageUtils {
  
  public static Configuration jsonPathConfiguration() {
    MappingProvider provider = new JacksonMappingProvider();
    JsonProvider jsonProvider = new JacksonJsonProvider();
    Configuration c = Configuration.builder().jsonProvider(jsonProvider).mappingProvider(provider).options(com.jayway.jsonpath.Option.REQUIRE_PROPERTIES).build();
    return c;
  }
  
  public static ByteBuffer getFileContent(File file) throws IOException {
    ByteBuffer jar;
    try (FileInputStream fis = new FileInputStream(file)) {
      byte[] buf = new byte[fis.available()];
      fis.read(buf);
      jar = ByteBuffer.wrap(buf);
    }
    return jar;
  }

  public static void postFile(SolrClient client, ByteBuffer buffer, String name, String sig)
      throws SolrServerException, IOException {
    String resource = "/api/cluster/files" + name;
    ModifiableSolrParams params = new ModifiableSolrParams();
    if (sig != null) {
      params.add("sig", sig);
    }
    V2Response rsp = new V2Request.Builder(resource)
        .withMethod(SolrRequest.METHOD.PUT)
        .withPayload(buffer)
        .forceV2(true)
        .withMimeType("application/octet-stream")
        .withParams(params)
        .build()
        .process(client);
    if (!name.equals(rsp.getResponse().get(CommonParams.FILE))) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Mismatch in file uploaded. Uploaded: " +
          rsp.getResponse().get(CommonParams.FILE)+", Original: "+name);
    }
  }

  public static String getStringFromStream(String url) {
    return get(url);
  }

  public static String get(String url) {
    try (CloseableHttpClient client = PackageUtils.createTrustAllHttpClientBuilder()) {
      HttpGet httpGet = new HttpGet(url);
      httpGet.setHeader("Content-type", "application/json");

      CloseableHttpResponse response = client.execute(httpGet);

      try {
        HttpEntity rspEntity = response.getEntity();
        if (rspEntity != null) {
          InputStream is = rspEntity.getContent();
          StringWriter writer = new StringWriter();
          IOUtils.copy(is, writer, "UTF-8");
          String results = writer.toString();

          return(results);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (Exception e1) {
      throw new RuntimeException(e1);
    }
    return null;
  }

  public static CloseableHttpClient createTrustAllHttpClientBuilder() throws Exception {
    SSLContextBuilder builder = new SSLContextBuilder();
    builder.loadTrustMaterial(null, (chain, authType) -> true);           
    SSLConnectionSocketFactory sslsf = new 
        SSLConnectionSocketFactory(builder.build(), NoopHostnameVerifier.INSTANCE);
    return HttpClients.custom().setSSLSocketFactory(sslsf).build();
  }

  public static boolean checkVersionConstraint(String solrVersion, String minInclusive, String maxInclusive) {
    String constraint = ">="+minInclusive + " & <="+maxInclusive;
    System.out.println("Current: "+solrVersion+", constraint: "+constraint);
    return Strings.isNullOrEmpty(constraint) || Version.valueOf(solrVersion).satisfies(constraint);
  }

  public static int compareVersions(String v1, String v2) {
    return Version.valueOf(v1).compareTo(Version.valueOf(v2));
  }

}
