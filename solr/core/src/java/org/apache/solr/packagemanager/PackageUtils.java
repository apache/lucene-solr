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

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.BlobRepository;
import org.apache.solr.filestore.DistribPackageStore;
import org.apache.solr.filestore.PackageStoreAPI;
import org.apache.solr.packagemanager.SolrPackage.Manifest;
import org.apache.solr.util.SolrJacksonAnnotationInspector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.zafarkhaja.semver.Version;
import com.google.common.base.Strings;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

public class PackageUtils {

  /**
   * Represents a version which denotes the latest version available at the moment.
   */
  public static String LATEST = "latest";
  
  public static String PACKAGE_PATH = "/api/cluster/package";
  public static String CLUSTER_PLUGINS_PATH = "/api/cluster/plugin";
  public static String REPOSITORIES_ZK_PATH = "/repositories.json";
  public static String CLUSTERPROPS_PATH = "/api/cluster/zk/data/clusterprops.json";
  
 
  public static Configuration jsonPathConfiguration() {
    MappingProvider provider = new JacksonMappingProvider();
    JsonProvider jsonProvider = new JacksonJsonProvider();
    Configuration c = Configuration.builder().jsonProvider(jsonProvider).mappingProvider(provider).options(com.jayway.jsonpath.Option.REQUIRE_PROPERTIES).build();
    return c;
  }

  public static ObjectMapper getMapper() {
    return new ObjectMapper().setAnnotationIntrospector(new SolrJacksonAnnotationInspector());
  }

  /**
   * Uploads a file to the package store / file store of Solr.
   * 
   * @param client A Solr client
   * @param buffer File contents
   * @param name Name of the file as it will appear in the file store (can be hierarchical)
   * @param sig Signature digest (public key should be separately uploaded to ZK)
   */
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

  /**
   * Download JSON from the url and deserialize into klass.
   */
  public static <T> T getJson(HttpClient client, String url, Class<T> klass) {
    try {
      return getMapper().readValue(getJsonStringFromUrl(client, url), klass);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } 
  }

  /**
   * Search through the list of jar files for a given file. Returns string of
   * the file contents or null if file wasn't found. This is suitable for looking
   * for manifest or property files within pre-downloaded jar files.
   * Please note that the first instance of the file found is returned.
   */
  public static String getFileFromJarsAsString(List<Path> jars, String filename) {
    for (Path jarfile: jars) {
      try (ZipFile zipFile = new ZipFile(jarfile.toFile())) {
        ZipEntry entry = zipFile.getEntry(filename);
        if (entry == null) continue;
        return IOUtils.toString(zipFile.getInputStream(entry), "UTF-8");
      } catch (Exception ex) {
        throw new SolrException(ErrorCode.BAD_REQUEST, ex);
      }
    }
    return null;
  }

  /**
   * Returns JSON string from a given URL
   */
  public static String getJsonStringFromUrl(HttpClient client, String url) {
    try {
      HttpResponse resp = client.execute(new HttpGet(url));
      if (resp.getStatusLine().getStatusCode() != 200) {
        throw new SolrException(ErrorCode.NOT_FOUND,
            "Error (code="+resp.getStatusLine().getStatusCode()+") fetching from URL: "+url);
      }
      return IOUtils.toString(resp.getEntity().getContent(), "UTF-8");
    } catch (UnsupportedOperationException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Checks whether a given version satisfies the constraint (defined by a semver expression)
   */
  public static boolean checkVersionConstraint(String ver, String constraint) {
    return Strings.isNullOrEmpty(constraint) || Version.valueOf(ver).satisfies(constraint);
  }

  /**
   * Fetches a manifest file from the File Store / Package Store. A SHA512 check is enforced after fetching.
   */
  public static Manifest fetchManifest(HttpSolrClient solrClient, String solrBaseUrl, String manifestFilePath, String expectedSHA512) throws MalformedURLException, IOException {
    String manifestJson = PackageUtils.getJsonStringFromUrl(solrClient.getHttpClient(), solrBaseUrl + "/api/node/files" + manifestFilePath);
    String calculatedSHA512 = BlobRepository.sha512Digest(ByteBuffer.wrap(manifestJson.getBytes("UTF-8")));
    if (expectedSHA512.equals(calculatedSHA512) == false) {
      throw new SolrException(ErrorCode.UNAUTHORIZED, "The manifest SHA512 doesn't match expected SHA512. Possible unauthorized manipulation. "
          + "Expected: " + expectedSHA512 + ", calculated: " + calculatedSHA512 + ", manifest location: " + manifestFilePath);
    }
    Manifest manifest = getMapper().readValue(manifestJson, Manifest.class);
    return manifest;
  }

  /**
   * Replace a templatized string with parameter substituted string. First applies the overrides, then defaults and then systemParams.
   */
  public static String resolve(String str, Map<String, String> defaults, Map<String, String> overrides, Map<String, String> systemParams) {
    // TODO: Should perhaps use Matchers etc. instead of this clumsy replaceAll().

    if (str == null) return null;
    if (defaults != null) {
      for (String param: defaults.keySet()) {
        str = str.replaceAll("\\$\\{"+param+"\\}", overrides.containsKey(param)? overrides.get(param): defaults.get(param));
      }
    }
    for (String param: overrides.keySet()) {
      str = str.replaceAll("\\$\\{"+param+"\\}", overrides.get(param));
    }
    for (String param: systemParams.keySet()) {
      str = str.replaceAll("\\$\\{"+param+"\\}", systemParams.get(param));
    }
    return str;
  }

  /**
   * Compares two versions v1 and v2. Returns negative if v1 isLessThan v2, positive if v1 isGreaterThan v2 and 0 if equal.
   */
  public static int compareVersions(String v1, String v2) {
    return Version.valueOf(v1).compareTo(Version.valueOf(v2));
  }

  public static String BLACK = "\u001B[30m";
  public static String RED = "\u001B[31m";
  public static String GREEN = "\u001B[32m";
  public static String YELLOW = "\u001B[33m";
  public static String BLUE = "\u001B[34m";
  public static String PURPLE = "\u001B[35m";
  public static String CYAN = "\u001B[36m";
  public static String WHITE = "\u001B[37m";

  /**
   * Console print using green color
   */
  public static void printGreen(Object message) {
    PackageUtils.print(PackageUtils.GREEN, message);
  }

  /**
   * Console print using red color
   */
  public static void printRed(Object message) {
    PackageUtils.print(PackageUtils.RED, message);
  }

  public static void print(Object message) {
    print(null, message);
  }

  @SuppressForbidden(reason = "Need to use System.out.println() instead of log4j/slf4j for cleaner output")
  public static void print(String color, Object message) {
    String RESET = "\u001B[0m";

    if (color != null) {
      System.out.println(color + String.valueOf(message) + RESET);
    } else {
      System.out.println(message);
    }
  }

  public static String[] validateCollections(String collections[]) {
    String collectionNameRegex = "^[a-zA-Z0-9_-]*$";
    for (String c: collections) {
      if (c.matches(collectionNameRegex) == false) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid collection name: " + c +
            ". Didn't match the pattern: '"+collectionNameRegex+"'");
      }
    }
    return collections;
  }
  
  /**
   * Replacement for Java 11's Map.of(). Borrowed from SolrTestCaseJ4's map().
   */
  public static Map map(Object... params) {
    LinkedHashMap ret = new LinkedHashMap();
    for (int i=0; i<params.length; i+=2) {
      Object o = ret.put(params[i], params[i+1]);
      // TODO: handle multi-valued map?
    }
    return ret;
  }

  public static String getCollectionParamsPath(String collection) {
    return "/api/collections/" + collection + "/config/params";
  }

  public static void uploadKey(byte bytes[], String path, Path home, HttpSolrClient client) throws IOException {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    PackageStoreAPI.MetaData meta = PackageStoreAPI._createJsonMetaData(buf, null);
    DistribPackageStore._persistToFile(home, path, buf, ByteBuffer.wrap(Utils.toJSON(meta)));
  }

}
