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
package org.apache.solr.handler.admin;

import com.google.common.base.Strings;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.http.MimeTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * This handler uses the RawResponseWriter to give client access to
 * files inside ${solr.home}/conf
 * <p>
 * If you want to selectively restrict access some configuration files, you can list
 * these files in the {@link #HIDDEN} invariants.  For example to hide 
 * synonyms.txt and anotherfile.txt, you would register:
 * <br>
 * <pre>
 * &lt;requestHandler name="/admin/file" class="org.apache.solr.handler.admin.ShowFileRequestHandler" &gt;
 *   &lt;lst name="defaults"&gt;
 *    &lt;str name="echoParams"&gt;explicit&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="invariants"&gt;
 *    &lt;str name="hidden"&gt;synonyms.txt&lt;/str&gt; 
 *    &lt;str name="hidden"&gt;anotherfile.txt&lt;/str&gt;
 *    &lt;str name="hidden"&gt;*&lt;/str&gt;
 *   &lt;/lst&gt;
 * &lt;/requestHandler&gt;
 * </pre>
 *
 * At present, there is only explicit file names (including path) or the glob '*' are supported. Variants like '*.xml'
 * are NOT supported.ere
 *
 * <p>
 * The ShowFileRequestHandler uses the {@link RawResponseWriter} (wt=raw) to return
 * file contents.  If you need to use a different writer, you will need to change 
 * the registered invariant param for wt.
 * <p>
 * If you want to override the contentType header returned for a given file, you can
 * set it directly using: {@link #USE_CONTENT_TYPE}.  For example, to get a plain text
 * version of schema.xml, try:
 * <pre>
 *   http://localhost:8983/solr/admin/file?file=schema.xml&amp;contentType=text/plain
 * </pre>
 *
 *
 * @since solr 1.3
 */
public class ShowFileRequestHandler extends RequestHandlerBase implements PermissionNameProvider
{
  public static final String HIDDEN = "hidden";
  public static final String USE_CONTENT_TYPE = "contentType";
  private static final Set<String> KNOWN_MIME_TYPES;

  protected Set<String> hiddenFiles;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static {
    KNOWN_MIME_TYPES = new HashSet<>(MimeTypes.getKnownMimeTypes());
    KNOWN_MIME_TYPES.add("text/xml");
    KNOWN_MIME_TYPES.add("text/javascript");
  }

  public ShowFileRequestHandler()
  {
    super();
  }

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    super.init( args );
    hiddenFiles = initHidden(invariants);
  }

  public static Set<String> initHidden(SolrParams invariants) {

    Set<String> hiddenRet = new HashSet<>();
    // Build a list of hidden files
    if (invariants != null) {
      String[] hidden = invariants.getParams(HIDDEN);
      if (hidden != null) {
        for (String s : hidden) {
          hiddenRet.add(s.toUpperCase(Locale.ROOT));
        }
      }
    }
    return hiddenRet;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
      throws InterruptedException, KeeperException, IOException {

    CoreContainer coreContainer = req.getCore().getCoreContainer();
    if (coreContainer.isZooKeeperAware()) {
      showFromZooKeeper(req, rsp, coreContainer);
    } else {
      showFromFileSystem(req, rsp);
    }
  }

  // Get a list of files from ZooKeeper for from the path in the file= parameter.
  private void showFromZooKeeper(SolrQueryRequest req, SolrQueryResponse rsp,
      CoreContainer coreContainer) throws KeeperException,
      InterruptedException, UnsupportedEncodingException {

    SolrZkClient zkClient = coreContainer.getZkController().getZkClient();

    String adminFile = getAdminFileFromZooKeeper(req, rsp, zkClient, hiddenFiles);

    if (adminFile == null) {
      return;
    }

    // Show a directory listing
    List<String> children = zkClient.getChildren(adminFile, null, true);
    if (children.size() > 0) {
      
      NamedList<SimpleOrderedMap<Object>> files = new SimpleOrderedMap<>();
      for (String f : children) {
        if (isHiddenFile(req, rsp, f, false, hiddenFiles)) {
          continue;
        }

        SimpleOrderedMap<Object> fileInfo = new SimpleOrderedMap<>();
        files.add(f, fileInfo);
        List<String> fchildren = zkClient.getChildren(adminFile + "/" + f, null, true);
        if (fchildren.size() > 0) {
          fileInfo.add("directory", true);
        } else {
          // TODO? content type
          fileInfo.add("size", f.length());
        }
        // TODO: ?
        // fileInfo.add( "modified", new Date( f.lastModified() ) );
      }
      rsp.add("files", files);
    } else {
      // Include the file contents
      // The file logic depends on RawResponseWriter, so force its use.
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(CommonParams.WT, "raw");
      req.setParams(params);
      ContentStreamBase content = new ContentStreamBase.ByteArrayStream(zkClient.getData(adminFile, null, null, true), adminFile);
      content.setContentType(getSafeContentType(req.getParams().get(USE_CONTENT_TYPE)));

      rsp.add(RawResponseWriter.CONTENT, content);
    }
    rsp.setHttpCaching(false);
  }

  // Return the file indicated (or the directory listing) from the local file system.
  private void showFromFileSystem(SolrQueryRequest req, SolrQueryResponse rsp) {
    File adminFile = getAdminFileFromFileSystem(req, rsp, hiddenFiles);

    if (adminFile == null) { // exception already recorded
      return;
    }

    // Make sure the file exists, is readable and is not a hidden file
    if( !adminFile.exists() ) {
      log.error("Can not find: {} [{}]", adminFile.getName(), adminFile.getAbsolutePath());
      rsp.setException(new SolrException
                       ( ErrorCode.NOT_FOUND, "Can not find: "+adminFile.getName()
                         + " ["+adminFile.getAbsolutePath()+"]" ));
      return;
    }
    if( !adminFile.canRead() || adminFile.isHidden() ) {
      log.error("Can not show: {} [{}]", adminFile.getName(), adminFile.getAbsolutePath());
      rsp.setException(new SolrException
                       ( ErrorCode.NOT_FOUND, "Can not show: "+adminFile.getName()
                         + " ["+adminFile.getAbsolutePath()+"]" ));
      return;
    }

    // Show a directory listing
    if( adminFile.isDirectory() ) {
      // it's really a directory, just go for it.
      int basePath = adminFile.getAbsolutePath().length() + 1;
      NamedList<SimpleOrderedMap<Object>> files = new SimpleOrderedMap<>();
      for( File f : adminFile.listFiles() ) {
        String path = f.getAbsolutePath().substring( basePath );
        path = path.replace( '\\', '/' ); // normalize slashes

        if (isHiddenFile(req, rsp, f.getName().replace('\\', '/'), false, hiddenFiles)) {
          continue;
        }

        SimpleOrderedMap<Object> fileInfo = new SimpleOrderedMap<>();
        files.add( path, fileInfo );
        if( f.isDirectory() ) {
          fileInfo.add( "directory", true );
        }
        else {
          // TODO? content type
          fileInfo.add( "size", f.length() );
        }
        fileInfo.add( "modified", new Date( f.lastModified() ) );
      }
      rsp.add("files", files);
    }
    else {
      // Include the file contents
      //The file logic depends on RawResponseWriter, so force its use.
      ModifiableSolrParams params = new ModifiableSolrParams( req.getParams() );
      params.set( CommonParams.WT, "raw" );
      req.setParams(params);

      ContentStreamBase content = new ContentStreamBase.FileStream( adminFile );
      content.setContentType(getSafeContentType(req.getParams().get(USE_CONTENT_TYPE)));

      rsp.add(RawResponseWriter.CONTENT, content);
    }
    rsp.setHttpCaching(false);
  }

  /**
   * Checks content type string and returns it if it is one of allowed types.
   * The allowed types are all standard mime types.
   * If an HTML type is requested, it is instead returned as text/plain
   */
  public static String getSafeContentType(String contentType) {
    if (Strings.isNullOrEmpty(contentType)) {
      log.debug("No contentType specified");
      return null;
    }
    String rawContentType = contentType.split(";")[0].trim().toLowerCase(Locale.ROOT); // Strip away charset part
    if (!KNOWN_MIME_TYPES.contains(rawContentType)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Requested content type '" + contentType + "' is not supported.");
    }
    if (rawContentType.contains("html")) {
      log.info("Using text/plain instead of {}", contentType);
      return "text/plain";
    }
    return contentType;
  }

  //////////////////////// Static methods //////////////////////////////

  public static boolean isHiddenFile(SolrQueryRequest req, SolrQueryResponse rsp, String fnameIn, boolean reportError,
                                     Set<String> hiddenFiles) {
    String fname = fnameIn.toUpperCase(Locale.ROOT);
    if (hiddenFiles.contains(fname) || hiddenFiles.contains("*")) {
      if (reportError) {
        log.error("Cannot access {}", fname);
        rsp.setException(new SolrException(SolrException.ErrorCode.FORBIDDEN, "Can not access: " + fnameIn));
      }
      return true;
    }

    // This is slightly off, a valid path is something like ./schema.xml. I don't think it's worth the effort though
    // to fix it to handle all possibilities though.
    if (fname.indexOf("..") >= 0 || fname.startsWith(".")) {
      if (reportError) {
        log.error("Invalid path: {}", fname);
        rsp.setException(new SolrException(SolrException.ErrorCode.FORBIDDEN, "Invalid path: " + fnameIn));
      }
      return true;
    }
    return false;
  }

  // Refactored to be usable from multiple methods. Gets the path of the requested file from ZK.
  // Returns null if the file is not found.
  //
  // Assumes that the file is in a parameter called "file".

  public static String getAdminFileFromZooKeeper(SolrQueryRequest req, SolrQueryResponse rsp, SolrZkClient zkClient,
                                                 Set<String> hiddenFiles)
      throws KeeperException, InterruptedException {
    String adminFile = null;
    SolrCore core = req.getCore();

    final ZkSolrResourceLoader loader = (ZkSolrResourceLoader) core
        .getResourceLoader();
    String confPath = loader.getConfigSetZkPath();

    String fname = req.getParams().get("file", null);
    if (fname == null) {
      adminFile = confPath;
    } else {
      fname = fname.replace('\\', '/'); // normalize slashes
      if (isHiddenFile(req, rsp, fname, true, hiddenFiles)) {
        return null;
      }
      if (fname.startsWith("/")) { // Only files relative to conf are valid
        fname = fname.substring(1);
      }
      adminFile = confPath + "/" + fname;
    }

    // Make sure the file exists, is readable and is not a hidden file
    if (!zkClient.exists(adminFile, true)) {
      log.error("Can not find: {}", adminFile);
      rsp.setException(new SolrException(SolrException.ErrorCode.NOT_FOUND, "Can not find: "
          + adminFile));
      return null;
    }

    return adminFile;
  }


  // Find the file indicated by the "file=XXX" parameter or the root of the conf directory on the local
  // file system. Respects all the "interesting" stuff around what the resource loader does to find files.
  public static File getAdminFileFromFileSystem(SolrQueryRequest req, SolrQueryResponse rsp,
                                                Set<String> hiddenFiles) {
    File adminFile = null;
    final SolrResourceLoader loader = req.getCore().getResourceLoader();
    File configdir = new File( loader.getConfigDir() );
    if (!configdir.exists()) {
      // TODO: maybe we should just open it this way to start with?
      try {
        configdir = new File( loader.getClassLoader().getResource(loader.getConfigDir()).toURI() );
      } catch (URISyntaxException e) {
        log.error("Can not access configuration directory!");
        rsp.setException(new SolrException( SolrException.ErrorCode.FORBIDDEN, "Can not access configuration directory!", e));
        return null;
      }
    }
    String fname = req.getParams().get("file", null);
    if( fname == null ) {
      adminFile = configdir;
    } else {
      fname = fname.replace('\\', '/'); // normalize slashes
      if (hiddenFiles.contains(fname.toUpperCase(Locale.ROOT))) {
        log.error("Can not access: {}", fname);
        rsp.setException(new SolrException(SolrException.ErrorCode.FORBIDDEN, "Can not access: " + fname));
        return null;
      }
      // A leading slash is unnecessary but supported and interpreted as start of config dir
      Path filePath = configdir.toPath().resolve(fname.startsWith("/") ? fname.substring(1) : fname);
      req.getCore().getCoreContainer().assertPathAllowed(filePath);
      if (!filePath.normalize().startsWith(configdir.toPath().normalize())) {
        log.error("Path must be inside core config directory");
        rsp.setException(
            new SolrException(ErrorCode.BAD_REQUEST, "Path must be inside core config directory"));
        return null;
      }
      adminFile = filePath.toFile();
    }
    return adminFile;
  }

  public final Set<String> getHiddenFiles() {
    return hiddenFiles;
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Admin Config File -- view or update config files directly";
  }
  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.CONFIG_READ_PERM;
  }
}
