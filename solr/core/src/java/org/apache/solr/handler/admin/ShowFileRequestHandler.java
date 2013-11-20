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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
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
 * <p>
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
 *   http://localhost:8983/solr/admin/file?file=schema.xml&contentType=text/plain
 * </pre>
 *
 *
 * @since solr 1.3
 *
 *
 * As of Solr 4.7, you can use this handler to modify any files in the conf directory, e.g. solrconfig.xml
 * or schema.xml, or even in sub-directories (e.g. velocity/error.vm) by POSTing a file. Here's an example cURL command
 * <pre>
 *   curl -X POST --form "fileupload=@schema.new" 'http://localhost:8983/solr/collection1/admin/file?op=write&file=schema.xml'
 * </pre>
 *
 * or
 * <pre>
 * curl -X POST --form "fileupload=@error.new" 'http://localhost:8983/solr/collection1/admin/file?op=write&file=velocity/error.vm'
 * </pre>
 *
 * For the first iteration, this is probably going to be used from the Solr admin screen.
 *
 * NOTE: Specifying a directory or simply leaving the any "file=XXX" parameters will list the contents of a directory.
 *
 * NOTE: <b>You must reload the core/collection for any changes made via this handler to take effect!</b>
 *
 * NOTE: <b>If the core does not load (say schema.xml is not well formed for instance) you may be unable to replace
 * the files with this interface.</b>
 *
 * Configuration files in ZooKeeper are supported.
 *
 * Writing files out, @since solr 4.7
 */
public class ShowFileRequestHandler extends RequestHandlerBase
{

  protected static final Logger log = LoggerFactory
      .getLogger(ShowFileRequestHandler.class);

  public static final String HIDDEN = "hidden";
  public static final String USE_CONTENT_TYPE = "contentType";
  
  protected Set<String> hiddenFiles;

  private final static String OP_PARAM = "op";
  private final static String OP_WRITE = "write";
  private final static String OP_TEST = "test";


  public ShowFileRequestHandler()
  {
    super();
  }

  @Override
  public void init(NamedList args) {
    super.init( args );

    // Build a list of hidden files
    hiddenFiles = new HashSet<String>();
    if( invariants != null ) {
      String[] hidden = invariants.getParams( HIDDEN );
      if( hidden != null ) {
        for( String s : hidden ) {
          hiddenFiles.add( s.toUpperCase(Locale.ROOT) );
        }
      }
    }
  }
  
  public Set<String> getHiddenFiles()
  {
    return hiddenFiles;
  }
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
      throws InterruptedException, KeeperException, IOException {

    CoreContainer coreContainer = req.getCore().getCoreDescriptor().getCoreContainer();
    String op = req.getParams().get(OP_PARAM);
    if (op == null) {
      if (coreContainer.isZooKeeperAware()) {
        showFromZooKeeper(req, rsp, coreContainer);
      } else {
        showFromFileSystem(req, rsp);
      }
    } else if (OP_WRITE.equalsIgnoreCase(op) || OP_TEST.equalsIgnoreCase(op)) {
      String fname = req.getParams().get("file", null);
      if (fname == null) {
        rsp.setException(new SolrException(ErrorCode.BAD_REQUEST, "No file name specified for write operation."));
      } else {
        fname = fname.replace('\\', '/');
        if (isHiddenFile(req, rsp, fname, true) == false) {
          if (coreContainer.isZooKeeperAware()) {
            writeToZooKeeper(req, rsp);
          } else {
            writeToFileSystem(req, rsp);
          }
        }
      }
    }
  }

  // See if we should deal with this file

  private boolean isHiddenFile(SolrQueryRequest req, SolrQueryResponse rsp, String fnameIn, boolean reportError) {
    String fname = fnameIn.toUpperCase(Locale.ROOT);
    if (hiddenFiles.contains(fname) || hiddenFiles.contains("*")) {
      if (reportError) {
        log.error("Cannot access " + fname);
        rsp.setException(new SolrException(ErrorCode.FORBIDDEN, "Can not access: " + fnameIn));
      }
      return true;
    }

    // This is slightly off, a valid path is something like ./schema.xml. I don't think it's worth the effort though
    // to fix it to handle all possibilities though.
    if (fname.indexOf("..") >= 0 || fname.startsWith(".")) {
      if (reportError) {
        log.error("Invalid path: " + fname);
        rsp.setException(new SolrException(ErrorCode.FORBIDDEN, "Invalid path: " + fnameIn));
      }
      return true;
    }

    // Make sure that if the schema is managed, we don't allow editing. Don't really want to put
    // this in the init since we're not entirely sure when the managed schema will get initialized relative to this
    // handler.
    SolrCore core = req.getCore();
    IndexSchema schema = core.getLatestSchema();
    if (schema instanceof ManagedIndexSchema) {
      String managed = schema.getResourceName();

      if (fname.equalsIgnoreCase(managed)) {
        return true;
      }
    }
    return false;
  }

  // Refactored to be usable from multiple methods. Gets the path of the requested file from ZK.
  // Returns null if the file is not found.
  //
  // Assumes that the file is in a parameter called "file".

  private String getAdminFileFromZooKeeper(SolrQueryRequest req, SolrQueryResponse rsp, SolrZkClient zkClient)
      throws KeeperException, InterruptedException {
    String adminFile = null;
    SolrCore core = req.getCore();

    final ZkSolrResourceLoader loader = (ZkSolrResourceLoader) core
        .getResourceLoader();
    String confPath = loader.getCollectionZkPath();

    String fname = req.getParams().get("file", null);
    if (fname == null) {
      adminFile = confPath;
    } else {
      fname = fname.replace('\\', '/'); // normalize slashes
      if (isHiddenFile(req, rsp, fname, true)) {
        return null;
      }
      if (fname.startsWith("/")) { // Only files relative to conf are valid
        fname = fname.substring(1);
      }
      adminFile = confPath + "/" + fname;
    }

    // Make sure the file exists, is readable and is not a hidden file
    if (!zkClient.exists(adminFile, true)) {
      log.error("Can not find: " + adminFile);
      rsp.setException(new SolrException(ErrorCode.NOT_FOUND, "Can not find: "
          + adminFile));
      return null;
    }

    return adminFile;
  }

  // write the file contained in the parameter "file=XXX" to ZooKeeper. The file may be a path, e.g.
  // file=velocity/error.vm or file=schema.xml
  //
  // Important: Assumes that the file already exists in ZK, so far we aren't creating files there.
  private void writeToZooKeeper(SolrQueryRequest req, SolrQueryResponse rsp)
      throws KeeperException, InterruptedException, IOException {

    CoreContainer coreContainer = req.getCore().getCoreDescriptor().getCoreContainer();
    SolrZkClient zkClient = coreContainer.getZkController().getZkClient();

    String adminFile = getAdminFileFromZooKeeper(req, rsp, zkClient);
    ContentStream stream = getOneInputStream(req, rsp);
    if (stream == null) {
      return; // Error already in rsp.
    }

    byte[] data = IOUtils.toByteArray(new InputStreamReader(stream.getStream(), "UTF-8"), "UTF-8");
    String fname = req.getParams().get("file", null);
    if (OP_TEST.equals(req.getParams().get(OP_PARAM)))  {
      testReloadSuccess(req, rsp, stream);
      return;
    }
    // Persist the managed schema
    try {
      // Assumption: the path exists
      zkClient.setData(adminFile, data, true);
      log.info("Saved " + fname + " to ZooKeeper successfully.");
    } catch (KeeperException.BadVersionException e) {
      log.error("Cannot save file: " + fname + " to Zookeeper, " +
          "ZooKeeper error: " + e.getMessage());
      rsp.setException(new SolrException(ErrorCode.SERVER_ERROR, "Cannot save file: " + fname + " to Zookeeper, " +
          "ZooKeeper error: " + e.getMessage()));
    }
  }

  // Get a list of files from ZooKeeper for from the path in the file= parameter.
  private void showFromZooKeeper(SolrQueryRequest req, SolrQueryResponse rsp,
      CoreContainer coreContainer) throws KeeperException,
      InterruptedException, UnsupportedEncodingException {

    SolrZkClient zkClient = coreContainer.getZkController().getZkClient();

    String adminFile = getAdminFileFromZooKeeper(req, rsp, zkClient);

    if (adminFile == null) {
      return;
    }

    // Show a directory listing
    List<String> children = zkClient.getChildren(adminFile, null, true);
    if (children.size() > 0) {
      
      NamedList<SimpleOrderedMap<Object>> files = new SimpleOrderedMap<SimpleOrderedMap<Object>>();
      for (String f : children) {
        if (isHiddenFile(req, rsp, f, false)) {
          continue;
        }

        SimpleOrderedMap<Object> fileInfo = new SimpleOrderedMap<Object>();
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
      content.setContentType(req.getParams().get(USE_CONTENT_TYPE));
      
      rsp.add(RawResponseWriter.CONTENT, content);
    }
    rsp.setHttpCaching(false);
  }


  // Used when POSTing the configuration files to Solr (either ZooKeeper or locally).
  //
  // It takes some effort to insure that there is one (and only one) stream provided, there's no provision for
  // more than one stream at present.
  private ContentStream getOneInputStream(SolrQueryRequest req, SolrQueryResponse rsp) {
    String file = req.getParams().get("file");
    if (file == null) {
      log.error("You must specify a file for the write operation.");
      rsp.setException(new SolrException(ErrorCode.BAD_REQUEST, "You must specify a file for the write operation."));
      return null;
    }

    // Now, this is truly clumsy
    Iterable<ContentStream> streams = req.getContentStreams();
    if (streams == null) {
      log.error("Input stream list was null for admin file write operation.");
      rsp.setException(new SolrException(ErrorCode.BAD_REQUEST, "Input stream list was null for admin file write operation."));
      return null;
    }
    Iterator<ContentStream> iter = streams.iterator();
    if (!iter.hasNext()) {
      log.error("No input streams were in the list for admin file write operation.");
      rsp.setException(new SolrException(ErrorCode.BAD_REQUEST, "No input streams were in the list for admin file write operation."));
      return null;
    }
    ContentStream stream = iter.next();
    if (iter.hasNext()) {
      log.error("More than one input stream was found for admin file write operation.");
      rsp.setException(new SolrException(ErrorCode.BAD_REQUEST, "More than one input stream was found for admin file write operation."));
      return null;
    }
    return stream;
  }


  // Write the data passed in from the stream to the file indicated by the file=XXX parameter on the local file system
  private void writeToFileSystem(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    ContentStream stream = getOneInputStream(req, rsp);
    if (stream == null) {
      return; // Error should already have been logged.
    }

    File adminFile = getAdminFileFromFileSystem(req, rsp);
    if (adminFile == null || adminFile.isDirectory()) {
      String fname = req.getParams().get("file", null);

      if (adminFile == null) {
        log.error("File " + fname + " was not found.");
        rsp.setException(new SolrException( ErrorCode.BAD_REQUEST, "File " + fname + " was not found."));
        return;
      }
      log.error("File " + fname + " is a directory.");
      rsp.setException(new SolrException( ErrorCode.BAD_REQUEST, "File " + fname + " is a directory."));
      return;
    }
    if (OP_TEST.equals(req.getParams().get(OP_PARAM))) {
      testReloadSuccess(req, rsp, stream);
      return;
    }

    FileUtils.copyInputStreamToFile(stream.getStream(), adminFile);
    log.info("Successfully saved file " + adminFile.getAbsolutePath() + " locally");
  }

  private boolean testReloadSuccess(SolrQueryRequest req, SolrQueryResponse rsp, ContentStream stream) {
    // Try writing the config to a temporary core and reloading to see that we don't allow people to shoot themselves
    // in the foot.
    File home = null;
    try {
      home = new File(FileUtils.getTempDirectory(), "SOLR_5459"); // Unlikely to name a core or collection this!
      FileUtils.writeStringToFile(new File(home, "solr.xml"), "<solr></solr>"); // Use auto-discovery
      File coll = new File(home, "SOLR_5459");

      SolrCore core = req.getCore();
      CoreDescriptor desc = core.getCoreDescriptor();
      CoreContainer coreContainer = desc.getCoreContainer();

      if (coreContainer.isZooKeeperAware()) {
        try {
          String confPath = ((ZkSolrResourceLoader) core.getResourceLoader()).getCollectionZkPath();

          ZkController.downloadConfigDir(coreContainer.getZkController().getZkClient(), confPath,
              new File(coll, "conf"));
        } catch (Exception ex) {
          log.error("Error when attempting to download conf from ZooKeeper: " + ex.getMessage());
          rsp.setException(new SolrException(ErrorCode.BAD_REQUEST,
              "Error when attempting to download conf from ZooKeeper" + ex.getMessage()));
          return false;
        }
      } else {
        FileUtils.copyDirectory(new File(desc.getInstanceDir(), "conf"),
            new File(coll, "conf"));
      }

      FileUtils.writeStringToFile(new File(coll, "core.properties"), "name=SOLR_5459");

      FileUtils.copyInputStreamToFile(stream.getStream(),
          new File(new File(coll, "conf"), req.getParams().get("file", null)));

      return tryReloading(rsp, home);

    } catch (IOException ex) {
      log.warn("Caught IO exception when trying to verify configs. " + ex.getMessage());
      rsp.setException(new SolrException(ErrorCode.SERVER_ERROR,
          "Caught IO exception when trying to verify configs. " + ex.getMessage()));
      return false;
    } finally {
      if (home != null) {
        try {
          FileUtils.deleteDirectory(home);
        } catch (IOException e) {
          log.warn("Caught IO exception trying to delete temporary directory " + home + e.getMessage());
          return true; // Don't fail for this reason!
        }
      }
    }
  }

  private boolean tryReloading(SolrQueryResponse rsp, File home) {
    CoreContainer cc = null;
    try {
      cc = CoreContainer.createAndLoad(home.getAbsolutePath(), new File(home, "solr.xml"));
      if (cc.getCoreInitFailures().size() > 0) {
        for (Exception ex : cc.getCoreInitFailures().values()) {
          log.error("Error when attempting to reload core: " + ex.getMessage());
          rsp.setException(new SolrException( ErrorCode.BAD_REQUEST,
              "Error when attempting to reload core after writing config" + ex.getMessage()));
        }
        return false;
      }
      return true;
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
    }
  }

  // Find the file indicated by the "file=XXX" parameter or the root of the conf directory on the local
  // file system. Respects all the "interesting" stuff around what the resource loader does to find files.
  private File getAdminFileFromFileSystem(SolrQueryRequest req, SolrQueryResponse rsp) {
    File adminFile = null;
    final SolrResourceLoader loader = req.getCore().getResourceLoader();
    File configdir = new File( loader.getConfigDir() );
    if (!configdir.exists()) {
      // TODO: maybe we should just open it this way to start with?
      try {
        configdir = new File( loader.getClassLoader().getResource(loader.getConfigDir()).toURI() );
      } catch (URISyntaxException e) {
        log.error("Can not access configuration directory!");
        rsp.setException(new SolrException( ErrorCode.FORBIDDEN, "Can not access configuration directory!", e));
        return null;
      }
    }
    String fname = req.getParams().get("file", null);
    if( fname == null ) {
      adminFile = configdir;
    }
    else {
      fname = fname.replace( '\\', '/' ); // normalize slashes
      if( hiddenFiles.contains( fname.toUpperCase(Locale.ROOT) ) ) {
        log.error("Can not access: "+ fname);
        rsp.setException(new SolrException( ErrorCode.FORBIDDEN, "Can not access: "+fname ));
        return null;
      }
      if( fname.indexOf( ".." ) >= 0 ) {
        log.error("Invalid path: "+ fname);
        rsp.setException(new SolrException( ErrorCode.FORBIDDEN, "Invalid path: "+fname ));
        return null;
      }
      adminFile = new File( configdir, fname );
    }
    return adminFile;
  }

  // Return the file indicated (or the directory listing) from the local file system.
  private void showFromFileSystem(SolrQueryRequest req, SolrQueryResponse rsp) {
    File adminFile = getAdminFileFromFileSystem(req, rsp);

    if (adminFile == null) { // exception already recorded
      return;
    }

    // Make sure the file exists, is readable and is not a hidden file
    if( !adminFile.exists() ) {
      log.error("Can not find: "+adminFile.getName() + " ["+adminFile.getAbsolutePath()+"]");
      rsp.setException(new SolrException
                       ( ErrorCode.NOT_FOUND, "Can not find: "+adminFile.getName() 
                         + " ["+adminFile.getAbsolutePath()+"]" ));
      return;
    }
    if( !adminFile.canRead() || adminFile.isHidden() ) {
      log.error("Can not show: "+adminFile.getName() + " ["+adminFile.getAbsolutePath()+"]");
      rsp.setException(new SolrException
                       ( ErrorCode.NOT_FOUND, "Can not show: "+adminFile.getName() 
                         + " ["+adminFile.getAbsolutePath()+"]" ));
      return;
    }
    
    // Show a directory listing
    if( adminFile.isDirectory() ) {
      // it's really a directory, just go for it.
      int basePath = adminFile.getAbsolutePath().length() + 1;
      NamedList<SimpleOrderedMap<Object>> files = new SimpleOrderedMap<SimpleOrderedMap<Object>>();
      for( File f : adminFile.listFiles() ) {
        String path = f.getAbsolutePath().substring( basePath );
        path = path.replace( '\\', '/' ); // normalize slashes

        if (isHiddenFile(req, rsp, f.getName().replace('\\', '/'), false)) {
          continue;
        }

        SimpleOrderedMap<Object> fileInfo = new SimpleOrderedMap<Object>();
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
      content.setContentType(req.getParams().get(USE_CONTENT_TYPE));

      rsp.add(RawResponseWriter.CONTENT, content);
    }
    rsp.setHttpCaching(false);
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Admin Config File -- view or update config files directly";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}
