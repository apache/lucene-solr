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
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.Config;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;

/**
 * This handler uses the RawResponseWriter to give client access to
 * files inside ${solr.home}/conf
 * <p/>
 * If you want to selectively restrict access some configuration files, you can list
 * these files in the hidden invariants.  For example to hide
 * synonyms.txt and anotherfile.txt, you would register:
 * <p/>
 * <pre>
 * &lt;requestHandler name="/admin/fileupdate" class="org.apache.solr.handler.admin.EditFileRequestHandler" &gt;
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
 * <p/>
 * At present, there is only explicit file names (including path) or the glob '*' are supported. Variants like '*.xml'
 * are NOT supported.ere
 * <p/>
 * <p/>
 * The EditFileRequestHandler uses the {@link RawResponseWriter} (wt=raw) to return
 * file contents.  If you need to use a different writer, you will need to change
 * the registered invariant param for wt.
 * <p/>
 * If you want to override the contentType header returned for a given file, you can
 * set it directly using: CONTENT_TYPE.  For example, to get a plain text
 * version of schema.xml, try:
 * <pre>
 *   http://localhost:8983/solr/admin/fileedit?file=schema.xml&contentType=text/plain
 * </pre>
 *
 * @since solr 4.7
 *        <p/>
 *        <p/>
 *        You can use this handler to modify any files in the conf directory, e.g. solrconfig.xml
 *        or schema.xml, or even in sub-directories (e.g. velocity/error.vm) by POSTing a file. Here's an example cURL command
 *        <pre>
 *                                            curl -X POST --form "fileupload=@schema.new" 'http://localhost:8983/solr/collection1/admin/fileedit?op=write&file=schema.xml'
 *                                           </pre>
 *
 *        or
 *        <pre>
 *                                            curl -X POST --form "fileupload=@error.new" 'http://localhost:8983/solr/collection1/admin/file?op=write&file=velocity/error.vm'
 *                                           </pre>
 *
 *        For the first iteration, this is probably going to be used from the Solr admin screen.
 *
 *        NOTE: Specifying a directory or simply leaving the any "file=XXX" parameters will list the contents of a directory.
 *
 *        NOTE: <b>You must reload the core/collection for any changes made via this handler to take effect!</b>
 *
 *        NOTE: <b>If the core does not load (say schema.xml is not well formed for instance) you may be unable to replace
 *        the files with this interface.</b>
 *
 *        NOTE: <b>Leaving this handler enabled is a security risk! This handler should be disabled in all but trusted
 *        (probably development only) environments!</b>
 *
 *        Configuration files in ZooKeeper are supported.
 */
public class EditFileRequestHandler extends RequestHandlerBase {

  protected static final Logger log = LoggerFactory.getLogger(EditFileRequestHandler.class);

  private final static String OP_PARAM = "op";
  private final static String OP_WRITE = "write";
  private final static String OP_TEST = "test";

  ContentStream stream;
  private byte[] data = null;
  Set<String> hiddenFiles;

  public EditFileRequestHandler() {
    super();
  }

  @Override
  public void init(NamedList args) {
    super.init(args);
    hiddenFiles = ShowFileRequestHandler.initHidden(invariants);
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
      throws InterruptedException, KeeperException, IOException {

    CoreContainer coreContainer = req.getCore().getCoreDescriptor().getCoreContainer();
    String op = req.getParams().get(OP_PARAM);
    if (OP_WRITE.equalsIgnoreCase(op) || OP_TEST.equalsIgnoreCase(op)) {
      String fname = req.getParams().get("file", null);
      if (fname == null) {
        rsp.setException(new SolrException(ErrorCode.BAD_REQUEST, "No file name specified for write operation."));
      } else {
        fname = fname.replace('\\', '/');
        stream = getOneInputStream(req, rsp);
        if (stream == null) {
          return; // Error already in rsp.
        }

        data = IOUtils.toByteArray(new InputStreamReader(stream.getStream(), "UTF-8"), "UTF-8");

        // If it's "solrconfig.xml", try parsing it as that object. Otherwise, if it ends in '.xml',
        // see if it at least parses.
        if ("solrconfig.xml".equals(fname)) {
          try {
            new SolrConfig("unused", new InputSource(new ByteArrayInputStream(data)));
          } catch (Exception e) {
            rsp.setException(new SolrException(ErrorCode.BAD_REQUEST, "Invalid solr config file: " + e.getMessage()));
            return;
          }
        } else if (fname.endsWith(".xml")) { // At least do a rudimentary test, see if the thing parses.
          try {
            new Config(null, null, new InputSource(new ByteArrayInputStream(data)), null, false);
          } catch (Exception e) {
            rsp.setException(new SolrException(ErrorCode.BAD_REQUEST, "Invalid XML file: " + e.getMessage()));
            return;
          }
        }
        if (ShowFileRequestHandler.isHiddenFile(req, rsp, fname, true, hiddenFiles) == false) {
          if (coreContainer.isZooKeeperAware()) {
            writeToZooKeeper(req, rsp);
          } else {
            writeToFileSystem(req, rsp);
          }
        }
      }
    }
  }

  // write the file contained in the parameter "file=XXX" to ZooKeeper. The file may be a path, e.g.
  // file=velocity/error.vm or file=schema.xml
  //
  // Important: Assumes that the file already exists in ZK, so far we aren't creating files there.
  private void writeToZooKeeper(SolrQueryRequest req, SolrQueryResponse rsp)
      throws KeeperException, InterruptedException, IOException {

    CoreContainer coreContainer = req.getCore().getCoreDescriptor().getCoreContainer();
    SolrZkClient zkClient = coreContainer.getZkController().getZkClient();

    String adminFile = ShowFileRequestHandler.getAdminFileFromZooKeeper(req, rsp, zkClient, hiddenFiles);
    String fname = req.getParams().get("file", null);
    if (OP_TEST.equals(req.getParams().get(OP_PARAM))) {
      testReloadSuccess(req, rsp);
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

    File adminFile = ShowFileRequestHandler.getAdminFileFromFileSystem(req, rsp, hiddenFiles);
    if (adminFile == null || adminFile.isDirectory()) {
      String fname = req.getParams().get("file", null);

      if (adminFile == null) {
        log.error("File " + fname + " was not found.");
        rsp.setException(new SolrException(ErrorCode.BAD_REQUEST, "File " + fname + " was not found."));
        return;
      }
      log.error("File " + fname + " is a directory.");
      rsp.setException(new SolrException(ErrorCode.BAD_REQUEST, "File " + fname + " is a directory."));
      return;
    }
    if (OP_TEST.equals(req.getParams().get(OP_PARAM))) {
      testReloadSuccess(req, rsp);
      return;
    }

    FileUtils.copyInputStreamToFile(stream.getStream(), adminFile);
    log.info("Successfully saved file " + adminFile.getAbsolutePath() + " locally");
  }

  private boolean testReloadSuccess(SolrQueryRequest req, SolrQueryResponse rsp) {
    // Try writing the config to a temporary core and reloading to see that we don't allow people to shoot themselves
    // in the foot.
    File home = null;
    try {
      home = new File(FileUtils.getTempDirectory(), "SOLR_5459"); // Unlikely to name a core or collection this!
      FileUtils.writeStringToFile(new File(home, "solr.xml"), "<solr></solr>", "UTF-8"); // Use auto-discovery
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

      FileUtils.writeStringToFile(new File(coll, "core.properties"), "name=SOLR_5459", "UTF-8");

      FileUtils.writeByteArrayToFile(new File(new File(coll, "conf"), req.getParams().get("file", null)), data);

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
          rsp.setException(new SolrException(ErrorCode.BAD_REQUEST,
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

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Admin Config File -- update config files directly";
  }

  @Override
  public String getSource() {
    return "$URL: https://svn.apache.org/repos/asf/lucene/dev/trunk/solr/core/src/java/org/apache/solr/handler/admin/ShowFileRequestHandler.java $";
  }
}
