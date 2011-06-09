/**
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.SolrQueryResponse;

/**
 * This handler uses the RawResponseWriter to give client access to
 * files inside ${solr.home}/conf
 * 
 * If you want to selectively restrict access some configuration files, you can list
 * these files in the {@link #HIDDEN} invariants.  For example to hide 
 * synonyms.txt and anotherfile.txt, you would register:
 * 
 * <pre>
 * &lt;requestHandler name="/admin/file" class="org.apache.solr.handler.admin.ShowFileRequestHandler" &gt;
 *   &lt;lst name="defaults"&gt;
 *    &lt;str name="echoParams"&gt;explicit&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="invariants"&gt;
 *    &lt;str name="hidden"&gt;synonyms.txt&lt;/str&gt; 
 *    &lt;str name="hidden"&gt;anotherfile.txt&lt;/str&gt; 
 *   &lt;/lst&gt;
 * &lt;/requestHandler&gt;
 * </pre>
 * 
 * The ShowFileRequestHandler uses the {@link RawResponseWriter} (wt=raw) to return
 * file contents.  If you need to use a different writer, you will need to change 
 * the registered invarient param for wt.
 * 
 * If you want to override the contentType header returned for a given file, you can
 * set it directly using: {@link #USE_CONTENT_TYPE}.  For example, to get a plain text 
 * version of schema.xml, try:
 * <pre>
 *   http://localhost:8983/solr/admin/file?file=schema.xml&contentType=text/plain
 * </pre>
 * 
 *
 * @since solr 1.3
 */
public class ShowFileRequestHandler extends RequestHandlerBase
{
  public static final String HIDDEN = "hidden";
  public static final String USE_CONTENT_TYPE = "contentType";
  
  protected Set<String> hiddenFiles;
  
  private static ShowFileRequestHandler instance;
  public ShowFileRequestHandler()
  {
    super();
    instance = this; // used so that getFileContents can access hiddenFiles
  }

  @Override
  public void init(NamedList args) {
    super.init( args );
    
    // by default, use wt=raw
    ModifiableSolrParams params = new ModifiableSolrParams( invariants );
    if( params.get( CommonParams.WT ) == null ) {
      params.set( CommonParams.WT, "raw" );
    }
    this.invariants = params;
    
    // Build a list of hidden files
    hiddenFiles = new HashSet<String>();
    if( invariants != null ) {
      String[] hidden = invariants.getParams( HIDDEN );
      if( hidden != null ) {
        for( String s : hidden ) {
          hiddenFiles.add( s.toUpperCase(Locale.ENGLISH) );
        }
      }
    }
  }
  
  public Set<String> getHiddenFiles()
  {
    return hiddenFiles;
  }
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException 
  {
    File adminFile = null;
    
    final SolrResourceLoader loader = req.getCore().getResourceLoader();
    File configdir = new File( loader.getConfigDir() );
    if (!configdir.exists()) {
      // TODO: maybe we should just open it this way to start with?
      try {
        configdir = new File( loader.getClassLoader().getResource(loader.getConfigDir()).toURI() );
      } catch (URISyntaxException e) {
        throw new SolrException( ErrorCode.FORBIDDEN, "Can not access configuration directory!");
      }
    }
    String fname = req.getParams().get("file", null);
    if( fname == null ) {
      adminFile = configdir;
    }
    else {
      fname = fname.replace( '\\', '/' ); // normalize slashes
      if( hiddenFiles.contains( fname.toUpperCase(Locale.ENGLISH) ) ) {
        throw new SolrException( ErrorCode.FORBIDDEN, "Can not access: "+fname );
      }
      if( fname.indexOf( ".." ) >= 0 ) {
        throw new SolrException( ErrorCode.FORBIDDEN, "Invalid path: "+fname );  
      }
      adminFile = new File( configdir, fname );
    }
    
    // Make sure the file exists, is readable and is not a hidden file
    if( !adminFile.exists() ) {
      throw new SolrException( ErrorCode.BAD_REQUEST, "Can not find: "+adminFile.getName() 
          + " ["+adminFile.getAbsolutePath()+"]" );
    }
    if( !adminFile.canRead() || adminFile.isHidden() ) {
      throw new SolrException( ErrorCode.BAD_REQUEST, "Can not show: "+adminFile.getName() 
          + " ["+adminFile.getAbsolutePath()+"]" );
    }
    
    // Show a directory listing
    if( adminFile.isDirectory() ) {
      
      int basePath = configdir.getAbsolutePath().length() + 1;
      NamedList<SimpleOrderedMap<Object>> files = new SimpleOrderedMap<SimpleOrderedMap<Object>>();
      for( File f : adminFile.listFiles() ) {
        String path = f.getAbsolutePath().substring( basePath );
        path = path.replace( '\\', '/' ); // normalize slashes
        if( hiddenFiles.contains( path.toUpperCase(Locale.ENGLISH) ) ) {
          continue; // don't show 'hidden' files
        }
        if( f.isHidden() || f.getName().startsWith( "." ) ) {
          continue; // skip hidden system files...
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
      rsp.add( "files", files );
    }
    else {
      // Include the file contents
      ContentStreamBase content = new ContentStreamBase.FileStream( adminFile );
      content.setContentType( req.getParams().get( USE_CONTENT_TYPE ) );
  
      rsp.add( RawResponseWriter.CONTENT, content );
    }
    rsp.setHttpCaching(false);
  }
  
  /**
   * This is a utility function that lets you get the contents of an admin file
   * 
   * It is only used so that we can get rid of "/admin/get-file.jsp" and include
   * "admin-extra.html" in "/admin/index.html" using jsp scriptlets
   */
  public static String getFileContents(SolrCore core, String path )
  {
    if( instance != null && instance.hiddenFiles != null ) {
      if( instance.hiddenFiles.contains( path ) ) {
        return ""; // ignore it...
      }
    }
    try {
      InputStream input = core.getResourceLoader().openResource(path);
      return IOUtils.toString( input, "UTF-8" );
    }
    catch( Exception ex ) {} // ignore it
    return "";
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Admin Get File -- view config files directly";
  }

  @Override
  public String getVersion() {
      return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}
