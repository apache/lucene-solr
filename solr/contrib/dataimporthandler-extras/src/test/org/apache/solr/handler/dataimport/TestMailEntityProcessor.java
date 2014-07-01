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
package org.apache.solr.handler.dataimport;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.handler.dataimport.config.Entity;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.icegreen.greenmail.imap.ImapHostManager;
import com.icegreen.greenmail.store.MailFolder;
import com.icegreen.greenmail.user.GreenMailUser;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.GreenMailUtil;
import com.icegreen.greenmail.util.ServerSetup;
import com.icegreen.greenmail.imap.ImapConstants;

import java.io.IOException;
import java.net.ServerSocket;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.mail.Flags;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;

/**
 * Test for MailEntityProcessor; uses GreenMail embedded Java mail server.
 *
 * @see org.apache.solr.handler.dataimport.MailEntityProcessor
 * @since solr 1.4
 */
public class TestMailEntityProcessor extends AbstractDataImportHandlerTestCase {
  
  // Credentials
  private static final String email = "test@localhost.com";
  private static final String user = "test";
  private static final String password = "secret";
  private static final String protocol = "imap";

  // embedded test mail server
  private ServerSetup serverSetup;
  private GreenMail greenMail;
  private GreenMailUser mailUser;
  private String hostAndPort;
  private String sep = ImapConstants.HIERARCHY_DELIMITER;
  
  private Calendar cal = Calendar.getInstance();
  
  /**
   * Setup an embedded GreenMail server for testing.
   */
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    
    int port = findAvailablePort(9103,9193);
    serverSetup = new ServerSetup(port, null, protocol);
    greenMail = new GreenMail(serverSetup);
    greenMail.start();
    mailUser = greenMail.setUser(email, user, password);    
    hostAndPort = "localhost:"+port;
    
    // Test mailbox is like this: foldername(mailcount)
    // top1(2) -> child11(6)
    //         -> child12(0)
    // top2(2) -> child21(1)
    //                  -> grandchild211(2)
    //                  -> grandchild212(1)
    //         -> child22(2)    
    ImapHostManager imapMgr = greenMail.getManagers().getImapHostManager();
    setupFolder(imapMgr, "top1", 2);    
    setupFolder(imapMgr, "top1"+sep+"child11", 6);    
    setupFolder(imapMgr, "top2", 2);    
    setupFolder(imapMgr, "top2"+sep+"child21", 1);    
    setupFolder(imapMgr, "top2"+sep+"child21"+sep+"grandchild211", 2);    
    setupFolder(imapMgr, "top2"+sep+"child21"+sep+"grandchild212", 1);    
    setupFolder(imapMgr, "top2"+sep+"child22", 2);
    setupFolder(imapMgr, "top3", 2);    
  }
    
  private int findAvailablePort(int min, int max) {
    for (int port = min; port < max; port++) {
      try {
        new ServerSocket(port).close();
        return port;
      } catch (IOException e) {
        // Port is in use
      }
    }
    throw new IllegalStateException("Could not find available port in range " + min + " to " + max);
  }  
  
  @Override
  @After
  public void tearDown() throws Exception {
    greenMail.stop();
    super.tearDown();
  }  
  
  /**
   * Creates 1 or more messages in the specified folder. 
   */
  protected void setupFolder(ImapHostManager imapMgr, String folderName, int numMessages) throws Exception {
    setupFolder(imapMgr, folderName, numMessages, 0);
  }
  
  protected void setupFolder(ImapHostManager imapMgr, String folderName, int numMessages, int startAt) throws Exception {
    cal.setTimeInMillis(System.currentTimeMillis());
    Date now = cal.getTime();
    MailFolder folder = imapMgr.getFolder(mailUser, folderName, false);
    if (folder == null)
      folder = imapMgr.createMailbox(mailUser, folderName);
    
    Session session = GreenMailUtil.getSession(serverSetup);
    for (int m=0; m < numMessages; m++) {
      int idx = m + startAt;
      MimeMessage msg = new MimeMessage(session);
      msg.setSubject("test"+idx);
      msg.setFrom("from@localhost.com");
      msg.setText("test"+idx);
      msg.setSentDate(now);
      folder.appendMessage(msg, new Flags(Flags.Flag.RECENT), now);
    }
    folder.getMessages();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testConnection() throws Exception {
    // also tests recurse = false and default settings
    Map<String, String> paramMap = new HashMap<>();
    paramMap.put("folders", "top1");
    paramMap.put("recurse", "false");
    paramMap.put("processAttachments", "false");
    
    DataImporter di = new DataImporter();
    di.loadAndInit(getConfigFromMap(paramMap));
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals("top1 did not return 2 messages", 2, swi.docs.size());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testRecursion() {
    Map<String, String> paramMap = new HashMap<>();
    paramMap.put("folders", "top2");
    paramMap.put("recurse", "true");
    paramMap.put("processAttachments", "false");
    DataImporter di = new DataImporter();
    di.loadAndInit(getConfigFromMap(paramMap));
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals("top2 and its children did not return 8 messages", 8, swi.docs.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testExclude() {
    Map<String, String> paramMap = new HashMap<>();
    paramMap.put("folders", "top2");
    paramMap.put("recurse", "true");
    paramMap.put("processAttachments", "false");
    paramMap.put("exclude", ".*grandchild.*");
    DataImporter di = new DataImporter();
    di.loadAndInit(getConfigFromMap(paramMap));
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals("top2 and its direct children did not return 5 messages", 5, swi.docs.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testInclude() {
    Map<String, String> paramMap = new HashMap<>();
    paramMap.put("folders", "top2");
    paramMap.put("recurse", "true");
    paramMap.put("processAttachments", "false");
    paramMap.put("include", ".*grandchild.*");
    DataImporter di = new DataImporter();
    di.loadAndInit(getConfigFromMap(paramMap));
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals("top2 and its direct children did not return 3 messages", 3, swi.docs.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testIncludeAndExclude() {
    Map<String, String> paramMap = new HashMap<>();
    paramMap.put("folders", "top1,top2");
    paramMap.put("recurse", "true");
    paramMap.put("processAttachments", "false");
    paramMap.put("exclude", ".*top1.*");
    paramMap.put("include", ".*grandchild.*");
    DataImporter di = new DataImporter();
    di.loadAndInit(getConfigFromMap(paramMap));
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals("top2 and its direct children did not return 3 messages", 3, swi.docs.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFetchTimeSince() throws ParseException {
    Map<String, String> paramMap = new HashMap<>();
    paramMap.put("folders", "top1"+sep+"child11");
    paramMap.put("recurse", "true");
    paramMap.put("processAttachments", "false");
    paramMap.put("fetchMailsSince", "2008-12-26 00:00:00");
    DataImporter di = new DataImporter();
    di.loadAndInit(getConfigFromMap(paramMap));
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    SolrWriterImpl swi = new SolrWriterImpl();
    di.runCmd(rp, swi);
    assertEquals("top1"+sep+"child11 and its direct children did not return 6 messages", 6, swi.docs.size());
  }

  // configures the data importer to use the MailEntityProcessor we're testing in this class
  private String getConfigFromMap(Map<String, String> params) {
    String conf =
      "<dataConfig>" +
      "<document>" +
      "<entity processor=\"org.apache.solr.handler.dataimport.MailEntityProcessor\" name=\"mail_entity\" " +
      "someconfig" +
      ">" +            
      "<field column=\"messageId\" name=\"id\" />"+
      "<field column=\"folder\" name=\"folder_s\" />"+
      "<field column=\"subject\" name=\"subject_s\" />"+
      "<field column=\"sentDate\" name=\"date_dt\" />"+
      "<field column=\"from\" name=\"from_s\" />"+
      "<field column=\"content\" name=\"content_s\" />"+
      "</entity>" +                    
      "</document>" +
      "</dataConfig>";
    params.put("user", user);
    params.put("password", password);
    params.put("host", hostAndPort);
    params.put("protocol", protocol);
    StringBuilder attribs = new StringBuilder("");
    for (String key : params.keySet())
      attribs.append(" ").append(key).append("=" + "\"").append(params.get(key)).append("\"");
    attribs.append(" ");
    return conf.replace("someconfig", attribs.toString());
  }

  // collects documents written by the DataImporter (from the MailEntityProcessor)
  static class SolrWriterImpl extends SolrWriter {
    List<SolrInputDocument> docs = new ArrayList<>();
    Boolean deleteAllCalled;
    Boolean commitCalled;

    public SolrWriterImpl() {
      super(null, null);
    }

    @Override
    public void close() {
      // no-op method to avoid NPE in super impl
    }

    @Override
    public boolean upload(SolrInputDocument doc) {      
      return docs.add(doc);
    }


    @Override
    public void doDeleteAll() {
      deleteAllCalled = Boolean.TRUE;
    }

    @Override
    public void commit(boolean b) {
      commitCalled = Boolean.TRUE;
    }
  }
}
