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

import com.sun.mail.imap.IMAPMessage;

import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.handler.dataimport.config.ConfigNameConstants;
import org.apache.solr.util.RTimer;
import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.AddressException;
import javax.mail.internet.ContentType;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.mail.search.*;

import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Supplier;

import com.sun.mail.gimap.GmailFolder;
import com.sun.mail.gimap.GmailRawSearchTerm;

/**
 * An EntityProcessor instance which can index emails along with their
 * attachments from POP3 or IMAP sources. Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler"
 * >http://wiki.apache.org/solr/DataImportHandler</a> for more details. <b>This
 * API is experimental and subject to change</b>
 * 
 * @since solr 1.4
 */
@SuppressWarnings("unchecked")
public class MailEntityProcessor extends EntityProcessorBase {
  
  private static final SimpleDateFormat sinceDateParser = 
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
  private static final SimpleDateFormat afterFmt = 
      new SimpleDateFormat("yyyy/MM/dd", Locale.ROOT);
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static interface CustomFilter {
    public SearchTerm getCustomSearch(Folder folder);
  }
  
  public void init(Context context) {
    super.init(context);
    // set attributes using XXX getXXXFromContext(attribute, defaultValue);
    // applies variable resolver and return default if value is not found or null
    // REQUIRED : connection and folder info
    user = getStringFromContext("user", null);
    password = getStringFromContext("password", null);
    host = getStringFromContext("host", null);
    protocol = getStringFromContext("protocol", null);
    folderNames = getStringFromContext("folders", null);
    // validate
    if (host == null || protocol == null || user == null || password == null
        || folderNames == null) throw new DataImportHandlerException(
        DataImportHandlerException.SEVERE,
        "'user|password|protocol|host|folders' are required attributes");
    
    // OPTIONAL : have defaults and are optional
    recurse = getBoolFromContext("recurse", true);
    
    exclude.clear();
    String excludes = getStringFromContext("exclude", "");
    if (excludes != null && !excludes.trim().equals("")) {
      exclude = Arrays.asList(excludes.split(","));
    }
    
    include.clear();
    String includes = getStringFromContext("include", "");
    if (includes != null && !includes.trim().equals("")) {
      include = Arrays.asList(includes.split(","));
    }
    batchSize = getIntFromContext("batchSize", 20);
    customFilter = getStringFromContext("customFilter", "");
    if (filters != null) filters.clear();
    folderIter = null;
    msgIter = null;
            
    String lastIndexTime = null;
    String command = 
        String.valueOf(context.getRequestParameters().get("command"));
    if (!DataImporter.FULL_IMPORT_CMD.equals(command))
      throw new IllegalArgumentException(this.getClass().getSimpleName()+
          " only supports "+DataImporter.FULL_IMPORT_CMD);
    
    // Read the last_index_time out of the dataimport.properties if available
    String cname = getStringFromContext("name", "mailimporter");
    String varName = ConfigNameConstants.IMPORTER_NS_SHORT + "." + cname + "."
        + DocBuilder.LAST_INDEX_TIME;
    Object varValue = context.getVariableResolver().resolve(varName);
    log.info(varName+"="+varValue);
    
    if (varValue != null && !"".equals(varValue) && 
        !"".equals(getStringFromContext("fetchMailsSince", ""))) {

      // need to check if varValue is the epoch, which we'll take to mean the
      // initial value, in which case means we should use fetchMailsSince instead
      Date tmp = null;
      try {
        tmp = sinceDateParser.parse((String)varValue);
        if (tmp.getTime() == 0) {
          log.info("Ignoring initial value "+varValue+" for "+varName+
              " in favor of fetchMailsSince config parameter");
          tmp = null; // don't use this value
        }
      } catch (ParseException e) {
        // probably ok to ignore this since we have other options below
        // as we're just trying to figure out if the date is 0
        log.warn("Failed to parse "+varValue+" from "+varName+" due to: "+e);
      }    
      
      if (tmp == null) {
        // favor fetchMailsSince in this case because the value from
        // dataimport.properties is the default/init value
        varValue = getStringFromContext("fetchMailsSince", "");
        log.info("fetchMailsSince="+varValue);
      }
    }
    
    if (varValue == null || "".equals(varValue)) {
      varName = ConfigNameConstants.IMPORTER_NS_SHORT + "."
          + DocBuilder.LAST_INDEX_TIME;
      varValue = context.getVariableResolver().resolve(varName);
      log.info(varName+"="+varValue);
    }
      
    if (varValue != null && varValue instanceof String) {
      lastIndexTime = (String)varValue;
      if (lastIndexTime != null && lastIndexTime.length() == 0)
        lastIndexTime = null;
    }
            
    if (lastIndexTime == null) 
      lastIndexTime = getStringFromContext("fetchMailsSince", "");

    log.info("Using lastIndexTime "+lastIndexTime+" for mail import");
    
    this.fetchMailsSince = null;
    if (lastIndexTime != null && lastIndexTime.length() > 0) {
      try {
        fetchMailsSince = sinceDateParser.parse(lastIndexTime);
        log.info("Parsed fetchMailsSince=" + lastIndexTime);
      } catch (ParseException e) {
        throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
            "Invalid value for fetchMailSince: " + lastIndexTime, e);
      }
    }
        
    fetchSize = getIntFromContext("fetchSize", 32 * 1024);
    cTimeout = getIntFromContext("connectTimeout", 30 * 1000);
    rTimeout = getIntFromContext("readTimeout", 60 * 1000);
    
    String tmp = context.getEntityAttribute("includeOtherUserFolders");
    includeOtherUserFolders = (tmp != null && Boolean.valueOf(tmp.trim()));
    tmp = context.getEntityAttribute("includeSharedFolders");
    includeSharedFolders = (tmp != null && Boolean.valueOf(tmp.trim()));
    
    setProcessAttachmentConfig();
    includeContent = getBoolFromContext("includeContent", true);
          
    logConfig();
  }
  
  private void setProcessAttachmentConfig() {
    processAttachment = true;
    String tbval = context.getEntityAttribute("processAttachments");
    if (tbval == null) {
      tbval = context.getEntityAttribute("processAttachement");
      if (tbval != null) processAttachment = Boolean.valueOf(tbval);
    } else processAttachment = Boolean.valueOf(tbval);
  }
    
  @Override
  public Map<String,Object> nextRow() {
    Message mail = null;
    Map<String,Object> row = null;
    do {
      // try till there is a valid document or folders get exhausted.
      // when mail == NULL, it means end of processing
      mail = getNextMail();   
      
      if (mail != null)
        row = getDocumentFromMail(mail);
      
      if (row != null && row.get("folder") == null) 
        row.put("folder", mail.getFolder().getFullName());
      
    } while (row == null && mail != null);
    return row;
  }
  
  private Message getNextMail() {
    if (!connected) {
      // this is needed to load the activation mail stuff correctly
      // otherwise, the JavaMail multipart support doesn't get configured
      // correctly, which leads to a class cast exception when processing
      // multipart messages: IMAPInputStream cannot be cast to
      // javax.mail.Multipart    
      if (false == withContextClassLoader(getClass().getClassLoader(), this::connectToMailBox)) {
        return null;
      }
      connected = true;
    }
    if (folderIter == null) {
      createFilters();
      folderIter = new FolderIterator(mailbox);
    }
    // get next message from the folder
    // if folder is exhausted get next folder
    // loop till a valid mail or all folders exhausted.
    while (msgIter == null || !msgIter.hasNext()) {
      Folder next = folderIter.hasNext() ? folderIter.next() : null;
      if (next == null) return null;
      
      msgIter = new MessageIterator(next, batchSize);
    }
    return msgIter.next();
  }
  
  private Map<String,Object> getDocumentFromMail(Message mail) {
    Map<String,Object> row = new HashMap<>();
    try {
      addPartToDocument(mail, row, true);
      return row;
    } catch (Exception e) {
      log.error("Failed to convert message [" + mail.toString()
          + "] to document due to: " + e, e);
      return null;
    }
  }
  
  public void addPartToDocument(Part part, Map<String,Object> row, boolean outerMost) throws Exception {
    if (part instanceof Message) {
      addEnvelopeToDocument(part, row);
    }
    
    String ct = part.getContentType().toLowerCase(Locale.ROOT);
    ContentType ctype = new ContentType(ct);
    if (part.isMimeType("multipart/*")) {
      Object content = part.getContent();
      if (content != null && content instanceof Multipart) {
        Multipart mp = (Multipart) part.getContent();
        int count = mp.getCount();
        if (part.isMimeType("multipart/alternative")) count = 1;
        for (int i = 0; i < count; i++)
          addPartToDocument(mp.getBodyPart(i), row, false);
      } else {
        log.warn("Multipart content is a not an instance of Multipart! Content is: "
            + (content != null ? content.getClass().getName() : "null")
            + ". Typically, this is due to the Java Activation JAR being loaded by the wrong classloader.");
      }
    } else if (part.isMimeType("message/rfc822")) {
      addPartToDocument((Part) part.getContent(), row, false);
    } else {
      String disp = part.getDisposition();
      if (includeContent
          && !(disp != null && disp.equalsIgnoreCase(Part.ATTACHMENT))) {
        InputStream is = part.getInputStream();
        Metadata contentTypeHint = new Metadata();
        contentTypeHint.set(Metadata.CONTENT_TYPE, ctype.getBaseType()
            .toLowerCase(Locale.ENGLISH));
        String content = (new Tika()).parseToString(is, contentTypeHint);
        if (row.get(CONTENT) == null) row.put(CONTENT, new ArrayList<String>());
        List<String> contents = (List<String>) row.get(CONTENT);
        contents.add(content.trim());
        row.put(CONTENT, contents);
      }
      if (!processAttachment || disp == null
          || !disp.equalsIgnoreCase(Part.ATTACHMENT)) return;
      InputStream is = part.getInputStream();
      String fileName = part.getFileName();
      Metadata contentTypeHint = new Metadata();
      contentTypeHint.set(Metadata.CONTENT_TYPE, ctype.getBaseType()
          .toLowerCase(Locale.ENGLISH));
      String content = (new Tika()).parseToString(is, contentTypeHint);
      if (content == null || content.trim().length() == 0) return;
      
      if (row.get(ATTACHMENT) == null) row.put(ATTACHMENT,
          new ArrayList<String>());
      List<String> contents = (List<String>) row.get(ATTACHMENT);
      contents.add(content.trim());
      row.put(ATTACHMENT, contents);
      if (row.get(ATTACHMENT_NAMES) == null) row.put(ATTACHMENT_NAMES,
          new ArrayList<String>());
      List<String> names = (List<String>) row.get(ATTACHMENT_NAMES);
      names.add(fileName);
      row.put(ATTACHMENT_NAMES, names);
    }
  }
  
  private void addEnvelopeToDocument(Part part, Map<String,Object> row)
      throws MessagingException {
    MimeMessage mail = (MimeMessage) part;
    Address[] adresses;
    if ((adresses = mail.getFrom()) != null && adresses.length > 0) row.put(
        FROM, adresses[0].toString());
    
    List<String> to = new ArrayList<>();
    if ((adresses = mail.getRecipients(Message.RecipientType.TO)) != null) addAddressToList(
        adresses, to);
    if ((adresses = mail.getRecipients(Message.RecipientType.CC)) != null) addAddressToList(
        adresses, to);
    if ((adresses = mail.getRecipients(Message.RecipientType.BCC)) != null) addAddressToList(
        adresses, to);
    if (to.size() > 0) row.put(TO_CC_BCC, to);
    
    row.put(MESSAGE_ID, mail.getMessageID());
    row.put(SUBJECT, mail.getSubject());
    
    Date d = mail.getSentDate();
    if (d != null) {
      row.put(SENT_DATE, d);
    }
    
    List<String> flags = new ArrayList<>();
    for (Flags.Flag flag : mail.getFlags().getSystemFlags()) {
      if (flag == Flags.Flag.ANSWERED) flags.add(FLAG_ANSWERED);
      else if (flag == Flags.Flag.DELETED) flags.add(FLAG_DELETED);
      else if (flag == Flags.Flag.DRAFT) flags.add(FLAG_DRAFT);
      else if (flag == Flags.Flag.FLAGGED) flags.add(FLAG_FLAGGED);
      else if (flag == Flags.Flag.RECENT) flags.add(FLAG_RECENT);
      else if (flag == Flags.Flag.SEEN) flags.add(FLAG_SEEN);
    }
    flags.addAll(Arrays.asList(mail.getFlags().getUserFlags()));
    if (flags.size() == 0) flags.add(FLAG_NONE);
    row.put(FLAGS, flags);
    
    String[] hdrs = mail.getHeader("X-Mailer");
    if (hdrs != null) row.put(XMAILER, hdrs[0]);
  }
  
  private void addAddressToList(Address[] adresses, List<String> to)
      throws AddressException {
    for (Address address : adresses) {
      to.add(address.toString());
      InternetAddress ia = (InternetAddress) address;
      if (ia.isGroup()) {
        InternetAddress[] group = ia.getGroup(false);
        for (InternetAddress member : group)
          to.add(member.toString());
      }
    }
  }
  
  private boolean connectToMailBox() {
    try {
      Properties props = new Properties();
      if (System.getProperty("mail.debug") != null) 
        props.setProperty("mail.debug", System.getProperty("mail.debug"));
      
      if (("imap".equals(protocol) || "imaps".equals(protocol))
          && "imap.gmail.com".equals(host)) {
        log.info("Consider using 'gimaps' protocol instead of '" + protocol
            + "' for enabling GMail specific extensions for " + host);
      }
      
      props.setProperty("mail.store.protocol", protocol);
      
      String imapPropPrefix = protocol.startsWith("gimap") ? "gimap" : "imap";
      props.setProperty("mail." + imapPropPrefix + ".fetchsize", "" + fetchSize);
      props.setProperty("mail." + imapPropPrefix + ".timeout", "" + rTimeout);
      props.setProperty("mail." + imapPropPrefix + ".connectiontimeout", "" + cTimeout);
      
      int port = -1;
      int colonAt = host.indexOf(":");
      if (colonAt != -1) {
        port = Integer.parseInt(host.substring(colonAt + 1));
        host = host.substring(0, colonAt);
      }
      
      Session session = Session.getDefaultInstance(props, null);
      mailbox = session.getStore(protocol);
      if (port != -1) {
        mailbox.connect(host, port, user, password);
      } else {
        mailbox.connect(host, user, password);
      }
      log.info("Connected to " + user + "'s mailbox on " + host);
      
      return true;
    } catch (MessagingException e) {      
      String errMsg = String.format(Locale.ENGLISH,
          "Failed to connect to %s server %s as user %s due to: %s", protocol,
          host, user, e.toString());
      log.error(errMsg, e);
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
          errMsg, e);
    }
  }
  
  private void createFilters() {
    if (fetchMailsSince != null) {
      filters.add(new MailsSinceLastCheckFilter(fetchMailsSince));
    }
    if (customFilter != null && !customFilter.equals("")) {
      try {
        Class<?> cf = Class.forName(customFilter);
        Object obj = cf.getConstructor().newInstance();
        if (obj instanceof CustomFilter) {
          filters.add((CustomFilter) obj);
        }
      } catch (Exception e) {
        throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
            "Custom filter could not be created", e);
      }
    }
  }
  
  private void logConfig() {
    if (!log.isInfoEnabled()) return;
    
    String lineSep = System.getProperty("line.separator"); 
    
    StringBuffer config = new StringBuffer();
    config.append("user : ").append(user).append(lineSep);
    config
        .append("pwd : ")
        .append(
            password != null && password.length() > 0 ? "<non-null>" : "<null>")
        .append(lineSep);
    config.append("protocol : ").append(protocol)
        .append(lineSep);
    config.append("host : ").append(host)
        .append(lineSep);
    config.append("folders : ").append(folderNames)
        .append(lineSep);
    config.append("recurse : ").append(recurse)
        .append(lineSep);
    config.append("exclude : ").append(exclude.toString())
        .append(lineSep);
    config.append("include : ").append(include.toString())
        .append(lineSep);
    config.append("batchSize : ").append(batchSize)
        .append(lineSep);
    config.append("fetchSize : ").append(fetchSize)
        .append(lineSep);
    config.append("read timeout : ").append(rTimeout)
        .append(lineSep);
    config.append("conection timeout : ").append(cTimeout)
        .append(lineSep);
    config.append("custom filter : ").append(customFilter)
        .append(lineSep);
    config.append("fetch mail since : ").append(fetchMailsSince)
        .append(lineSep);
    config.append("includeContent : ").append(includeContent)
        .append(lineSep);
    config.append("processAttachments : ").append(processAttachment)
        .append(lineSep);
    config.append("includeOtherUserFolders : ").append(includeOtherUserFolders)
        .append(lineSep);
    config.append("includeSharedFolders : ").append(includeSharedFolders)
        .append(lineSep);
    log.info(config.toString());
  }
  
  class FolderIterator implements Iterator<Folder> {
    private Store mailbox;
    private List<String> topLevelFolders;
    private List<Folder> folders = null;
    private Folder lastFolder = null;
    
    public FolderIterator(Store mailBox) {
      this.mailbox = mailBox;
      folders = new ArrayList<>();
      getTopLevelFolders(mailBox);
      if (includeOtherUserFolders) getOtherUserFolders();
      if (includeSharedFolders) getSharedFolders();
    }
    
    public boolean hasNext() {
      return !folders.isEmpty();
    }
    
    public Folder next() {
      try {
        boolean hasMessages = false;
        Folder next;
        do {
          if (lastFolder != null) {
            lastFolder.close(false);
            lastFolder = null;
          }
          if (folders.isEmpty()) {
            mailbox.close();
            return null;
          }
          next = folders.remove(0);
          if (next != null) {
            String fullName = next.getFullName();
            if (!excludeFolder(fullName)) {
              hasMessages = (next.getType() & Folder.HOLDS_MESSAGES) != 0;
              next.open(Folder.READ_ONLY);
              lastFolder = next;
              log.info("Opened folder : " + fullName);
            }
            if (recurse && ((next.getType() & Folder.HOLDS_FOLDERS) != 0)) {
              Folder[] children = next.list();
              log.info("Added its children to list  : ");
              for (int i = children.length - 1; i >= 0; i--) {
                folders.add(0, children[i]);
                log.info("child name : " + children[i].getFullName());
              }
              if (children.length == 0) log.info("NO children : ");
            }
          }
        } while (!hasMessages);
        return next;
      } catch (Exception e) {
        log.warn("Failed to read folders due to: "+e);
        // throw new
        // DataImportHandlerException(DataImportHandlerException.SEVERE,
        // "Folder open failed", e);
      }
      return null;
    }
    
    public void remove() {
      throw new UnsupportedOperationException("It's read only mode...");
    }
    
    private void getTopLevelFolders(Store mailBox) {
      if (folderNames != null) topLevelFolders = Arrays.asList(folderNames
          .split(","));
      for (int i = 0; topLevelFolders != null && i < topLevelFolders.size(); i++) {
        try {
          folders.add(mailbox.getFolder(topLevelFolders.get(i)));
        } catch (MessagingException e) {
          // skip bad ones unless it's the last one and still no good folder
          if (folders.size() == 0 && i == topLevelFolders.size() - 1) throw new DataImportHandlerException(
              DataImportHandlerException.SEVERE, "Folder retreival failed");
        }
      }
      if (topLevelFolders == null || topLevelFolders.size() == 0) {
        try {
          folders.add(mailBox.getDefaultFolder());
        } catch (MessagingException e) {
          throw new DataImportHandlerException(
              DataImportHandlerException.SEVERE, "Folder retreival failed");
        }
      }
    }
    
    private void getOtherUserFolders() {
      try {
        Folder[] ufldrs = mailbox.getUserNamespaces(null);
        if (ufldrs != null) {
          log.info("Found " + ufldrs.length + " user namespace folders");
          for (Folder ufldr : ufldrs)
            folders.add(ufldr);
        }
      } catch (MessagingException me) {
        log.warn("Messaging exception retrieving user namespaces: "
            + me.getMessage());
      }
    }
    
    private void getSharedFolders() {
      try {
        Folder[] sfldrs = mailbox.getSharedNamespaces();
        if (sfldrs != null) {
          log.info("Found " + sfldrs.length + " shared namespace folders");
          for (Folder sfldr : sfldrs)
            folders.add(sfldr);
        }
      } catch (MessagingException me) {
        log.warn("Messaging exception retrieving shared namespaces: "
            + me.getMessage());
      }
    }
    
    private boolean excludeFolder(String name) {
      for (String s : exclude) {
        if (name.matches(s)) return true;
      }
      for (String s : include) {
        if (name.matches(s)) return false;
      }
      return include.size() > 0;
    }
  }
  
  class MessageIterator extends SearchTerm implements Iterator<Message> {
    private Folder folder;
    private Message[] messagesInCurBatch = null;
    private int current = 0;
    private int currentBatch = 0;
    private int batchSize = 0;
    private int totalInFolder = 0;
    private boolean doBatching = true;
    
    public MessageIterator(Folder folder, int batchSize) {
      super();
      
      try {
        this.folder = folder;
        this.batchSize = batchSize;
        SearchTerm st = getSearchTerm();
        
        log.info("SearchTerm=" + st);
        
        if (st != null || folder instanceof GmailFolder) {
          doBatching = false;
          // Searching can still take a while even though we're only pulling
          // envelopes; unless you're using gmail server-side filter, which is
          // fast
          log.info("Searching folder " + folder.getName() + " for messages");
          final RTimer searchTimer = new RTimer();

          // If using GMail, speed up the envelope processing by doing a
          // server-side
          // search for messages occurring on or after the fetch date (at
          // midnight),
          // which reduces the number of envelopes we need to pull from the
          // server
          // to apply the precise DateTerm filter; GMail server-side search has
          // date
          // granularity only but the local filters are also applied
                    
          if (folder instanceof GmailFolder && fetchMailsSince != null) {
            String afterCrit = "after:" + afterFmt.format(fetchMailsSince);
            log.info("Added server-side gmail filter: " + afterCrit);
            Message[] afterMessages = folder.search(new GmailRawSearchTerm(
                afterCrit));
            
            log.info("GMail server-side filter found " + afterMessages.length
                + " messages received " + afterCrit + " in folder " + folder.getName());
            
            // now pass in the server-side filtered messages to the local filter
            messagesInCurBatch = folder.search((st != null ? st : this), afterMessages);
          } else {
            messagesInCurBatch = folder.search(st);
          }          
          totalInFolder = messagesInCurBatch.length;
          folder.fetch(messagesInCurBatch, fp);
          current = 0;
          log.info("Total messages : " + totalInFolder);
          log.info("Search criteria applied. Batching disabled. Took {} (ms)", searchTimer.getTime());
        } else {
          totalInFolder = folder.getMessageCount();
          log.info("Total messages : " + totalInFolder);
          getNextBatch(batchSize, folder);
        }
      } catch (MessagingException e) {
        throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
            "Message retreival failed", e);
      }
    }
    
    private void getNextBatch(int batchSize, Folder folder)
        throws MessagingException {
      // after each batch invalidate cache
      if (messagesInCurBatch != null) {
        for (Message m : messagesInCurBatch) {
          if (m instanceof IMAPMessage) ((IMAPMessage) m).invalidateHeaders();
        }
      }
      int lastMsg = (currentBatch + 1) * batchSize;
      lastMsg = lastMsg > totalInFolder ? totalInFolder : lastMsg;
      messagesInCurBatch = folder.getMessages(currentBatch * batchSize + 1,
          lastMsg);
      folder.fetch(messagesInCurBatch, fp);
      current = 0;
      currentBatch++;
      log.info("Current Batch  : " + currentBatch);
      log.info("Messages in this batch  : " + messagesInCurBatch.length);
    }
    
    public boolean hasNext() {
      boolean hasMore = current < messagesInCurBatch.length;
      if (!hasMore && doBatching && currentBatch * batchSize < totalInFolder) {
        // try next batch
        try {
          getNextBatch(batchSize, folder);
          hasMore = current < messagesInCurBatch.length;
        } catch (MessagingException e) {
          throw new DataImportHandlerException(
              DataImportHandlerException.SEVERE, "Message retreival failed", e);
        }
      }
      return hasMore;
    }
    
    public Message next() {
      return hasNext() ? messagesInCurBatch[current++] : null;
    }
    
    public void remove() {
      throw new UnsupportedOperationException("It's read only mode...");
    }
    
    private SearchTerm getSearchTerm() {
      if (filters.size() == 0) return null;
      if (filters.size() == 1) return filters.get(0).getCustomSearch(folder);
      SearchTerm last = filters.get(0).getCustomSearch(folder);
      for (int i = 1; i < filters.size(); i++) {
        CustomFilter filter = filters.get(i);
        SearchTerm st = filter.getCustomSearch(folder);
        if (st != null) {
          last = new AndTerm(last, st);
        }
      }
      return last;
    }
    
    public boolean match(Message message) {
      return true;
    }
  }

  static class MailsSinceLastCheckFilter implements CustomFilter {
    
    private Date since;
    
    public MailsSinceLastCheckFilter(Date date) {
      since = date;
    }
    
    @SuppressWarnings("serial")
    public SearchTerm getCustomSearch(final Folder folder) {
      log.info("Building mail filter for messages in " + folder.getName()
          + " that occur after " + sinceDateParser.format(since));
      return new DateTerm(ComparisonTerm.GE, since) {
        private int matched = 0;
        private int seen = 0;
        
        @Override
        public boolean match(Message msg) {
          boolean isMatch = false;
          ++seen;
          try {
            Date msgDate = msg.getReceivedDate();
            if (msgDate == null) msgDate = msg.getSentDate();
            
            if (msgDate != null && msgDate.getTime() >= since.getTime()) {
              ++matched;
              isMatch = true;
            } else {
              String msgDateStr = (msgDate != null) ? sinceDateParser.format(msgDate) : "null";
              String sinceDateStr = (since != null) ? sinceDateParser.format(since) : "null";
              log.debug("Message " + msg.getSubject() + " was received at [" + msgDateStr
                  + "], since filter is [" + sinceDateStr + "]");
            }
          } catch (MessagingException e) {
            log.warn("Failed to process message due to: "+e, e);
          }
          
          if (seen % 100 == 0) {
            log.info("Matched " + matched + " of " + seen + " messages since: "
                + sinceDateParser.format(since));
          }
          
          return isMatch;
        }
      };
    }
  }
  
  // user settings stored in member variables
  private String user;
  private String password;
  private String host;
  private String protocol;
  
  private String folderNames;
  private List<String> exclude = new ArrayList<>();
  private List<String> include = new ArrayList<>();
  private boolean recurse;
  
  private int batchSize;
  private int fetchSize;
  private int cTimeout;
  private int rTimeout;
  
  private Date fetchMailsSince;
  private String customFilter;
  
  private boolean processAttachment = true;
  private boolean includeContent = true;
  private boolean includeOtherUserFolders = false;
  private boolean includeSharedFolders = false;
  
  // holds the current state
  private Store mailbox;
  private boolean connected = false;
  private FolderIterator folderIter;
  private MessageIterator msgIter;
  private List<CustomFilter> filters = new ArrayList<>();
  private static FetchProfile fp = new FetchProfile();
  
  static {
    fp.add(FetchProfile.Item.ENVELOPE);
    fp.add(FetchProfile.Item.FLAGS);
    fp.add("X-Mailer");
  }
  
  // Fields To Index
  // single valued
  private static final String MESSAGE_ID = "messageId";
  private static final String SUBJECT = "subject";
  private static final String FROM = "from";
  private static final String SENT_DATE = "sentDate";
  private static final String XMAILER = "xMailer";
  // multi valued
  private static final String TO_CC_BCC = "allTo";
  private static final String FLAGS = "flags";
  private static final String CONTENT = "content";
  private static final String ATTACHMENT = "attachment";
  private static final String ATTACHMENT_NAMES = "attachmentNames";
  // flag values
  private static final String FLAG_NONE = "none";
  private static final String FLAG_ANSWERED = "answered";
  private static final String FLAG_DELETED = "deleted";
  private static final String FLAG_DRAFT = "draft";
  private static final String FLAG_FLAGGED = "flagged";
  private static final String FLAG_RECENT = "recent";
  private static final String FLAG_SEEN = "seen";
  
  private int getIntFromContext(String prop, int ifNull) {
    int v = ifNull;
    try {
      String val = context.getEntityAttribute(prop);
      if (val != null) {
        val = context.replaceTokens(val);
        v = Integer.parseInt(val);
      }
    } catch (NumberFormatException e) {
      // do nothing
    }
    return v;
  }
  
  private boolean getBoolFromContext(String prop, boolean ifNull) {
    boolean v = ifNull;
    String val = context.getEntityAttribute(prop);
    if (val != null) {
      val = context.replaceTokens(val);
      v = Boolean.valueOf(val);
    }
    return v;
  }
  
  private String getStringFromContext(String prop, String ifNull) {
    String v = ifNull;
    String val = context.getEntityAttribute(prop);
    if (val != null) {
      val = context.replaceTokens(val);
      v = val;
    }
    return v;
  }

  @SuppressForbidden(reason = "Uses context class loader as a workaround to inject correct classloader to 3rd party libs")
  private static <T> T withContextClassLoader(ClassLoader loader, Supplier<T> action) {
    Thread ct = Thread.currentThread();
    ClassLoader prev = ct.getContextClassLoader();
    try {
      ct.setContextClassLoader(loader);
      return action.get();
    } finally {
      ct.setContextClassLoader(prev);
    }
  }
  
}
