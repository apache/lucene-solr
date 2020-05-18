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

package org.apache.lucene.luke.models.util.twentynewsgroups;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.luke.util.LoggerFactory;

/** 20 Newsgroups (http://kdd.ics.uci.edu/databases/20newsgroups/20newsgroups.html) message files parser */
public class MessageFilesParser  extends SimpleFileVisitor<Path> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Path root;

  private final List<Message> messages = new ArrayList<>();

  public MessageFilesParser(Path root) {
    this.root = root;
  }

  public FileVisitResult visitFile(Path file, BasicFileAttributes attr) {
    try {
      if (attr.isRegularFile()) {
        Message message = parse(file);
        if (message != null) {
          messages.add(parse(file));
        }
      }
    } catch (IOException e) {
      log.warn("Invalid file? {}", file);
    }
    return FileVisitResult.CONTINUE;
  }

  Message parse(Path file) throws IOException {
    try (BufferedReader br = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      String line = br.readLine();

      Message message = new Message();
      while (!line.equals("")) {
        String[] ary = line.split(":", 2);
        if (ary.length < 2) {
          line = br.readLine();
          continue;
        }
        String att = ary[0].trim();
        String val = ary[1].trim();
        switch (att) {
          case "From":
            message.setFrom(val);
            break;
          case "Newsgroups":
            message.setNewsgroups(val.split(","));
            break;
          case "Subject":
            message.setSubject(val);
            break;
          case "Message-ID":
            message.setMessageId(val);
            break;
          case "Date":
            message.setDate(val);
            break;
          case "Organization":
            message.setOrganization(val);
            break;
          case "Lines":
            try {
              message.setLines(Integer.parseInt(ary[1].trim()));
            } catch (NumberFormatException e) {}
            break;
          default:
            break;
        }

        line = br.readLine();
      }

      StringBuilder sb = new StringBuilder();
      while (line != null) {
        sb.append(line);
        sb.append(" ");
        line = br.readLine();
      }
      message.setBody(sb.toString());

      return message;
    }
  }

  public List<Message> parseAll() throws IOException {
    Files.walkFileTree(root, this);
    return messages;
  }

}
