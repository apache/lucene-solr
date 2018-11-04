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

package org.apache.lucene.luke.app.desktop.components;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.nio.file.FileSystems;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;

public final class LogsPanelProvider implements Provider<JPanel> {

  private static final String LOG_FILE = System.getProperty("user.home") +
      FileSystems.getDefault().getSeparator() + ".luke.d" +
      FileSystems.getDefault().getSeparator() + "luke.log";

  private final JTextArea logTextArea;

  @Inject
  public LogsPanelProvider(@Named("log_area") JTextArea logTextArea) {
    this.logTextArea = logTextArea;
  }

  @Override
  public JPanel get() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

    JPanel header = new JPanel(new FlowLayout(FlowLayout.LEADING));
    header.setOpaque(false);
    header.add(new JLabel(MessageUtils.getLocalizedMessage("logs.label.see_also")));

    JLabel logPathLabel = new JLabel(LOG_FILE);
    header.add(logPathLabel);

    panel.add(header, BorderLayout.PAGE_START);

    panel.add(new JScrollPane(logTextArea), BorderLayout.CENTER);
    return panel;
  }

}
