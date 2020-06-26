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

package org.apache.lucene.luke.app.desktop.components.dialog.menubar;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTextArea;
import javax.swing.SwingWorker;
import java.awt.BorderLayout;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.luke.app.DirectoryHandler;
import org.apache.lucene.luke.app.DirectoryObserver;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.IndexObserver;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.ImageUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.StyleConstants;
import org.apache.lucene.luke.app.desktop.util.TextAreaPrintStream;
import org.apache.lucene.luke.models.tools.IndexTools;
import org.apache.lucene.luke.models.tools.IndexToolsFactory;
import org.apache.lucene.luke.util.LoggerFactory;
import org.apache.lucene.util.NamedThreadFactory;

/** Factory of check index dialog */
public final class CheckIndexDialogFactory implements DialogOpener.DialogFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static CheckIndexDialogFactory instance;

  private final Preferences prefs;

  private final IndexToolsFactory indexToolsFactory;

  private final DirectoryHandler directoryHandler;

  private final IndexHandler indexHandler;

  private final JLabel resultLbl = new JLabel();

  private final JLabel statusLbl = new JLabel();

  private final JLabel indicatorLbl = new JLabel();

  private final JButton repairBtn = new JButton();

  private final JTextArea logArea = new JTextArea();

  private JDialog dialog;

  private LukeState lukeState;

  private CheckIndex.Status status;

  private IndexTools toolsModel;

  private final ListenerFunctions listeners = new ListenerFunctions();

  public synchronized static CheckIndexDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new CheckIndexDialogFactory();
    }
    return instance;
  }

  private CheckIndexDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
    this.indexToolsFactory = new IndexToolsFactory();
    this.indexHandler = IndexHandler.getInstance();
    this.directoryHandler = DirectoryHandler.getInstance();

    indexHandler.addObserver(new Observer());
    directoryHandler.addObserver(new Observer());

    initialize();
  }

  private void initialize() {
    repairBtn.setText(FontUtils.elegantIconHtml("&#xe036;", MessageUtils.getLocalizedMessage("checkidx.button.fix")));
    repairBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    repairBtn.setMargin(new Insets(3, 3, 3, 3));
    repairBtn.setEnabled(false);
    repairBtn.addActionListener(listeners::repairIndex);

    indicatorLbl.setIcon(ImageUtils.createImageIcon("indicator.gif", 20, 20));

    logArea.setEditable(false);
  }


  @Override
  public JDialog create(Window owner, String title, int width, int height) {
    dialog = new JDialog(owner, title, Dialog.ModalityType.APPLICATION_MODAL);
    dialog.add(content());
    dialog.setSize(new Dimension(width, height));
    dialog.setLocationRelativeTo(owner);
    dialog.getContentPane().setBackground(prefs.getColorTheme().getBackgroundColor());
    return dialog;
  }

  private JPanel content() {
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
    panel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));

    panel.add(controller());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(logs());

    return panel;
  }

  private JPanel controller() {
    JPanel panel = new JPanel(new GridLayout(3, 1));
    panel.setOpaque(false);

    JPanel idxPath = new JPanel(new FlowLayout(FlowLayout.LEADING));
    idxPath.setOpaque(false);
    idxPath.add(new JLabel(MessageUtils.getLocalizedMessage("checkidx.label.index_path")));
    JLabel idxPathLbl = new JLabel(lukeState.getIndexPath());
    idxPathLbl.setToolTipText(lukeState.getIndexPath());
    idxPath.add(idxPathLbl);
    panel.add(idxPath);

    JPanel results = new JPanel(new GridLayout(2, 1));
    results.setOpaque(false);
    results.setBorder(BorderFactory.createEmptyBorder(0, 5, 0, 0));
    results.add(new JLabel(MessageUtils.getLocalizedMessage("checkidx.label.results")));
    results.add(resultLbl);
    panel.add(results);

    JPanel execButtons = new JPanel(new FlowLayout(FlowLayout.TRAILING));
    execButtons.setOpaque(false);
    JButton checkBtn = new JButton(FontUtils.elegantIconHtml("&#xe0f7;", MessageUtils.getLocalizedMessage("checkidx.button.check")));
    checkBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    checkBtn.setMargin(new Insets(3, 0, 3, 0));
    checkBtn.addActionListener(listeners::checkIndex);
    execButtons.add(checkBtn);

    JButton closeBtn = new JButton(MessageUtils.getLocalizedMessage("button.close"));
    closeBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    closeBtn.setMargin(new Insets(3, 0, 3, 0));
    closeBtn.addActionListener(e -> dialog.dispose());
    execButtons.add(closeBtn);
    panel.add(execButtons);

    return panel;
  }

  private JPanel logs() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);

    JPanel header = new JPanel();
    header.setOpaque(false);
    header.setLayout(new BoxLayout(header, BoxLayout.PAGE_AXIS));

    JPanel repair = new JPanel(new FlowLayout(FlowLayout.LEADING));
    repair.setOpaque(false);
    repair.add(repairBtn);

    JTextArea warnArea = new JTextArea(MessageUtils.getLocalizedMessage("checkidx.label.warn"), 3, 30);
    warnArea.setLineWrap(true);
    warnArea.setEditable(false);
    warnArea.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));

    repair.add(warnArea);
    header.add(repair);

    JPanel note = new JPanel(new FlowLayout(FlowLayout.LEADING));
    note.setOpaque(false);
    note.add(new JLabel(MessageUtils.getLocalizedMessage("checkidx.label.note")));
    header.add(note);

    JPanel status = new JPanel(new FlowLayout(FlowLayout.LEADING));
    status.setOpaque(false);
    status.add(new JLabel(MessageUtils.getLocalizedMessage("label.status")));
    statusLbl.setText("Idle");
    status.add(statusLbl);
    indicatorLbl.setVisible(false);
    status.add(indicatorLbl);
    header.add(status);

    panel.add(header, BorderLayout.PAGE_START);

    logArea.setText("");
    panel.add(new JScrollPane(logArea), BorderLayout.CENTER);

    return panel;
  }

  private class Observer implements IndexObserver, DirectoryObserver {

    @Override
    public void openIndex(LukeState state) {
      lukeState = state;
      toolsModel = indexToolsFactory.newInstance(state.getIndexReader(), state.useCompound(), state.keepAllCommits());
    }

    @Override
    public void closeIndex() {
      close();
    }

    @Override
    public void openDirectory(LukeState state) {
      lukeState = state;
      toolsModel = indexToolsFactory.newInstance(state.getDirectory());
    }

    @Override
    public void closeDirectory() {
      close();
    }

    private void close() {
      toolsModel = null;
    }
  }

  private class ListenerFunctions {

    void checkIndex(ActionEvent e) {
      ExecutorService executor = Executors.newFixedThreadPool(1, new NamedThreadFactory("check-index-dialog-check"));

      SwingWorker<CheckIndex.Status, Void> task = new SwingWorker<CheckIndex.Status, Void>() {

        @Override
        protected CheckIndex.Status doInBackground() {
          setProgress(0);
          statusLbl.setText("Running...");
          indicatorLbl.setVisible(true);
          TextAreaPrintStream ps;
          try {
            ps = new TextAreaPrintStream(logArea);
            CheckIndex.Status status = toolsModel.checkIndex(ps);
            ps.flush();
            return status;
          } catch (UnsupportedEncodingException e) {
            // will not reach
          } catch (Exception e) {
            statusLbl.setText(MessageUtils.getLocalizedMessage("message.error.unknown"));
            throw e;
          } finally {
            setProgress(100);
          }
          return null;
        }

        @Override
        protected void done() {
          try {
            CheckIndex.Status st = get();
            resultLbl.setText(createResultsMessage(st));
            indicatorLbl.setVisible(false);
            statusLbl.setText("Done");
            if (!st.clean) {
              repairBtn.setEnabled(true);
            }
            status = st;
          } catch (Exception e) {
            log.error("Error checking index", e);
            statusLbl.setText(MessageUtils.getLocalizedMessage("message.error.unknown"));
          }
        }
      };

      executor.submit(task);
      executor.shutdown();
    }

    private String createResultsMessage(CheckIndex.Status status) {
      String msg;
      if (status == null) {
        msg = "?";
      } else if (status.clean) {
        msg = "OK";
      } else if (status.toolOutOfDate) {
        msg = "ERROR: Can't check - tool out-of-date";
      } else {
        StringBuilder sb = new StringBuilder("BAD:");
        if (status.missingSegments) {
          sb.append(" Missing segments.");
        }
        if (status.numBadSegments > 0) {
          sb.append(" numBadSegments=");
          sb.append(status.numBadSegments);
        }
        if (status.totLoseDocCount > 0) {
          sb.append(" totLoseDocCount=");
          sb.append(status.totLoseDocCount);
        }
        msg = sb.toString();
      }
      return msg;
    }

    void repairIndex(ActionEvent e) {
      if (status == null) {
        return;
      }

      ExecutorService executor = Executors.newFixedThreadPool(1, new NamedThreadFactory("check-index-dialog-repair"));

      SwingWorker<CheckIndex.Status, Void> task = new SwingWorker<CheckIndex.Status, Void>() {

        @Override
        protected CheckIndex.Status doInBackground() {
          setProgress(0);
          statusLbl.setText("Running...");
          indicatorLbl.setVisible(true);
          logArea.setText("");
          TextAreaPrintStream ps;
          try {
            ps = new TextAreaPrintStream(logArea);
            toolsModel.repairIndex(status, ps);
            statusLbl.setText("Done");
            ps.flush();
            return status;
          } catch (UnsupportedEncodingException e) {
            // will not occur
          } catch (Exception e) {
            statusLbl.setText(MessageUtils.getLocalizedMessage("message.error.unknown"));
            throw e;
          } finally {
            setProgress(100);
          }
          return null;
        }

        @Override
        protected void done() {
          indexHandler.open(lukeState.getIndexPath(), lukeState.getDirImpl());
          logArea.append("Repairing index done.");
          resultLbl.setText("");
          indicatorLbl.setVisible(false);
          repairBtn.setEnabled(false);
        }
      };

      executor.submit(task);
      executor.shutdown();
    }
  }

}
