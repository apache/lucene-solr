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
import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JSpinner;
import javax.swing.JTextArea;
import javax.swing.SpinnerNumberModel;
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

/** Factory of optimize index dialog */
public final class OptimizeIndexDialogFactory implements DialogOpener.DialogFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static OptimizeIndexDialogFactory instance;

  private final Preferences prefs;

  private final IndexToolsFactory indexToolsFactory = new IndexToolsFactory();

  private final IndexHandler indexHandler;

  private final JCheckBox expungeCB = new JCheckBox();

  private final JSpinner maxSegSpnr = new JSpinner();

  private final JLabel statusLbl = new JLabel();

  private final JLabel indicatorLbl = new JLabel();

  private final JTextArea logArea = new JTextArea();

  private final ListenerFunctions listeners = new ListenerFunctions();

  private JDialog dialog;

  private IndexTools toolsModel;

  public synchronized static OptimizeIndexDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new OptimizeIndexDialogFactory();
    }
    return instance;
  }

  private OptimizeIndexDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
    this.indexHandler = IndexHandler.getInstance();
    indexHandler.addObserver(new Observer());

    initialize();
  }

  private void initialize() {
    expungeCB.setText(MessageUtils.getLocalizedMessage("optimize.checkbox.expunge"));
    expungeCB.setOpaque(false);

    maxSegSpnr.setModel(new SpinnerNumberModel(1, 1, 100, 1));
    maxSegSpnr.setPreferredSize(new Dimension(100, 30));

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
    JPanel panel = new JPanel(new GridLayout(4, 1));
    panel.setOpaque(false);

    JPanel idxPath = new JPanel(new FlowLayout(FlowLayout.LEADING));
    idxPath.setOpaque(false);
    idxPath.add(new JLabel(MessageUtils.getLocalizedMessage("optimize.label.index_path")));
    JLabel idxPathLbl = new JLabel(indexHandler.getState().getIndexPath());
    idxPathLbl.setToolTipText(indexHandler.getState().getIndexPath());
    idxPath.add(idxPathLbl);
    panel.add(idxPath);

    JPanel expunge = new JPanel(new FlowLayout(FlowLayout.LEADING));
    expunge.setOpaque(false);

    expunge.add(expungeCB);
    panel.add(expunge);

    JPanel maxSegs = new JPanel(new FlowLayout(FlowLayout.LEADING));
    maxSegs.setOpaque(false);
    maxSegs.add(new JLabel(MessageUtils.getLocalizedMessage("optimize.label.max_segments")));
    maxSegs.add(maxSegSpnr);
    panel.add(maxSegs);

    JPanel execButtons = new JPanel(new FlowLayout(FlowLayout.TRAILING));
    execButtons.setOpaque(false);
    JButton optimizeBtn = new JButton(FontUtils.elegantIconHtml("&#xe0ff;", MessageUtils.getLocalizedMessage("optimize.button.optimize")));
    optimizeBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    optimizeBtn.setMargin(new Insets(3, 0, 3, 0));
    optimizeBtn.addActionListener(listeners::optimize);
    execButtons.add(optimizeBtn);
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

    JPanel header = new JPanel(new GridLayout(2, 1));
    header.setOpaque(false);
    header.add(new JLabel(MessageUtils.getLocalizedMessage("optimize.label.note")));
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

  private class ListenerFunctions {

    void optimize(ActionEvent e) {
      ExecutorService executor = Executors.newFixedThreadPool(1, new NamedThreadFactory("optimize-index-dialog"));

      SwingWorker<Void, Void> task = new SwingWorker<Void, Void>() {

        @Override
        protected Void doInBackground() {
          setProgress(0);
          statusLbl.setText("Running...");
          indicatorLbl.setVisible(true);
          TextAreaPrintStream ps;
          try {
            ps = new TextAreaPrintStream(logArea);
            toolsModel.optimize(expungeCB.isSelected(), (int) maxSegSpnr.getValue(), ps);
            ps.flush();
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
          indicatorLbl.setVisible(false);
          statusLbl.setText("Done");
          indexHandler.reOpen();
        }
      };

      executor.submit(task);
      executor.shutdown();
    }

  }

  private class Observer implements IndexObserver {

    @Override
    public void openIndex(LukeState state) {
      toolsModel = indexToolsFactory.newInstance(state.getIndexReader(), state.useCompound(), state.keepAllCommits());
    }

    @Override
    public void closeIndex() {
      toolsModel = null;
    }

  }

}
