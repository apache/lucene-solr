/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

package de.lanlab.larm.gui;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;


public class FetcherSummaryFrame extends JFrame
{
    JPanel lowerPanel = new JPanel();
    JPanel progressPanel = new JPanel();
    JPanel middlePanel = new JPanel();
    JPanel rightPanel = new JPanel();
    BorderLayout borderLayout1 = new BorderLayout();
    JPanel propertyPanel = new JPanel();
    JLabel hostLabel = new JLabel();
    JLabel urlRestrictionFrame = new JLabel();
    JTextField startURL = new JTextField();
    JTextField restrictTo = new JTextField();
    JButton startButton = new JButton();
    GridLayout gridLayout1 = new GridLayout();
    JProgressBar urlQueuedProgress = new JProgressBar(0,100);
    JLabel urlQueuedLabel = new JLabel();
    JLabel scopeFilteredLabel = new JLabel();
    JProgressBar scopeFilteredProgress = new JProgressBar(0,100);
    JLabel visitedFilteredLabel = new JLabel();
    JProgressBar visitedFilteredProgress = new JProgressBar(0,100);
    JLabel workingThreadsLabel = new JLabel();
    JProgressBar workingThreadsProgress = new JProgressBar(0,100);
    JLabel idleThreadsLabel = new JLabel();
    JProgressBar idleThreadsProgress = new JProgressBar(0,100);
    JLabel busyThreadsLabel = new JLabel();
    JProgressBar busyThreadsProgress = new JProgressBar(0,100);
    JLabel requestQueueLabel = new JLabel();
    JProgressBar requestQueueProgress = new JProgressBar();
    JLabel stalledThreadsLabel = new JLabel();
    JProgressBar stalledThreadsProgress = new JProgressBar();
    JLabel dnsLabel = new JLabel();
    JProgressBar dnsProgress = new JProgressBar(0,100);
    JLabel freeMemLabel = new JLabel();
    JLabel freeMemText = new JLabel();
    JLabel totalMemLabel = new JLabel();
    JLabel totalMemText = new JLabel();
    JLabel bpsLabel = new JLabel();
    JLabel bpsText = new JLabel();
    JLabel docsLabel = new JLabel();
    JLabel docsText = new JLabel();
    JLabel docsReadLabel = new JLabel();
    JLabel docsReadText  = new JLabel();
    JProgressBar urlsCaughtProgress = new JProgressBar(0,100);
    JLabel urlsCaughtText = new JLabel();
    JLabel robotsTxtsText = new JLabel();
    JProgressBar robotsTxtsProgress = new JProgressBar(0,100);

    public FetcherSummaryFrame()
    {
        try
        {
           jbInit();
           this.setTitle("LARM - LANLab Retrieval Machine");
           this.setSize(new Dimension(640,350));
           this.urlQueuedProgress.setStringPainted(true);
           this.urlQueuedProgress.setString("0");
           this.scopeFilteredProgress.setStringPainted(true);
           this.scopeFilteredProgress.setString("0");
           this.visitedFilteredProgress.setStringPainted(true);
           this.visitedFilteredProgress.setString("0");
           workingThreadsProgress.setStringPainted(true);
           workingThreadsProgress.setString("0");
           idleThreadsProgress.setStringPainted(true);
           idleThreadsProgress.setString("0");
           busyThreadsProgress.setStringPainted(true);
           busyThreadsProgress.setString("0");
           stalledThreadsProgress.setStringPainted(true);
           stalledThreadsProgress.setString("0");
           requestQueueProgress.setStringPainted(true);
           requestQueueProgress.setString("0");
           dnsProgress.setStringPainted(true);
           dnsProgress.setString("0");
           urlsCaughtProgress.setStringPainted(true);
           urlsCaughtProgress.setString("0");
           robotsTxtsProgress.setStringPainted(true);
           robotsTxtsProgress.setString("0");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    private void jbInit() throws Exception
    {
        this.getContentPane().setLayout(borderLayout1);
        propertyPanel.setMinimumSize(new Dimension(10, 70));
        propertyPanel.setPreferredSize(new Dimension(10, 80));
        propertyPanel.setLayout(null);
        hostLabel.setText("Startseite");
        hostLabel.setBounds(new Rectangle(18, 15, 76, 17));
        urlRestrictionFrame.setText("URL-Restriction (regul. Ausdruck)");
        urlRestrictionFrame.setBounds(new Rectangle(18, 37, 208, 17));
        startURL.setBounds(new Rectangle(224, 14, 281, 21));
        restrictTo.setBounds(new Rectangle(224, 38, 281, 21));
        startButton.setActionCommand("start");
        startButton.setText("Start");
        startButton.setBounds(new Rectangle(528, 14, 79, 47));
        lowerPanel.setLayout(gridLayout1);
        urlQueuedLabel.setToolTipText("");
        urlQueuedLabel.setText("URLs queued");
        scopeFilteredLabel.setToolTipText("");
        scopeFilteredLabel.setText("Scope-gefiltert");
        visitedFilteredLabel.setText("Visited gefiltert");
        workingThreadsLabel.setText("Number of Working Threads");
        idleThreadsLabel.setText("Idle Threads");
        busyThreadsLabel.setText("Busy Threads");
        requestQueueLabel.setText("requests queued");
        stalledThreadsLabel.setText("stalled Threads");
        stalledThreadsProgress.setPreferredSize(new Dimension(190, 25));
        requestQueueProgress.setPreferredSize(new Dimension(190, 25));
        busyThreadsProgress.setPreferredSize(new Dimension(190, 25));
        idleThreadsProgress.setPreferredSize(new Dimension(190, 25));
        workingThreadsProgress.setPreferredSize(new Dimension(190, 25));
        urlQueuedProgress.setPreferredSize(new Dimension(190, 25));
        scopeFilteredProgress.setPreferredSize(new Dimension(190, 25));
        visitedFilteredProgress.setPreferredSize(new Dimension(190, 25));
        dnsLabel.setText("DNS Hosts cached");
        dnsProgress.setPreferredSize(new Dimension(190, 25));
        freeMemLabel.setText("Free Mem");
        freeMemLabel.setPreferredSize(new Dimension(60, 17));
        freeMemText.setText("0");
        freeMemText.setPreferredSize(new Dimension(120, 17));
        freeMemText.setMinimumSize(new Dimension(100, 17));
        totalMemLabel.setText("total Mem");
        totalMemLabel.setPreferredSize(new Dimension(60, 17));
        totalMemText.setText("0");
        totalMemText.setPreferredSize(new Dimension(120, 17));
        totalMemText.setMinimumSize(new Dimension(100, 17));
        bpsLabel.setPreferredSize(new Dimension(60, 17));
        bpsLabel.setText("Bytes/s");
        bpsText.setMinimumSize(new Dimension(100, 17));
        bpsText.setPreferredSize(new Dimension(120, 17));
        bpsText.setText("0");
        docsLabel.setText("Docs/s");
        docsLabel.setPreferredSize(new Dimension(60, 17));
        docsText.setText("0");
        docsText.setPreferredSize(new Dimension(120, 17));
        docsText.setMinimumSize(new Dimension(100, 17));
        docsReadLabel.setText("Docs read");
        docsReadLabel.setPreferredSize(new Dimension(60, 17));
        docsReadText.setText("0");
        docsReadText.setPreferredSize(new Dimension(120, 17));
        docsReadText.setMinimumSize(new Dimension(100, 17));
        urlsCaughtProgress.setPreferredSize(new Dimension(190, 25));
        urlsCaughtText.setText("URLs caught by Robots.txt");
        robotsTxtsText.setText("Robots.txts found");
        robotsTxtsProgress.setPreferredSize(new Dimension(190, 25));
        this.getContentPane().add(lowerPanel, BorderLayout.CENTER);
        lowerPanel.add(progressPanel, null);
        progressPanel.add(urlQueuedLabel, null);
        progressPanel.add(urlQueuedProgress, null);
        progressPanel.add(scopeFilteredLabel, null);
        progressPanel.add(scopeFilteredProgress, null);
        progressPanel.add(visitedFilteredLabel, null);
        progressPanel.add(visitedFilteredProgress, null);
        progressPanel.add(dnsLabel, null);
        progressPanel.add(dnsProgress, null);
        progressPanel.add(robotsTxtsText, null);
        progressPanel.add(robotsTxtsProgress, null);
        progressPanel.add(urlsCaughtText, null);
        progressPanel.add(urlsCaughtProgress, null);
        lowerPanel.add(middlePanel, null);
        middlePanel.add(workingThreadsLabel, null);
        middlePanel.add(workingThreadsProgress, null);
        middlePanel.add(idleThreadsLabel, null);
        middlePanel.add(idleThreadsProgress, null);
        middlePanel.add(busyThreadsLabel, null);
        middlePanel.add(busyThreadsProgress, null);
        middlePanel.add(requestQueueLabel, null);
        middlePanel.add(requestQueueProgress, null);
        middlePanel.add(stalledThreadsLabel, null);
        middlePanel.add(stalledThreadsProgress, null);
        lowerPanel.add(rightPanel, null);
        rightPanel.add(docsLabel, null);
        rightPanel.add(docsText, null);
        rightPanel.add(docsReadLabel, null);
        rightPanel.add(docsReadText, null);
        rightPanel.add(bpsLabel, null);
        rightPanel.add(bpsText, null);
        rightPanel.add(totalMemLabel, null);
        rightPanel.add(totalMemText, null);
        rightPanel.add(freeMemLabel, null);
        rightPanel.add(freeMemText, null);
        this.getContentPane().add(propertyPanel, BorderLayout.NORTH);
        propertyPanel.add(urlRestrictionFrame, null);
        propertyPanel.add(restrictTo, null);
        propertyPanel.add(hostLabel, null);
        propertyPanel.add(startButton, null);
        propertyPanel.add(startURL, null);
    }

    public void setCounterProgressBar(JProgressBar p, int value)
    {
        int oldMax = p.getMaximum();
        int oldValue = p.getValue();

        if(value > oldMax)
        {
            p.setMaximum(oldMax * 2);
        }
        else if (value < oldMax / 2 && oldValue >= oldMax / 2)
        {
            p.setMaximum(oldMax / 2);
        }
        p.setValue(value);
        p.setString("" + value);
    }

    public void setURLsQueued(int queued)
    {
        setCounterProgressBar(this.urlQueuedProgress, queued);
    }

    public void setScopeFiltered(int filtered)
    {
        setCounterProgressBar(this.scopeFilteredProgress, filtered);
    }

    public void setVisitedFiltered(int filtered)
    {
        setCounterProgressBar(this.visitedFilteredProgress, filtered);
    }

    public void setWorkingThreadsCount(int threads)
    {
        setCounterProgressBar(this.workingThreadsProgress, threads);
    }

    public void setIdleThreadsCount(int threads)
    {
        setCounterProgressBar(this.idleThreadsProgress, threads);
    }

    public void setBusyThreadsCount(int threads)
    {
        setCounterProgressBar(this.busyThreadsProgress, threads);
    }

    public void setRequestQueueCount(int requests)
    {
        setCounterProgressBar(this.requestQueueProgress, requests);
    }

    public void setDNSCount(int count)
    {
        setCounterProgressBar(this.dnsProgress, count);
    }

    public void setURLsCaughtCount(int count)
    {
        setCounterProgressBar(this.urlQueuedProgress, count);
    }

    public void addStartButtonListener(ActionListener a)
    {
        startButton.addActionListener(a);
    }



    public String getRestrictTo()
    {
       return restrictTo.getText();
    }
    public void setRestrictTo(String restrictTo)
    {
       this.restrictTo.setText(restrictTo);
    }
    public String getStartURL()
    {
       return startURL.getText();
    }
    public void setStartURL(String startURL)
    {
        this.startURL.setText(startURL);
    }

    public void setStalledThreads(int stalled)
    {
        stalledThreadsProgress.setValue(stalled);
    }

    public void setBytesPerSecond(double bps)
    {
        bpsText.setText("" + bps);
    }


    public void setDocsPerSecond(double docs)
    {
        bpsText.setText("" + docs);
    }

    public void setFreeMem(long freeMem)
    {
        freeMemText.setText("" + freeMem);
    }

    public void setTotalMem(long totalMem)
    {
        totalMemText.setText("" + totalMem);
    }

    public void setRobotsTxtCount(int robotsTxtCount)
    {
        setCounterProgressBar(robotsTxtsProgress, robotsTxtCount);
    }

    public void setDocsRead(int docs)
    {
        bpsText.setText("" + docs);
    }

}

