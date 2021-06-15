/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.tests.nightlybenchmarks;

import java.io.File;

import org.apache.log4j.Logger;

enum ZookeeperAction {
	ZOOKEEPER_START, ZOOKEEPER_STOP, ZOOKEEPER_CLEAN
}

/**
 * This class provides blueprint for Zookeeper Node.
 * 
 * @author Vivek Narang
 *
 */
public class Zookeeper {

	public final static Logger logger = Logger.getLogger(Zookeeper.class);

	public static String zooCommand;
	public static String zooCleanCommand;

	static {
		zooCommand = System.getProperty("os.name") != null && System.getProperty("os.name").startsWith("Windows")
				? "bin" + File.separator + "zkServer.cmd " : "bin" + File.separator + "zkServer.sh ";
	}

	/**
	 * Constructor.
	 * 
	 * @throws Exception
	 */
	Zookeeper() throws Exception {
		super();
		this.install();
	}

	/**
	 * A method for setting up zookeeper node.
	 * 
	 * @throws Exception
	 */
	private void install() throws Exception {

		logger.info("Installing Zookeeper Node ...");

		File base = new File(Util.ZOOKEEPER_DIR);
		if (!base.exists()) {
			base.mkdir();
			base.setExecutable(true);
		}

		File release = new File(Util.DOWNLOAD_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + ".tar.gz");
		if (!release.exists()) {
			logger.info("Attempting to download zookeeper release ..." + " : " + Util.ZOOKEEPER_RELEASE);

			String fileName = "zookeeper-" + Util.ZOOKEEPER_RELEASE + ".tar.gz";

			Util.download(
					Util.ZOOKEEPER_DOWNLOAD_URL + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + fileName,
					Util.DOWNLOAD_DIR + fileName);
		} else {
			logger.info("Release present nothing to download ...");
		}

		File urelease = new File(Util.DOWNLOAD_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE);
		if (!urelease.exists()) {

			Util.execute("tar -xf " + Util.DOWNLOAD_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + ".tar.gz" + " -C "
					+ Util.ZOOKEEPER_DIR, Util.ZOOKEEPER_DIR);

			Util.execute(
					"mv " + Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + "conf"
							+ File.separator + "zoo_sample.cfg " + Util.ZOOKEEPER_DIR + "zookeeper-"
							+ Util.ZOOKEEPER_RELEASE + File.separator + "conf" + File.separator + "zoo.cfg",
					Util.ZOOKEEPER_DIR);

		} else {
			logger.info("Release extracted already nothing to do ..." + " : " + Util.ZOOKEEPER_RELEASE);
		}
	}

	/**
	 * A method to act on the zookeeper node (start, stop etc...)
	 * 
	 * @param action
	 * @return
	 * @throws Exception
	 */
	public int doAction(ZookeeperAction action) throws Exception {

		new File(Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + zooCommand)
				.setExecutable(true);

		if (action == ZookeeperAction.ZOOKEEPER_START) {
			return Util.execute(
					Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + zooCommand + " start",
					Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator);
		} else if (action == ZookeeperAction.ZOOKEEPER_STOP) {
			return Util.execute(
					Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + zooCommand + " stop",
					Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator);
		} else if (action == ZookeeperAction.ZOOKEEPER_CLEAN) {
			Util.execute("rm -r -f " + Util.ZOOKEEPER_DIR, Util.ZOOKEEPER_DIR);
			return Util.execute("rm -r -f /tmp/zookeeper/", "/tmp/zookeeper/");
		}

		return -1;
	}

	/**
	 * A method for getting the zookeeper IP.
	 * 
	 * @return
	 */
	public String getZookeeperIp() {
		return Util.ZOOKEEPER_IP;
	}

	/**
	 * A method for getting the zookeeper Port.
	 * 
	 * @return
	 */
	public String getZookeeperPort() {
		return Util.ZOOKEEPER_PORT;
	}

}