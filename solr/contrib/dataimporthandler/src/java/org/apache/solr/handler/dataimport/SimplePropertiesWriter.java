package org.apache.solr.handler.dataimport;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplePropertiesWriter implements DIHPropertiesWriter {
	private static final Logger log = LoggerFactory.getLogger(SimplePropertiesWriter.class);

	static final String IMPORTER_PROPERTIES = "dataimport.properties";

	static final String LAST_INDEX_KEY = "last_index_time";

	private String persistFilename = IMPORTER_PROPERTIES;

	private String configDir = null;



    public void init(DataImporter dataImporter) {
      SolrCore core = dataImporter.getCore();
      String configDir = core ==null ? ".": core.getResourceLoader().getConfigDir();
      String persistFileName = dataImporter.getHandlerName();

      this.configDir = configDir;
	  if(persistFileName != null){
        persistFilename = persistFileName + ".properties";
      }
    }



	
	private File getPersistFile() {
    String filePath = configDir;
    if (configDir != null && !configDir.endsWith(File.separator))
      filePath += File.separator;
    filePath += persistFilename;
    return new File(filePath);
  }

    public boolean isWritable() {
        File persistFile =  getPersistFile();
        return persistFile.exists() ? persistFile.canWrite() : persistFile.getParentFile().canWrite();

    }

    @Override
	public void persist(Properties p) {
		OutputStream propOutput = null;

		Properties props = readIndexerProperties();

		try {
			props.putAll(p);
			String filePath = configDir;
			if (configDir != null && !configDir.endsWith(File.separator))
				filePath += File.separator;
			filePath += persistFilename;
			propOutput = new FileOutputStream(filePath);
			props.store(propOutput, null);
			log.info("Wrote last indexed time to " + persistFilename);
		} catch (Exception e) {
			throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "Unable to persist Index Start Time", e);
		} finally {
			try {
				if (propOutput != null)
					propOutput.close();
			} catch (IOException e) {
				propOutput = null;
			}
		}
	}

	@Override
	public Properties readIndexerProperties() {
		Properties props = new Properties();
		InputStream propInput = null;

		try {
			propInput = new FileInputStream(configDir + persistFilename);
			props.load(propInput);
			log.info("Read " + persistFilename);
		} catch (Exception e) {
			log.warn("Unable to read: " + persistFilename);
		} finally {
			try {
				if (propInput != null)
					propInput.close();
			} catch (IOException e) {
				propInput = null;
			}
		}

		return props;
	}

}
