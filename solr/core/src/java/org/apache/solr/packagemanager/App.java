package org.apache.solr.packagemanager;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;

import org.pf4j.PluginException;
import org.pf4j.update.UpdateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App {
	
    private static final Logger log = LoggerFactory.getLogger(App.class);

	public static void main( String[] args ) throws PluginException {
		SolrPluginManager pluginManager = new SolrPluginManager(new File("./plugins"));
				
		pluginManager.loadPlugins();

		// create update manager
		UpdateManager updateManager = new SolrUpdateManager(pluginManager, Paths.get("repositories.json"));
		System.out.println(updateManager.getAvailablePlugins());

		updateManager.installPlugin("question-answer", "1.0.0");
		System.out.println(updateManager.hasUpdates());
		
		System.out.println(pluginManager.deployPlugin("question-answer", Collections.singletonList("abc")));
		
		// >> keep system up-to-date <<
		//boolean systemUpToDate = true;
        // check for updates
        /*if (updateManager.hasUpdates()) {
            List<PluginInfo> updates = updateManager.getUpdates();
            log.debug("Found {} updates", updates.size());
            for (PluginInfo plugin : updates) {
                log.debug("Found update for plugin '{}'", plugin.id);
                PluginInfo.PluginRelease lastRelease = updateManager.getLastPluginRelease(plugin.id);
                String lastVersion = lastRelease.version;
                String installedVersion = pluginManager.getPlugin(plugin.id).getDescriptor().getVersion();
                log.debug("Update plugin '{}' from version {} to version {}", plugin.id, installedVersion, lastVersion);
                boolean updated = updateManager.updatePlugin(plugin.id, lastVersion);
                if (updated) {
                    log.debug("Updated plugin '{}'", plugin.id);
                } else {
                    log.error("Cannot update plugin '{}'", plugin.id);
                    systemUpToDate = false;
                }
            }
        } else {
            log.debug("No updates found");
        }*/
	}
	
}
