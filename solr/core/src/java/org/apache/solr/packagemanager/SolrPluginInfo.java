package org.apache.solr.packagemanager;


import java.util.List;

import org.pf4j.update.PluginInfo;

import com.google.gson.annotations.SerializedName;

/**
 * {@code PluginInfo} describing a plugin from a repository.
 */
public class SolrPluginInfo extends PluginInfo {

  public List<SolrPluginRelease> versions;

    public static class SolrPluginRelease extends PluginRelease {

    	@SerializedName("setup-commands")
        public List<String> setupCommands;

      @SerializedName("update-commands")
      public List<String> updateCommands;

        @Override
        public String toString() {
            return "SolrPluginRelease{" +
                "version='" + version + '\'' +
                ", date=" + date +
                ", requires='" + requires + '\'' +
                ", url='" + url + '\'' +
                ", setupCommand='" + setupCommands + '\'' +
                ", updateCommand='" + updateCommands + '\'' +
                ", sha512sum='" + sha512sum + '\'' +
                '}';
        }
    }

}

