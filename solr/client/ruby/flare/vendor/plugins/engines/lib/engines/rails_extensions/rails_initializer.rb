# Enhances the Rails::Initializer class to be a bit smarter about
# plugins. See Engines::RailsExtensions::RailsInitializer for more
# details.

require "engines/rails_extensions/rails"
require 'engines/plugin_list'

# The engines plugin changes the way that Rails actually loads other plugins.
# It creates instances of the Plugin class to represent each plugin, stored
# in the <tt>Rails.plugins</tt> PluginList.
#
# ---
#
# Three methods from the original Rails::Initializer module are overridden
# by Engines::RailsExtensions::RailsInitializer:
#
# [+load_plugin+] which now creates Plugin instances and calls Plugin#load
# [+after_initialize+] which now performs Engines.after_initialize in addition
#                      to the given config block
# [<tt>plugin_enabled?</tt>]  which now respects the result of 
#                             Engines.load_all_plugins?
#
module Engines::RailsExtensions::RailsInitializer
  def self.included(base) #:nodoc:
    base.class_eval do
      alias_method_chain :load_plugin, :engine_additions
      alias_method_chain :after_initialize, :engine_additions
      alias_method_chain :plugin_enabled?, :engine_additions    
    end
  end
  
  # Loads all plugins in configuration.plugin_paths, regardless of the contents
  # of configuration.plugins
  def load_all_plugins
    # a nil value implies we don't care about plugins; load 'em all in a reliable order
    find_plugins(configuration.plugin_paths).sort.each { |path| load_plugin path }
  end
  
  # Loads a plugin, performing the extra load path/public file magic of
  # engines by calling Plugin#load.
  def load_plugin_with_engine_additions(directory)
    name = plugin_name(directory)
    return false if loaded_plugins.include?(name)
    
    logger.debug "loading plugin from #{directory} with engine additions"
    
    # add the Plugin object
    plugin = Plugin.new(plugin_name(directory), directory)
    Rails.plugins << plugin
          
    # do the other stuff that load_plugin used to do. This includes
    # allowing the plugin's init.rb to set configuration options on
    # it's instance, which can then be used in it's initialization
    load_plugin_without_engine_additions(directory)

    # perform additional loading tasks like mirroring public assets
    # and adding app directories to the appropriate load paths
    plugin.load
          
    true
  end
  
  # Allow the engines plugin to do whatever it needs to do after Rails has
  # loaded, and then call the actual after_initialize block. Currently, this
  # is call Engines.after_initialize.
  def after_initialize_with_engine_additions
    Engines.after_initialize
    after_initialize_without_engine_additions
  end
  
  protected
  
    # Returns true if the plugin at the given path should be loaded; false
    # otherwise. If Engines.load_all_plugins? is true, this method will return
    # true regardless of the path given.
    def plugin_enabled_with_engine_additions?(path)
      Engines.load_all_plugins? || plugin_enabled_without_engine_additions?(path)
    end
        
    # Returns the name of the plugin at the given path.
    def plugin_name(path)
      File.basename(path)
    end    
end

::Rails::Initializer.send(:include, Engines::RailsExtensions::RailsInitializer)