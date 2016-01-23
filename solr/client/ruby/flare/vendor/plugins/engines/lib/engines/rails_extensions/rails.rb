# In order to give a richer infrastructure for dealing with plugins, the engines
# plugin adds two new attributes to the Rails module:
#
# [+plugins+]       A PluginList instance which holds the currently loaded plugins
# [+configuration+] The current Rails::Configuration instance, so that we can
#                   query any parameters that might be set *after* Rails has
#                   loaded, as well as during plugin initialization
#
#--
# Here we just re-open the Rails module and add our custom accessors; it
# may be cleaner to seperate them into a module, but in this case that seems
# like overkill.
#++
module Rails
  # The set of all loaded plugins
  mattr_accessor :plugins
  
  # The Rails::Initializer::Configuration object
  mattr_accessor :configuration
end
