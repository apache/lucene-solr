# The PluginList class is an array, enhanced to allow access to loaded plugins
# by name, and iteration over loaded plugins in order of priority. This array is used
# by Engines::RailsExtensions::RailsInitializer to create the Rails.plugins array.
#
# Each loaded plugin has a corresponding Plugin instance within this array, and 
# the order the plugins were loaded is reflected in the entries in this array.
#
# For more information, see the Rails module.
class PluginList < Array
  # Finds plugins with the set with the given name (accepts Strings or Symbols), or
  # index. So, Rails.plugins[0] returns the first-loaded Plugin, and Rails.plugins[:engines]
  # returns the Plugin instance for the engines plugin itself.
  def [](name_or_index)
    if name_or_index.is_a?(Fixnum)
      super
    else
      self.find { |plugin| plugin.name.to_s == name_or_index.to_s }
    end
  end
  
  # Go through each plugin, highest priority first (last loaded first). Effectively,
  # this is like <tt>Rails.plugins.reverse</tt>, except when given a block, when it behaves
  # like <tt>Rails.plugins.reverse.each</tt>.
  def by_precedence(&block)
    if block_given?
      reverse.each { |x| yield x }
    else 
      reverse
    end
  end
end