$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "rails", "railties", "lib")
silence_warnings { require 'rails/version' } # it may already be loaded.

unless Rails::VERSION::MAJOR >= 1 && Rails::VERSION::MINOR >= 2
  puts <<-end_of_warning

         !!!=== IMPORTANT NOTE ===!!!

Support for Rails < 1.2 has been dropped; if you are using 
Rails =< 1.1.6, please use Engines 1.1.6, available from: 

  >>  http://svn.rails-engines.org/engines/tags/rel_1.1.6

For more details about changes in Engines 1.2, please see 
the changelog or the website: 

  >>  http://www.rails-engines.org

end_of_warning
else
  puts <<-end_of_message
  
The engines plugin is now installed. Feels good, right? Yeah. 
You knew it would.

Once the warm, fuzzy glow has subsided, be sure to read the contents 
of the README and UPGRADING files if you're migrating this application 
from Rails 1.1.x to 1.2.x.

Have a great day!
end_of_message
end