class SparklinesGenerator < Rails::Generator::Base

  def manifest
    record do |m|
      m.directory File.join("app/controllers")
      m.directory File.join("test/functional")

      m.file "controller.rb", File.join("app/controllers/sparklines_controller.rb")
      m.file "functional_test.rb", File.join("test/functional/sparklines_controller_test.rb")
    end
  end

end
