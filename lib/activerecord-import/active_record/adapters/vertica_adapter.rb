require "active_record/connection_adapters/vertica_adapter"
require "activerecord-import/adapters/vertica_adapter"

class ActiveRecord::ConnectionAdapters::VerticaAdapter
  include ActiveRecord::Import::VerticaAdapter
end
