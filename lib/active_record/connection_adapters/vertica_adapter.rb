require 'active_record/connection_adapters/abstract_adapter'
require 'arel/visitors/bind_visitor'

module ActiveRecord
  class Base

    ##
    # Establishes a connection to the database that's used by all Active Record objects
    ##
    def self.vertica_connection(config)
      unless defined? Vertica
        begin
          require 'vertica'
        rescue LoadError
          raise "Vertica Gem not installed"
        end
      end

      config = config.symbolize_keys
      host = config[:host]
      port = config[:port] || 5433
      username = config[:username].to_s if config[:username]
      password = config[:password].to_s if config[:password]
      schema = config[:schema].to_s if config[:schema]

      if config.has_key?(:database)
        database = config[:database]
      else
        raise ArgumentError, "No database specified. Missing argument: database."
      end

      conn = Vertica.connect({ :user => username, 
                               :password => password, 
                               :host => host, 
                               :port => port, 
                               :database => database, 
                               :schema => schema })

      options = [host, username, password, database, port,schema]

      ConnectionAdapters::VerticaAdapter.new(conn, options, config)
    end  
  end

  module ConnectionAdapters
    class VerticaColumn < Column
      def initialize(name, default, sql_type = nil, null = true)
        super(name, self.class.extract_value_from_default(default), sql_type, null)
      end

      def self.extract_value_from_default(default)
        case default
          # Numeric types
        when /\A\(?(-?\d+(\.\d*)?\)?)\z/
          $1
          # Character types
        when /\A'(.*)'::(?:character varchar|varying|bpchar|text)\z/m
          $1
          # Character types (8.1 formatting)
        when /\AE'(.*)'::(?:character varchar|varying|bpchar|text)\z/m
          $1.gsub(/\\(\d\d\d)/) { $1.oct.chr }
          # Binary data types
        when /\A'(.*)'::bytea\z/m
          $1
          # Date/time types
        when /\A'(.+)'::(?:time(?:stamp)? with(?:out)? time zone|date)\z/
          $1
        when /\A'(.*)'::interval\z/
          $1
          # Boolean type
        when 'true'
          true
        when 'false'
          false
          # Geometric types
        when /\A'(.*)'::(?:point|line|lseg|box|"?path"?|polygon|circle)\z/
          $1
          # Network address types
        when /\A'(.*)'::(?:cidr|inet|macaddr)\z/
          $1
          # Bit string types
        when /\AB'(.*)'::"?bit(?: varying)?"?\z/
          $1
          # XML type
        when /\A'(.*)'::xml\z/m
          $1
          # Arrays
        when /\A'(.*)'::"?\D+"?\[\]\z/
          $1
          # Object identifier types
        when /\A-?\d+\z/
          $1
        else
          # Anything else is blank, some user type, or some function
          # and we can't know the value of that, so return nil.
          nil
        end
      end
    end

    # Deal with Arel visitors so this works with Rails 3.2
    class BindSubstitution < Arel::Visitors::PostgreSQL
      include Arel::Visitors::BindVisitor
    end

    class VerticaAdapter < AbstractAdapter

      ADAPTER_NAME = 'Vertica'.freeze

      NATIVE_DATABASE_TYPES = {
        :primary_key => "integer not null primary key",
        :string      => { :name => "varchar", :limit => 255 },
        :text        => { :name => "varchar", :limit => 15000 },
        :integer     => { :name => "integer" },
        :float       => { :name => "float" },
        :decimal     => { :name => "decimal" },
        :datetime    => { :name => "datetime" },
        :timestamp   => { :name => "timestamp" },
        :time        => { :name => "time" },
        :date        => { :name => "date" },
        :binary      => { :name => "bytea" },
        :boolean     => { :name => "boolean" },
        :xml         => { :name => "xml" }
      }

      ##
      # Constructor
      #
      def initialize(connection, connection_options, config)
        super(connection)
        @connection_options, @config = connection_options, config
        @quoted_column_names, @quoted_table_names = {}, {}
        @visitor = BindSubstitution.new(self)
      end

      ##
      # Instance Methods
      #
      def active?
        @connection.opened?
      end

      def adapter_name
        ADAPTER_NAME
      end

      def add_index(table_name, column_name, options = {})
        #noop
      end

      def columns(table_name, name = nil)
        sql = "SELECT column_name, data_type, column_default, is_nullable FROM v_catalog.columns WHERE table_name = #{quote_column_name(table_name)} AND table_schema = #{quote_column_name(schema_name)}"

        columns = []

        execute(sql, name) do |field|
          columns << VerticaColumn.new(
            field[:column_name],
            field[:column_default],
            field[:data_type],
            field[:is_nullable]
          )
        end

        columns
      end

      # Close the connection.
      def disconnect!
        @connection.close rescue nil
      end

      # return raw object
      def execute(sql, name=nil)
        log(sql,name) do
          if block_given?
            @connection.query(sql) { |row| yield row }
          else
            @connection.query(sql)
          end
        end
      end

      def native_database_types
        NATIVE_DATABASE_TYPES
      end 

      def primary_key(table)
        ''
      end

      ## QUOTING
      def quote_column_name(name)
        "'#{name}'"
      end

      def quote_table_name(name)
        if schema_name.blank?
          name
        else
          "#{schema_name}.#{name}"
        end
      end

      # Disconnects from the database if already connected, and establishes a
      # new connection with the database.
      def reconnect!
        @connection.reset_connection
      end

      def remove_index(table_name, options = {})
        # no-op in vertica 
      end

      def remove_index!(table_name, index_name)
        # no-op in vertica 
      end

      def rename_index(table_name, old_name, new_name)
        # no-op in vertica 
      end

      def reset
        reconnect!
      end

      def schema_name
        @schema = @schema || @connection.options[:schema] || "public"
      end

      def select(sql, name = nil, binds = [])
        rows = []
        @connection.query(sql) { |row| rows << row }
        rows
      end

      # Returns an array of arrays containing the field values.
      # Order is the same as that returned by +columns+.
      def select_rows(sql, name = nil)
        res = execute(sql, name)
        res.map(&:values)
      end

      def tables(name = nil)
        sql = "SELECT table_name FROM v_catalog.tables WHERE table_schema = '#{schema_name}'"

        tables = []
        execute(sql, name) { |field| tables << field[:table_name] }
        tables
      end

      def table_exists?(name)
        name          = name.to_s
        schema, table = name.split('.', 2)

        unless table # A table was provided without a schema
          table  = schema
          schema = nil
        end

        if name =~ /^"/ # Handle quoted table names
          table  = name
          schema = nil
        end

        @connection.query(<<-SQL).first[0].to_i > 0
          SELECT COUNT(*)
          FROM v_catalog.tables
          WHERE table_name = '#{table.gsub(/(^"|"$)/,'')}'
        SQL
      end
    end  
  end
end
