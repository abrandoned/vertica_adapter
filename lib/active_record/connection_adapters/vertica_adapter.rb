require 'active_record/connection_adapters/abstract_adapter'

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
    end

    class VerticaAdapter < AbstractAdapter
      ADAPTER_NAME = 'Vertica'.freeze

      ##
      # Constructor
      #
      def initialize(connection, connection_options, config)
        super(connection)
        @connection_options, @config = connection_options, config
        @quoted_column_names, @quoted_table_names = {}, {}
        # connect
      end

      ##
      # Instance Methods
      #
      def active?
        @connection.opened?
      end

      def adapter_name #:nodoc:
        ADAPTER_NAME
      end

      def columns(table_name, name = nil) #:nodoc:
        sql = "SELECT * FROM columns WHERE table_name = #{quote_column_name(table_name)} AND table_schema = #{quote_column_name(schema_name)}"

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
            @connection = ::Vertica.connect(@connection.options)
            @connection.query(sql) {|row| yield row }
            @connection.close
          else
            @connection = ::Vertica.connect(@connection.options)
            results = @connection.query(sql)
            @connection.close
            results
          end
        end
      end

      def primary_key(table)
        ''
      end

      ## QUOTING
      def quote_column_name(name) #:nodoc:
        "'#{name}'"
      end

      def quote_table_name(name) #:nodoc:
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

      def reset
        reconnect!
      end

      def schema_name
        @schema ||= @connection.options[:schema]
      end

      def select(sql, name = nil, binds = [])
        rows = []
        @connection = ::Vertica.connect(@connection.options)
        @connection.query(sql) {|row| rows << row }
        @connection.close
        rows
      end

      def tables(name = nil) #:nodoc:
        sql = "SELECT * FROM tables WHERE table_schema = #{quote_column_name(schema_name)}"

        tables = []
        execute(sql, name) { |field| tables << field[:table_name] }
        tables
      end
    end  
  end
end
