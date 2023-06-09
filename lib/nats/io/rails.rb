require "rails"

module NATS
  class Rails < ::Rails::Engine
    # This class is used to free resources managed by Rails (e.g. database connections)
    # that were implicitly acquired in subscription callbacks
    # Implementation is based on https://github.com/sidekiq/sidekiq/blob/5e1a77a6d03193dd977fbfe8961ab78df91bb392/lib/sidekiq/rails.rb
    class Reloader
      def initialize(app = ::Rails.application)
        @app = app
      end

      def call
        params = (::Rails::VERSION::STRING >= "7.1") ? {source: "gem.nats"} : {}
        @app.reloader.wrap(**params) do
          yield
        end
      end

      def inspect
        "#<NATS::Rails::Reloader @app=#{@app.class.name}>"
      end
    end

    config.after_initialize do
      NATS::Client.default_reloader = NATS::Rails::Reloader.new
    end
  end
end
