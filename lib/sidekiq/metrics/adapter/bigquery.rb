# frozen_string_literal: true

require 'time'
require 'sidekiq/worker'
require 'sidekiq/metrics/adapter/base'

module Sidekiq
  module Metrics
    module Adapter
      class Bigquery < Base
        class Worker
          class InsertError < StandardError; end

          include Sidekiq::Worker

          def perform(worker_status)
            @worker_status = {
              queue: worker_status['queue'],
              class: worker_status['class'],
              retry: worker_status['retry'],
              jid: worker_status['jid'],
              status: worker_status['status'],
              enqueued_at: worker_status['enqueued_at'],
              started_at: worker_status['started_at'],
              finished_at: worker_status['finished_at']
            }

            table_suffix = Time.at(worker_status['enqueued_at']).strftime('%Y%m%d')
            table = Sidekiq::Metrics.configuration.adapter.table(table_suffix)
            result = table.insert([@worker_status])

            error = result.insert_error_for(@worker_status)
            raise InsertError, error.errors.to_json if error
          end
        end

        # @param [Google::Cloud::Bigquery::Datset] dataset
        # @param [String] table
        # @param [boolean] async
        # @param [Hash] sidekiq_worker_options
        def initialize(dataset,
                       table,
                       async: true,
                       sidekiq_worker_options: {
                         queue: :default,
                         retry: 5
                       })
          @dataset = dataset
          @table = table
          Worker.sidekiq_options(sidekiq_worker_options)

          Sidekiq::Metrics.configure do |config|
            config.excludes << Worker.name
          end
        end

        def write(worker_status)
          Worker.perform_async(worker_status)
        end

        def table(suffix = nil)
          unless table = @dataset.table("#{@table}_#{suffix}")
            # TODO: create table?
            raise 'Table not found'
          end
          table
        end
      end
    end
  end
end
