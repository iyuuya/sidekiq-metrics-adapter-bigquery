# frozen_string_literal: true

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
            @worker_status = worker_status
            # TODO: table-suffix
            table = Sidekiq::Metrics.configuration.adapter.table_with_suffix(table_suffiix)
            result = table.insert([worker_status])

            error = result.insert_error_for(worker_staus)
            raise InsertError, error.errors.to_json if error
          end

          private

          # _YYYYMMDD
          def table_suffix
            "_#{Time.at(@owrker_status['enqueed_at']).strftime('%y%m%d')}"
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
        end

        def write(worker_status)
          Worker.perform_async(worker_status)
        end

        # TODO: check table exist
        def table_with_suffix(suffix = nil)
          @dataset.table("#{@table}#{suffix}")
        end
      end
    end
  end
end
