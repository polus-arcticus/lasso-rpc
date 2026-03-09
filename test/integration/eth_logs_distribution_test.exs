defmodule Lasso.RPC.EthLogsDistributionTest do
  @moduledoc """
  Integration tests for eth_getLogs parallel distribution.

  Verifies that eth_getLogs requests spanning more blocks than a provider's
  max_block_range are automatically split into chunks and dispatched in parallel
  through the normal request pipeline, with results merged before returning.
  """

  use Lasso.Test.LassoIntegrationCase

  @moduletag :integration
  @moduletag timeout: 30_000

  alias Lasso.RPC.{RequestOptions, RequestPipeline, Response}
  alias Lasso.Testing.MockProviderBehavior

  # Deliberately small so we can trigger distribution with a modest block range
  @test_chunk_size 100

  describe "eth_getLogs distribution" do
    test "distributes a large range and returns merged logs", %{chain: chain} do
      test_pid = self()

      # Track how many times eth_getLogs is called by sending messages back
      # to the test process. Each chunk dispatch should produce one call.
      call_tracker =
        MockProviderBehavior.parameter_sensitive(fn method, params, _state ->
          if method == "eth_getLogs" do
            send(test_pid, {:chunk_called, params})
          end

          {:ok, []}
        end)

      setup_providers([
        %{
          id: "provider",
          priority: 10,
          behavior: call_tracker,
          capabilities: %{limits: %{max_block_range: @test_chunk_size}}
        }
      ])

      # 250-block range → should produce 3 chunks: [0,99], [100,199], [200,249]
      filter = %{"fromBlock" => "0x0", "toBlock" => "0xF9"}

      {:ok, result, _ctx} =
        RequestPipeline.execute_via_channels(
          chain,
          "eth_getLogs",
          [filter],
          %RequestOptions{strategy: :load_balanced, timeout_ms: 10_000}
        )

      IO.puts("\nresult type: #{inspect(result.__struct__)}")

      assert %Response.Success{} = result

      {:ok, logs} = Response.Success.decode_result(result)
      IO.inspect(logs, label: "merged logs")
      assert is_list(logs)
    end

    test "chunks fire in parallel - all chunks received before timeout", %{chain: chain} do
      test_pid = self()

      call_tracker =
        MockProviderBehavior.parameter_sensitive(fn method, params, _state ->
          if method == "eth_getLogs" do
            send(test_pid, {:chunk_called, List.first(params)["fromBlock"]})
          end

          {:ok, []}
        end)

      setup_providers([
        %{
          id: "provider",
          priority: 10,
          behavior: call_tracker,
          capabilities: %{limits: %{max_block_range: @test_chunk_size}}
        }
      ])

      # 300-block range → 3 chunks
      filter = %{"fromBlock" => "0x0", "toBlock" => "0x12B"}

      {:ok, _result, _ctx} =
        RequestPipeline.execute_via_channels(
          chain,
          "eth_getLogs",
          [filter],
          %RequestOptions{strategy: :load_balanced, timeout_ms: 10_000}
        )

      # Collect all chunk call messages (non-blocking, they should already be in mailbox)
      chunks_called =
        Enum.reduce_while(1..10, [], fn _, acc ->
          receive do
            {:chunk_called, from_block} -> {:cont, [from_block | acc]}
          after
            200 -> {:halt, acc}
          end
        end)

      IO.inspect(Enum.sort(chunks_called), label: "chunk fromBlocks dispatched")

      assert length(chunks_called) == 3
      assert "0x0" in chunks_called
      assert "0x64" in chunks_called
      assert "0xC8" in chunks_called
    end

    test "passes through unchanged when range is within chunk size", %{chain: chain} do
      test_pid = self()

      call_tracker =
        MockProviderBehavior.parameter_sensitive(fn method, params, _state ->
          if method == "eth_getLogs" do
            send(test_pid, {:direct_call, params})
          end

          {:ok, []}
        end)

      setup_providers([
        %{
          id: "provider",
          priority: 10,
          behavior: call_tracker,
          capabilities: %{limits: %{max_block_range: @test_chunk_size}}
        }
      ])

      # 50-block range → fits in one chunk, distributor should not fire
      filter = %{"fromBlock" => "0x0", "toBlock" => "0x31"}

      {:ok, result, _ctx} =
        RequestPipeline.execute_via_channels(
          chain,
          "eth_getLogs",
          [filter],
          %RequestOptions{strategy: :load_balanced, timeout_ms: 10_000}
        )

      assert %Response.Success{} = result

      assert_receive {:direct_call, _params}, 500
    end

    test "passes through when toBlock is a tag", %{chain: chain} do
      setup_providers([
        %{
          id: "provider",
          priority: 10,
          behavior: :healthy,
          capabilities: %{limits: %{max_block_range: @test_chunk_size}}
        }
      ])

      filter = %{"fromBlock" => "0x0", "toBlock" => "latest"}

      {:ok, result, _ctx} =
        RequestPipeline.execute_via_channels(
          chain,
          "eth_getLogs",
          [filter],
          %RequestOptions{strategy: :load_balanced, timeout_ms: 10_000}
        )

      IO.puts("\ntag passthrough result: #{inspect(result.__struct__)}")
      assert %Response.Success{} = result
    end
  end
end
