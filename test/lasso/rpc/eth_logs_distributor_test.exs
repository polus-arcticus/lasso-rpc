defmodule Lasso.RPC.EthLogsDistributorTest do
  use ExUnit.Case, async: true

  alias Lasso.RPC.{EthLogsDistributor, RequestOptions, Response}

  # ---------------------------------------------------------------------------
  # should_distribute?/3
  # ---------------------------------------------------------------------------

  describe "should_distribute?/3" do
    test "true when range exceeds fallback chunk size" do
      params = [%{"fromBlock" => "0x0", "toBlock" => "0x27100"}]
      result = EthLogsDistributor.should_distribute?(params, "ethereum", opts())
      IO.puts("\nshould_distribute? large hex range → #{result}")
      assert result
    end

    test "false when range is within chunk size" do
      params = [%{"fromBlock" => "0x0", "toBlock" => "0x64"}]
      result = EthLogsDistributor.should_distribute?(params, "ethereum", opts())
      IO.puts("\nshould_distribute? small range (0x64 = 100 blocks) → #{result}")
      refute result
    end

    test "false when fromBlock is a tag" do
      params = [%{"fromBlock" => "latest", "toBlock" => "0x27100"}]
      result = EthLogsDistributor.should_distribute?(params, "ethereum", opts())
      IO.puts("\nshould_distribute? fromBlock=latest → #{result}")
      refute result
    end

    test "false when toBlock is a tag" do
      params = [%{"fromBlock" => "0x0", "toBlock" => "latest"}]
      result = EthLogsDistributor.should_distribute?(params, "ethereum", opts())
      IO.puts("\nshould_distribute? toBlock=latest → #{result}")
      refute result
    end

    test "false when both blocks are tags" do
      params = [%{"fromBlock" => "latest", "toBlock" => "latest"}]
      result = EthLogsDistributor.should_distribute?(params, "ethereum", opts())
      IO.puts("\nshould_distribute? both tags → #{result}")
      refute result
    end

    test "false when params is empty" do
      result = EthLogsDistributor.should_distribute?([], "ethereum", opts())
      IO.puts("\nshould_distribute? empty params → #{result}")
      refute result
    end

    test "false when filter is not a map" do
      result = EthLogsDistributor.should_distribute?(["invalid"], "ethereum", opts())
      IO.puts("\nshould_distribute? non-map filter → #{result}")
      refute result
    end
  end

  # ---------------------------------------------------------------------------
  # split_range/3
  # ---------------------------------------------------------------------------

  describe "split_range/3" do
    test "splits evenly" do
      result = EthLogsDistributor.split_range(0, 2999, 1000)
      IO.inspect(result, label: "\nsplit_range(0, 2999, 1000)")
      assert result == [{0, 999}, {1000, 1999}, {2000, 2999}]
    end

    test "last chunk is smaller when range does not divide evenly" do
      result = EthLogsDistributor.split_range(0, 2500, 1000)
      IO.inspect(result, label: "\nsplit_range(0, 2500, 1000)")
      assert result == [{0, 999}, {1000, 1999}, {2000, 2500}]
    end

    test "single chunk when range fits in one chunk" do
      result = EthLogsDistributor.split_range(0, 500, 1000)
      IO.inspect(result, label: "\nsplit_range(0, 500, 1000)")
      assert result == [{0, 500}]
    end

    test "single block range" do
      result = EthLogsDistributor.split_range(5, 5, 1000)
      IO.inspect(result, label: "\nsplit_range(5, 5, 1000)")
      assert result == [{5, 5}]
    end

    test "non-zero starting block" do
      result = EthLogsDistributor.split_range(1000, 2999, 1000)
      IO.inspect(result, label: "\nsplit_range(1000, 2999, 1000)")
      assert result == [{1000, 1999}, {2000, 2999}]
    end

    test "chunk size of 1" do
      result = EthLogsDistributor.split_range(0, 2, 1)
      IO.inspect(result, label: "\nsplit_range(0, 2, 1)")
      assert result == [{0, 0}, {1, 1}, {2, 2}]
    end
  end

  # ---------------------------------------------------------------------------
  # build_chunk_filters/2
  # ---------------------------------------------------------------------------

  describe "build_chunk_filters/2" do
    test "overrides fromBlock and toBlock for each range" do
      filter = %{"address" => "0xabc", "fromBlock" => "0x0", "toBlock" => "0x2710"}
      ranges = [{0, 999}, {1000, 1999}]

      [f1, f2] = EthLogsDistributor.build_chunk_filters(filter, ranges)
      IO.inspect(f1, label: "\nbuild_chunk_filters chunk 1")
      IO.inspect(f2, label: "build_chunk_filters chunk 2")

      assert f1["fromBlock"] == "0x0"
      assert f1["toBlock"] == "0x3E7"
      assert f2["fromBlock"] == "0x3E8"
      assert f2["toBlock"] == "0x7CF"
    end

    test "preserves all other filter fields" do
      topics = ["0xddf252"]
      filter = %{"address" => "0xabc", "topics" => topics}

      [f1] = EthLogsDistributor.build_chunk_filters(filter, [{0, 99}])
      IO.inspect(f1, label: "\nbuild_chunk_filters preserves fields")

      assert f1["address"] == "0xabc"
      assert f1["topics"] == topics
    end

    test "returns empty list for empty ranges" do
      result = EthLogsDistributor.build_chunk_filters(%{}, [])
      IO.inspect(result, label: "\nbuild_chunk_filters empty ranges")
      assert result == []
    end
  end

  # ---------------------------------------------------------------------------
  # merge_results/1
  # ---------------------------------------------------------------------------

  describe "merge_results/1" do
    test "merges and sorts logs by blockNumber then logIndex" do
      chunk_a = success_response([log("0x2", "0x1"), log("0x1", "0x0")])
      chunk_b = success_response([log("0x3", "0x0"), log("0x2", "0x0")])

      {:ok, logs} = EthLogsDistributor.merge_results([chunk_a, chunk_b])
      sorted = Enum.map(logs, &{&1["blockNumber"], &1["logIndex"]})
      IO.inspect(sorted, label: "\nmerge_results sorted order")

      assert sorted == [
               {"0x1", "0x0"},
               {"0x2", "0x0"},
               {"0x2", "0x1"},
               {"0x3", "0x0"}
             ]
    end

    test "handles empty log arrays" do
      chunk = success_response([])
      result = EthLogsDistributor.merge_results([chunk])
      IO.inspect(result, label: "\nmerge_results empty logs")
      assert {:ok, []} = result
    end

    test "returns error when a response cannot be decoded" do
      bad = {:ok, %Response.Success{id: 1, jsonrpc: "2.0", raw_bytes: "not json"}, %{}}
      result = EthLogsDistributor.merge_results([bad])
      IO.inspect(result, label: "\nmerge_results bad JSON")
      assert {:error, _} = result
    end

    test "returns error when result is not a list" do
      not_a_list = success_response("0x1")
      result = EthLogsDistributor.merge_results([not_a_list])
      IO.inspect(result, label: "\nmerge_results non-list result")
      assert {:error, _} = result
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp opts do
    %RequestOptions{timeout_ms: 30_000, profile: "default", strategy: :load_balanced}
  end

  defp log(block_number, log_index) do
    %{"blockNumber" => block_number, "logIndex" => log_index, "data" => "0x"}
  end

  defp success_response(result) do
    raw = Jason.encode!(%{"jsonrpc" => "2.0", "id" => 1, "result" => result})
    {:ok, %Response.Success{id: 1, jsonrpc: "2.0", raw_bytes: raw}, %{}}
  end
end
