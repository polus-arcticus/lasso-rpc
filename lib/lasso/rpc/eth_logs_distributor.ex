defmodule Lasso.RPC.EthLogsDistributor do
  @moduledoc """
  Distributes eth_getLogs requests across multiple providers in parallel.

  When a request spans more blocks than a provider's max_block_range, splits the
  range into chunks sized to the smallest known provider limit and dispatches
  all chunks simultaneously via the normal request pipeline. Results are merged
  and sorted by blockNumber then logIndex before returning.

  Only triggers when both fromBlock and toBlock are explicit hex block numbers.
  Requests using tags like "latest" or "pending" are passed through unchanged.
  """

  require Logger

  alias Lasso.Config.ConfigStore
  alias Lasso.JSONRPC.Error, as: JError
  alias Lasso.RPC.{RequestContext, RequestOptions, Response}

  @type range_pair :: {non_neg_integer(), non_neg_integer()}
  @type pipeline_result ::
          {:ok, Response.Success.t(), RequestContext.t()}
          | {:error, JError.t(), RequestContext.t()}

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Returns true when the request is eligible for parallel distribution.

  Eligibility requires:
  - Exactly one filter param
  - Both fromBlock and toBlock are hex block numbers (not tags)
  - The range exceeds `min_chunk_size/2`
  """
  @spec should_distribute?(list(), String.t(), RequestOptions.t()) :: boolean()
  def should_distribute?([%{"fromBlock" => from, "toBlock" => to}], chain, opts)
      when is_binary(from) and is_binary(to) do
    with {:ok, from_num} <- parse_block_number(from),
         {:ok, to_num} <- parse_block_number(to),
         true <- to_num >= from_num,
         range <- to_num - from_num + 1,
         true <- range > min_chunk_size(opts, chain) do
      true
    else
      _ -> false
    end
  end

  def should_distribute?(_params, _chain, _opts), do: false

  @doc """
  Executes an eth_getLogs request by splitting the range and dispatching chunks
  in parallel through the normal request pipeline.

  Returns the same `{:ok, result, ctx} | {:error, error, ctx}` shape as the
  pipeline so callers can treat it identically.

  ## Failure behaviour

  If any chunk returns an error, execution short-circuits and returns that error.
  Partial results are discarded.
  TODO: decide whether to return partial results or always fail-all on any chunk error.
  """
  @spec execute(String.t(), list(), RequestOptions.t(), RequestContext.t()) :: pipeline_result()
  def execute(chain, [filter], %RequestOptions{} = opts, parent_ctx) do
    {:ok, from_num} = parse_block_number(filter["fromBlock"])
    {:ok, to_num} = parse_block_number(filter["toBlock"])

    chunk_size = min_chunk_size(opts, chain)
    ranges = split_range(from_num, to_num, chunk_size)
    chunk_filters = build_chunk_filters(filter, ranges)

    Logger.debug("EthLogsDistributor: dispatching #{length(ranges)} chunks",
      chain: chain,
      from: from_num,
      to: to_num,
      chunk_size: chunk_size,
      request_id: parent_ctx.request_id
    )

    chunk_results = dispatch_chunks(chain, chunk_filters, opts)

    case collect_results(chunk_results) do
      {:ok, successes} ->
        case merge_results(successes) do
          {:ok, merged_logs} ->
            last_ctx = successes |> List.last() |> elem(2)
            result = build_merged_response(merged_logs, opts)
            {:ok, result, last_ctx}

          {:error, reason} ->
            jerr = JError.new(-32_000, "Failed to merge eth_getLogs chunks: #{inspect(reason)}")
            {:error, jerr, parent_ctx}
        end

      {:error, jerr, ctx} ->
        {:error, jerr, ctx}
    end
  end

  @doc """
  Splits a block range into a list of {from, to} pairs each at most chunk_size wide.

  ## Examples

      iex> EthLogsDistributor.split_range(0, 2999, 1000)
      [{0, 999}, {1000, 1999}, {2000, 2999}]

      iex> EthLogsDistributor.split_range(0, 500, 1000)
      [{0, 500}]

      iex> EthLogsDistributor.split_range(5, 5, 1000)
      [{5, 5}]
  """
  @spec split_range(non_neg_integer(), non_neg_integer(), pos_integer()) :: [range_pair()]
  def split_range(from, to, chunk_size)
      when is_integer(from) and is_integer(to) and to >= from and is_integer(chunk_size) and
             chunk_size > 0 do
    from
    |> Stream.iterate(&(&1 + chunk_size))
    |> Stream.take_while(&(&1 <= to))
    |> Enum.map(fn chunk_from ->
      {chunk_from, min(chunk_from + chunk_size - 1, to)}
    end)
  end

  @doc """
  Builds a narrowed filter for each chunk range.

  Copies all original filter fields and overrides fromBlock/toBlock with hex values.

  ## Examples

      iex> EthLogsDistributor.build_chunk_filters(%{"address" => "0xabc"}, [{0, 999}, {1000, 1999}])
      [
        %{"address" => "0xabc", "fromBlock" => "0x0", "toBlock" => "0x3E7"},
        %{"address" => "0xabc", "fromBlock" => "0x3E8", "toBlock" => "0x7CF"}
      ]
  """
  @spec build_chunk_filters(map(), [range_pair()]) :: [map()]
  def build_chunk_filters(filter, ranges) when is_map(filter) and is_list(ranges) do
    Enum.map(ranges, fn {from, to} ->
      filter
      |> Map.put("fromBlock", int_to_hex(from))
      |> Map.put("toBlock", int_to_hex(to))
    end)
  end

  @doc """
  Merges log arrays from multiple successful chunk responses.

  Decodes each response, concatenates the log arrays, and sorts by
  blockNumber then logIndex. Fails if any response is not a log array.
  """
  @spec merge_results([{:ok, Response.Success.t(), RequestContext.t()}]) ::
          {:ok, [map()]} | {:error, term()}
  def merge_results(successes) when is_list(successes) do
    Enum.reduce_while(successes, {:ok, []}, fn {:ok, response, _ctx}, {:ok, acc} ->
      case Response.Success.decode_result(response) do
        {:ok, logs} when is_list(logs) ->
          {:cont, {:ok, acc ++ logs}}

        {:ok, other} ->
          {:halt, {:error, {:unexpected_result_type, other}}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, logs} -> {:ok, sort_logs(logs)}
      error -> error
    end
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp dispatch_chunks(chain, chunk_filters, opts) do
    # Strip request_context so each chunk initialises its own
    chunk_opts = %{opts | request_context: nil}

    Task.async_stream(
      chunk_filters,
      fn chunk_filter ->
        Lasso.RPC.RequestPipeline.execute_via_channels(
          chain,
          "eth_getLogs",
          [chunk_filter],
          chunk_opts
        )
      end,
      timeout: opts.timeout_ms,
      on_timeout: :kill_task
    )
    |> Enum.to_list()
  end

  defp collect_results(task_results) do
    Enum.reduce_while(task_results, {:ok, []}, fn
      {:ok, {:ok, _, _} = success}, {:ok, acc} ->
        {:cont, {:ok, acc ++ [success]}}

      {:ok, {:error, jerr, ctx}}, _acc ->
        {:halt, {:error, jerr, ctx}}

      {:exit, reason}, _acc ->
        jerr = JError.new(-32_000, "Chunk task failed: #{inspect(reason)}")
        {:halt, {:error, jerr, %RequestContext{}}}
    end)
  end

  defp build_merged_response(logs, %RequestOptions{jsonrpc_id: id}) do
    raw =
      Jason.encode!(%{
        "jsonrpc" => "2.0",
        "id" => id,
        "result" => logs
      })

    %Response.Success{id: id, jsonrpc: "2.0", raw_bytes: raw}
  end

  defp sort_logs(logs) do
    Enum.sort_by(logs, fn log ->
      {parse_hex_integer(log["blockNumber"]), parse_hex_integer(log["logIndex"])}
    end)
  end

  @fallback_chunk_size 10_000

  defp min_chunk_size(%RequestOptions{profile: profile} = _opts, chain) do
    case ConfigStore.get_providers(profile, chain) do
      {:ok, providers} ->
        providers
        |> Enum.map(fn p -> get_in(p.capabilities, [:limits, :max_block_range]) end)
        |> Enum.reject(&is_nil/1)
        |> case do
          [] -> @fallback_chunk_size
          limits -> Enum.min(limits)
        end

      _ ->
        @fallback_chunk_size
    end
  end

  defp int_to_hex(n) when is_integer(n) and n >= 0 do
    "0x" <> Integer.to_string(n, 16)
  end

  defp parse_block_number("0x" <> hex) do
    case Integer.parse(hex, 16) do
      {n, ""} -> {:ok, n}
      _ -> :error
    end
  end

  defp parse_block_number(_), do: :error

  defp parse_hex_integer("0x" <> hex) do
    case Integer.parse(hex, 16) do
      {n, ""} -> n
      _ -> 0
    end
  end

  defp parse_hex_integer(_), do: 0
end
