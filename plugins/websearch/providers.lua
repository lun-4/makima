-- Every backend here is an MCP server that answers tools/call with a text
-- content block, so one SSE parser serves them all. What changes is the
-- endpoint, the tool name, the argument names, and how a key rides along.

return {
  exa = {
    label = "Exa AI",
    endpoint = "https://mcp.exa.ai/mcp",
    env = "EXA_API_KEY",
    auth_header = "x-api-key",
    tool = "web_search_exa",
    arguments = function(query, num_results)
      return {
        query = query,
        numResults = num_results,
        type = "auto",
        livecrawl = "fallback",
      }
    end,
  },
  youcom = {
    label = "You.com",
    endpoint = "https://api.you.com/mcp",
    -- Without a key the plain endpoint answers 401, the free profile serves.
    keyless_suffix = "?profile=free",
    env = "YDC_API_KEY",
    auth_header = "Authorization",
    auth_prefix = "Bearer ",
    tool = "you-search",
    arguments = function(query, num_results)
      return {
        query = query,
        count = num_results,
      }
    end,
  },
}
