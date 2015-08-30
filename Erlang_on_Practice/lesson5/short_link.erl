-module(short_link).

-export([init/0, create_short/2, get_long/2, rand_str/1]).

%%% module API

init() ->
    %% init randomizer
    <<A:32, B:32, C:32>> = crypto:rand_bytes(12),
    random:seed({A,B,C}),
    %% BEGIN (write your solution here)
    dict:new().
    %% END



create_short(LongLink, State) ->
    %% BEGIN (write your solution here)
    case dict:find(LongLink, State) of
      {ok, ShortLink} -> {ShortLink, State};
      _ -> ShortLink = "http://hexlet.io/" ++ rand_str(8),
           NewState = dict:store(LongLink, ShortLink, State),
           {ShortLink, NewState}
    end.  
    %% END

get_long(ShortLink, State) ->
    %% BEGIN (write your solution here)
    F = fun({_, ShortLink2}) -> not string:equal(ShortLink2, ShortLink) end,
    case lists:dropwhile(F, dict:to_list(State)) of
      [] -> {error, not_found};
      [{LongLink, _}| _] -> {ok, LongLink}
    end.
    %% END



%% generates random string of chars [a-zA-Z0-9]
rand_str(Length) ->
    lists:map(fun(Char) when Char > 83 -> Char + 13;
                 (Char) when Char > 57 -> Char + 7;
                 (Char) -> Char
              end,
              [crypto:rand_uniform(48, 110) || _ <- lists:seq(1, Length)]).

