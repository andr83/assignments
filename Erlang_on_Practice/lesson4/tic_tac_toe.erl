-module(tic_tac_toe).

-export([new_game/0, win/1, move/3, find_winner/1]).


new_game() ->
    {{f, f, f},
     {f, f, f},
     {f, f, f}}.


win(GameState) ->
    %% BEGIN
    {{A, B, C},
     {D, E, F},
     {G, H, I}} = GameState,
    FlatState = [{A, B, C},
                 {D, E, F},
                 {G, H, I},
                 {A, D, G},
                 {B, E, H},
                 {C, F, I},
                 {A, E, I},
                 {C, E, G}],
    find_winner(FlatState).
    %% END
      
find_winner(FlatState) -> 
   case lists:dropwhile(fun({A, B, C}) -> (A /= B) or (A /= C) or (A == f) or (B == f) or (C == f) end, FlatState) of
     [] -> no_win;
     [{A, B, C} | _] -> {win, A}
   end.      


move(Cell, Player, GameState) when Cell < 10 ->
    %% BEGIN
    {{A, B, C},
     {D, E, F},
     {G, H, I}} = GameState,
    FlatState = [A, B, C, 
                 D, E, F,
                 G, H, I],
    case lists:nth(Cell, FlatState) of
        f -> {ok, update_state(Cell, FlatState, Player)};
        _ -> {error,invalid_move}
    end;
    %% END
move(Cell, Player, GameState) -> {error,invalid_move}.

update_state(Cell, FlatState, Val) ->
    [A,B,C,D,E,F,G,H,I] = case Cell of
        0 -> [Val] ++ lists:nthtail(0, FlatState);
        _ -> lists:sublist(FlatState, Cell-1) ++ [Val] ++ lists:nthtail(Cell, FlatState)
    end,
    {{A, B, C},
     {D, E, F},
     {G, H, I}}.
