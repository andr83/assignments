-module(great_ideas_catalogue).

-include_lib("stdlib/include/ms_transform.hrl").

-export([init/0,
         add_idea/5, get_idea/1,
         ideas_by_author/1, ideas_by_rating/1,
         get_authors/0]).

-record(idea, {id, title, author, rating, description}).


init() ->
    ets:new(great_ideas_table, [set, named_table, {keypos, 2}]),
    ets:insert(great_ideas_table,
               [#idea{id = 1, title = "Мороженое с огурцами", author = "Боб Бобов", rating = 100,
                      description = "Крошим огурцы кубиками и добавляем в мороженое"},
                #idea{id = 2, title = "Добыча воды на Марсе", author = "Билл Билов", rating = 500,
                      description = "Бурим скважины на Марсе, доставляем воду на Землю ракетами"},
                #idea{id = 3, title = "Извлечение энергии квазаров", author = "П. И. Шурупов", rating = 100500,
                      description = "Секретно"},
                #idea{id = 4, title = "Куртка с тремя рукавами", author = "Боб Бобов", rating = 15,
                      description = "Рукава из разных материалов, расчитаны на разную погоду."},
                #idea{id = 5, title = "Кроссовки-степлеры", author = "Олечка", rating = 78,
                      description = "Полезная вещь для офиса и фитнеса"},
                #idea{id = 6, title = "Способ ловли кузнечиков", author = "Алекс Аквамаринов", rating = 777,
                      description = "Сачком их, сачком."},
                #idea{id = 7, title = "Вулканический зонт", author = "Боб Бобов", rating = 12,
                      description = "Защищает самолеты от вулканической пыли."},
                #idea{id = 8, title = "Телефон-шар", author = "Див Стобс", rating = 8383,
                      description = "Удобно лежит в руке, имеет устройство ввода на основе гироскопа"},
                #idea{id = 9, title = "Автоматическая кормушка для котов", author = "П. И. Шурупов", rating = 9000,
                      description = "Нужно использовать энергию квазаров для этой цели"},
                #idea{id = 10, title = "Самодвижущаяся лестница", author = "Васисуалий Л.", rating = 42,
                      description = "Имеет большой потенциал применения в небоскребах."}]),
    ok.


add_idea(Id, Title, Author, Rating, Description) ->
    %% BEGIN (write your solution here)
    ets:insert(great_ideas_table, #idea{id = Id, title = Title, author = Author, rating = Rating, description = Description}),
    ok.
    %% END



get_idea(Id) ->
    %% BEGIN (write your solution here)
    case ets:lookup(great_ideas_table, Id) of
      [] -> not_found;
      [Idea] -> {ok, Idea}
    end.           
    %% END



ideas_by_author(Author) ->
    %% BEGIN (write your solution here)
    ets:match_object(great_ideas_table, {idea, '_', '_', Author, '_', '_'}).
    %% END



ideas_by_rating(Rating) ->
    %% BEGIN (write your solution here)
    MS = ets:fun2ms(fun(Idea = {idea, Id, Title, Author, IdeaRating, Description})
                      when IdeaRating >= Rating -> Idea
                    end),
    ets:select(great_ideas_table, MS).
    %% END



get_authors() ->
    %% BEGIN (write your solution here)
    F = fun({idea, Id, Title, Author, Rating, Description}, Acc) ->
          Count = case maps:find(Author, Acc) of
                    {ok, Cnt} -> Cnt;
                    error -> 0
                  end,
          maps:put(Author, Count + 1, Acc)
        end,
    F2 = fun({A1, C1}, {A2, C2}) -> {C1, A2} > {C2, A1} end,
    M = ets:foldr(F, maps:new(), great_ideas_table),
    lists:sort(F2, maps:to_list(M)).
    %% END


