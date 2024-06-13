#!lua name=queue

--- Библиотека для работы с очередью

---
-- Перемешивает элементы в списке
--
-- @param list список
-- @return перемешанный список
--
local function shuffle_list(list)
    for i = #list, 2, -1 do
        local j = math.random(i)
        list[i], list[j] = list[j], list[i]
    end
end

---
-- Добавляет элемент в конец очереди
--
-- @param keys[1] ключ очереди
-- @param argv[1] значение, которое необходимо добавить
-- @return количество элементов в очереди
--
local function push(keys, argv)
    return redis.call('RPUSH', keys[1], argv[1])
end

---
-- Удаляет и возвращает первый элемент очереди
--
-- @param keys[1] ключ очереди
-- @return первый элемент очереди
--
local function pop(keys)
    return redis.call('LPOP', keys[1])
end

---
-- Возвращает все элементы очереди
--
-- @param keys[1] ключ очереди
-- @return все элементы очереди
--
local function get(keys)
	return redis.call('LRANGE', keys[1], 0, -1)
end

---
-- Удаляет все элементы очереди
--
-- @param keys[1] ключ очереди
-- @return количество удаленных элементов
--
local function clear(keys)
    return redis.call('DEL', keys[1])
end

---
-- Возвращает количество элементов в очереди
--
-- @param keys[1] ключ очереди
-- @return количество элементов в очереди
--
local function length(keys)
    -- Если такого list нет, то вернуть ошибку
    if redis.call('EXISTS', keys[1]) == 0 then
        return redis.error_reply('ERR no such key')
    end
    return redis.call('LLEN', keys[1])
end

---
-- Перемешивает элементы в очереди
--
-- @param keys[1] ключ очереди
--
local function shuffle(keys)
    local list = redis.call('LRANGE', keys[1], 0, -1)
    shuffle_list(list)

    redis.call('DEL', keys[1])
    for _, value in ipairs(list) do
        redis.call('RPUSH', keys[1], value)
    end
end

---
-- Перемещение элемента по индексам
--
-- @param keys[1] ключ очереди
-- @param argv[1] индекс перемещаемого элемента
-- @param argv[2] индекс, перед которым необходимо вставить элемент
--
local function move(keys, argv)
    local value = redis.call('LINDEX', keys[1], argv[1])
    if value then
        redis.call('LREM', keys[1], 0, value)
        redis.call('LINSERT', keys[1], 'BEFORE', redis.call('LINDEX', keys[1], argv[2]), value)
    end
end

---
-- Удаление элемента по индексу
--
-- @param keys[1] ключ очереди
-- @param argv[1] индекс удаляемого элемента
--
local function remove(keys, argv)
    local value = redis.call('LINDEX', keys[1], argv[1])
    if value then
        redis.call('LREM', keys[1], 0, value)
    end
end

---
-- Вставка элемента по индексу
--
-- @param keys[1] ключ очереди
-- @param argv[1] индекс, перед которым необходимо вставить элемент
-- @param argv[2] значение, которое необходимо вставить
--
local function insert(keys, argv)
    redis.call('LINSERT', keys[1], 'BEFORE', redis.call('LINDEX', keys[1], argv[1]), argv[2])
end

---
-- Возвращает элемент по индексу
--
-- @param keys[1] ключ очереди
-- @param argv[1] индекс элемента
-- @return элемент очереди
--
local function index(keys, argv)
    return redis.call('LINDEX', keys[1], argv[1])
end

---
-- Скип n первых элементов
--
-- @param keys[1] ключ очереди
-- @param argv[1] количество элементов, которые необходимо пропустить
--
local function skip(keys, argv)
    local n = argv[1] or 1
    for i = 1, n do
        redis.call('LPOP', keys[1])
    end
end

---
-- Добавление элемента в начало очереди
--
-- @param keys[1] ключ очереди
-- @param argv[1] значение, которое необходимо добавить
-- @return количество элементов в очереди
--
local function unshift(keys, argv)
    return redis.call('LPUSH', keys[1], argv[1])
end

---
-- Добавить новый элемент в очередь, учитывая максимальную длину
--
-- @param keys[1] ключ очереди
-- @param argv[1] значение, которое необходимо добавить
-- @param argv[2] максимальная длина очереди
-- @return количество элементов в очереди
--
local function push_with_max_length(keys, argv)
    -- Проверяем, что очередь меньше максимальной длины
    local length = redis.call('LLEN', keys[1])
    if length < argv[2] then
        return redis.call('RPUSH', keys[1], argv[1])
    end
    -- Вернуть ошибку, если очередь полна
    return redis.error_reply('ERR Queue is full')
end


local function push_with_check(keys, argv)
    local queue_id = keys[1]:match('queue:(%d+)')
    -- Вытягиваем json из session_data:queue_id
    local max_length = cjson.decode(redis.call('GET', 'session_data:' .. queue_id))['max_length']
    -- Проверяем, что очередь меньше максимальной длины
    local length = redis.call('LLEN', keys[1])
    if length < max_length then
        return redis.call('RPUSH', keys[1], argv[1])
    end
    -- Вернуть ошибку, если очередь полна
    return redis.error_reply('ERR Queue is full')
end

redis.register_function('push_with_max_length', push_with_max_length)
redis.register_function('push_with_check', push_with_check)
redis.register_function('push', push)
redis.register_function('pop', pop)
redis.register_function('get', get)
redis.register_function('clear', clear)
redis.register_function('length', length)
redis.register_function('shuffle', shuffle)
redis.register_function('move', move)
redis.register_function('remove', remove)
redis.register_function('insert', insert)
redis.register_function('index', index)
redis.register_function('skip', skip)
redis.register_function('unshift', unshift)

redis.register_function(
    'lib_version',
    function()
        return 'queue.lua 0.0.2'
    end
)
