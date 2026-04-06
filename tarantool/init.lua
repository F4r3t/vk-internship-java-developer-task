box.cfg {
    listen = 3301
}

box.once("bootstrap_kv_space", function()
    local s = box.schema.space.create("KV", {
        if_not_exists = true,
        engine = "memtx"
    })

    s:format({
        { name = "key", type = "string" },
        { name = "value", type = "varbinary", is_nullable = true }
    })

    s:create_index("primary", {
        type = "TREE",
        unique = true,
        if_not_exists = true,
        parts = {
            { field = 1, type = "string" }
        }
    })

    box.schema.user.grant("guest", "read,write,execute", "universe", nil, { if_not_exists = true })
end)

function kv_put(key, value)
    local existed = box.space.KV:get({ key }) ~= nil
    box.space.KV:put({ key, value })
    return {
        overwritten = existed
    }
end

function kv_get(key)
    local tuple = box.space.KV:get({ key })
    if tuple == nil then
        return {
            found = false
        }
    end

    return {
        found = true,
        key = tuple[1],
        value = tuple[2]
    }
end

function kv_delete(key)
    local deleted = box.space.KV:delete({ key }) ~= nil
    return {
        deleted = deleted
    }
end

function kv_count()
    return {
        count = box.space.KV:len()
    }
end

function kv_range_page(key_since, key_to, after_key, limit)
    local items = {}
    local last_key
    local skip_until_after = after_key ~= nil and after_key ~= ""

    for _, tuple in box.space.KV.index.primary:pairs({ key_since }, { iterator = "GE" }) do
        local current_key = tuple[1]

        if current_key > key_to then
            break
        end

        if skip_until_after then
            if current_key <= after_key then
                goto continue
            end
            skip_until_after = false
        end

        table.insert(items, {
            key = tuple[1],
            value = tuple[2]
        })

        last_key = tuple[1]

        if #items >= limit then
            break
        end

        ::continue::
    end

    local next_after = nil

    if last_key ~= nil then
        for _, tuple in box.space.KV.index.primary:pairs({ last_key }, { iterator = "GT" }) do
            if tuple[1] <= key_to then
                next_after = last_key
            end
            break
        end
    end

    return {
        items = items,
        next_after = next_after
    }
end