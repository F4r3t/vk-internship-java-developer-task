# vk-internship-java-developer-task

gRPC-сервис key-value хранилища на **Java 25**, **Maven** и **Tarantool 3.2**.

## Обзор

Проект реализует gRPC KV-сервис со следующим API:

- `put(key, value)` — создать новую запись или перезаписать существующую
- `get(key)` — получить значение по ключу
- `delete(key)` — удалить ключ
- `range(key_since, key_to)` — потоково вернуть пары ключ-значение из заданного диапазона
- `count()` — вернуть количество записей в хранилище

В качестве хранилища данных используется **Tarantool 3.2**.

## Технологический стек

- Java 25
- Maven
- gRPC Java
- Protocol Buffers
- Tarantool 3.2
- Tarantool Java SDK 1.5.0
- Docker Compose
- JUnit 5
- Mockito

## Структура проекта

```text
.
├─ .github/workflows/
├─ docker-compose.yml
├─ pom.xml
├─ README.md
├─ tarantool/
│  └─ init.lua
└─ src/
   ├─ main/
   │  ├─ java/ru/F4r3t/kv/
   │  │  ├─ Application.java
   │  │  ├─ config/
   │  │  ├─ grpc/
   │  │  ├─ model/
   │  │  ├─ repository/
   │  │  └─ service/
   │  ├─ proto/
   │  │  └─ kvstore.proto
   │  └─ resources/
   └─ test/
      └─ java/ru/F4r3t/kv/
```

## API

### put(key, value)
Создаёт новую запись или перезаписывает существующую.

### get(key)
Возвращает значение для указанного ключа.
Если ключ отсутствует, сервис возвращает `NOT_FOUND`.

### delete(key)
Удаляет ключ и возвращает признак того, была ли удалена запись.

### range(key_since, key_to)
Возвращает все записи во **включительном лексикографическом диапазоне** `[key_since, key_to]`.

Ответ передаётся как **server-streaming gRPC**.

### count()
Возвращает количество записей в space `KV`.

## Поддержка `null`

Поле `value` поддерживает значение `null`.

Это реализовано через:

Схему Tarantool:

``` lua
{ name = "value", type = "varbinary", is_nullable = true }
```

И protobuf-контракт:

``` proto
optional bytes value = 2;
```

Это позволяет различать:

- `null`
- пустой массив байтов
- непустой массив байтов

## Схема данных Tarantool

Space `KV` использует следующий формат:

``` lua
{
  { name = "key", type = "string" },
  { name = "value", type = "varbinary", is_nullable = true }
}
```

Primary index создаётся по полю `key`.

## Локальный запуск

### 1. Поднять Tarantool

```bash
docker compose up -d
```

### 2. Собрать проект

```bash
mvn clean verify
```

### 3. Запустить приложение

```bash
mvn exec:java
```

Либо можно запустить `Application.java` напрямую из IntelliJ IDEA.

## Переменные окружения

Приложение читает конфигурацию из application.properties:

- `TT_HOST` — хост Tarantool, по умолчанию `localhost`
- `TT_PORT` — порт Tarantool, по умолчанию `3301`
- `TT_USER` — пользователь Tarantool, по умолчанию `guest`
- `TT_PASSWORD` — пароль Tarantool, по умолчанию пустой
- `GRPC_PORT` — порт gRPC-сервера, по умолчанию `9090`
- `RANGE_PAGE_SIZE` — размер страницы для потоковой выдачи диапазона, по умолчанию `1000`

## Поведение `range`

Метод `range(key_since, key_to)` реализован через постраничное чтение из Tarantool и потоковую отправку результата клиенту.

Это позволяет не загружать весь диапазон целиком в память.

## Тесты

Рекомендуемое покрытие:

- unit-тесты для service-слоя

Ключевые сценарии:

- put/get с обычным значением
- put/get с `null`
- перезапись существующего ключа
- удаление существующего ключа
- удаление отсутствующего ключа
- count после серии изменений
- range на пустом и непустом диапазоне