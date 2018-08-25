# lento <sup id="a1">[1](#f1)</sup>

**Streaming Node.js client for [Presto](https://prestodb.io/), the "Distributed SQL Query Engine for Big Data".**

[![npm status](http://img.shields.io/npm/v/lento.svg?style=flat-square)](https://www.npmjs.org/package/lento) [![node](https://img.shields.io/node/v/lento.svg?style=flat-square)](https://www.npmjs.org/package/lento) [![Travis build status](https://img.shields.io/travis/vweevers/lento.svg?style=flat-square&label=travis)](http://travis-ci.org/vweevers/lento) [![AppVeyor build status](https://img.shields.io/appveyor/ci/vweevers/lento.svg?style=flat-square&label=appveyor)](https://ci.appveyor.com/project/vweevers/lento) [![Dependency status](https://img.shields.io/david/vweevers/lento.svg?style=flat-square)](https://david-dm.org/vweevers/lento)

## features

- Stream rows or pages of rows
- Cancelation through [`stream.destroy()`](https://nodejs.org/api/stream.html#stream_readable_destroy_error)
- Set session properties
- Set time limit (enforced by Presto)
- Get rows as objects or arrays
- Uses [Presto HTTP protocol v1](https://github.com/prestodb/presto/wiki/HTTP-Protocol)
- Keep-alive HTTP connections
- Supports Gzip and Deflate content encoding
- Retries [HTTP 503 and other failures](#builtin-retry).

## example

Convert a Presto table to CSV:

```js
const lento = require('lento')
const csvWriter = require('csv-write-stream')
const stdout = require('stdout-stream')
const pump = require('pump')

const client = lento({
  hostname: 'example',
  port: 8080,
  catalog: 'hive',
  schema: 'test'
})

const source = client.createRowStream('SELECT * FROM events')
const transform = csvWriter()

pump(source, transform, stdout, (err) => {
  if (err) throw err
})
```

If the destination streams close or error, the source stream is destroyed (courtesy of [`pump`](https://github.com/mafintosh/pump)) and the Presto query canceled.

## API

- [`lento([options])`](#lentooptions)
- [`createPageStream(sql[, options])`](#createpagestreamsql-options)
	- [cancelation](#cancelation)
	- [events](#events)
- [`createRowStream(sql[, options])`](#createrowstreamsql-options)
- [`query(sql[, options], callback)`](#querysql-options-callback)
- [`setTimeout(duration[, options], callback)`](#settimeoutduration-options-callback)
- [`resetTimeout([options, ]callback)`](#resettimeoutoptions-callback)
- [`set(key, value[, options], callback)`](#setkey-value-options-callback)
- [`reset(key[, options], callback)`](#resetkey-options-callback)
- [`session([options, ]callback)`](#sessionoptions-callback)
- [errors](#errors)

### `lento([options])`

Options:

- `hostname`: string, default is `localhost`
- `port`: number, default 8080
- `protocol`: string, one of `http:` (default) or `https:`
- `user`: string, default none. Sent as `X-Presto-User` header.
- `timezone`: string, for example `UTC`, default none. Sent as `X-Presto-Time-Zone` header.

You can specify a [catalog](https://prestodb.io/docs/current/overview/concepts.html#catalog) and [schema](https://prestodb.io/docs/current/overview/concepts.html#schema) to avoid writing fully-qualified table names in queries:

- `catalog`: string, for example `hive`, default none. Sent as `X-Presto-Catalog` header.
- `schema`: string, for example `logs`, default none. Sent as `X-Presto-Schema` header.

Control delays and retries:

- `pollInterval`: number (milliseconds) or string with unit (e.g. `1s`, `500ms`). How long to wait for server-side state changes, before sending another HTTP request. Default is 1 second.
- `maxRetries`: number of retries if Presto responds with [HTTP 503 or other failures](#builtin-retry). Default is 10.

<a name="createpagestream"></a>
### `createPageStream(sql[, options])`

Execute a query. Returns a [readable stream](https://nodejs.org/api/stream.html#stream_readable_streams) that yields pages of rows.

```js
const through2 = require('through2')

client
  .createPageStream('SELECT * FROM events')
  .pipe(through2.obj((page, enc, next) => {
    for (let row of page) {
      // ..
    }

    // Process next page
    next()
  }))
```

Options:

- `pageSize`: number, default 500
- `highWaterMark`: number, default 0
- `rowFormat`: string, one of `object` (default) or `array`.

The `pageSize` specifies the maximum number of rows per page. Presto may return less per page. If Presto returns more rows than `pageSize`, the surplus is buffered and the stream will not make another HTTP request to Presto until fully drained. Note that Presto retains finished queries for 15 minutes. If `pageSize` is <= 0 the stream emits pages as returned by Presto, without slicing them up.

The `highWaterMark` specifies how many pages to fetch preemptively. The maximum numbers of rows held in memory is approximately `(highWaterMark || 1) * pageSize`, plus any surplus from the last HTTP request. Because Presto can return thousands of rows per request, the default `highWaterMark` is 0 so that we *don't* preemptively fetch and only hold the number of rows contained in the last HTTP request.

If you care more about throughput, you can opt to increase `highWaterMark`. This can also help if you notice Presto going into `BLOCKING` state because you're not reading fast enough. Additionally you can increase `pageSize` if processing time is minimal or if you don't mind blocking the rest of your app while processing a page.

For tuning the Presto side of things, use [`set()`](#set).

#### cancelation

[Destroying the stream](https://nodejs.org/api/stream.html#stream_readable_destroy_error) will cancel the query with a `DELETE` request to Presto, unless no requests were made yet. If the initial request is in flight the stream will wait for a response, which contains the query id that can then be cancelled. If cancelation fails the stream will emit an `error` (open an issue if you think it shouldn't). Regardless of success, the streams emits `close` as the last event.

#### events

Besides the usual [Node.js stream events](https://nodejs.org/api/stream.html#stream_class_stream_readable), the stream emits:

- `id`: emitted with query id once known
- `info`: emitted with fully qualified info URL
- `columns`: emitted with an array of column metadata as returned by Presto
- `stats`: emitted for each HTTP response, with raw data.

**Note** The `id`, `info` and `columns` events may be emitted more than once due to retries. Subsequent `stats` events pertain to that retried query as well.

<a name="createrowstream"></a>
### `createRowStream(sql[, options])`

Execute a query. Returns a [readable stream](https://nodejs.org/api/stream.html#stream_readable_streams) that yields rows. Options:

- `highWaterMark`: number, default 16
- `rowFormat`: string, one of `object` (default) or `array`.

<a name="query"></a>
### `query(sql[, options], callback)`

Same as above but non-streaming, meant for simple queries. The `callback` function will receive an error if any and an array of rows.

```js
client.query('DESCRIBE events', (err, rows) => {
  // ..
})
```

I'll take a PR for Promise support.

<a name="set-timeout"></a>
### `setTimeout(duration[, options], callback)`

Set `query_max_run_time` for subsequent queries. If those take longer than `duration`, Presto will return an error with code `EXCEEDED_TIME_LIMIT` (see [errors](#errors)). The `duration` can be a number (in milliseconds) or a string parsed by Presto with the format `<value><unit>` - for example `5d` or `100ms`. Options are passed to [`query()`](#query) via [`set()`](#set), as this method is a shortcut for:

```js
client.set('query_max_run_time', duration[, options], callback)
```

<a name="reset-timeout"></a>
### `resetTimeout([options, ]callback)`

Reset `query_max_run_time` to Presto's default. Options are passed to [`query()`](#query) via [`reset()`](#reset).

<a name="set"></a>
### `set(key, value[, options], callback)`

Set a session property. Executes `SET SESSION ..` to prevalidate input, then sets `X-Presto-Session` header on subsequent queries. Value can be a boolean, number or string. Options are passed to [`query()`](#query).

```js
client.set('redistribute_writes', false, (err) => {
  if (err) return console.error('failed to set', err)

  // Subsequent queries now use redistribute_writes=false
})
```

<a name="reset"></a>
### `reset(key[, options], callback)`

Reset a session property to its default value. Options are passed to [`query()`](#query).

```js
client.reset('redistribute_writes', (err) => {
  if (err) return console.error('failed to reset', err)

  // Subsequent queries now use the default value of redistribute_writes
})
```

<a name="show-session"></a>
### `session([options, ]callback)`

Converts the result of `SHOW SESSION` into a tree, coerces boolean and integer values to JSON types. Options are passed to [`query()`](#query). Callback signature is `(err, session)`. Partial example of a `session`:

```json
{
  "execution_policy": {
    "key": "execution_policy",
    "value": "all-at-once",
    "default": "all-at-once",
    "type": "varchar",
    "description": "Policy used for scheduling query tasks"
  },
  "hash_partition_count": {
    "key": "hash_partition_count",
    "value": 100,
    "default": 100,
    "type": "integer",
    "description": "Number of partitions for distributed joins and aggregations"
  },
  "hive": {
    "bucket_execution_enabled": {
      "key": "hive.bucket_execution_enabled",
      "value": true,
      "default": true,
      "type": "boolean",
      "description": "Enable bucket-aware execution: only use a single worker per bucket"
    }
  }
}
```

See [Presto Properties](https://prestodb.io/docs/current/admin/properties.html) for a detailed description of (some of) the properties.

### errors

Errors are enriched with a `code` and `type` (string) and optionally `info`. For example:

```js
client.setTimeout('1ms', (err) => {
  if (err) return console.error(err.code, err)

  client.query('SELECT * FROM big_table', (err) => {
    console.error(err.message) // 'EXCEEDED_TIME_LIMIT: Query exceeded maximum time limit of 1.00ms'
    console.error(err.code) // 'EXCEEDED_TIME_LIMIT'
    console.error(err.type) // 'INSUFFICIENT_RESOURCES'
  })
})
```

### builtin retry

Per the specification of Presto HTTP Protocol v1, `lento` will sleep for 50-100ms and retry an HTTP request if Presto responds with 503. In addition, `lento` retries if the TCP connection is refused.

Lastly, a query (consisting of one or more HTTP requests) will be retried if Presto returns an error like `SERVER_STARTING_UP` or `HIVE_METASTORE_ERROR`, but only if no data was received yet, to avoid emitting duplicates.

I wish retries could be handled at a higher level, but as it stands, `lento` is both a low-level HTTP client and a streaming client, so retries have to be handled here. This may change in the future.

## install

With [npm](https://npmjs.org) do:

```
npm install lento
```

## license

[MIT](http://opensource.org/licenses/MIT) © Vincent Weevers

---

<sup><b id="f1">1</b></sup> Because streams are about [slowing down](https://www.youtube.com/watch?v=Yn-U6ygClyU)! [↩](#a1)
