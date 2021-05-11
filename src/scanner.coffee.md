
# Scanner operations

Scanner are the most efficient way to retrieve multiple 
rows and columns from HBase. Internally, it implements the native 
[Node.js Stream Readable API]().

## Dependencies

    util = require 'util'
    utils = require './utils'
    Table = require './table'
    {Readable} = require 'stream'

## Constructor

    Scanner = (client, @options={}) ->
      @options.objectMode = true
      Readable.call @, @options
      @client = client
      @fetching = false
      @initializing = false
      # @table = if typeof table is 'string' then table else table.name
      # @id = id or null
      @options = table: @options if typeof @options is 'string'
      @options.batch ?= 1000
      @options.types ?= {}
      throw Error 'Missing required option "table"' unless @options.table
      @options.id = null
      @callback = null

    util.inherits Scanner, Readable

## `scanner.init(callback)`

Internal method to create a new scanner and retrieve its ID.

    Scanner::init = (callback) ->
      # options = utils.merge {}, @options
      params = {}
      params.batch ?= @options.batch
      key = "/#{@options.table}/scanner"
      encoding = if @options.encoding is 'undefined' then @options.encoding else @client.options.encoding
      params.startRow = utils.base64.encode(@options.startRow, encoding) if @options.startRow
      params.endRow = utils.base64.encode(@options.endRow, encoding) if @options.endRow
      params.startTime = @options.startTime if @options.startTime
      params.endTime = @options.endTime if @options.endTime
      params.maxVersions = @options.maxVersions if @options.maxVersions
      if @options.column
        params.column = []
        @options.column = [@options.column] if typeof @options.column is 'string'
        @options.column.forEach (column, i) ->
          params.column[i] = utils.base64.encode column, encoding
      if @options.filter
        encode = (obj) ->
          for k of obj
            if k is 'value' and (not obj['type'] or obj['type'] isnt 'RegexStringComparator' and obj['type'] isnt 'PageFilter')
              obj[k] = utils.base64.encode obj[k], encoding
            else encode obj[k]  if typeof obj[k] is 'object'
        encode @options.filter
        params.filter = JSON.stringify(@options.filter)
      @client.connection.put key, params, (err, data, response) =>
        return callback err if err
        id = /scanner\/(\w+)$/.exec(response.headers.location)[1]
        @options.id = id
        callback null, id

## `scanner.get(callback)`

Internal method to retrieve a batch of records.

    Scanner::get = (callback) ->
      key = "/#{@options.table}/scanner/#{@options.id}"
      @client.connection.get key, (err, data, response) =>
        # result is successful but the scanner is exhausted, returns HTTP 204 status (no content)
        return callback() if response and response.statusCode is 204
        return callback err if err
        cells = []
        data.Row.forEach (row) =>
          key = utils.base64.decode row.key, @client.options.encoding
          row.Cell.forEach (cell) =>
            data = {}
            data.key = key
            data.column = utils.base64.decode cell.column, @client.options.encoding
            data.timestamp = cell.timestamp
            type = @options.types[data.column]
            switch type
                when "long" then data.$ = parseInt(utils.base64.decode(cell.$, "hex"), 16);
                else data.$ = utils.base64.decode cell.$, @client.options.encoding
            cells.push data
        callback null, cells

## `scanner.delete(callback)`

Internal method to unregister the scanner from the HBase server.

    Scanner::delete = (callback) ->
      @client.connection.delete "/#{@options.table}/scanner/#{@options.id}", callback

## `scanner._read(size)`

Implementation of the `stream.Readable` API.

    Scanner::_read = (size) ->
      return if @done
      return if @initializing
      return if @fetching
      unless @options.id
        @initializing = true
        return @init (err, id) =>
          @initializing = false
          return @emit 'error', err if err
          @_read()
      @fetching = true
      handler = (err, cells) =>
        return if @done
        return @emit 'error', err if err
        unless cells
          @done = true
          return @delete (err) =>
            return @emit 'error', err if err
            @push null
            @fetching = false
        more = true
        for cell in cells
          more = false unless @push cell
        if more
        then @get handler 
        else @fetching = false
      @get handler

    module.exports = Scanner
