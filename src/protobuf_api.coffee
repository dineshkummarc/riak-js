Net    = require 'net'
Fs     = require 'fs'
Path   = require 'path'
Buffer = require('buffer').Buffer
Events = require('events')

exports.connect = (port, host) ->
  new Connection port, host

class Connection extends Events.EventEmitter
  constructor: (port, host) ->
    @port      = port || 8087
    @host      = host || '127.0.0.1'
    @queue     = []
    @callbacks = []
    @state     = 0

  # Queues up a Riak message to send to the server.  If no message is 
  # currently being processed, send it immediately.  Otherwise, it will be 
  # sent after previously queued requests have been sent.
  #
  # name - String name of the Riak message, without the 'Rpb' prefix.  
  #        (ex: "PingReq")
  # data - The Object to be serialized as a protocol buffer stream.
  # cb   - Optional Function callback that is called with (err, resp).
  #
  # Throws an exception if this Connection will be ending soon.
  # Returns nothing.
  enqueue: (name, data, cb) ->
    throw "Connecting is closing" if @ending
    @queue.push @prepare(name, data)
    @callbacks.push cb
    if @ready and @conn.writable
      @send()
    else
      @connect()

  # Gracefully ends the current connection.  The connection will wait for any
  # queued messages.
  end: (cb) ->
    @ending = cb
    if @ready
      @conn.end()

  connect: ->
    @conn = Net.createConnection @port, @host
    @conn.on 'connect', =>
      @emit 'connect'
      @ready = true
      @send()

    @conn.on 'data', (buffer) =>
      @data buffer

    @conn.on 'end', =>
      @ending() if @ending
      @emit 'end'

  prepare: (name, data) ->
    type = ProtoBuf[name]
    len  = 1
    if data
      msg  = type.serialize(data)
      len += msg.length
    head    = new Buffer(5)
    head[0] = len >>>  24
    head[1] = len >>>  16
    head[2] = len >>>   8
    head[3] = len &   255
    head[4] = type.riak_code
    [head, msg]

  # PRIVATE

  # Pops a message off the queue and writes it to the connection socket.
  #
  # Returns nothing.
  send: ->
    cmd = @queue.shift()
    if not cmd
      @conn.end() if @ending
      return
    @conn.write cmd[0]
    @conn.write cmd[1] if cmd[1]

  # Passes the incoming buffer to the parsing state machine.
  #
  # buffer - The received Buffer instance.
  #
  # Returns nothing.
  data: (buffer) ->
    @[Connection.states[@state]] buffer

  parse_header: (buffer) ->
    @resp_len = (buffer[0] << 24) + 
                (buffer[1] << 16) +
                (buffer[2] <<  8) +
                 buffer[3]  -  1
    @type     = ProtoBuf.type buffer[4]
    @buffers  = []
    @state    = 1
    @pos      = 0
    @callback = @callbacks.shift()
    if buffer.length > 5
      @data buffer.slice(5, buffer.length)

  parse_content: (buffer) ->
    buffer_len = buffer.length
    if buffer_len > 0
      if buffer_len + @pos > @resp_len
        end       = @resp_len - @pos
        remainder = buffer.slice end, buffer.length
        buffer    = buffer.slice 0,   end
      @buffers.push buffer if @callback
      @pos += buffer.length
    if @resp_len == @pos
      @state = 2
      @data remainder
    else
      @data remainder if remainder

  send_content: (remainder) ->
    @state = 0
    if @callback
      if @buffers.length == 1
        msg = @buffers[0]
      else
        pos = 0
        msg = new Buffer @resp_len
        @buffers.forEach (buf) ->
          buf.copy msg, pos, 0
          pos += buf.length
      resp = @type.parse msg if msg.length > 0
      @callback(null, resp)
      @callback = null
      @buffers  = []
    if remainder?.length > 0
      @data remainder
    else
      @send()

Connection.states = [
    "parse_header"
    "parse_content"
    "send_content"
  ]

ProtoBuf = 
  types: ["ErrorResp", "PingReq", "PingResp", "GetClientIdReq", 
  "GetClientIdResp", "SetClientIdReq", "SetClientIdResp", "GetServerInfoReq", 
  "GetServerInfoResp", "GetReq", "GetResp", "PutReq", "PutResp", "DelReq", 
  "DelResp", "ListBucketsReq", "ListBucketsResp", "ListKeysReq", 
  "ListKeysResp", "GetBucketReq", "GetBucketResp", "SetBucketReq", 
  "SetBucketResp", "MapRedReq", "MapRedResp"]

  # Find a ProtoBuf type given its riak code.
  type: (num) ->
    @[@types[num]]

  schemaFile: Path.join Path.dirname(module.filename), 'riak.desc'

# lazily load protobuf schema
ProtoBuf.__defineGetter__ 'schema', ->
  @_schema ||= new (require('protobuf_for_node').Schema)(Fs.readFileSync(ProtoBuf.schemaFile))

# lazily load protobuf types
ProtoBuf.types.forEach (name) ->
  cached_name = "_#{name}"

  ProtoBuf.__defineGetter__ name, ->
    if @[cached_name]
      @[cached_name]
    else
      code = ProtoBuf.types.indexOf(name)
      if sch = ProtoBuf.schema["Rpb#{name}"]
        sch.riak_code  = code
        @[cached_name] = sch
      else
        @[cached_name] = 
          riak_code: code
          parse: -> true

assert = require 'assert'
calls  = 0

c = new Connection
c.on 'connect', ->
  c.end()
c.on 'end', ->
  console.log 'END'

# {clientId: '\u0000\ufffd|\ufffd'}
full = new Buffer 11
full[0]  = 0
full[1]  = 0
full[2]  = 0
full[3]  = 7
full[4]  = 4
full[5]  = 10
full[6]  = 4
full[7]  = 0
full[8]  = 215
full[9]  = 124
full[10] = 236

header  = full.slice(0, 5)
content = full.slice(5, full.length)
first   = content.slice(0, 3)
last    = content.slice(3, content.length)
extra   = new Buffer last.length + full.length
last.copy extra, 0, 0
full.copy extra, last.length, 0

test_buffers = [
  [header, new Buffer(0), content]
  [header, first, new Buffer(0), last]
  [header, first, extra]
]

cb = (err, msg) ->
  calls += 1
  assert.equal '\u0000\ufffd|\ufffd', msg.clientId
  next = test_buffers.shift()
  if next
    c.callbacks.push cb
    next.forEach (buffer) -> c.data buffer
  else
    c.callbacks.push (err, msg) ->
      calls += 1
      assert.equal '\u0000\ufffd|\ufffd', msg.clientId

c.callbacks.push cb
c.data full

process.on 'exit', ->
  assert.equal 5, calls
  console.log '.'