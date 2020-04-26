const level = require('level')
const charwise = require('charwise')
const d64 = require('d64')

const MSG_IDX = 0
const MSG = 1
const LEAVES = 2
const ENTITY = 3

function iterator (ldb, opts) {
  const it = ldb.iterator(opts)

  return {
    [Symbol.asyncIterator] () {
      return this
    },
    next: () => {
      return new Promise(resolve => {
        it.next((err, key, value) => {
          const hasKey = typeof key !== 'undefined'
          const hasValue = typeof value !== 'undefined'

          if (!hasKey && !hasValue) {
            return it.end(() => resolve({ done: true }))
          }

          const data = { key, value }
          resolve({ value: { err, data } })
        })
      })
    }
  }
}

class Storage {
  /**
   * In-memory persistence.
   *
   * @class
   */
  constructor () {
    this.channelData = new Map()
    this.entities = new Map()
  }

  async open (location = './test/db') {
    if (this.db) return this.db

    const opts = {
      encode: JSON.stringify,
      keyEncode: charwise.encode,
      valueEncoding: 'json',
      decode: JSON.parse
    }

    this.db = await level(location, opts)
  }

  async close () {
    if (!this.db) return
    await this.db.close()
  }

  /**
   * Add new message to the persistent storage. Automatically recompute leafs.
   *
   * @param {Message} message - message to be added
   * @returns {Promise}
   */
  async addMessage (message) {
    const channelId = d64.encode(message.channelId)
    const height = message.height
    const hash = d64.encode(message.hash)

    // The key will be scanable and contain data (fast ranges)
    const key = [MSG, channelId, height, hash]

    // An index to directly look up a message
    const heightByHashIndex = [MSG_IDX, channelId, hash]

    const value = {
      channelId,
      hash,
      height,
      parents: message.parents.map(hash => d64.encode(hash)),
      data: d64.encode(message.data)
    }

    const batch = [
      { type: 'put', key, value },
      { type: 'put', key: heightByHashIndex, value: height },
      { type: 'put', key: [LEAVES, channelId, hash], value: hash }
    ]

    for (const parentHash of message.parents) {
      const key = [LEAVES, channelId, d64.encode(parentHash)]
      batch.push({ type: 'del', key })
    }

    try {
      await this.db.batch(batch)
    } catch (err) {
      return { err }
    }
  }

  /**
   * Get message count in linearized DAG.
   *
   * @param {Buffer} channelId - id of the Channel instance
   * @returns {Promise}
   */
  async getMessageCount (channelId) {
    const prefix = [MSG, d64.encode(channelId)]
    const params = {
      gte: [...prefix],
      lte: [...prefix, '~']
    }

    const itr = iterator(this.db, params)

    let count = 0

    for await (const { err } of itr) {
      if (err) {
        return { err }
      }

      ++count
    }

    return count
  }

  /**
   * Get current leaves for the channel.
   *
   * @param {Buffer} channelId - id of the Channel instance
   * @returns {Promise} array of resulting hashes
   */
  async getLeafHashes (channelId) {
    const prefix = [LEAVES, d64.encode(channelId)]

    const itr = iterator(this.db, {
      gte: [...prefix],
      lte: [...prefix, '~']
    })

    const result = []

    for await (const { err, data } of itr) {
      if (err) return { err }

      const key = data.key.split(',')
      result.push(d64.decode(key.pop()))
    }

    return result
  }

  /**
   * Check if the message with `hash` is present in specified channel.
   *
   * @param {Buffer} channelId - id of the Channel instance
   * @returns {Promise} boolean value: `true` - present, `false` - not present
   */
  async hasMessage (channelId, hash) {
    const key = [
      MSG_IDX,
      d64.encode(channelId),
      d64.encode(hash)
    ]

    try {
      await this.db.get(key)
    } catch (err) {
      return false
    }

    return true
  }

  /**
   * Find and return the message with `hash` in specified channel.
   *
   * @param {Buffer} channelId - id of the Channel instance
   * @param {Buffer} hash - Message hash
   * @returns {Promise} `Message` instance or `undefined`
   */
  async getMessage (channelId, hash) {
    const index = [MSG_IDX, d64.encode(channelId), d64.encode(hash)]

    let height = 0

    try {
      height = await this.db.get(index)
    } catch (err) {
      return { err }
    }

    const key = [MSG, d64.encode(channelId), height, d64.encode(hash)]
    let value = null

    try {
      value = await this.db.get(key)
    } catch (err) {
      return { err }
    }

    return d64.decode(value.data)
  }

  /**
   * Find and return several messages with hash in `hashes` in specified
   * channel.
   *
   * @param {Buffer} channelId - id of the Channel instance
   * @param {Buffer[]} hashes - Message hashes
   * @returns {Promise} A list of `Message` instances or `undefined`s
   *     (`(Message | undefined)[]`)
   */
  async getMessages (channelId, hashes) {
    return Promise.all(hashes.map(async (hash) => {
      return this.getMessage(channelId, hash)
    }))
  }

  /**
   * Get hashes starting from specific integer offset using CRDT order.
   *
   * @param {Buffer} channelId - id of the Channel instance
   * @param {number} offset - Message offset. MUST be greater or equal to zero
   *    and less than `getMessageCount()` result
   * @param {limit} offset - Maximum number of messages to return
   * @returns {Promise} Array of `Message` instances
   */
  async getHashesAtOffset (channelId, offset, limit, reverse) {
    const prefix = [MSG, d64.encode(channelId)]

    const params = {
      values: false,
      reverse
    }

    params.gte = [...prefix]
    params.lte = [...prefix, '~']

    if (!offset) {
      params.limit = limit
    }

    const itr = iterator(this.db, params)

    const results = []
    let skipped = 0

    for await (const { err, data } of itr) {
      if (err) return { err }

      if (offset && (skipped < offset)) {
        skipped++
        continue
      }

      const key = data.key.split(',')
      results.push(d64.decode(key.pop()))
    }

    return results
  }

  async getReverseHashesAtOffset (channelId, offset, limit) {
    return this.getHashesAtOffset(channelId, offset, limit, true)
  }

  async query (channelId, cursor, isBackward, limit) {
    const cid = d64.encode(channelId)
    let start = []
    const defaults = {
      abbreviatedMessages: [],
      forwardHash: null,
      backwardHash: null
    }

    //
    // We will either know cursor.height or cursor.hash.
    //
    if (cursor.height) {
      start = [MSG, cid, cursor.height]
    }

    //
    // When query by hash, we need to find the height
    // of the hash that was provided by the cursor.
    //
    if (cursor.hash) {
      const index = [
        MSG_IDX,
        d64.encode(channelId),
        d64.encode(cursor.hash)
      ]

      let height = 0

      try {
        height = await this.db.get(index)
      } catch (err) {
        if (err.notFound) return defaults
        return { err }
      }

      start = [MSG, cid, height, d64.encode(cursor.hash)]
    }

    const params = {
      limit,
      reverse: isBackward
    }

    if (isBackward) {
      params.lte = start
      // params.gte = [MSG, cid]
    } else {
      params.gte = start
      params.lte = [MSG, cid, '~']
    }

    //
    // if there is no hash, we will continue by height (fast),
    // otherwise this will essentially be a scan for the hash.
    //
    const itr = iterator(this.db, params)

    const results = []
    let count = 0
    let lastKey = null
    let head = null

    for await (const { err, data } of itr) {
      if (err) return { err }

      lastKey = data.key
      count++

      if (count === 1) {
        head = data.value

        // skip self, according to the protocol, use GT instead?
        if (isBackward) continue
      }

      results.push(data.value)
    }

    if (count === 0) {
      return { abbreviatedMessages: [], forwardHash: null, backwardHash: null }
    }

    const abbreviatedMessages = results.map(({ hash, parents }) => {
      hash = d64.decode(hash)
      // parents = this.decodeHashList(parents)
      return { hash, parents }
    })

    const result = {
      abbreviatedMessages
    }

    //
    // A second iterator gets the next key
    //
    const params2 = {
      reverse: isBackward,
      limit: 1
    }

    if (isBackward) {
      params2.lt = lastKey
      params2.gt = [MSG, cid]
    } else {
      params2.gt = lastKey
      params2.lt = [MSG, cid, '~']
    }

    const itr2 = iterator(this.db, params2)

    let next = null

    for await (const { err, data } of itr2) {
      if (err) return { err }
      next = data.value
    }

    if (isBackward) {
      result.forwardHash = d64.decode(head.hash)
      result.backwardHash = next ? d64.decode(next.hash) : null
    } else {
      result.backwardHash = d64.decode(head.hash)
      result.forwardHash = next ? d64.decode(next.hash) : null
    }

    return result
  }

  async removeChannelMessages (channelId) {
    const key = d64.encode(channelId)

    const itr = iterator(this.db, {
      gte: [MSG, key],
      lte: [MSG, key, '~']
    })

    const batch = []

    for await (const { err, data } of itr) {
      if (err) return { err }

      // delete the index and the key
      const index = [MSG_IDX, key, data.key.split(',').pop()]
      batch.push({ type: 'del', key: index })
      batch.push({ type: 'del', key: data.key })
    }

    try {
      await this.db.batch(batch)
    } catch (err) {
      return { err }
    }

    return {}
  }

  //
  // Entities (Identity, ChannelList, so on)
  //
  async storeEntity (prefix, id, blob) {
    const key = [ENTITY, prefix, id]

    try {
      await this.db.put(key, d64.encode(blob))
    } catch (err) {
      return { err }
    }

    return {}
  }

  async retrieveEntity (prefix, id) {
    const key = [ENTITY, prefix, id]
    let value = null

    try {
      value = await this.db.get(key)
    } catch (err) {
      if (err.notFound) return false
      return { err }
    }

    return d64.decode(value)
  }

  async removeEntity (prefix, id) {
    const key = [ENTITY, prefix, id]

    try {
      await this.db.del(key)
    } catch (err) {
      return { err }
    }
  }

  async getEntityKeys (namespace) {
    const prefix = [ENTITY]
    const params = {
      gte: [...prefix],
      lte: [...prefix, '~']
    }

    if (namespace) {
      params.gte.push(namespace)
      params.lte.splice(1, 0, namespace)
    }

    const itr = iterator(this.db, params)

    const results = []

    for await (const { err, data } of itr) {
      if (err) return { err }
      const key = data.key.split(',')
      results.push(key.pop())
    }

    return results
  }

  async getEntityCount () {
    const r = await this.getEntityKeys()
    if (r.err) {
      return r
    }

    return r.length
  }

  //
  // Miscellaneous
  //

  async clear () {
  }

  //
  // Private
  //

  encodeHashList (list) {
    return list.map(el => d64.encode(el))
  }

  decodeHashList (list) {
    return list.map(el => d64.decode(el))
  }
}

module.exports = Storage
