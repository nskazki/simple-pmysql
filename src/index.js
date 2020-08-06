import { debugEvents, debugMethods } from 'simple-debugger'
import { noop, isFunction, isNull } from 'lodash'
import P from 'bluebird'
import EventEmitter from 'events'
import Debug from 'debug'
import mysql from 'mysql'
import e2p from 'simple-e2p'

const pMysqlDebug = new Debug('libs-pmysql')

export default class PMysql extends EventEmitter {
  static format(...args) {
    return mysql.format.apply(mysql, args)
  }

  constructor(dbParams) {
    super()
    this.setMaxListeners(0)

    debugEvents(this)
    debugMethods(this, [
      'on',
      'once',
      'emit',
      'addListener',
      'removeListener'
    ])

    this.dbParams = dbParams

    this._isIdle = true
    this._isBroken = false
    this._isConnected = false

    this._dbLink = null
    this._pingProcessId = null
    this._pingProcessInterval = 500
    this._pingProblemHandler = null

    this._reinitTimeoutId = null
    this._reinitAttemptInterval = 2e3
    this._reinitBrokenDelay = 60e3
  }

  _wait() {
    if (this._isIdle) {
      this._isIdle = false
      return P.resolve()
    } else {
      return e2p(this, '_idle').then(this._wait.bind(this))
    }
  }

  _free() {
    this._isIdle = true
    this.emit('_idle')
    return P.resolve()
  }

  _startPingProcess() {
    this._pingProblemHandler = this._reinit.bind(this)
    this.once('_pingProblem', this._pingProblemHandler)
    this._pingProcessId = setTimeout(() => {
      if (this._isBroken) {
        pMysqlDebug('ping - not performed, module broken -> just skip')
        return
      }

      P.resolve()
        .then(() => this._wait())
        .then(() => P.fromNode(cb => this._dbLink.ping(cb)))
        .then(() => {
          if (this._isBroken) {
            pMysqlDebug('ping - success, module broken -> just skip')
            return
          }

          pMysqlDebug('ping - success -> #_restartPingProcess')
          this._restartPingProcess()
        }, err => {
          if (this._isBroken) {
            pMysqlDebug(`ping - problem, module broken -> just skip
              \n\t err: ${err.message}`)
            return
          }

          pMysqlDebug(`ping - problem -> !_pingProblem\
            \n\t err: ${err.message}`)
          this.emit('_pingProblem')
        })
        .finally(() => this._free())
    }, this._pingProcessInterval)
  }

  _stopPingProcess() {
    if (isFunction(this._pingProblemHandler)) {
      this.removeListener('_pingProblem', this._pingProblemHandler)
      this._pingProblemHandler = null
    }
    clearTimeout(this._pingProcessId)
    this._pingProcessId = null
  }

  _restartPingProcess() {
    this._stopPingProcess()
    this._startPingProcess()
  }

  init() {
    if (this._isBroken) {
      pMysqlDebug('init - not performed, module broken -> throw Error')
      return P.reject(new Error('init - not performed, becouse module is broken'))
    }

    return P.resolve()
      .then(() => this._wait())
      .then(() => {
        this._dbLink = mysql.createConnection(this.dbParams)
        this._dbLink.once('error', noop)
      })
      .then(() => P.fromNode(cb => {
        this._dbLink.query('SELECT 1', cb)
      }))
      .then(() => {
        if (this._isBroken) {
          pMysqlDebug('init - success, module broken -> throw Error')
          return P.reject(new Error('init - success, but module is broken'))
        }

        pMysqlDebug('init - success -> #_restartPingProcess and !_connected')
        this._restartPingProcess()
        this._isConnected = true
        this.emit('_connected')
      })
      .catch(err => {
        pMysqlDebug(`init - problem -> #_stopPingProcess and !_notconnected\
          \n\t err: ${err.message}`)
        this._stopPingProcess()
        this._isConnected = false
        this.emit('_notconnected')

        throw err
      })
      .finally(() => this._free())
  }

  kill() {
    if (this._isBroken) {
      pMysqlDebug('kill - not performed, module broken -> throw Error')
      return P.reject(new Error('kill - not performed, becouse module is broken'))
    }

    return P.resolve()
      .then(() => this._wait())
      .then(() => {
        pMysqlDebug('kill - started -> #_stopPingProcess and !_disconnected')
        this._stopPingProcess()
        this._isConnected = false
        this.emit('_disconnected')
      })
      .then(() => P.fromNode(cb => {
        this._dbLink.end(cb)
      }))
      .then(() => {
        if (this._isBroken) {
          pMysqlDebug('kill - success, module broken -> throw Error')
          return P.reject(new Error('kill - success, but module is broken'))
        }

        pMysqlDebug('kill - success')
      })
      .catch(err => {
        pMysqlDebug(`kill - problem\
          \n\t err: ${err.message}`)
        throw err
      })
      .finally(() => this._free())
  }

  _reinit() {
    if (this._isBroken) {
      pMysqlDebug('_reinit - not performed, module broken -> skip')
      return
    }

    pMysqlDebug('_reinit - started ->  #_initReinitTimeout')
    this._initReinitTimeout()

    P.resolve()
      .then(() => this.kill().catch(noop))
      .then(() => this.init())
      .then(() => {
        pMysqlDebug('_reinit - success -> #_killReinitTimeout')
        this._killReinitTimeout()
      }, err => {
        pMysqlDebug(`_reinit - problem -> #_reinit after delay\
          \n\t err: ${err.message}`)
        return P.resolve()
          .delay(this._reinitAttemptInterval)
          .then(() => this._reinit())
      })
  }

  _initReinitTimeout() {
    if (!isNull(this._reinitTimeoutId)) {
      pMysqlDebug('_initReinitTimeout - already exists')
    } else {
      pMysqlDebug('_initReinitTimeout - setup')
      this._reinitTimeoutId = setTimeout(() => {
        pMysqlDebug('_initReinitTimeout - timeout -> !_broken')
        this._isBroken = true
        this.emit('_broken', new Error('module was broken'))
      }, this._reinitBrokenDelay)
    }
  }

  _killReinitTimeout() {
    clearTimeout(this._reinitTimeoutId)
    this._reinitTimeoutId = null
  }

  query(...args) {
    if (this._isBroken) {
      pMysqlDebug('query - not performed, module broken -> throw Error')
      return P.reject(new Error('connection has been lost, restore was failed'))
    }

    if (this._isConnected) {
      pMysqlDebug('query - started')
      return P.resolve()
        .then(() => this._wait())
        .then(() => P.fromNode(cb => {
          this._dbLink.query(...args, cb)
        }, { multiArgs: true }))
        .tap(() => pMysqlDebug('query - success'))
        .catch(err => {
          pMysqlDebug(`query - problem\
            \n\t err: ${err.message}`)
          throw err
        })
        .finally(() => this._free())
    } else {
      pMysqlDebug('query - pause, module not connected')
      return e2p(this, '_connected', '_broken')
        .catch(err => {
          pMysqlDebug(`query - broken\
            \n\t err: ${err.message}`)
          throw err
        })
        .then(() => pMysqlDebug('query - unpause'))
        .then(() => this.query.apply(this, args))
    }
  }
}
