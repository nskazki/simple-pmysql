# PMysql

```
npm i -S simple-pmysql
```

```js
import P from 'bluebird'
import PMysql, { format as qFormat } from 'simple-pmysql'

let bdParams // = { host: ... }
let pMysql = PMysql(bdParams)
let select = qFormat('SELECT * FROM example WHERE id != ?', [ 0 ])

P.resolve()
    .then(() => pMysql.init())
    .then(() => pMysql.query(select).then(console.log))
    .then(() => pMysql.kill())
```
