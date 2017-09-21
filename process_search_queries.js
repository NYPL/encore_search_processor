let parse = require('csv-parse')
let fs = require('fs')

// Create the parser
let parser = parse({from: 2})

let results = new Proxy({}, {
  get: (target, name) => name in target ? target[name] : 0
})

parser.on('readable', function () {
  let record, normalizedQuery, hits
  while ((record = parser.read())) {
    try {
      [normalizedQuery, hits] = processRecord(record)
    } catch (e) {
      console.warn('Problem processing record: ' + record)
      continue
    }
    if (normalizedQuery) {
      results[normalizedQuery] += hits
    }
  }
})

// Catch any error
parser.on('error', function (err) {
  console.warn(err.message)
})

parser.on('finish', function () {
  let stringify = require('csv-stringify')

  stringify(Object.keys(results).sort((a, b) => results[b] - results[a]).map(r => [r, results[r], queryType(r)]))
    .pipe(process.stdout)
})

let fileName = '20170829__search_queries__all_traffic.csv'

let input = fs.createReadStream(fileName)

input.pipe(parser)

function processRecord (record) {
  // e.g., C__Sukulele__Orightresult__U?lang=eng&suite=def, but also C__Sukulele?, ?lang=eng...
  let [, rawQuery] = record[0].split('__')
  let query = rawQuery
    .substr(1) // Remove leading 'C'
    .replace(/[. ]$/, '') // Remove trailing '.' and space
    .split('?')[0] // Get rid of query_string
    .replace('Pw==', '') // removing weird '?' encoding (simpler than replacing)
    .replace('Lw==', '/') // replace '/' for it's weird encoding
    .toLowerCase()

  return [query, parseInt(record[1])]
}

let config = setupTypesData(require('./config.json'))
let types = config.query_types

function queryType (query) {
  for (let type in types) {
    if (types[type].has(query)) {
      return type
    }
  }
  return 'title query'
}

function setupTypesData (config) {
  let types = config.query_types
  for (let type in types) {
    types[type] = new Set(types[type])
  }
  return types
}
