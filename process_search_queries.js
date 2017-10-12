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

let fileName = '20171011_encore.csv'

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
    .replace('l:eng', '') // replace English language specifier (but may be useful to know sometimes)
    .replace(/[ ]+/g, ' ') // compress spaces
    .toLowerCase()

  query = config.mappedTerms[query]

  return [query, parseInt(record[1])]
}

let config = processConfig(require('./config.json'))
let types = config.query_types

function queryType (query) {
  for (let type in types) {
    if (types[type].has(query)) {
      return type
    }
  }
  return 'title query'
}

function processConfig (config) {
  setupTypesData(config)
  setupMappedTerms(config)
  return config
}

function setupTypesData (config) {
  let types = config.query_types
  for (let type in types) {
    types[type] = new Set(types[type])
  }
}

function setupMappedTerms (config) {
  let mappedTerms = new Proxy({}, { get: (target, name) => name in target ? target[name] : name })

  for (let [term, synonyms] of Object.entries(config.termMaps)) {
    for (let synonym of synonyms) {
      mappedTerms[synonym] = term
    }
  }

  config.mappedTerms = mappedTerms
}
