#!/usr/bin/env node

const fs = require('fs')
const csv = require('csv-parser')
const H = require('highland')
const axios = require('axios')

// ==================================================================
// Filename & URL of source data:
// ==================================================================

const csvFilename = './krayenhof-1815-jan-hartmann.csv'
const sheetsDatesUrl = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR2LApK-Qoqn4W-gblT2FyZUI7i-YPhS4Z8syJH_Ny6XtsCBi5Cgncj0J3nPh-HF_lOKDh-alvieIw1/pub?gid=0&single=true&output=csv'
const sheetsTrianglesUrl = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR2LApK-Qoqn4W-gblT2FyZUI7i-YPhS4Z8syJH_Ny6XtsCBi5Cgncj0J3nPh-HF_lOKDh-alvieIw1/pub?gid=1226986217&single=true&output=csv'

// ==================================================================
// Helper functions:
// ==================================================================

const geojson = {
  open: '{"type":"FeatureCollection","features":[',
  close: ']}\n',
  separator: ',\n'
}

function toGeoJSON (rows) {
  H([
    H([geojson.open]),
    H(rows)
      .map(JSON.stringify)
      .intersperse(',\n'),
    H([geojson.close])
  ]).compact()
    .sequence()
    .pipe(process.stdout)
}

function parseDate (str) {
  const [day, month, year] = str.split('-')
  return `${year}-${month}-${day}`
}

function degMinSecFromRow (row, key) {
  const degree = parseInt(row[`${key}graad`])
  const minute = parseInt(row[`${key}minuut`])
  const second = parseInt(row[`${key}seconde`]) + parseInt(row[`${key}nr`]) / 10000

  return [degree, minute, second]
}

function degMinSecToDecimal (deg, min, sec) {
  return deg + min / 60 + sec / 60 / 60
}

// ==================================================================
// Read Jan Hartmann's CSV file:
// ==================================================================

function readCSVFile () {
  return new Promise((resolve, reject) => {
    const csvRows = H(fs.createReadStream(csvFilename))
      .pipe(csv({
        separator: '\t',
      }))

    // Coordinates in CSV need to me moved a little towards Amersfoort!
    const toAmersfoort = 2.338
    // 0.001100000000000989


    // Uit CSV, Naarden:
    //   lat: 52° 17" 46.3766'
    //   lon   2° 49" 38.3724'


    // Longitude Westerkerk 4.88352
    //
    // Haasbroek, Naarden:
    //    lat: 52, 17, 48.161
    //      52.29671138888889
    //    lon: 0,  16, 43.590
    //      0.278775 + 4.88352 = 5.162295

    // https://bertspaan.nl/latlong/#18/52.29671/5.162295








    H(csvRows)
      .map((row) => ({
        type: 'Feature',
        properties: {
          // gid: '59',
          nummer: parseInt(row.nr),
          volgnummer: parseInt(row.volgnummer),
          standplaats: row.standplaats,
          place: row['huidige plaatsnaam']
        },
        geometry: {
          type: 'Point',
          coordinates: [
            degMinSecToDecimal(...degMinSecFromRow(row, 'o'))
              + toAmersfoort,
            degMinSecToDecimal(...degMinSecFromRow(row, 'n'))
          ]
        }
      }))
      .filter((feature) => {
        // const coordinates = [...feature.geometry.coordinates]
        // const url = `https://bertspaan.nl/latlong/#18/${coordinates.reverse().join('/')}`
        // console.error(feature.properties.huidigePlaatsnaam + ':')
        // console.error('  ', url)

        if (!feature.properties.place) {
          console.error(`Geen huidige plaatsnaam in CSV: ${feature.properties.standplaats}`)
        } else {
          return true
        }
      })
      .toArray((features) => {
        resolve(features)
      })
    })
}

// ==================================================================
// Read Google Sheets:
// ==================================================================

function readSheetsDates () {
  return new Promise((resolve, reject) => {
    axios({
      method: 'get',
      url: sheetsDatesUrl,
      responseType: 'stream'
    })
    .then((response) => {
      const sheetsRows = response.data.pipe(csv({
        separator: ','
      }))

      H(sheetsRows)
        .filter((row) => row['Plaatsnaam'])
        .map((row) => {
          let geometry = null

          if (row['Coördinaten']) {
            geometry = {
              type: 'Point',
              coordinates: row['Coördinaten'].split(',')
                .map((coordinate) => parseFloat(coordinate))
                .reverse()
            }
          }

          return {
            type: 'Feature',
            properties: {
              book: parseInt(row['Band']),
              place: row['Huidige plaatsnaam'],
              originalPlace: row['Plaatsnaam'],
              description: row['Omschrijving'],
              date: parseDate(row['Datum 1e meting'])
            },
            geometry
          }
        })
      .filter((feature) => {
        if (!feature.properties.place) {
          console.error(`Geen huidige naam in Google Sheets: ${feature.properties.originalPlace}`)
        } else {
          return true
        }
      })
      .toArray((features) => {
        resolve(features)
      })
    })
  })
}

// ==================================================================
// Read Google Sheets:
// ==================================================================

function readSheetsTriangles () {
  return new Promise((resolve, reject) => {
    axios({
      method: 'get',
      url: sheetsTrianglesUrl,
      responseType: 'stream'
    })
    .then((response) => {
      const sheetsRows = response.data.pipe(csv({
        separator: ','
      }))

      H(sheetsRows)
        .map((row) => ({
          from: row.Plaatsnaam,
          to: [
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[9]
          ].filter((to) => to)
        }))
        .toArray((features) => {
          resolve(features)
        })
    })
  })
}

// ==================================================================
// Combine!!!
// ==================================================================

Promise.all([readCSVFile(), readSheetsDates(), readSheetsTriangles()])
  .then(([csvFeatures, sheetsFeatures, sheetsTriangles]) => {

    const csvFeaturesByPlace = {}
    const sheetsFeaturesByPlace = {}

    csvFeatures
      .forEach((feature) => {
        const place = feature.properties.place
        csvFeaturesByPlace[place] = feature
      })

    sheetsFeatures
      .forEach((feature) => {
        const place = feature.properties.place
        sheetsFeaturesByPlace[place] = feature
      })

    function getGeometry (place) {
      const csvGeometry = csvFeaturesByPlace[place] && csvFeaturesByPlace[place].geometry
      const sheetsGeometry = sheetsFeaturesByPlace[place] && sheetsFeaturesByPlace[place].geometry
      const geometry = sheetsGeometry || csvGeometry

      if (!geometry) {
        console.error(`No geometry found for: ${place}`)
      }

      return geometry
    }

    const edges = new Set()
      sheetsTriangles.forEach((place) => {
        if (!sheetsFeaturesByPlace[place.from]) {
          console.error(`Driehoek! Controleer deze: ${place.from}`)
        }

        place.to
          .map((to) => {
            if (!sheetsFeaturesByPlace[to]) {
              console.error(`Driehoek! Controleer deze: ${to}`)
            }
          })

        place.to
          .map((to) => [place.from, to].sort())
          .forEach((edge) => edges.add(JSON.stringify(edge)))
      })

    const edgesByPlace = {}
    Array.from(edges).map(JSON.parse).forEach(([from, to]) => {
      try {
        const dateFrom = new Date(sheetsFeaturesByPlace[from].properties.date)
        const dateTo = new Date(sheetsFeaturesByPlace[to].properties.date)

        const edge = dateFrom < dateTo ? [to, from] : [from, to]

        if (!edgesByPlace[edge[0]]) {
          edgesByPlace[edge[0]] = []
        }

        edgesByPlace[edge[0]].push(edge[1])
      } catch (err) {
        console.error(`Error adding edge: ${from} - ${to}`)
      }
    })

    const finalFeatures = sheetsFeatures
      .filter((feature) => {
        const place = feature.properties.place
        if (!csvFeaturesByPlace[place]) {
          console.error(`Huidige plaatsnaam uit Google Sheets niet gevonden in CSV: ${place}`)
        } else {
          return true
        }
      })
      .map((feature) => {
        const place = feature.properties.place
        const geometry = getGeometry(place)

        let edges = []
        if (edgesByPlace[place]) {
          edges = edgesByPlace[place]
            .map((to) => {
              const geometry = getGeometry(to)
              const coordinates = geometry && geometry.coordinates

              return {
                place: to,
                coordinates
              }
            })
            .filter((edge) => edge.coordinates)
        }

        const properties = {
          ...feature.properties,
          edges
        }

        return {
          ...feature,
          properties,
          geometry
        }
      })

    toGeoJSON(finalFeatures)
  })
