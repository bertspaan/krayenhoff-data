#!/usr/bin/env node

const fs = require('fs')
const H = require('highland')
const JSONStream = require('JSONStream')

const features = process.stdin.pipe(JSONStream.parse('features.*'))

const geojson = {
  open: '{"type":"FeatureCollection","features":[',
  close: ']}\n',
  separator: ',\n'
}

H(features)
  .map((feature) => feature.properties.edges.map((edge) => ({
    type: 'Feature',
    properties: {},
    geometry: {
      type: 'LineString',
      coordinates: [feature.geometry.coordinates, edge.coordinates]
    }
  })))
  .flatten()
  .pipe(JSONStream.stringify(geojson.open, geojson.separator, geojson.close))
  .pipe(process.stdout)
