# geopandas-parallelise - WORK-IN-PROGRESS

A repo for parallelising common geopandas operations. Includes spatial joins using efficient STRTree queryin and roundtrip reproject-buffer-reproject allowing precise buffering in meters in UTM coordinate reference systems.

## Overview

### Buffer

Buffer operations are commonly used when looking for spatial proximity between a sufficiently large number of objects that calculating the distance between all objects is unfeasible. Additionally, typical geometry buffer operations assume a euclidean geometry, i.e. where scales are constant, and so are unsuitable for map coordinates, where as an example, the distance between coordinates close to the equator is significantly larger than the distance at the poles. To address this, geometries can first be converted to near-euclidean UTM coordinates and then buffered using a distance in meters. The buffered geometries can then be transformed back to WGS84 lat/lon coordinates. The challenge is that each of the 60 UTM zones is a different coordinate reference system, so on global datasets, this operation can be expensive if not parallelised.

### Spatial Join

The addition of [PyGEOS](https://pygeos.readthedocs.io/en/latest/) to [GeoPandas 0.8](https://geopandas.org/index.html) makes STRtree querying much faster, but with geometries more complex than points, the `intersects`,`contains`, and `within` operations take much longer. This repo uses PyGEOS to first assemble and query an STRTree, and then parallelises the actual spatial operation.

## Useage


## Performance

#### Basic Example - NY Taxi Rides

#### Advanced Example - World Protected Areas and Global Human Settlements




    ipython kernel install --user --name=geoml