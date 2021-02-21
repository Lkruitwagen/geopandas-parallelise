import os, json
import geopandas as gpd
import pandas as pd
import multiprocessing as mp
from multiprocessing.shared_memory import SharedMemory
import numpy as np
import pyproj
import pygeos

from shapely import geometry, ops, wkt
from functools import partial

    
def chunk_worker(left_spec, right_spec, Q_slice):
    
    # recover the SharedMemory bloc
    left_shm = SharedMemory(left_spec['name'])
    right_shm = SharedMemory(right_spec['name'])
    # Create the np.recarray from the buffer of the shared memory
    left_arr = np.recarray(shape=left_spec['shape'], dtype=left_spec['dtype'], buf=left_shm.buf)
    right_arr = np.recarray(shape=right_spec['shape'], dtype=right_spec['dtype'], buf=right_shm.buf) 
    
    results = []
    for idx_pair in Q_slice:
        geom1 = left_arr[idx_pair[1]][1]
        geom2 = right_arr[idx_pair[0]][1]

        
        if (geom1!=geom2)  and (geom1.intersects(geom2)):
            results.append((idx_pair[0], idx_pair[1] ))
        
    return results
    
def to_shm(gdf, name):
    array = gdf[[gdf.geometry.name]].to_records(index=True)
    shape, dtype = array.shape, array.dtype
    
    shm = SharedMemory(name=name, create=True, size=array.nbytes)
    shm_array = np.recarray(shape=shape, dtype=dtype, buf=shm.buf)
    shm_spec = {'name':name, 'shape':shape, 'dtype':dtype}
    
    np.copyto(shm_array, array)
    
    return shm, shm_spec

def mp_sjoin(gdf_left, gdf_right,n_workers, how='inner', op='intersects'):
    
    ### get and query tree using pygeos
    # construct RTree with pygeos and query
    tree = pygeos.STRtree([pygeos.io.from_shapely(el) for el in gdf_left.geometry.values])
    Q = tree.query_bulk([pygeos.io.from_shapely(el) for el in gdf_right.geometry.values])
    
    # Assign sharedmemory
    left_shm, left_spec = to_shm(gdf_left,'left')
    right_shm, right_spec = to_shm(gdf_right,'right')
    
    # do the geospatial operation asyncronously
    chunk = Q.shape[1]//n_workers +1
    
    args = [(left_spec, right_spec, Q.T[ii*chunk:(ii+1)*chunk,:]) for ii in range(n_workers)]
    
    with mp.Pool(n_workers) as pool:
        res = pool.starmap(chunk_worker, args)
        
    res = [item for sublist in res for item in sublist]
    
    # release shared mem
    left_shm.close()
    left_shm.unlink()
    right_shm.close()
    right_shm.unlink()
    
    # reconstruct the joined dataframes
    idx_df = pd.DataFrame(res, columns=['index_right','index_left'])
    
    if how=='inner':
        return pd.merge(
                    pd.merge(gdf_left, idx_df,how='inner',left_index=True, right_on='index_left'),
                    gdf_right, 
                    how='inner',
                    left_on='index_right',
                    right_index=True) \
                .rename(columns={'index_left':'index','geometry_x':'geometry'}) \
                .set_index('index') \
                .drop(columns=['geometry_y']) \
                .set_geometry('geometry')
    elif how=='left':
        return pd.merge(
                    pd.merge(gdf_left, idx_df, how='left',left_index=True,right_on='index_left'), 
                    gdf_right, 
                    how='left',
                    left_on='index_right',
                    right_index=True) \
                .rename(columns={'index_left':'index','geometry_x':'geometry'}) \
                .set_index('index') \
                .drop(columns=['geometry_y']) \
                .set_geometry('geometry')
    elif how=='right':
        return pd.merge(
                    pd.merge(gdf_left, idx_df, how='right',left_index=True,right_on='index_left'), 
                    gdf_right, 
                    how='right',
                    left_on='index_right',
                    right_index=True) \
                .rename(columns={'index_right':'index','geometry_y':'geometry'}) \
                .set_index('index') \
                .drop(columns=['geometry_x']) \
                .set_geometry('geometry')