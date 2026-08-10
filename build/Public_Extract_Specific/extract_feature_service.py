import asyncio
import aiohttp
import geopandas as gpd
from pathlib import Path
from shapely.geometry import MultiPolygon, Polygon
import time

async def fetch_batch_with_retry(session, url, params, max_retries=3, retry_delay=2):
    """Fetch a single batch with retry logic."""
    for attempt in range(max_retries):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=120)) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'features' in data:
                        return data
                elif response.status == 500:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay * (attempt + 1))
                        continue
                    return None
                else:
                    return None
        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                print(f"Timeout at offset {params.get('resultOffset')}, retrying...")
                await asyncio.sleep(retry_delay * (attempt + 1))
                continue
            print(f"Failed after {max_retries} attempts at offset {params.get('resultOffset')}")
            return None
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
                continue
            print(f"Error at offset {params.get('resultOffset')}: {str(e)[:100]}")
            return None
    
    return None

async def fetch_all_features(rest_url, total_count, max_records=1000, max_concurrent=5):
    """Fetch all features with controlled concurrency and retries."""
    query_url = f"{rest_url}/query"
    
    offsets = list(range(0, total_count, max_records))
    print(f"Fetching {len(offsets)} batches with {max_concurrent} concurrent requests...")
    
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def fetch_with_semaphore(offset):
        async with semaphore:
            params = {
                'where': '1=1',
                'outFields': '*',
                'returnGeometry': 'true',
                'f': 'geojson',
                'resultOffset': offset,
                'resultRecordCount': max_records
            }
            result = await fetch_batch_with_retry(session, query_url, params)
            return result
    
    connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=max_concurrent)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_with_semaphore(offset) for offset in offsets]
        
        results = []
        successful = 0
        failed = 0
        
        for i, coro in enumerate(asyncio.as_completed(tasks)):
            result = await coro
            results.append(result)
            
            if result is not None:
                successful += 1
            else:
                failed += 1
            
            if (i + 1) % 25 == 0 or (i + 1) == len(tasks):
                print(f"Progress: {i + 1}/{len(tasks)} | Success: {successful} | Failed: {failed}")
        
        print(f"\nFinal: {successful} successful, {failed} failed batches")
        return results

def extract_rest_to_gpkg(rest_url, output_gpkg_path, layer_name=None, max_records=1000, max_concurrent=10):
    """
    Extract all features from ESRI REST endpoint to GeoPackage.
    
    Parameters:
    -----------
    rest_url : str
        URL to the ESRI REST service endpoint
    output_gpkg_path : str
        Path for the output GeoPackage
    layer_name : str, optional
        Name for the layer
    max_records : int
        Maximum records per request (default 1000)
    max_concurrent : int
        Number of concurrent requests (default 10)
    """
    
    import requests
    
    rest_url = rest_url.rstrip('/')
    print(f"Connecting to: {rest_url}")
    
    # Get service metadata
    metadata_url = f"{rest_url}?f=json"
    response = requests.get(metadata_url, timeout=30)
    response.raise_for_status()
    metadata = response.json()
    
    if layer_name is None:
        layer_name = metadata.get('name', 'layer')
    
    print(f"Layer: {layer_name}")
    print(f"Geometry Type: {metadata.get('geometryType', 'Unknown')}")
    
    # Get total feature count
    count_url = f"{rest_url}/query"
    count_params = {
        'where': '1=1',
        'returnCountOnly': 'true',
        'f': 'json'
    }
    count_response = requests.get(count_url, params=count_params, timeout=30)
    count_response.raise_for_status()
    total_count = count_response.json().get('count', 0)
    
    print(f"Total features: {total_count}")
    
    if total_count == 0:
        print("No features to extract.")
        return
    
    # Fetch all features
    start_time = time.time()
    results = asyncio.run(fetch_all_features(rest_url, total_count, max_records, max_concurrent))
    elapsed = time.time() - start_time
    
    # Combine all features
    print("\nCombining features...")
    all_features = []
    for result in results:
        if result and 'features' in result:
            all_features.extend(result['features'])
    
    print(f"Downloaded {len(all_features)} features in {elapsed:.1f} seconds")
    
    if len(all_features) < total_count * 0.9:
        print(f"\n⚠ WARNING: Only retrieved {len(all_features)}/{total_count} features ({100*len(all_features)/total_count:.1f}%)")
        print("Consider reducing max_concurrent or running again to get missing features")
    
    if not all_features:
        print("No features extracted.")
        return
    
    # Convert to GeoDataFrame
    print("Converting to GeoDataFrame...")
    geojson = {
        'type': 'FeatureCollection',
        'features': all_features
    }
    
    gdf = gpd.GeoDataFrame.from_features(geojson)
    
    # Set CRS
    if gdf.crs is None:
        spatial_ref = metadata.get('spatialReference', {})
        wkid = spatial_ref.get('wkid', 4326)
        gdf.set_crs(epsg=wkid, inplace=True)
    
    print(f"CRS: {gdf.crs}")
    print(f"Original shape: {gdf.shape}")
    
    # Handle geometries
    print("Processing geometries...")
    
    # Convert null geometries to empty MultiPolygons
    null_geoms = gdf.geometry.isna().sum()
    if null_geoms > 0:
        print(f"Converting {null_geoms} null geometries to empty MultiPolygons")
        gdf.loc[gdf.geometry.isna(), 'geometry'] = MultiPolygon()
    
    # Convert all to MultiPolygon
    def force_multipolygon(geom):
        if geom is None or (hasattr(geom, 'is_empty') and geom.is_empty):
            return MultiPolygon()
        if isinstance(geom, MultiPolygon):
            return geom
        elif isinstance(geom, Polygon):
            return MultiPolygon([geom])
        else:
            return MultiPolygon()
    
    gdf['geometry'] = gdf['geometry'].apply(force_multipolygon)
    
    # Fix invalid geometries
    invalid = ~gdf.geometry.is_valid
    if invalid.any():
        print(f"Fixing {invalid.sum()} invalid geometries")
        gdf.loc[invalid, 'geometry'] = gdf.loc[invalid, 'geometry'].buffer(0)
    
    print(f"Final shape: {gdf.shape}")
    print(f"Records retained: {len(gdf)} / {total_count}")
    
    # Create output directory
    output_path = Path(output_gpkg_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write to GeoPackage
    # Update 12/6/2025: added engine="pyogrio" because of error using fiona to write the gdf to file 
    print(f"\nWriting to {output_gpkg_path}...")
    gdf.to_file(output_gpkg_path, layer=layer_name, driver="GPKG", engine="pyogrio")
    
    print(f"✓ SUCCESS: Exported {len(gdf)} features to {output_gpkg_path}")
    print(f"✓ Layer name: {layer_name}")

if __name__ == "__main__":
    urls = [
        #"https://gis.blm.gov/nlsdb/rest/services/Fluid_Minerals/FluidMinerals_Case/MapServer/0/query",
        "https://gis.blm.gov/nlsdb/rest/services/Land_Tenure/Land_Tenure_Case/MapServer/0/query",
        #"https://gis.blm.gov/nlsdb/rest/services/Land_Use_Authorizations/Land_Use_Auth_Case/MapServer/0/query",
        #"https://gis.blm.gov/nlsdb/rest/services/Mining_Claims/MiningClaims/MapServer/0/query",
        #"https://gis.blm.gov/nlsdb/rest/services/Solid_Minerals/Solid_Minerals_Case/MapServer/0/query",
        #"https://gis.blm.gov/nlsdb/rest/services/Case_Lands/MapServer/0/query"
    ]
    for url in urls:
        rest_endpoint = url.rsplit('/query', 1)[0]
        output_gpkg = f"{url.split('/')[6]}_case.gpkg"
        
        # Extract ALL features with 1900 records per batch and 10 concurrent requests for speed
        extract_rest_to_gpkg(
            rest_url=rest_endpoint,
            output_gpkg_path=output_gpkg,
            layer_name="case_lands",
            max_records=1800,
            max_concurrent=7  # Increase to 15-20 for even more speed
        )