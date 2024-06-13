# Script to systematically download all the HSC coadd images from the HSC DR3 repo
import os
import aiohttp
import asyncio
import aiofiles
import argparse
from astropy.table import Table
from tqdm import tqdm
from aiohttp import BasicAuth

# Define the URL for the HSC DR3 repo
hsc_root_url = 'https://hsc-release.mtk.nao.ac.jp/archive/filetree'

async def get_remote_file_size(session, url):
    try:
        async with session.head(url) as response:
            response.raise_for_status()
            size = response.headers.get('Content-Length')
            return int(size) if size else None
    except Exception as e:
        print(f"Failed to get remote file size for {url}: {e}")
        return None

async def download_file(session, base_url, local_path, file_path):
    url = f'{base_url}/{file_path}'
    local_path = os.path.join(local_path , file_path)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    remote_size = await get_remote_file_size(session, url)
    if remote_size and os.path.exists(local_path) and os.path.getsize(local_path) == remote_size:
        return f"Skipped (already exists and same size): {local_path}"
    
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            async with aiofiles.open(local_path, 'wb') as f:
                async for chunk in response.content.iter_chunked(1024):
                    await f.write(chunk)
        return f"Downloaded: {local_path}"
    except Exception as e:
        print(e)
        return f"Failed to download {url}: {e}"

async def main(args):
    username = os.environ['SSP_PDR_USR']
    password = os.environ['SSP_PDR_PWD']

    auth = BasicAuth(login=username, password=password)

    # Read the catalog file
    catalog = Table.read(args.catalog)
    catalog = catalog['tract', 'patch']

    # Get a list of unique tract, patch combinations
    groups = catalog.group_by(['tract', 'patch'])

    # Build the list of tract patch combinations
    file_paths = []
    for group in groups.groups:
        tract = group['tract'][0]
        patch = group['patch'][0]
        patch = f"{patch // 100},{patch % 10}"
        for filter in ['HSC-G', 'HSC-R', 'HSC-I', 'HSC-Z', 'HSC-Y']:
            file_paths.append(f'{filter}/{tract}/{patch}/calexp-{filter}-{tract}-{patch}.fits')
    
    print(f"Preparing to download {len(file_paths)} images.")

    os.makedirs(args.output_dir, exist_ok=True)
    chunk_size = 10

    download_url = f'{hsc_root_url}/{args.rerun}/deepCoadd-results'

    for chunk in tqdm(range(0, len(file_paths)//chunk_size+1)):
        connector = aiohttp.TCPConnector(limit=args.max_connections, force_close=True)
        async with aiohttp.ClientSession(connector=connector, auth=auth) as session:
            tasks = [download_file(session, download_url, args.output_dir, file_path) for file_path in file_paths[chunk*chunk_size:min((chunk+1)*chunk_size, len(file_paths))]]
            results = await asyncio.gather(*tasks)
            for result in results:
                if ('Failed' in result) or ('Skipped' in result):
                    print(result)
        connector.close()

    print('Downloaded all images.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download HSC coadd images from the HSC DR3 repo.')
    parser.add_argument('--catalog', type=str, help='Path to the catalog file containing the list of all objects to cutout.')
    parser.add_argument('--rerun', type=str, default='pdr3_dud', help='The rerun to download the images from.')
    parser.add_argument('--output_dir', type=str, default='.', help='Output directory for the downloaded images.')
    parser.add_argument('--max_connections', type=int, default=8, help='Maximum number of concurrent connections to the server.')
    args = parser.parse_args()
    asyncio.run(main(args))