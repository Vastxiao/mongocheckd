import asyncio

from loguru import logger
import aiofiles


async def get_files():
    try:
        async with aiofiles.open('skip_number.txt', mode='r') as f:
            contents = await f.read()
    except FileNotFoundError:
        pass
    else:
        logger.info(contents)


asyncio.run(get_files())
