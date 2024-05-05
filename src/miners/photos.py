"""
CongressPeople photos class.
"""

import os
import time
import asyncio
from io import BytesIO
import pandas as pd
from PIL import Image
from base import BaseMiner


def jpg_to_png(jpg_bytes: bytes) -> bytes:
    # Open JPG image from bytes
    jpg_image = Image.open(BytesIO(jpg_bytes))

    # Create a buffer for PNG bytes
    png_buffer = BytesIO()

    # Convert JPG to PNG
    jpg_image.save(png_buffer, format="PNG")

    # Get PNG bytes from the buffer
    png_bytes = png_buffer.getvalue()

    return png_bytes


class CongressPeoplePhotos(BaseMiner):
    """
    CongressPeoplePhotos class.
    """

    def __init__(self, **kwargs) -> None:
        """
        CongressPeoplePhotos constructor.
        """
        self.output_path = kwargs.get("output_path", "data/miners/photos/")
        concurrency = asyncio.Semaphore(25)
        super().__init__(
            name="Photos",
            log_file="logs/miners/photos.log",
            output_path=self.output_path,
            terminal=True,
            concurrency=concurrency,
        )
        self.base_url = "https://www.camara.leg.br/internet/deputado/bandep/"

    async def get_all_photos(self, ids: list[int]) -> dict:
        """
        Fetch all congresspeople photos asynchronously.

        Returns:
            list: List of congresspeople photos.
        """
        ids_params = [f"{id}.jpg" for id in ids]
        photos = await self.fetch_endpoint_list(
            self.base_url, ids_params, "", {}, {}, True
        )
        return photos

    async def save_photos(self, photos: dict[str, bytes]) -> None:
        """
        Save photos to disk.

        Args:
            photos (dict): Photos to save.
        """
        for photo_id, photo in photos.items():
            if photo is None:
                log_msg = f"Photo {photo_id} not found."
                self.logger.warning(log_msg)
                continue
            if not isinstance(photo, bytes):
                log_msg = f"Photo {photo_id} is not bytes."
                self.logger.error(log_msg)
                continue
            try:
                photo = jpg_to_png(photo)
            except Exception as e:
                log_msg = f"Error converting photo {photo_id} to PNG. {e}"
                self.logger.error(log_msg)
                continue
            try:
                photo_id = photo_id.split(".")[0]
                with open(f"{self.output_path}/{photo_id}.png", "wb") as file:
                    file.write(photo)
            except Exception as e:
                log_msg = f"Error saving photo {photo_id}. {e}"
                self.logger.error(log_msg)
                continue

    async def _mine(self, ids: list[int]) -> None:
        """
        Mine photos. It is done in batches of 50, due to memory constraints.
        """
        photos = await self.get_all_photos(ids)
        await self.save_photos(photos)

    def mine(self, path: str = "data/miners/congresspeople/congresspeople.csv") -> None:
        """
        Mine photos.

        Args:
            path (str): Path to the congresspeople data.
        """
        # if file is not found, sleep until it is created
        retry = 0
        while not os.path.exists(path):
            self.logger.info("File not found. Sleeping for 600 seconds.")
            time.sleep(600)
            retry += 1
            if retry > 10:
                self.logger.error("File not found. Exiting.")
                raise FileNotFoundError("File not found.")

        congresspeople_ids = pd.read_csv(path, encoding="utf-8")["id"].tolist()
        congresspeople_ids = list(set(congresspeople_ids))

        photos_at_a_time, congresspeople_ids_len = 25, len(congresspeople_ids)
        for i in range(0, congresspeople_ids_len, photos_at_a_time):
            ids = congresspeople_ids[i : i + photos_at_a_time]
            self.logger.info(
                "Mining photos %d to %d of %d.",
                i,
                i + photos_at_a_time,
                congresspeople_ids_len,
            )
            asyncio.run(self._mine(ids))
            self.logger.info(
                "Finished mining photos %d to %d.", i, i + photos_at_a_time
            )
