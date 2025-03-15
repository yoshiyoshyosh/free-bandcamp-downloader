import atexit
import glob
import html
import json
import logging
import os
import pprint
import re
import time
import zipfile
import mutagen
import pyrfc6266
import requests

from bs4 import BeautifulSoup
from docopt import docopt
from tqdm import tqdm
from configparser import ConfigParser
from dataclasses import dataclass
from http.cookiejar import MozillaCookieJar
from typing import Dict, Optional, Set, Tuple
from urllib.parse import urljoin, urlsplit

from free_bandcamp_downloader import logger
from free_bandcamp_downloader.bandcamp_http_adapter import *
from free_bandcamp_downloader.guerrillamail import GMSession

@dataclass
class BCFreeDownloaderOptions:
    country: str = None
    zipcode: str = None
    email: str = None
    format: str = None
    dir: str = None

class BCFreeDownloadError(Exception):
    pass

class BCFreeDownloader:
    CHUNK_SIZE = 1024 * 1024
    LINK_REGEX = re.compile(r'<a href="(?P<url>[^"]*)">')
    RETRY_URL_REGEX = re.compile(r'"retry_url":"(?P<retry_url>[^"]*)"')
    FORMATS = {
        "FLAC": "flac",
        "V0MP3": "mp3-v0",
        "320MP3": "mp3-320",
        "AAC": "aac-hi",
        "Ogg": "vorbis",
        "ALAC": "alac",
        "WAV": "wav",
        "AIFF": "aiff-lossless",
    }

    def __init__(
        self,
        options: BCFreeDownloaderOptions,
        config_dir: str,
        download_history_file: str,
        cookies_file: Optional[str] = None,
        identity: Optional[str] = None,
    ):
        self.options = options
        self.config_dir = config_dir
        self.download_history_file = download_history_file
        self.downloaded: Set[str] = set()  # can be URL or ID
        self.mail_session = None
        self.queued_emails = {} # { ("album"|"track", id): {info} }
        self.session = None
        self._init_email()
        self._init_session(cookies_file, identity)

    def _init_email(self):
        if not self.options.email or self.options.email == "auto":
            self.mail_session = GMSession()
            self.options.email = self.mail_session.get_email_address()

    def _init_session(self, cookies_file: Optional[str], identity: Optional[str]):
        self.session = requests.Session()
        self.session.mount("https://", BandcampHTTPAdapter())
        if cookies_file:
            cj = MozillaCookieJar(cookies_file)
            cj.load()
            self.session.cookies = cj
        if identity:
            self.session.cookies.set("identity", identity)

    def _download_file(self, download_page_url: str, format: str) -> dict:
        r = self.session.get(download_page_url)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        album_url = soup.find("div", class_="download-artwork").find("a").attrs["href"]

        data = json.loads(
                soup.find("div", {"id": "pagedata"}).attrs["data-blob"]
            )["digital_items"][0]
        download_url = data["downloads"][self.FORMATS[format]]["url"]
        id = (data["type"], data["item_id"])

        def download(download_url: str) -> str:
            with self.session.get(download_url, stream=True) as r:
                r.raise_for_status()
                size = int(r.headers["content-length"])
                name = pyrfc6266.requests_response_to_filename(r)
                file_name = os.path.join(self.options.dir, name)
                with tqdm(total=size, unit="iB", unit_scale=True) as pbar:
                    with open(file_name, "wb") as f:
                        for chunk in r.iter_content(chunk_size=self.CHUNK_SIZE):
                            f.write(chunk)
                            pbar.update(len(chunk))
                return file_name

        try:
            file_name = download(download_url)
        except Exception:
            statdownload_url = download_url.replace("/download/", "/statdownload/")
            with self.session.get(statdownload_url) as r:
                r.raise_for_status()
                download_url = self.RETRY_URL_REGEX.search(r.text).group("retry_url")
            if download_url:
                file_name = download(download_url)
            else:
                # retry requires email address
                raise BCFreeDownloadError(
                    "Download expired. Make sure your payment email is linked "
                    "to your fan account (Settings > Fan > Payment email addresses)"
                )

        logger.info(f"Downloaded {file_name}")

        return { "id": id, "file_name": file_name }

    @staticmethod
    def unzip_album(file_name: str) -> list[str]:
        if file_name.endswith(".zip"):
            # Unzip archive
            dir_name = file_name[:-4]
            with zipfile.ZipFile(file_name, "r") as f:
                f.extractall(dir_name)
            logger.info(f"Unzipped {file_name}.")
            os.remove(file_name)
            return glob.glob(os.path.join(dir_name, "*"))
        return None

    # Tag downloaded audio file with url & comment
    @staticmethod
    def tag_file(file_name: str, head_data: dict):
        logger.info("Setting tags...")
        f = mutagen.File(file_name)
        if f is None:
            return

        f["website"] = head_data["@id"]
        if head_data.get("keywords"):
            f["genre"] = head_data["keywords"]
        comment = ""
        comment += ("\n\n" + head_data.get("description", "")).strip()
        comment += ("\n\n" + head_data.get("creditText", "")).strip()
        f["comment"] = comment
        f.save()

    def _download_purchased_album(self, user_id: int, tralbum_data: dict) -> dict:
        logger.info("Downloading album from collection...")
        logger.debug(f"Searching for album: '{tralbum_data['current']['title']}'")
        data = {
            "fan_id": user_id,
            "search_key": tralbum_data['current']['title'],
            "search_type": "collection",
        }
        r = self.session.post(
            "https://bandcamp.com/api/fancollection/1/search_items", json=data
        )
        r.raise_for_status()
        results = r.json()
        tralbums = results["tralbums"]
        redownload_urls = results["redownload_urls"]
        wanted_id = f"{tralbum['item_type'][0]}:{tralbum['id']}"
        try:
            tralbum = next(
                filter(
                    lambda tralbum: f"{tralbum['tralbum_type']}:{tralbum['tralbum_id']}"
                    == wanted_id,
                    tralbums,
                )
            )
        except StopIteration:
            raise BCFreeDownloadError("Could not find album in collection")
        sale_id = f"{tralbum['sale_item_type']}{tralbum['sale_item_id']}"
        if sale_id not in redownload_urls:
            raise BCFreeDownloadError("Could not find album download URL in collection")
        download_url = redownload_urls[sale_id]
        logger.debug(f"Got download URL: {download_url}")
        return self._download_file(download_url, self.options.format)

    # download from release page
    def download_album(self, soup: BeautifulSoup) -> dict:
        album_data = BCFreeDownloader.get_album_info(soup)
        tralbum_data = album_data["tralbum_data"]
        head_data = album_data["head_data"]
        album_data['is_downloaded'] = False
        album_data['email_queued'] = False
        url = tralbum_data["url"]

        logger.debug(f"tralbum data: {tralbum_data}")
        logger.debug(f"album head data: {head_data}")

        if not tralbum_data["hasAudio"]:
            logger.error(f"{url} has no audio.")
            return album_data

        head_id = head_data.get("@id")
        # fallback if a track link was provided
        # track releases have this inAlbum key even if they're standalone
        album_release = head_data.get("inAlbum", head_data)["albumRelease"]
        # find the albumRelease object that matches the overall album @id link
        # this will ensure that strictly the page release is downloaded
        album_release = next(obj for obj in album_release if obj["@id"] == head_id)

        if "offers" not in album_release:
            logger.error(f"{url} has no available offers.")

        if tralbum_data["freeDownloadPage"]:
            logger.info(f"{url} does not require email")
            dlret = self._download_file(
                tralbum_data["freeDownloadPage"], self.options.format
            )
        elif album_release["offers"]["price"] == 0.0:
            logger.info(f"{url} requires email")
            email_post_url = urljoin(url, "/email_download")
            r = self.session.post(
                email_post_url,
                data={
                    "encoding_name": "none",
                    "item_id": tralbum_data["current"]["id"],
                    "item_type": tralbum_data["current"]["type"],
                    "address": self.options.email,
                    "country": self.options.country,
                    "postcode": self.options.zipcode,
                },
            )
            r.raise_for_status()
            r = r.json()
            if not r["ok"]:
                raise ValueError(f"Bad response when sending email address: {r}")
            type = tralbum_data["current"]["type"]
            id = tralbum_data["current"]["id"]
            album_data["email_queued"] = True
            self.queued_emails[(type, id)] = album_data
            return album_data
        elif tralbum_data["is_purchased"]:
            collection_info = soup.find(
                "script", {"data-tralbum-collect-info": True}
            ).attrs["data-tralbum-collect-info"]
            collection_info = json.loads(collection_info)
            dlret = self._download_purchased_album(
                collection_info["fan_id"], tralbum_data
            )
        else:
            logger.error(
                f"{url} is not free. If you have purchased this album, "
                "use the --cookies flag or --identity flag to pass your login cookie."
            )
            return album_data

        album_data["is_downloaded"] = True
        album_data["file_name"] = dlret["file_name"]

        return album_data

    # unconditionally download from release page
    def download_label(self, soup: BeautifulSoup) -> dict:
        info = BCFreeDownloader.get_label_info(soup)

        for release in info["releases"]:
            logger.info(f"Downloading {release["url"]}")

            r = self.session.get(release["url"])
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            try:
                ret = self.download_album(soup)
                release["release_info"] = ret
            except BCFreeDownloadError as ex:
                logger.info(ex)

        return info

    # unconditionally downloads the provided url
    # returns either the result of download_album or download_label
    # with the `page_type` set to album|song|band
    # exception if download error
    def download_url(self, url: str, force: bool = False):
        r = self.session.get(url)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        page_type = self.get_page_info(soup)["type"]
        if page_type == "album" or page_type == "song":
            ret = self.download_album(soup, force)
        elif page_type == "band":
            ret = self.download_label(soup, force)
        else:
            raise BCFreeDownloadError(f"{url} does not have a valid og:type value")

        ret["page_type"] = page_type

        return ret

    def flush_email_downloads(self) -> set:
        checked_ids = set()
        downloaded = dict()
        # timeout count--if we go 30 seconds without any new emails
        # and we are still waiting, we probably had some emails dropped / expired
        timeout_count = 0
        while len(self.queued_emails) > 0:
            logger.info(f"Waiting for {len(self.queued_emails)} emails from Bandcamp...")
            time.sleep(5)
            email_list = self.mail_session.get_all_emails()
            for email in email_list:
                timeout_count = 0
                if email["mail_id"] in checked_ids:
                    continue

                checked_ids.add(email["mail_id"])
                if (
                    email["mail_from"] == ("noreply@bandcamp.com")
                    and "download" in email["mail_subject"]
                ):
                    logger.info(f'Received email "{email["mail_subject"]}"')
                    email = self.mail_session.get_email(email["mail_id"])
                    match = self.LINK_REGEX.search(email["mail_body"])
                    if match:
                        download_url = match.group("url")
                        dlret = self._download_file(download_url, self.options.format)
                        self.queued_emails.pop(dlret["id"])
                        downloaded[dlret["id"]] = dlret["file_name"]
            if email_list:
                self.mail_session.del_emails([e['mail_id'] for e in email_list])
            timeout_count += 1

            if timeout_count > 5:
                logger.info(f'Not all emails received. Resending missed ones...')
                self.queued_emails = 0
                for album_data in self.queued_emails:
                    soup = self.get_url(album_data["tralbum_data"]["url"])
                    self.download_album(soup)
        return downloaded

    # get_url_x can't be staticmethods because of special session context
    def get_url(self, url: str) -> BeautifulSoup:
        r = self.session.get(url)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")

    def get_url_info(self, url: str) -> dict:
        r = self.session.get(url)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        return self.get_page_info(soup)

    @staticmethod
    def get_page_info(soup: BeautifulSoup) -> dict:
        page_type = soup.head.find("meta", attrs={"property": "og:type"}).get("content")

        match page_type:
            case "album" | "song":
                return { "type": page_type, "info": BCFreeDownloader.get_album_info(soup) }
            case "band":
                return { "type": page_type, "info": BCFreeDownloader.get_label_info(soup) }
            case _:
                logger.error(f"{url} does not have a valid og:type value")
                return None

    @staticmethod
    def get_label_info(soup: BeautifulSoup):
        label_info = soup.find("script", attrs={"data-band": True})
        if label_info is None:
            raise BCFreeDownloadError("Page has no data-band.")
        label_info = json.loads(label_info["data-band"])

        releases = []
        # needed for releases
        local_url = label_info["local_url"]

        # bandcamp splits the release between this music-grid html and some json blob
        grid = soup.find("ol", id="music-grid")
        for li in grid.find_all("li"):
            if "display:none" in li.get("style", ""):
                continue

            data = li["data-item-id"].split("-")
            # most important fields
            releases.append({
                "type": data[0],
                "id": int(data[1]),
                "url": li.a["href"],
                "band_id": li["data-band-id"]
            })
        for obj in json.loads(html.unescape(grid.get("data-client-items", {}))):
            if obj.get("filtered"):
                continue
            # normalize to fit the other half
            obj["url"] = obj.pop("page_url")
            releases.append(obj)

        # fixup local urls into global ones
        for release in releases:
            if release["url"][0] == "/":
                release["url"] = urljoin(local_url, release["url"])

        return {
            "label_info": label_info,
            "releases": releases
        }

    @staticmethod
    def get_album_info(soup: BeautifulSoup) -> dict:
        tralbum_data = soup.find("script", {"data-tralbum": True}).attrs["data-tralbum"]
        tralbum_data = json.loads(tralbum_data)
        head_data = soup.head.find("script", {"type": "application/ld+json"}, recursive=False).string
        head_data = json.loads(head_data)

        return {
            'tralbum_data': tralbum_data,
            'head_data': head_data
        }

class BCFreeDownloaderConfig:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.parser = ConfigParser()
        self.parser.read(config_path)
        atexit.register(self.save)

    def get(self, key):
        return self.parser["free-bandcamp-downloader"].get(key, None)

    def set(self, key, value):
        self.parser["free-bandcamp-downloader"][key] = value

    def save(self):
        with open(self.config_path, "w") as f:
            self.parser.write(f)

    def __str__(self):
        return pprint.pformat(dict(self.parser["free-bandcamp-downloader"]), indent=2)
