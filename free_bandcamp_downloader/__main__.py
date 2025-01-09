"""Download free albums and tracks from Bandcamp
Usage:
    bcdl-free [--debug] [--force] [--no-unzip] [-al]
        [-d <dir>] [-e <email>] [-z <zipcode>] [-c <country>] [-f <format>]
        [--cookies <file>] [--identity <value>] URL...
    bcdl-free setdefault [-d <dir>] [-e <email>] [-z <zipcode>]
        [-c <country>] [-f <format>]
    bcdl-free defaults
    bcdl-free clear
    bcdl-free -h | --help | --version

Arguments:
    URL            URL to download. Can be a link to a label or release page

Subcommands:
    defaults       list default configuration options
    setdefaults    set default configuration options
    clear          clear default configuration options

Options:
    -h --help                            Show this screen
    --version                            Show version
    --force                              Download even if album has been downloaded before
    --no-unzip                           Don't unzip downloaded albums
    --debug                              Set loglevel to debug
    -a -l                                Dummy options, for backwards compatibility
    -d <dir> --dir <dir>                 Set download directory
    -c <country> --country <country>     Set country
    -z <zipcode> --zipcode <zipcode>     Set zipcode
    -e <email> --email <email>           Set email (set to 'auto' to automatically download from a disposable email)
    -f <format> --format <format>        Set format
    --cookies <file>                     Path to cookies.txt file so albums in your collection can be downloaded
    --identity <value>                   Value of identity cookie so albums in your collection can be downloaded

Formats:
    - FLAC
    - V0MP3
    - 320MP3
    - AAC
    - Ogg
    - ALAC
    - WAV
    - AIFF
"""

import dataclasses
import sys
import os
from typing import Tuple

from free_bandcamp_downloader import __version__
from free_bandcamp_downloader.bc_free_downloader import *

def get_config_dir():
    if "XDG_CONFIG_HOME" in os.environ:
        config_dir = os.path.join(
            os.environ["XDG_CONFIG_HOME"], "free-bandcamp-downloader"
        )
    else:
        config_dir = os.path.join(
            os.path.expanduser("~"), ".config", "free-bandcamp-downloader"
        )
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
    return config_dir

def get_data_dir():
    if "XDG_DATA_HOME" in os.environ:
        data_dir = os.path.join(
            os.environ["XDG_DATA_HOME"], "free-bandcamp-downloader"
        )
    else:
        data_dir = os.path.join(
            os.path.expanduser("~"), ".local", "share", "free-bandcamp-downloader"
        )
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    history_file = os.path.join(data_dir, "downloaded.txt")
    if not os.path.exists(history_file):
        with open(history_file, "w") as f:
            pass
    return data_dir

def get_config(data_dir: str, config_dir: str):
    default_config = f"""[free-bandcamp-downloader]
        country = United States
        zipcode = 00000
        email = auto
        format = FLAC
        dir = ."""
    config_file = os.path.join(config_dir, "free-bandcamp-downloader.cfg")
    if not os.path.exists(config_file):
        with open(config_file, "w") as f:
            f.write(default_config)
    config = BCFreeDownloaderConfig(config_file)
    return config

def is_downloaded(downloaded_set, id: Tuple[str, int], url: str = None):
    return id in downloaded_set or url in downloaded_set

def add_to_dl_file(data_dir: str, id: Tuple[str, int]):
    history_file = os.path.join(data_dir, "downloaded.txt")
    with open(history_file, "a") as f:
        f.write(f"{id[0][0]}:{id[1]}\n")

def get_downloaded(data_dir: str):
    downloaded = set()
    history_file = os.path.join(data_dir, "downloaded.txt")
    with open(history_file, "r") as f:
        for line in f:
            match line.strip()[:2]:
                case "a:":
                    type = "album"
                    data = int(line[2:])
                case "t:":
                    type = "track"
                    data = int(line[2:])
                case _:
                    type = "url"
                    data = line.strip()
            downloaded.add((type, data))

    return downloaded

def main():
    data_dir = get_data_dir()
    config_dir = get_config_dir()
    config = get_config(data_dir, config_dir)
    options = BCFreeDownloaderOptions()
    arguments = docopt(__doc__, version=__version__)

    if arguments["--debug"]:
        logger.setLevel(logging.DEBUG)

    # set options if needed
    if arguments["URL"] or arguments["setdefault"]:
        for field in dataclasses.fields(options):
            option = field.name
            arg = f"--{option}"
            if arguments[arg]:
                setattr(options, option, arguments[arg])
            else:
                setattr(options, option, config.get(option))
            if not getattr(options, option):
                logger.error(
                    f'{option} is not set, use "bcdl-free setdefault {arg} <{option}>"'
                )
                sys.exit(1)
        if options.format not in BCFreeDownloader.FORMATS:
            logger.error(
                f'{options["format"]} is not a valid format. See "bcdl-free -h" for valid formats'
            )
            sys.exit(1)

    if arguments["setdefault"]:
        # write arguments to config
        for field in dataclasses.fields(options):
            option = field.name
            arg = f"--{option}"
            if arguments[arg]:
                config.set(option, arguments[arg])
        sys.exit(0)

    if arguments["clear"]:
        with open(config.get("download_history_file"), "w"):
            pass
        sys.exit(0)

    if arguments["defaults"]:
        print(str(config))
        sys.exit(0)

    if arguments["URL"]:
        # init downloader
        downloader = BCFreeDownloader(
            options,
            config_dir,
            arguments["--cookies"],
            arguments["--identity"],
        )
        do_unzip = not arguments["--no-unzip"]
        downloaded = get_downloaded(data_dir)

        for url in arguments["URL"]:
            url_info = downloader.get_url_info(url)

            match url_info["type"]:
                case "album" | "song":
                    tralbum = url_info["info"]["tralbum_data"]
                    type = tralbum["current"]["type"]
                    id = tralbum["current"]["id"]
                    url = tralbum["url"]
                    if not arguments["--force"] and is_downloaded(downloaded, (type, id), url):
                        logger.error(
                            f"{url} already downloaded. To download anyways, use --force."
                        )
                        continue
                    soup = downloader.get_url(url)
                    ret = downloader.download_album(soup)
                    if ret["is_downloaded"]:
                        add_to_dl_file(data_dir, (type, id))
                        downloaded.add(data_dir, (type, id))
                case "band":
                    for rel in url_info["info"]["releases"]:
                        type = rel["type"]
                        id = rel["id"]
                        url = rel["url"]
                        if not arguments["--force"] and is_downloaded(downloaded, (type, id), ):
                            logger.error(
                                f"{url} already downloaded. "
                                "To download anyways, use --force."
                            )
                            continue
                        soup = downloader.get_url(url)
                        ret = downloader.download_album(soup)
                        if ret["is_downloaded"]:
                            add_to_dl_file(data_dir, (type, id))
                            downloaded.add((type, id))
                case _:
                    continue

        # finish up downloading
        ret = downloader.flush_email_downloads()
        for relid in ret.keys():
            add_to_dl_file(data_dir, relid)
            downloaded.add(relid)

if __name__ == "__main__":
    main()
