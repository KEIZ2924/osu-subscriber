import csv
import re
import time
import json
import pathlib
import threading
import requests

from datetime import datetime, timezone
from urllib.parse import unquote
from http.cookiejar import MozillaCookieJar
from concurrent.futures import ThreadPoolExecutor, as_completed

from constants import API_BASE, TOKEN_URL, DOWNLOAD_INFO_FILE


class OsuDownloaderCore:
    def __init__(self, config, log_func, progress_func, speed_func, stop_event):
            self.config = config
            self.log = log_func
            self.update_progress = progress_func
            self.update_speed = speed_func
            self.stop_event = stop_event

            self.info_file_path = pathlib.Path(DOWNLOAD_INFO_FILE)
            self.info_lock = threading.Lock()

    def get_access_token(self):
        self.log("正在获取 osu! API Access Token...")

        resp = requests.post(
            TOKEN_URL,
            json={
                "client_id": int(self.config["client_id"]),
                "client_secret": self.config["client_secret"],
                "grant_type": "client_credentials",
                "scope": "public",
            },
            timeout=30,
        )

        if resp.status_code != 200:
            self.log("获取 Access Token 失败")
            self.log(f"HTTP 状态码: {resp.status_code}")
            self.log(resp.text)
            resp.raise_for_status()

        self.log("Access Token 获取成功")
        return resp.json()["access_token"]

    def api_get(self, session, path, params=None):
        url = API_BASE + path

        while not self.stop_event.is_set():
            resp = session.get(url, params=params, timeout=30)

            if resp.status_code == 429:
                self.log("API 请求过快，触发限流，等待 30 秒...")
                time.sleep(30)
                continue

            if resp.status_code >= 400:
                self.log("API 请求失败")
                self.log(f"URL: {resp.url}")
                self.log(f"HTTP 状态码: {resp.status_code}")
                self.log(resp.text)

            resp.raise_for_status()
            return resp.json()

        raise RuntimeError("任务已停止")

    def fetch_user_info(self, session, user_id):
        self.log(f"正在获取用户信息: {user_id}")

        data = self.api_get(
            session,
            f"/users/{user_id}",
            params=None,
        )

        username = data.get("username", str(user_id))

        return {
            "id": data.get("id", user_id),
            "username": username,
        }

    def update_mapper_csv(self, csv_path, mapper_id, mapper_name):
        if not csv_path:
            return

        path = pathlib.Path(csv_path)

        if not path.exists():
            self.log(f"CSV 文件不存在，跳过更新: {path}")
            return

        mapper_id = str(mapper_id).strip()
        mapper_name = str(mapper_name).strip()

        if not mapper_id or not mapper_name:
            return

        name_separator = "，"

        rows = []
        fieldnames = ["mapper_id", "mapper_name"]

        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)

                if reader.fieldnames:
                    fieldnames = list(reader.fieldnames)

                    if "mapper_id" not in fieldnames:
                        self.log("CSV 缺少 mapper_id 字段，无法更新")
                        return

                    if "mapper_name" not in fieldnames:
                        fieldnames.append("mapper_name")

                    for row in reader:
                        rows.append(row)
                else:
                    self.log("CSV 文件没有表头，将使用标准表头 mapper_id,mapper_name")

        except Exception as e:
            self.log(f"读取 CSV 失败: {path}")
            self.log(str(e))
            return

        found_id = False
        changed = False

        for row in rows:
            old_id = str(row.get("mapper_id", "")).strip()

            if old_id != mapper_id:
                continue

            found_id = True

            old_names_text = str(row.get("mapper_name", "")).strip()

            if old_names_text:
                old_names = re.split(r"[，,|]+", old_names_text)
                old_names = [name.strip() for name in old_names if name.strip()]
            else:
                old_names = []

            if mapper_name in old_names:
                self.log(f"CSV 已存在 Mapper 名称: {mapper_id}, {mapper_name}")
                return

            old_names.append(mapper_name)
            row["mapper_name"] = name_separator.join(old_names)

            changed = True

            self.log(
                f"CSV 更新 Mapper 名称: {mapper_id}, "
                f"{old_names_text or '空'} -> {row['mapper_name']}"
            )

            break

        if not found_id:
            new_row = {}

            for key in fieldnames:
                new_row[key] = ""

            new_row["mapper_id"] = mapper_id
            new_row["mapper_name"] = mapper_name

            rows.append(new_row)

            changed = True

            self.log(f"CSV 追加新 Mapper: {mapper_id}, {mapper_name}")

        if not changed:
            return

        try:
            with open(path, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            self.log(f"CSV 已更新: {path}")

        except Exception as e:
            self.log(f"写入 CSV 失败: {path}")
            self.log(str(e))

    def fetch_user_beatmapsets(self, session, user_id, beatmapset_types):
        all_sets = {}

        for map_type in beatmapset_types:
            if self.stop_event.is_set():
                break

            self.log(f"正在获取用户 {user_id} 的 {map_type} 谱面...")

            offset = 0
            limit = 50

            while not self.stop_event.is_set():
                data = self.api_get(
                    session,
                    f"/users/{user_id}/beatmapsets/{map_type}",
                    params={
                        "limit": limit,
                        "offset": offset,
                    },
                )

                if not data:
                    break

                for beatmapset in data:
                    beatmapset_id = beatmapset["id"]
                    all_sets[beatmapset_id] = beatmapset

                self.log(f"  {map_type}: 本次获取 {len(data)} 个，offset={offset}")

                if len(data) < limit:
                    break

                offset += limit
                time.sleep(0.4)

        return list(all_sets.values())

    def load_browser_cookies(self, session, quiet=False):
        def qlog(message):
            if not quiet:
                self.log(message)

        cookies_file = self.config.get("cookies_file", "cookies.txt")

        if not cookies_file:
            qlog("未配置 cookies_file，osu! 官网回退下载可能失败。")
            return False

        cookies_path = pathlib.Path(cookies_file)

        if not cookies_path.exists():
            qlog(f"找不到 Cookie 文件: {cookies_path}")
            qlog("osu! 官网回退下载可能失败。")
            return False

        try:
            cookie_jar = MozillaCookieJar(str(cookies_path))
            cookie_jar.load(ignore_discard=True, ignore_expires=True)
            session.cookies.update(cookie_jar)

            qlog(f"已加载 Cookie 文件: {cookies_path}")

            cookie_names = [cookie.name for cookie in cookie_jar]

            if "osu_session" in cookie_names:
                qlog("已检测到 osu_session")
            else:
                qlog("警告：cookies.txt 中没有检测到 osu_session，osu! 官网下载可能失败。")

            if "cf_clearance" in cookie_names:
                qlog("已检测到 cf_clearance")

            return True

        except Exception as e:
            qlog(f"加载 Cookie 失败: {cookies_path}")
            qlog(str(e))
            return False

    def create_download_session(self, quiet=True):
        session = requests.Session()

        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8,"
                "application/octet-stream,application/zip"
            ),
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": "https://osu.ppy.sh/",
        })

        self.load_browser_cookies(session, quiet=quiet)
        return session

    @staticmethod
    def safe_filename(name):
        name = re.sub(r'[\\/:*?"<>|]', "_", str(name))
        name = name.strip()
        return name[:180]
    
    @staticmethod
    def parse_osu_datetime(value):
        """
        osu! API 常见时间格式：
        2026-02-15T12:34:56+00:00
        2026-02-15T12:34:56Z
        """
        if not value:
            return None

        try:
            value = str(value).strip()

            if value.endswith("Z"):
                value = value[:-1] + "+00:00"

            dt = datetime.fromisoformat(value)

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            return dt.astimezone(timezone.utc)

        except Exception:
            return None

    @staticmethod
    def now_utc_iso():
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def build_official_url(self, beatmapset_id):
        return f"https://osu.ppy.sh/beatmapsets/{beatmapset_id}"

    def load_all_download_info_unlocked(self):
        """
        注意：调用这个方法前应当已经持有 self.info_lock。
        """
        if not self.info_file_path.exists():
            return {
                "version": 1,
                "beatmapsets": {}
            }

        try:
            with open(self.info_file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not isinstance(data, dict):
                return {
                    "version": 1,
                    "beatmapsets": {}
                }

            if "beatmapsets" not in data or not isinstance(data["beatmapsets"], dict):
                data["beatmapsets"] = {}

            if "version" not in data:
                data["version"] = 1

            return data

        except Exception as e:
            self.log(f"读取统一信息 JSON 失败: {self.info_file_path}")
            self.log(str(e))

            return {
                "version": 1,
                "beatmapsets": {}
            }

    def save_all_download_info_unlocked(self, data):
        """
        注意：调用这个方法前应当已经持有 self.info_lock。
        使用临时文件写入，避免中途失败导致 JSON 损坏。
        """
        temp_path = self.info_file_path.with_suffix(".json.tmp")

        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            temp_path.replace(self.info_file_path)

        except Exception as e:
            self.log(f"写入统一信息 JSON 失败: {self.info_file_path}")
            self.log(str(e))

            try:
                if temp_path.exists():
                    temp_path.unlink()
            except Exception:
                pass

    def get_download_info(self, beatmapset_id):
        beatmapset_id = str(beatmapset_id)

        with self.info_lock:
            data = self.load_all_download_info_unlocked()
            return data.get("beatmapsets", {}).get(beatmapset_id)

    def save_download_info(
        self,
        beatmapset,
        filename,
        file_path,
        used_source,
        used_url,
        downloaded_at,
        with_video=None,
    ):
        beatmapset_id = beatmapset.get("id")

        if beatmapset_id is None:
            return

        beatmapset_id_str = str(beatmapset_id)

        submitted_date = beatmapset.get("submitted_date")
        last_updated = beatmapset.get("last_updated")

        artist = beatmapset.get("artist", "")
        title = beatmapset.get("title", "")
        creator = beatmapset.get("creator", "")
        user_id = beatmapset.get("user_id", "")

        info = {
            "beatmapset_id": beatmapset_id,
            "official_url": self.build_official_url(beatmapset_id),

            "song": {
                "artist": artist,
                "title": title,
                "artist_unicode": beatmapset.get("artist_unicode", ""),
                "title_unicode": beatmapset.get("title_unicode", ""),
                "source": beatmapset.get("source", ""),
                "tags": beatmapset.get("tags", ""),
            },

            "mapper": {
                "user_id": user_id,
                "username": creator,
            },

            "time": {
                "submitted_date": submitted_date,
                "last_updated": last_updated,
                "downloaded_at": downloaded_at,
            },

            "download": {
                "filename": filename,
                "file_path": str(file_path),
                "source": used_source,
                "url": used_url,
                "with_video": with_video,
            }
        }

        with self.info_lock:
            data = self.load_all_download_info_unlocked()

            data.setdefault("version", 1)
            data.setdefault("beatmapsets", {})

            data["beatmapsets"][beatmapset_id_str] = info

            self.save_all_download_info_unlocked(data)

        self.log(f"谱面信息已写入统一 JSON: {self.info_file_path} | {beatmapset_id}")

    def find_existing_osz(self, download_dir, beatmapset_id):
        files = list(download_dir.glob(f"{beatmapset_id} *.osz"))

        if files:
            return files[0]

        return None

    def should_redownload_existing_file(self, beatmapset, existing_path):
        """
        返回：
        True  = 需要重新下载
        False = 可以跳过
        """
        beatmapset_id = beatmapset.get("id")
        api_last_updated_raw = beatmapset.get("last_updated")

        api_last_updated = self.parse_osu_datetime(api_last_updated_raw)

        if api_last_updated is None:
            self.log(f"谱面 {beatmapset_id} 没有有效更新时间，跳过已存在文件")
            return False

        info = self.get_download_info(beatmapset_id)

        downloaded_at = None

        if info:
            downloaded_at_raw = (
                info.get("time", {}).get("downloaded_at")
                or info.get("downloaded_at")
            )
            downloaded_at = self.parse_osu_datetime(downloaded_at_raw)

        if downloaded_at is None:
            try:
                mtime = existing_path.stat().st_mtime
                downloaded_at = datetime.fromtimestamp(mtime, timezone.utc)
                self.log(
                    f"谱面 {beatmapset_id} 没有统一 JSON 下载记录，使用文件修改时间判断"
                )
            except Exception:
                self.log(f"谱面 {beatmapset_id} 无法判断本地下载时间，将重新下载")
                return True

        if downloaded_at < api_last_updated:
            self.log(
                f"谱面 {beatmapset_id} 已更新，需要重新下载: "
                f"本地下载时间={downloaded_at.isoformat(timespec='seconds')}，"
                f"官网更新时间={api_last_updated.isoformat(timespec='seconds')}"
            )
            return True

        self.log(
            f"跳过已存在且未更新: {beatmapset_id}，"
            f"本地下载时间={downloaded_at.isoformat(timespec='seconds')}，"
            f"官网更新时间={api_last_updated.isoformat(timespec='seconds')}"
        )

        return False


    def build_api_filename(self, beatmapset):
        beatmapset_id = beatmapset["id"]
        artist = beatmapset.get("artist", "unknown")
        title = beatmapset.get("title", "unknown")

        filename = f"{beatmapset_id} {artist} - {title}.osz"
        return self.safe_filename(filename)

    def build_user_download_dir(self, output_dir, user_id, username):
        base_dir = pathlib.Path(output_dir)
        base_dir.mkdir(parents=True, exist_ok=True)

        user_id_str = str(user_id)
        safe_username = self.safe_filename(username)

        new_folder_name = self.safe_filename(f"{user_id} [{safe_username}]")
        new_user_dir = base_dir / new_folder_name

        if new_user_dir.exists() and new_user_dir.is_dir():
            return new_user_dir

        existing_same_id_dirs = []

        for item in base_dir.iterdir():
            if not item.is_dir():
                continue

            if (
                item.name.startswith(user_id_str + " ")
                or item.name.startswith(user_id_str + "_")
                or item.name == user_id_str
            ):
                existing_same_id_dirs.append(item)

        if existing_same_id_dirs:
            old_dir = existing_same_id_dirs[0]

            if old_dir == new_user_dir:
                return new_user_dir

            try:
                old_dir.rename(new_user_dir)
                self.log(f"目录已重命名: {old_dir} -> {new_user_dir}")
                return new_user_dir
            except Exception as e:
                self.log(f"目录重命名失败，继续使用旧目录: {old_dir}")
                self.log(str(e))
                return old_dir

        new_user_dir.mkdir(parents=True, exist_ok=True)

        return new_user_dir

    def build_download_urls(self, beatmapset_id, with_video=False):
        prefer_sayo = bool(self.config.get("prefer_sayo", True))
        fallback_to_osu = bool(self.config.get("fallback_to_osu", True))

        sayo_base_url = self.config.get(
            "sayo_base_url",
            "https://txy1.sayobot.cn"
        ).rstrip("/")

        if with_video:
            sayo_url = f"{sayo_base_url}/beatmaps/download/full/{beatmapset_id}"
            osu_url = f"https://osu.ppy.sh/beatmapsets/{beatmapset_id}/download"
        else:
            sayo_url = f"{sayo_base_url}/beatmaps/download/novideo/{beatmapset_id}"
            osu_url = f"https://osu.ppy.sh/beatmapsets/{beatmapset_id}/download?noVideo=1"

        if prefer_sayo:
            urls = [("Sayo", sayo_url)]
            if fallback_to_osu:
                urls.append(("osu!", osu_url))
        else:
            urls = [("osu!", osu_url)]
            if fallback_to_osu:
                urls.append(("Sayo", sayo_url))

        return urls

    def get_filename_from_response(self, resp, fallback):
        content_disposition = resp.headers.get("Content-Disposition", "")

        match = re.search(
            r"filename\*=UTF-8''([^;]+)",
            content_disposition,
            re.IGNORECASE,
        )
        if match:
            filename = match.group(1).strip()
            filename = unquote(filename)
            return self.safe_filename(filename)

        match = re.search(
            r'filename="([^"]+)"',
            content_disposition,
            re.IGNORECASE,
        )
        if match:
            filename = match.group(1).strip()
            filename = unquote(filename)
            return self.safe_filename(filename)

        match = re.search(
            r"filename=([^;]+)",
            content_disposition,
            re.IGNORECASE,
        )
        if match:
            filename = match.group(1).strip().strip('"')
            filename = unquote(filename)
            return self.safe_filename(filename)

        return fallback

    @staticmethod
    def is_probably_osz_response(resp):
        content_type = resp.headers.get("Content-Type", "").lower()
        content_disposition = resp.headers.get("Content-Disposition", "").lower()

        if "application/x-osu-beatmap-archive" in content_type:
            return True

        if "application/octet-stream" in content_type:
            return True

        if "application/zip" in content_type:
            return True

        if ".osz" in content_disposition:
            return True

        return False

    def is_bad_download_response(self, resp):
        content_type = resp.headers.get("Content-Type", "").lower()
        content_disposition = resp.headers.get("Content-Disposition", "").lower()

        if resp.status_code != 200:
            return True

        if "text/html" in content_type:
            return True

        if "application/json" in content_type:
            return True

        if "text/plain" in content_type:
            return True

        if ".osz" in content_disposition:
            return False

        if "application/octet-stream" in content_type:
            return False

        if "application/x-osu-beatmap-archive" in content_type:
            return False

        if "application/zip" in content_type:
            return False

        return False

    def save_debug_response(self, resp, download_dir, beatmapset_id, source_name):
        content_type = resp.headers.get("Content-Type", "").lower()

        if "text/html" in content_type:
            suffix = "html"
        elif "application/json" in content_type:
            suffix = "json"
        else:
            suffix = "txt"

        debug_path = download_dir / f"debug_{source_name}_{beatmapset_id}.{suffix}"

        try:
            with open(debug_path, "w", encoding="utf-8", errors="ignore") as f:
                f.write(resp.text)
            self.log(f"调试响应已保存: {debug_path}")
        except Exception as e:
            self.log(f"保存调试响应失败: {e}")

    def download_beatmapset(self, beatmapset, download_dir, with_video=False):
        if self.stop_event.is_set():
            return False

        session = self.create_download_session(quiet=True)
        resp = None

        try:
            beatmapset_id = beatmapset["id"]

            artist = beatmapset.get("artist", "unknown")
            title = beatmapset.get("title", "unknown")

            fallback_name = self.build_api_filename(beatmapset)

            existing_path = self.find_existing_osz(download_dir, beatmapset_id)
            force_redownload = False

            if existing_path:
                if self.should_redownload_existing_file(beatmapset, existing_path):
                    force_redownload = True
                    self.log(f"准备重新下载已更新谱面: {existing_path}")
                else:
                    self.update_progress(100, 0, "已存在且未更新，跳过")

                    existing_info = self.get_download_info(beatmapset_id)

                    if not existing_info:
                        downloaded_at = datetime.fromtimestamp(
                            existing_path.stat().st_mtime,
                            timezone.utc
                        ).isoformat(timespec="seconds")

                        self.save_download_info(
                            beatmapset=beatmapset,
                            filename=existing_path.name,
                            file_path=existing_path,
                            used_source="existing",
                            used_url="",
                            downloaded_at=downloaded_at,
                            with_video=with_video,
                        )

                    return True


            download_urls = self.build_download_urls(beatmapset_id, with_video)

            self.log(f"正在下载: {beatmapset_id} - {artist} - {title}")

            used_source = None
            used_url = None

            for source_name, url in download_urls:
                if self.stop_event.is_set():
                    return False

                self.log(f"尝试使用 {source_name} 下载: {url}")

                try:
                    resp = session.get(
                        url,
                        stream=True,
                        timeout=120,
                        allow_redirects=True,
                    )
                except requests.RequestException as e:
                    self.log(f"{source_name} 下载请求失败: {beatmapset_id}")
                    self.log(str(e))
                    continue

                if resp.status_code == 429:
                    self.log(f"{source_name} 触发限速 HTTP 429，尝试下一个下载源...")
                    try:
                        resp.close()
                    except Exception:
                        pass
                    continue

                if resp.status_code in [401, 403]:
                    self.log(f"{source_name} 下载失败: HTTP {resp.status_code}")
                    self.log(f"下载链接: {url}")
                    try:
                        resp.close()
                    except Exception:
                        pass
                    continue

                if resp.status_code != 200:
                    self.log(f"{source_name} 下载失败: HTTP {resp.status_code}")
                    self.log(f"下载链接: {url}")
                    try:
                        resp.close()
                    except Exception:
                        pass
                    continue

                if self.is_bad_download_response(resp):
                    self.log(f"{source_name} 返回内容不像 osz 文件")
                    self.log(f"Content-Type: {resp.headers.get('Content-Type', '')}")
                    self.log(f"Content-Disposition: {resp.headers.get('Content-Disposition', '')}")
                    self.log(f"下载链接: {url}")

                    try:
                        self.save_debug_response(resp, download_dir, beatmapset_id, source_name)
                    except Exception:
                        pass

                    try:
                        resp.close()
                    except Exception:
                        pass

                    continue

                used_source = source_name
                used_url = url
                break

            if resp is None or used_source is None:
                self.log(f"所有下载源均失败: {beatmapset_id}")
                return False

            if not self.is_probably_osz_response(resp):
                self.log(f"警告: {used_source} 返回内容没有明确标记为 osz")
                self.log(f"Content-Type: {resp.headers.get('Content-Type', '')}")
                self.log(f"Content-Disposition: {resp.headers.get('Content-Disposition', '')}")
                self.log("仍会尝试保存。")

            use_api_filename = bool(self.config.get("use_api_filename", True))

            if use_api_filename:
                filename = fallback_name
            else:
                filename = self.get_filename_from_response(resp, fallback_name)

            if not filename.lower().endswith(".osz"):
                filename += ".osz"

            filename = self.safe_filename(filename)
            path = download_dir / filename

            if path.exists() and not force_redownload:
                self.log(f"跳过已存在: {path}")
                self.update_progress(100, 0, "已存在，跳过")
                try:
                    resp.close()
                except Exception:
                    pass
                return True


            temp_path = download_dir / f"{filename}.part"

            try:
                total = int(resp.headers.get("Content-Length", 0))
                downloaded = 0

                last_time = time.time()
                last_downloaded = 0

                self.log(f"使用下载源: {used_source}")
                self.log(f"最终下载链接: {used_url}")

                with open(temp_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 512):
                        if self.stop_event.is_set():
                            self.log("检测到停止请求，正在中止当前下载...")
                            raise RuntimeError("用户停止任务")

                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)

                            now = time.time()
                            elapsed = now - last_time

                            if elapsed >= 0.3:
                                speed = (downloaded - last_downloaded) / elapsed
                                last_time = now
                                last_downloaded = downloaded

                                if total > 0:
                                    percent = downloaded * 100 / total
                                    status = (
                                        f"{used_source} | "
                                        f"{downloaded / 1024 / 1024:.2f} MB / "
                                        f"{total / 1024 / 1024:.2f} MB"
                                    )
                                    self.update_progress(percent, downloaded, status)
                                else:
                                    self.update_progress(
                                        0,
                                        downloaded,
                                        f"{used_source} | {downloaded / 1024 / 1024:.2f} MB"
                                    )

                                self.update_speed(speed)

                temp_path.replace(path)


            except Exception as e:
                self.log(f"保存文件失败或下载中止: {path}")
                self.log(str(e))

                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except Exception:
                        pass

                return False

            downloaded_at = self.now_utc_iso()

            self.save_download_info(
                beatmapset=beatmapset,
                filename=filename,
                file_path=path,
                used_source=used_source,
                used_url=used_url,
                downloaded_at=downloaded_at,
                with_video=with_video,
            )

            self.update_progress(100, downloaded, f"{used_source} | 完成")
            self.update_speed(0)
            self.log(f"已保存: {path}")

            return True

        finally:
            try:
                if resp is not None:
                    resp.close()
            except Exception:
                pass

            try:
                session.close()
            except Exception:
                pass

    def run_download_task(self, user_ids, selected_types, with_video, output_dir, mapper_csv_path=None):
        token = self.get_access_token()

        api_session = requests.Session()
        api_session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "osu-map-downloader-gui/1.0",
        })

        test_download_session = self.create_download_session(quiet=False)
        try:
            test_download_session.close()
        except Exception:
            pass

        prefer_sayo = bool(self.config.get("prefer_sayo", True))
        fallback_to_osu = bool(self.config.get("fallback_to_osu", True))
        sayo_base_url = self.config.get("sayo_base_url", "https://txy1.sayobot.cn")
        max_workers = int(self.config.get("max_workers", 3))
        max_workers = max(1, min(max_workers, 8))
        use_api_filename = bool(self.config.get("use_api_filename", True))

        self.log("")
        self.log(f"下载源设置: prefer_sayo={prefer_sayo}, fallback_to_osu={fallback_to_osu}")
        self.log(f"Sayo Base URL: {sayo_base_url}")
        self.log(f"并行下载线程数: {max_workers}")
        self.log(f"统一使用 API 文件名: {use_api_filename}")
        self.log(f"Mapper CSV: {mapper_csv_path or '未使用'}")

        for user_index, user_id in enumerate(user_ids, start=1):
            if self.stop_event.is_set():
                break

            self.log("")
            self.log("=" * 60)
            self.log(f"开始处理 Mapper ID: {user_id}  [{user_index}/{len(user_ids)}]")
            self.log("=" * 60)

            user_info = self.fetch_user_info(api_session, user_id)
            username = user_info.get("username", str(user_id))

            self.update_mapper_csv(
                csv_path=mapper_csv_path,
                mapper_id=user_id,
                mapper_name=username,
            )

            user_download_dir = self.build_user_download_dir(
                output_dir=output_dir,
                user_id=user_id,
                username=username,
            )

            self.log(f"Mapper 名称: {username}")
            self.log(f"保存目录: {user_download_dir}")

            beatmapsets = self.fetch_user_beatmapsets(
                api_session,
                user_id,
                selected_types,
            )

            total_maps = len(beatmapsets)

            self.log(f"用户 {user_id} 共找到 {total_maps} 个谱面集")

            if total_maps == 0:
                continue

            completed_count = 0
            success_count = 0
            fail_count = 0

            future_map = {}

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for index, beatmapset in enumerate(beatmapsets, start=1):
                    if self.stop_event.is_set():
                        break

                    beatmapset_id = beatmapset.get("id", "unknown")

                    self.log(f"提交下载任务 [{index}/{total_maps}]: {beatmapset_id}")

                    future = executor.submit(
                        self.download_beatmapset,
                        beatmapset,
                        user_download_dir,
                        with_video,
                    )

                    future_map[future] = {
                        "index": index,
                        "beatmapset": beatmapset,
                    }

                for future in as_completed(future_map):
                    info = future_map[future]
                    beatmapset = info["beatmapset"]
                    beatmapset_id = beatmapset.get("id", "unknown")

                    if self.stop_event.is_set():
                        self.log("检测到停止请求，正在等待已启动的下载线程结束...")
                        for f in future_map:
                            f.cancel()
                        break

                    try:
                        ok = future.result()
                    except Exception as e:
                        ok = False
                        self.log(f"下载任务异常: {beatmapset_id}")
                        self.log(str(e))

                    completed_count += 1

                    if ok:
                        success_count += 1
                    else:
                        fail_count += 1

                    overall_percent = completed_count * 100 / max(total_maps, 1)

                    self.update_progress(
                        overall_percent,
                        0,
                        (
                            f"用户 {user_id} 总体进度 "
                            f"{completed_count}/{total_maps} | "
                            f"成功 {success_count} | 失败 {fail_count}"
                        )
                    )

                    self.log(
                        f"完成进度: {completed_count}/{total_maps} | "
                        f"成功 {success_count} | 失败 {fail_count}"
                    )

        try:
            api_session.close()
        except Exception:
            pass

        if self.stop_event.is_set():
            self.log("")
            self.log("任务已停止。")
        else:
            self.log("")
            self.log("全部任务完成。")
