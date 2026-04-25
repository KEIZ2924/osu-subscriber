import json
import csv
import re
import time
import pathlib
import threading
import queue
import requests
import tkinter as tk

from tkinter import ttk, filedialog, messagebox
from urllib.parse import unquote
from http.cookiejar import MozillaCookieJar
from concurrent.futures import ThreadPoolExecutor, as_completed


CONFIG_FILE = "config.json"

API_BASE = "https://osu.ppy.sh/api/v2"
TOKEN_URL = "https://osu.ppy.sh/oauth/token"

VALID_TYPES = [
    "favourite",
    "ranked",
    "loved",
    "pending",
    "graveyard",
]


class OsuDownloaderCore:
    def __init__(self, config, log_func, progress_func, speed_func, stop_event):
        self.config = config
        self.log = log_func
        self.update_progress = progress_func
        self.update_speed = speed_func
        self.stop_event = stop_event

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
        """
        获取 Mapper 用户信息。
        """
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
        """
        更新 Mapper CSV。

        CSV 格式：
        mapper_id,mapper_name

        规则：
        1. 每个 mapper_id 只保留一行。
        2. mapper_name 保存该 ID 的历史名字列表。
        3. 如果 API 返回的新名字已存在，不重复添加。
        4. 如果 API 返回的新名字不存在，追加到 mapper_name 字段。
        5. 如果 mapper_id 不存在，新增一行。
        """
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

    def build_api_filename(self, beatmapset):
        """
        对齐 osu! 官网命名：
        2444352 BABYMETAL - Road of Resistance.osz
        """
        beatmapset_id = beatmapset["id"]
        artist = beatmapset.get("artist", "unknown")
        title = beatmapset.get("title", "unknown")

        filename = f"{beatmapset_id} {artist} - {title}.osz"
        return self.safe_filename(filename)

    def build_user_download_dir(self, output_dir, user_id, username):
        """
        创建 Mapper 保存目录。

        目标格式：
        osu_maps/{user_id} [{username}]

        例如：
        osu_maps/8570499 [shiyu]

        如果已经存在同 ID 开头的目录：
        1. 如果是旧格式会尝试重命名
        2. 如果已经是新格式直接复用
        """
        base_dir = pathlib.Path(output_dir)
        base_dir.mkdir(parents=True, exist_ok=True)

        user_id_str = str(user_id)
        safe_username = self.safe_filename(username)

        new_folder_name = self.safe_filename(f"{user_id} [{safe_username}]")
        new_user_dir = base_dir / new_folder_name

        # 如果新格式目录已经存在，直接使用
        if new_user_dir.exists() and new_user_dir.is_dir():
            return new_user_dir

        # 查找同 ID 的旧目录或其他格式目录
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

        # 如果找到同 ID 目录，优先尝试迁移成新目录名
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

        # 没有任何同 ID 目录，则创建新格式目录
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

            existing_files = list(download_dir.glob(f"{beatmapset_id} *.osz"))
            if existing_files:
                self.log(f"跳过已存在: {beatmapset_id}")
                self.update_progress(100, 0, "已存在，跳过")
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

            if path.exists():
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

                temp_path.rename(path)

            except Exception as e:
                self.log(f"保存文件失败或下载中止: {path}")
                self.log(str(e))

                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except Exception:
                        pass

                return False

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


class OsuDownloaderGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("osu! Subscriber")
        self.root.geometry("1050x780")

        self.log_queue = queue.Queue()
        self.progress_queue = queue.Queue()
        self.speed_queue = queue.Queue()

        self.worker_thread = None
        self.stop_event = threading.Event()

        self.config = self.load_config()

        self.create_widgets()
        self.root.after(100, self.process_queues)

    def load_config(self):
        config_path = pathlib.Path(CONFIG_FILE)

        if not config_path.exists():
            messagebox.showerror(
                "错误",
                f"找不到配置文件 {CONFIG_FILE}，请先创建 config.json"
            )
            raise FileNotFoundError(CONFIG_FILE)

        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)

        required_keys = [
            "client_id",
            "client_secret",
            "default_user_id",
            "download_dir",
            "with_video",
            "types",
        ]

        for key in required_keys:
            if key not in config:
                messagebox.showerror("错误", f"config.json 缺少字段: {key}")
                raise RuntimeError(f"config.json 缺少字段: {key}")

        config.setdefault("cookies_file", "cookies.txt")
        config.setdefault("prefer_sayo", True)
        config.setdefault("sayo_base_url", "https://txy1.sayobot.cn")
        config.setdefault("fallback_to_osu", True)
        config.setdefault("max_workers", 3)
        config.setdefault("use_api_filename", True)

        return config

    def create_widgets(self):
        main_frame = ttk.Frame(self.root, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)

        input_frame = ttk.LabelFrame(main_frame, text="MapperId", padding=10)
        input_frame.pack(fill=tk.X)

        ttk.Label(input_frame, text="Mapper ID:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)

        self.mapper_id_var = tk.StringVar(value=str(self.config.get("default_user_id", "")))
        self.mapper_entry = ttk.Entry(input_frame, textvariable=self.mapper_id_var, width=30)
        self.mapper_entry.grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)

        ttk.Label(input_frame, text="Mappers CSV:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)

        self.batch_file_var = tk.StringVar(value="")
        self.batch_file_entry = ttk.Entry(input_frame, textvariable=self.batch_file_var, width=70)
        self.batch_file_entry.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)

        self.select_file_button = ttk.Button(input_frame, text="选择 CSV", command=self.select_batch_file)
        self.select_file_button.grid(row=1, column=2, padx=5, pady=5)

        ttk.Label(input_frame, text="osz保存目录:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)

        self.output_dir_var = tk.StringVar(value=self.config.get("download_dir", "osu_maps"))
        self.output_dir_entry = ttk.Entry(input_frame, textvariable=self.output_dir_var, width=70)
        self.output_dir_entry.grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)

        self.select_output_button = ttk.Button(input_frame, text="选择目录", command=self.select_output_dir)
        self.select_output_button.grid(row=2, column=2, padx=5, pady=5)

        option_frame = ttk.LabelFrame(main_frame, text="下载选项", padding=10)
        option_frame.pack(fill=tk.X, pady=10)

        self.with_video_var = tk.BooleanVar(value=bool(self.config.get("with_video", False)))
        self.with_video_check = ttk.Checkbutton(
            option_frame,
            text="下载带视频版本",
            variable=self.with_video_var
        )
        self.with_video_check.grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)

        self.prefer_sayo_var = tk.BooleanVar(value=bool(self.config.get("prefer_sayo", True)))
        self.prefer_sayo_check = ttk.Checkbutton(
            option_frame,
            text="优先使用Sayo镜像",
            variable=self.prefer_sayo_var
        )
        self.prefer_sayo_check.grid(row=0, column=1, sticky=tk.W, padx=10, pady=5)

        self.fallback_to_osu_var = tk.BooleanVar(value=bool(self.config.get("fallback_to_osu", True)))
        self.fallback_to_osu_check = ttk.Checkbutton(
            option_frame,
            text="失败后使用 osu! 官网",
            variable=self.fallback_to_osu_var
        )
        self.fallback_to_osu_check.grid(row=0, column=2, sticky=tk.W, padx=10, pady=5)

        self.use_api_filename_var = tk.BooleanVar(value=bool(self.config.get("use_api_filename", True)))
        self.use_api_filename_check = ttk.Checkbutton(
            option_frame,
            text="统一文件名（建议开启）",
            variable=self.use_api_filename_var
        )
        self.use_api_filename_check.grid(row=0, column=3, sticky=tk.W, padx=10, pady=5)

        ttk.Label(option_frame, text="Sayo 地址:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)

        self.sayo_base_url_var = tk.StringVar(
            value=self.config.get("sayo_base_url", "https://txy1.sayobot.cn")
        )
        self.sayo_base_url_entry = ttk.Entry(
            option_frame,
            textvariable=self.sayo_base_url_var,
            width=50
        )
        self.sayo_base_url_entry.grid(row=1, column=1, columnspan=3, sticky=tk.W, padx=5, pady=5)

        ttk.Label(option_frame, text="并行线程数:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)

        self.max_workers_var = tk.IntVar(value=int(self.config.get("max_workers", 3)))
        self.max_workers_spinbox = tk.Spinbox(
            option_frame,
            from_=1,
            to=8,
            textvariable=self.max_workers_var,
            width=6
        )
        self.max_workers_spinbox.grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)

        ttk.Label(
            option_frame,
            text="建议2-4避免触发限速"
        ).grid(row=2, column=2, columnspan=2, sticky=tk.W, padx=5, pady=5)

        type_frame = ttk.LabelFrame(main_frame, text="谱面类型", padding=10)
        type_frame.pack(fill=tk.X)

        self.type_vars = {}
        default_types = set(self.config.get("types", ["ranked", "loved", "pending", "graveyard"]))

        for i, map_type in enumerate(VALID_TYPES):
            var = tk.BooleanVar(value=map_type in default_types)
            self.type_vars[map_type] = var

            check = ttk.Checkbutton(
                type_frame,
                text=map_type,
                variable=var
            )
            check.grid(row=0, column=i, sticky=tk.W, padx=12, pady=5)

        progress_frame = ttk.LabelFrame(main_frame, text="进度", padding=10)
        progress_frame.pack(fill=tk.X, pady=10)

        ttk.Label(progress_frame, text="进度:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)

        self.file_progress_var = tk.DoubleVar(value=0)
        self.file_progress_bar = ttk.Progressbar(
            progress_frame,
            variable=self.file_progress_var,
            maximum=100,
            length=700
        )
        self.file_progress_bar.grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)

        self.file_progress_label_var = tk.StringVar(value="0%")
        self.file_progress_label = ttk.Label(progress_frame, textvariable=self.file_progress_label_var)
        self.file_progress_label.grid(row=0, column=2, sticky=tk.W, padx=5, pady=5)

        ttk.Label(progress_frame, text="下载速度:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)

        self.speed_label_var = tk.StringVar(value="0 KB/s")
        self.speed_label = ttk.Label(progress_frame, textvariable=self.speed_label_var)
        self.speed_label.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)

        ttk.Label(progress_frame, text="状态:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)

        self.status_label_var = tk.StringVar(value="等待开始")
        self.status_label = ttk.Label(progress_frame, textvariable=self.status_label_var)
        self.status_label.grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)

        self.start_button = ttk.Button(button_frame, text="开始下载", command=self.start_download)
        self.start_button.pack(side=tk.LEFT, padx=5)

        self.stop_button = ttk.Button(button_frame, text="停止", command=self.stop_download, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5)

        self.clear_log_button = ttk.Button(button_frame, text="清空日志", command=self.clear_log)
        self.clear_log_button.pack(side=tk.LEFT, padx=5)

        log_frame = ttk.LabelFrame(main_frame, text="日志输出", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True)

        self.log_text = tk.Text(log_frame, wrap=tk.WORD, height=20)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.log_text.configure(yscrollcommand=scrollbar.set)

    def select_batch_file(self):
        file_path = filedialog.askopenfilename(
            title="选择批量 Mapper CSV 文件",
            filetypes=[
                ("CSV files", "*.csv"),
                ("All files", "*.*"),
            ],
        )

        if file_path:
            self.batch_file_var.set(file_path)

    def select_output_dir(self):
        dir_path = filedialog.askdirectory(title="选择保存目录")

        if dir_path:
            self.output_dir_var.set(dir_path)

    def parse_user_ids(self):
        ids = []

        single_id = self.mapper_id_var.get().strip()

        if single_id:
            if single_id.isdigit():
                ids.append(int(single_id))
            else:
                raise ValueError("单个 Mapper ID 必须是数字")

        csv_file = self.batch_file_var.get().strip()

        if csv_file:
            path = pathlib.Path(csv_file)

            if not path.exists():
                raise FileNotFoundError(f"Mapper CSV 文件不存在: {csv_file}")

            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)

                if not reader.fieldnames:
                    raise ValueError("CSV 文件为空或没有表头")

                if "mapper_id" not in reader.fieldnames:
                    raise ValueError("CSV 文件必须包含 mapper_id 字段")

                for row in reader:
                    mapper_id = str(row.get("mapper_id", "")).strip()

                    if not mapper_id:
                        continue

                    if not mapper_id.isdigit():
                        raise ValueError(f"CSV 中存在非法 mapper_id: {mapper_id}")

                    ids.append(int(mapper_id))

        ids = list(dict.fromkeys(ids))

        if not ids:
            raise ValueError("请至少输入一个 Mapper ID 或选择 Mapper CSV 文件")

        return ids

    def get_selected_types(self):
        selected = [
            map_type
            for map_type, var in self.type_vars.items()
            if var.get()
        ]

        if not selected:
            raise ValueError("请至少选择一种谱面类型")

        return selected

    def start_download(self):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("提示", "任务已经在运行中")
            return

        try:
            user_ids = self.parse_user_ids()
            selected_types = self.get_selected_types()
        except Exception as e:
            messagebox.showerror("输入错误", str(e))
            return

        output_dir = self.output_dir_var.get().strip()
        mapper_csv_path = self.batch_file_var.get().strip() or None

        if not output_dir:
            messagebox.showerror("输入错误", "保存目录不能为空")
            return

        pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

        with_video = self.with_video_var.get()

        self.config["prefer_sayo"] = self.prefer_sayo_var.get()
        self.config["fallback_to_osu"] = self.fallback_to_osu_var.get()
        self.config["use_api_filename"] = self.use_api_filename_var.get()
        self.config["sayo_base_url"] = (
            self.sayo_base_url_var.get().strip()
            or "https://txy1.sayobot.cn"
        )

        try:
            max_workers = int(self.max_workers_var.get())
        except Exception:
            max_workers = 3

        max_workers = max(1, min(max_workers, 8))
        self.config["max_workers"] = max_workers

        self.stop_event.clear()

        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)

        self.log_message("准备开始下载任务")
        self.log_message(f"Mapper ID 列表: {user_ids}")
        self.log_message(f"谱面类型: {selected_types}")
        self.log_message(f"保存目录: {output_dir}")
        self.log_message(f"Mapper CSV: {mapper_csv_path or '未使用'}")
        self.log_message(f"下载视频: {'是' if with_video else '否'}")
        self.log_message(f"优先使用 Sayo: {'是' if self.config['prefer_sayo'] else '否'}")
        self.log_message(f"Sayo 地址: {self.config['sayo_base_url']}")
        self.log_message(f"失败后回退 osu!: {'是' if self.config['fallback_to_osu'] else '否'}")
        self.log_message(f"并行线程数: {self.config['max_workers']}")
        self.log_message(f"统一使用 API 文件名: {'是' if self.config['use_api_filename'] else '否'}")
        self.log_message("")

        core = OsuDownloaderCore(
            config=self.config,
            log_func=self.log_message_threadsafe,
            progress_func=self.update_progress_threadsafe,
            speed_func=self.update_speed_threadsafe,
            stop_event=self.stop_event,
        )

        self.worker_thread = threading.Thread(
            target=self.worker_wrapper,
            args=(core, user_ids, selected_types, with_video, output_dir, mapper_csv_path),
            daemon=True,
        )

        self.worker_thread.start()

    def worker_wrapper(self, core, user_ids, selected_types, with_video, output_dir, mapper_csv_path):
        try:
            core.run_download_task(
                user_ids=user_ids,
                selected_types=selected_types,
                with_video=with_video,
                output_dir=output_dir,
                mapper_csv_path=mapper_csv_path,
            )
        except Exception as e:
            self.log_message_threadsafe("任务发生错误:")
            self.log_message_threadsafe(str(e))
        finally:
            self.log_message_threadsafe("__TASK_FINISHED__")

    def stop_download(self):
        if self.worker_thread and self.worker_thread.is_alive():
            self.stop_event.set()
            self.log_message("正在请求停止任务，请等待当前下载线程结束...")
            self.stop_button.config(state=tk.DISABLED)

    def clear_log(self):
        self.log_text.delete("1.0", tk.END)

    def log_message(self, message):
        timestamp = time.strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)

    def log_message_threadsafe(self, message):
        self.log_queue.put(message)

    def update_progress_threadsafe(self, percent, downloaded, status):
        self.progress_queue.put((percent, downloaded, status))

    def update_speed_threadsafe(self, speed):
        self.speed_queue.put(speed)

    @staticmethod
    def format_speed(speed):
        if speed <= 0:
            return "0 KB/s"

        if speed < 1024:
            return f"{speed:.0f} B/s"

        if speed < 1024 * 1024:
            return f"{speed / 1024:.2f} KB/s"

        return f"{speed / 1024 / 1024:.2f} MB/s"

    def process_queues(self):
        try:
            while True:
                message = self.log_queue.get_nowait()

                if message == "__TASK_FINISHED__":
                    self.start_button.config(state=tk.NORMAL)
                    self.stop_button.config(state=tk.DISABLED)
                    self.status_label_var.set("任务结束")
                    self.speed_label_var.set("0 KB/s")
                else:
                    self.log_message(message)

        except queue.Empty:
            pass

        try:
            while True:
                percent, downloaded, status = self.progress_queue.get_nowait()

                if percent < 0:
                    percent = 0

                if percent > 100:
                    percent = 100

                self.file_progress_var.set(percent)
                self.file_progress_label_var.set(f"{percent:.1f}%")
                self.status_label_var.set(status)

        except queue.Empty:
            pass

        try:
            while True:
                speed = self.speed_queue.get_nowait()
                self.speed_label_var.set(self.format_speed(speed))

        except queue.Empty:
            pass

        self.root.after(100, self.process_queues)


def main():
    root = tk.Tk()
    app = OsuDownloaderGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
