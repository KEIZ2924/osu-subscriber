import json
import csv
import time
import pathlib
import threading
import queue
import tkinter as tk
import json
import csv


from tkinter import ttk, filedialog, messagebox

from constants import CONFIG_FILE, VALID_TYPES, DOWNLOAD_INFO_FILE, DOWNLOAD_INFO_CSV_FILE
from core import OsuDownloaderCore


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

        input_frame = ttk.LabelFrame(main_frame, text="Mapper_id", padding=10)
        input_frame.pack(fill=tk.X)

        ttk.Label(input_frame, text="Mapper ID:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)

        self.mapper_id_var = tk.StringVar(value=str(self.config.get("default_user_id", "")))
        self.mapper_entry = ttk.Entry(input_frame, textvariable=self.mapper_id_var, width=30)
        self.mapper_entry.grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)

        ttk.Label(input_frame, text="Mappers csv:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)

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

        self.export_csv_button = ttk.Button(
            button_frame,
            text="导出记录 CSV",
            command=self.export_download_info_csv
        )
        self.export_csv_button.pack(side=tk.LEFT, padx=5)

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

    def export_download_info_csv(self):
        json_path = pathlib.Path(DOWNLOAD_INFO_FILE)

        if not json_path.exists():
            messagebox.showwarning(
                "提示",
                f"找不到下载记录文件:\n{json_path}"
            )
            return

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            messagebox.showerror(
                "错误",
                f"读取 JSON 失败:\n{json_path}\n\n{e}"
            )
            return

        beatmapsets = data.get("beatmapsets", {})

        if not isinstance(beatmapsets, dict) or not beatmapsets:
            messagebox.showwarning(
                "提示",
                "JSON 中没有可导出的 beatmapsets 数据"
            )
            return

        default_csv_path = pathlib.Path(DOWNLOAD_INFO_CSV_FILE)

        csv_path = filedialog.asksaveasfilename(
            title="导出下载记录 CSV",
            initialfile=default_csv_path.name,
            defaultextension=".csv",
            filetypes=[
                ("CSV files", "*.csv"),
                ("All files", "*.*"),
            ],
        )

        if not csv_path:
            return

        fieldnames = [
            "beatmapset_id",
            "official_url",

            "artist",
            "title",
            "artist_unicode",
            "title_unicode",
            "source",
            "tags",

            "mapper_id",
            "mapper_name",

            "submitted_date",
            "last_updated",
            "downloaded_at",

            "download_source",
            "download_url",
            "with_video",
            "filename",
            "file_path",
        ]

        rows = []

        for beatmapset_id, info in beatmapsets.items():
            if not isinstance(info, dict):
                continue

            song = info.get("song", {}) or {}
            mapper = info.get("mapper", {}) or {}
            time_info = info.get("time", {}) or {}
            download = info.get("download", {}) or {}

            row = {
                "beatmapset_id": info.get("beatmapset_id", beatmapset_id),
                "official_url": info.get("official_url", ""),

                "artist": song.get("artist", ""),
                "title": song.get("title", ""),
                "artist_unicode": song.get("artist_unicode", ""),
                "title_unicode": song.get("title_unicode", ""),
                "source": song.get("source", ""),
                "tags": song.get("tags", ""),

                "mapper_id": mapper.get("user_id", ""),
                "mapper_name": mapper.get("username", ""),

                "submitted_date": time_info.get("submitted_date", ""),
                "last_updated": time_info.get("last_updated", ""),
                "downloaded_at": time_info.get("downloaded_at", ""),

                "download_source": download.get("source", ""),
                "download_url": download.get("url", ""),
                "with_video": download.get("with_video", ""),
                "filename": download.get("filename", ""),
                "file_path": download.get("file_path", ""),
            }

            rows.append(row)

        def sort_key(row):
            try:
                return int(row.get("beatmapset_id", 0))
            except Exception:
                return 0

        rows.sort(key=sort_key)

        try:
            with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

        except Exception as e:
            messagebox.showerror(
                "错误",
                f"导出 CSV 失败:\n{csv_path}\n\n{e}"
            )
            return

        self.log_message(f"已导出下载记录 CSV: {csv_path}")
        self.log_message(f"导出数量: {len(rows)}")

        messagebox.showinfo(
            "完成",
            f"CSV 导出完成。\n\n文件:\n{csv_path}\n\n数量: {len(rows)}"
        )


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
