# osu! Subscriber

本工具用于根据 osu! Mapper ID 批量下载谱面。

首次使用前，请先配置 `config.json`。

---

## 一、首次使用

1. 复制 `config.example.json`
2. 将复制出来的文件重命名为：

   ```text
   config.json
   ```

3. 打开 `config.json`
4. 填写自己的 `client_id` 和 `client_secret`
5. 保存后双击运行：

   ```text
   osu_mapper_downloader.exe
   ```

> **注意**
>
> 不要把自己的 `config.json` 发给别人。  
> `config.json` 里包含 `client_secret`，属于敏感信息。

---

## 二、如何获取 client_id 和 client_secret

1. 登录 osu! 官网：

   <https://osu.ppy.sh/>

2. 打开账号设置页面：

   <https://osu.ppy.sh/home/account/edit>

3. 找到 `OAuth Applications` / `OAuth 应用`

4. 新建一个 OAuth Application

   应用名称可以随便填，例如：

   ```text
   osu mapper downloader
   ```

   Callback URL 如果必须填写，可以填：

   ```text
   http://localhost
   ```

5. 创建后复制：

   ```text
   client_id
   client_secret
   ```

6. 填入 `config.json` 对应位置。

---

## 三、config.json 主要填写说明

### 1. client_id

填写 osu! OAuth Application 中的 `client_id`。

示例：

```json
"client_id": "12345"
```

---

### 2. client_secret

填写 osu! OAuth Application 中的 `client_secret`。

示例：

```json
"client_secret": "xxxxxxxxxxxxxxxxxxxxxxxx"
```

> **注意**
>
> `client_secret` 不要发给别人，不要上传到网上。

---

### 3. default_user_id

程序启动时默认显示的 Mapper ID。

示例：

```json
"default_user_id": 1234567
```

---

### 4. download_dir

谱面保存目录。

示例：

```json
"download_dir": "osu_maps"
```

表示保存到程序同目录下的 `osu_maps` 文件夹。

也可以填写绝对路径：

```json
"download_dir": "D:/osu_maps"
```

---

### 5. with_video

是否下载带视频版本。

```text
false = 不下载视频，推荐
true  = 下载带视频版本，文件更大
```

推荐：

```json
"with_video": true
```

---

### 6. types

要下载的谱面类型。

示例：

```json
"types": ["ranked", "loved", "pending", "graveyard"]
```

可选值：

| 值 | 说明 |
|---|---|
| `ranked` | 已上架 |
| `loved` | Loved |
| `pending` | 待定 |
| `graveyard` | 坟场 |
| `favourite` | 收藏 |

---

### 7. cookies_file

osu! 官网回退下载时使用的 cookies 文件。

推荐保持：

```json
"cookies_file": "cookies.txt"
```

如果只用 Sayo 镜像站，通常不需要 `cookies.txt`。

---

### 8. prefer_sayo

是否优先使用 Sayo 镜像站。

推荐：

```json
"prefer_sayo": true
```

---

### 9. sayo_base_url

Sayo 镜像站地址。

默认：

```json
"sayo_base_url": "https://txy1.sayobot.cn"
```

---

### 10. fallback_to_osu

Sayo 下载失败后，是否尝试从 osu! 官网下载。

推荐：

```json
"fallback_to_osu": true
```

> **注意**
>
> 从 osu! 官网下载可能需要 `cookies.txt`。

---

### 11. max_workers

同时下载数量。

推荐 `2` 到 `4`。

示例：

```json
"max_workers": 3
```

---

### 12. use_api_filename

是否使用统一文件名。

推荐：

```json
"use_api_filename": true
```

---

## 四、config.json 示例

```json
{
  "client_id": "你的_client_id",
  "client_secret": "你的_client_secret",
  "default_user_id": "",
  "download_dir": "osu_maps",
  "with_video": false,
  "types": ["ranked", "loved", "pending", "graveyard"],
  "cookies_file": "cookies.txt",
  "prefer_sayo": true,
  "sayo_base_url": "https://txy1.sayobot.cn",
  "fallback_to_osu": true,
  "max_workers": 3,
  "use_api_filename": true
}
```

---

## 五、如何生成 cookies.txt

`cookies.txt` 只在 osu! 官网回退下载时可能需要。

如果 Sayo 镜像站可以正常下载，一般不用生成 `cookies.txt`。

### 生成方法

1. 使用 Chrome、Edge 或 Firefox 浏览器

2. 安装 cookies 导出插件，例如：

   ```text
   Get cookies.txt LOCALLY
   ```

3. 打开 osu! 官网并登录：

   <https://osu.ppy.sh/>

4. 使用插件导出当前网站 cookies

5. 保存文件名为：

   ```text
   cookies.txt
   ```

6. 将 `cookies.txt` 放到 exe 同一目录下

目录示例：

```text
osu_mapper_downloader/
├─ osu_mapper_downloader.exe
├─ config.json
└─ cookies.txt
```

> **注意**
>
> `cookies.txt` 非常敏感。  
> 不要分享 `cookies.txt`。  
> 不要上传到网上。  
> 不要放进公开压缩包。
>
> 如果 `cookies.txt` 泄露，建议退出 osu! 网页登录并重新登录。

---

## 六、批量 Mapper CSV 格式

如果需要批量导入 Mapper，请准备 CSV 文件。

CSV 必须包含表头：

```csv
mapper_id,mapper_name
```

示例：

```csv
mapper_id,mapper_name
id_1,mapper_1
id_2,mapper_2
id_3,
```

说明：

- `mapper_id` 必填
- `mapper_name` 可以留空,程序会自动补充

---

## 七、常见问题

### 1. 双击 exe 没反应

检查是否已经创建 `config.json`。

---

### 2. 提示 client_id 或 client_secret 为空

打开 `config.json`，填写自己的 `client_id` 和 `client_secret`。

---

### 3. 下载失败

可以尝试：

- 检查网络
- 降低 `max_workers`
- 更换 `sayo_base_url`
- 开启 `fallback_to_osu`
- 生成 `cookies.txt` 后再试

---

### 4. osu! 官网下载失败

通常需要 `cookies.txt`。

请登录 osu! 官网后重新导出 `cookies.txt`。

---

## 八、最终文件结构

首次下载后，目录一般是：

```text
osu_mapper_downloader/
├─ osu_mapper_downloader.exe
├─ config.example.json
├─ config.json
├─ README.md
├─ cookies.txt
└─ osu_maps/
```

说明：

| 文件 / 文件夹 | 说明 |
|---|---|
| `config.example.json` | 配置模板，可保留，也可删除 |
| `config.json` | 用户自己的配置文件，不要分享 |
| `cookies.txt` | 用户自己的 cookies 文件，不要分享 |
| `osu_maps/` | 下载后的谱面目录 |

---

## 九、不要分享的文件

不要分享以下文件：

```text
config.json
cookies.txt
osu_maps/
```

尤其是：

- `config.json` 可能包含 `client_secret`
- `cookies.txt` 可能包含你的 osu! 登录状态
