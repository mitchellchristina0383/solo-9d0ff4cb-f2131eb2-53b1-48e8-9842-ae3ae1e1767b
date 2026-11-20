# 流式故障切换网关（FastAPI + 两个可复现本地模拟节点）

在网页输入提示词、输出词元上限与可选的**回答结束标记**，观察回复**逐词元生成**；
首节点（node-a）中途超时后，网关用「原提示词 + 已采纳词元」让备用节点（node-b）
续写。网关只追加新词元、忽略首节点迟到回包，整条回复共用一个词元上限；接续成功
时结果与无故障场景**逐词元完全一致**；两节点模型/分词器版本不一致或备用续写
失败时，保留已显示文本并标记为**未完成**。

另有一个可选的**回答结束标记**：按空格分隔的完整词元匹配，首次完整命中即结束
生成，命中的词元计入上限但不发给调用方；标记可以横跨主备切换；留空时行为不变。

## 架构

```
浏览器 (web/)  ──SSE──▶  网关 gateway/app.py  ──SSE──▶  node-a (首节点)
                          │  读超时/出错后                │  挂起后仍产生
                          └──────────────▶  node-b (备用)  └─▶ 迟到回包（被忽略）
```

* `common/deterministic.py`：确定性「解码」。第 i 个词元是
  `sha256("mock-llm/v1:" + prompt + ":" + i)` 对共享词表取模的纯函数，无随机数、
  无时钟、无机器标识。两个节点共用同一模块，因此健康续写必然复现同一词元流，
  结果可复现（同提示词多次请求、跨节点结果一致，有测试覆盖）。
* `node/app.py`：同一个 FastAPI 应用部署为 node-a / node-b 两个实例，SSE 流式
  输出 `meta / token / error / done` 事件；支持按请求注入挂起、失败、版本伪装，
  并在挂起后由脱离请求的后台任务继续产生「迟到回包」（记录在 `/debug/late`）。
* `gateway/app.py`：编排状态机，负责读超时判定、只追加接受、共享预算、身份校验
  与故障切换，并向浏览器转发 SSE 事件。
* `web/`：原生 HTML/CSS/JS 单页，逐词元渲染（按来源节点着色）、展示切换/未完成
  事件，并自动与 `/api/expected` 的无故障基准核对；另有结束标记输入框、演示预设
  按钮，以及「命中标记 / 达到上限 / 未完成」三种终态展示。

## 启动

### Docker Compose（交付方式）

```bash
docker compose up --build
# 打开 http://localhost:8080
```

三个服务：node-a、node-b（仅内网 expose，带健康检查）、gateway（映射 8080）。
节点每 0.12s 出一个词元，网关读超时 `GATEWAY_READ_TIMEOUT=1.0s`，挂起场景会在
约 1s 后触发切换。

### 无 Docker 时本地运行（本机试跑所用方式）

```bash
python -m pip install -r requirements.txt
python run_demo.py          # 真实 uvicorn + HTTP 跑 4 个原场景 + 4 个标记场景
python -m pytest            # 21 项端到端/单元行为测试
```

网页演示同样可以在本地跑（本机验证即用这种方式）。下面的命令与 compose 里的
环境变量一致，只是把地址换成本机端口，打开 `http://127.0.0.1:8080`：

```bash
# Web demo without Docker: same commands/env as the containers, local ports.
NODE_A_URL=http://127.0.0.1:8901 NODE_B_URL=http://127.0.0.1:8902 \
  GATEWAY_READ_TIMEOUT=1.0 \
  python -m uvicorn gateway.app:app --host 0.0.0.0 --port 8080 &
NODE_ID=node-a EMIT_INTERVAL=0.12 \
  python -m uvicorn node.app:app --host 127.0.0.1 --port 8901 &
NODE_ID=node-b EMIT_INTERVAL=0.12 \
  python -m uvicorn node.app:app --host 127.0.0.1 --port 8902 &
```

## 四个演示场景（网关侧按请求注入，可重复）

| 场景 | node-a 行为 | node-b 行为 | 最终结果 |
|---|---|---|---|
| `healthy` | 正常生成 | 不参与 | 12/12 词元，基准结果 |
| `failover_success` | 第 4 个词元后挂起 → 网关读超时 | 用原提示词 + 4 个已采纳词元续写 | 12/12，与 healthy **完全一致** |
| `failover_backup_error` | 同上挂起 | 续写至绝对下标 7 时失败 | 保留 7 个词元，标记未完成 |
| `version_mismatch` | 同上挂起 | 伪装为 `mock-llm-7b-hotfix` / `tok-v2` | 拒绝接续，保留 4 个词元，标记未完成 |

## 需求与实现的对应

1. **网页输入提示词与词元上限、逐步生成**：`web/index.html` + SSE，每个 `token`
   事件即时追加渲染，计数器实时更新。
2. **首节点中途超时与迟到回包**：节点收到 `stall_before` 后挂起；网关用
   `asyncio.wait_for` 做**逐事件读超时**（不是整体请求超时）。挂起节点脱离请求
   的后台任务 1.5s 后继续产出迟到词元并打日志/落 `/debug/late`。
3. **备用节点用原提示词 + 已采纳词元续写**：网关携带 `prompt`、`start=已采纳数`、
   `accepted_ids`、剩余预算调用 node-b；节点侧对 `accepted_ids` 做前缀校验
   （不匹配返回 409）。
4. **只追加、忽略迟到输出、单一词元上限**：网关维护 `next_index`，只接受严格
   等于下一下标的词元，重复/乱序（含迟到）一律忽略；备用节点预算为
   `max_tokens - 已采纳数`，全流总量恒等于一个上限。前端有同款下标守卫。
5. **接续成功与无故障一致**：确定性解码 + 绝对下标续写，测试与页面对照
   `/api/expected` 双重断言词元 id 序列逐位相同。
6. **版本不一致 / 续写失败 → 保留已显示文本并标未完成**：切换前网关先 GET
   node-b `/info` 比对 `(model_id, tokenizer_version)`，不一致直接发 `incomplete`
   且不发起续写；续写流返回错误或提前结束同样发 `incomplete`，事件中带回完整
   `prefix_text`，已下发词元不会被撤回或改写。
7. **可选结束标记**：见下一节；网关在 `token` 事件与调用方之间加了一层
   `MarkerGate`，只暂存可能拼成标记的尾部，命中后吞掉标记并终止，暂存状态跨
   主备切换保留。

## 回答结束标记（可选）

`POST /api/chat` 增加可选字段 `stop_marker`（字符串，≥1 个空格分隔的词元，最长
200 字符；留空或全空白 = 不启用）。语义：

* **按完整词元匹配**：标记按空格拆成词元序列，与节点输出的词元文本逐位比较；
  不会在词元内部或下标中间切分。
* **不属于标记的词元照常逐个发出**：网关照常每收到一个词元就转发；只有「仍可能
  拼成标记」的尾缀（长度 < 标记词元数）会被暂存。候选一旦被下一个词元打破就立即
  补发，因此正常文本几乎无额外延迟。
* **首次完整命中即结束**：整段命中词元被吞掉（不进入 `/api/chat` 输出流），之后
  不再生成；命中的词元仍计入 `max_tokens` 上限。`done` 事件带
  `reason="stop_marker"`、`marker_index`、`marker_end_index`、`token_count`
  （含标记）与 `emitted_count`（实际下发）。
* **标记可横跨主备切换**：切换时备用节点从网关**已采纳的全部词元**续写，包括
  还没发出去的暂存词元（`accepted_ids`/`start` 都按已采纳数，不受暂存影响）。
  被暂存的前缀词元与备用节点的词元拼接后同样能命中标记。
* **上限用尽或故障终止时的尾部**：若末尾只是标记的一部分（例如上限刚好停在标记
  的中间），这部分不再等待，**照常作为正文发出**；`done`/`incomplete` 事件用
  `partial_marker` 指出这段尾部。
* **调用方取正文**：拿 `token` 事件的 `text`（或拼 `prefix_text`），不要包含
  `done.marker_index` 起的词元——它们本来就没发到输出流；`emitted_count` 可直接
  用于核对取到的词元数。
* **前端三种终态**：`命中结束标记`（展示被吞掉、未下发的词元）、`达到上限`、
  `未完成`（并提示尾部半截标记已发出）。

网页「结束标记快速填充」按钮会根据当前提示词从确定性词元流里选出演示用标记
（无故障命中、跨节点命中、上限/故障时的尾部半截），方便在页面上复现。

## SSE 事件（网关 → 浏览器）

`meta`（请求信息与标记词元）→ 若干 `token`（含绝对下标 index、词元 id、来源
node）→ 可能的 `fallback`（切换原因、已采纳数、因标记而暂存的数量）
→ 更多 `token` → `done`（`reason` 为 `max_tokens` 或 `stop_marker`，含各节点
词元占比、`emitted_count`、命中的标记下标或 `partial_marker`）或
`incomplete`（原因、身份信息、保留前缀、`partial_marker`）。

> `token_count`/`accepted_count` 统计**已采纳**词元（含被标记吞掉的部分），
> `emitted_count` 才是调用方实际收到的词元数；未启用标记时两者相等。

## 验证结果

本机（无 Docker，使用真实 uvicorn socket）已跑通：

* `python -m pytest`：**21/21 通过**，覆盖健康基准、接续等价性、共享预算、备用
  失败、版本不一致、迟到回包被忽略、节点前缀校验、跨节点/跨次确定性，以及结束
  标记的主节点命中、上限处命中、跨主备命中、整个标记落在备用节点上、候选被打破
  后补发、空标记保持原行为、三种尾部半截标记（上限、续写失败、版本不一致拒绝
  续写）以及标记匹配器（暂存、首次命中、重叠候选、强制放行）单元测试。
* `python run_demo.py`：四个原场景 + 四个标记场景全部通过自动核对。
  failover_success 的最终词元序列与 healthy 逐位相同（node-a 4 个 + node-b 8 个）；
  两个失败场景保留前缀与基准同位置前缀一致；标记场景分别验证了主节点命中
  （`forest bridge`，下发 2 个）、跨节点命中（`bridge jasmine`，node-a 暂存
  `bridge`、node-b 的 `jasmine` 补全后两者一起被吞，下发 3 个）、上限处半截
  标记（`over zzz` 下发全部 12 个并标出半截）和故障处半截标记（`anchor zzz`
  保留 7 个并标出半截）；node-a 被放弃后确实产生了迟到词元且均未被采纳。
* 静态资源冒烟：`/`、`/app.js`、`/styles.css` 与各 API 均 200。
* 页面行为（jsdom 驱动真实网关 HTTP 流）：无标记时 12 个词元 + 「达到上限」；
  `forest bridge` → 「命中结束标记」且只显示 `garden dragon`；`bridge jasmine`
  + failover_success → 只显示 `garden dragon forest`，并提示 1 个词元被暂存；
  `over zzz` → 12 个词元 + 「达到上限」并提示尾部半截；`anchor zzz` +
  failover_backup_error → 7 个词元 + 「未完成」并提示尾部半截；全部核对通过。

## 遗留事项与简化

1. **Docker Compose 未在本机构建验证**：当前环境没有 docker 守护进程、也没有
   安装权限（apt 无权限、无法拉取 docker 安装脚本），因此 `docker compose up`
   未能实际执行；本次演示与全部验证使用的是与容器内相同的启动命令
   （`uvicorn node.app:app` / `uvicorn gateway.app:app`）+ 同样的环境变量，只把
   `node-a`/`node-b` 的地址换成 `127.0.0.1`。compose/Dockerfile 未改动，仍建议在
   有 Docker 的环境执行一次 `docker compose up --build` 确认。
2. 故障注入是**演示机制**：场景表和 `X-Mock-*` 头只用于可重复演示，生产网关
   不会信任节点的自我身份声明；真实系统应改用带外配置/证书校验模型身份。
3. 仅模拟单一首要→单一备用、一次切换；未实现多级备用、重试退避、熔断与
   跨请求持久化（迟到回包记录保存在节点内存中，重启即失）。
4. 确定性「模型」用词表哈希代替真实推理，重点是可复现的词元契约与切换语义，
   不代表真实 LLM 的解码质量。
5. 前端为零依赖原生页面，未做无障碍/国际化等增强；接口仅做了基本入参校验
   （提示词长度、词元上限 1–128、标记最长 200 字符，超出返回 422）。
6. 结束标记的匹配是**逐词元文本比较**（区分大小写、按空格拆分），不做子词/
   标点归一化；标记最长按 200 字符限制，且只支持首次匹配后停止，不支持多个
   停止序列或「不吞掉标记」的可选项。
