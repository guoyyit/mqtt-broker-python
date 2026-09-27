# 本地 MQTT 3.1.1 消息代理

一个内嵌在现有 FastAPI 服务进程中的本地 MQTT 3.1.1 broker。客户端通过本地回环
地址建立 TCP 连接即可完成发布/订阅消息交换，**不依赖任何外部服务**（无第三方
MQTT 库、无外部数据库）。协议编解码与状态机均为本仓库内的纯 Python 实现。

- 协议：MQTT 3.1.1（protocol level 4），QoS 0 / 1 / 2，保留消息，遗嘱，持久会话
- 传输：明文 TCP，默认仅监听 `127.0.0.1:1883`
- 持久化：可选本地 JSONL 预写日志（WAL）+ 原子快照，支持崩溃恢复
- 同进程 HTTP 接口：`GET /` 与 `GET /health`

## 快速开始

```bash
# 启动（默认账号 admin/public，可被环境变量覆盖）
uv run uvicorn main:app --host 127.0.0.1 --port 8000

# 运行测试
uv run pytest
```

HTTP 健康检查示例：

```json
GET /health
{"status":"running","listening":"127.0.0.1:1883",
 "online_connections":0,"sessions":0,"retained_topics":0}
```

## 配置项（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `MQTT_HOST` | `127.0.0.1` | 监听地址，默认仅本地回环 |
| `MQTT_PORT` | `1883` | MQTT TCP 端口 |
| `MQTT_CREDENTIALS` | `admin:public`（main 内置默认） | `user:pass[,user:pass]`；**置空串表示允许匿名** |
| `MQTT_DATA_DIR` | 未设置（纯内存） | WAL/快照目录；设置后启用持久会话与崩溃恢复 |
| `MQTT_RETRY_INTERVAL` | `5.0` | 未确认下行 QoS1/2 报文重传间隔（秒，保留参数） |
| `MQTT_AUDIT_LOG` | 未设置（写 stderr） | 接入审计 JSONL 文件路径 |
| `MQTT_NOSYNC` | `0` | `1` 时关闭 WAL fsync（仅压测用，会削弱崩溃语义） |

纯库方式使用 `mqtt_broker.config.BrokerConfig(...)` + `MQTTServer`，可完全程序化
配置（见 `tests/conftest.py`）。

## 支持的协议语义

### 1. 鉴权与审计

- 服务端配置了账号表时，CONNECT 必须携带匹配的用户名/口令，否则立即回
  CONNACK（rc=4 用户名/口令错误，rc=5 未授权）并关闭 TCP，后续报文一律不处理。
- 账号表为空时允许匿名接入。
- 接入成功、接入失败、正常/异常断开均输出一行 JSON 审计事件：
  `ts/event/outcome/client_id/username/remote/reason`。

### 2. 主题与通配符

- `+` 匹配恰好一个层级：`sport/+/player1` 匹配 `sport/tennis/player1`。
- `#` 只能作为末层且独占整层，匹配零个或多个层级：`sport/#` 同时匹配 `sport`。
- 发布主题名不允许含 `+`/`#` 或为空；非法过滤器 SUBACK 返回 0x80。
- `$` 开头的系统主题不被以 `+`/`#` 开头的过滤器匹配。
- 匹配结果在单把状态锁内确定，多过滤器命中同一订阅时取**最高有效 QoS**。

### 3. QoS 等级与重复/丢失边界

- **QoS0**：最多一次，无确认、不重传；离线持久会话不缓存 QoS0。
- **QoS1**：至少一次，PUBACK 确认；重连/重传可能导致重复投递（符合协议）。
- **QoS2**：恰好一次，服务端对入站报文按 packet id 建立两阶段事务
  （PUBLISH→PUBREC，PUBREL→PUBCOMP）；同一 packet id 的重传 PUBLISH
  幂等处理，**绝不二次投递**。
- 投递 QoS 取 `min(发布 QoS, 订阅最大 QoS)`，SUBACK 返回实际授予等级。
- 网络中断不改变既有确认结果：已收到 PUBACK/PUBCOMP 的消息不会因重连重放；
  未确认的下行消息在（持久会话）重连后以 DUP=1 按原顺序重传。

### 4. 保留消息

- 带 RETAIN 的发布更新（或空载荷时清除）该主题的保留消息。
- 保留消息在发布瞬间**不投递给既有订阅者**（MQTT 3.1.1 §3.3.1-3）。
- 每次**新订阅建立**时，若存在匹配的保留消息，立即以 RETAIN=1 下发最新一条；
  多个过滤器只对“新增”的过滤器触发，重复订阅不重复推送。
- 遗嘱消息是 §3.3.1-3 的例外：RETAIN 遗嘱既更新保留存储，也正常投递给当时的
  匹配订阅者（线上帧 RETAIN=0，仅新订阅即时下发时 RETAIN=1）。

### 5. 持久会话与遗嘱

- CONNECT 的 CleanSession=0：订阅、QoS1/2 离线消息、在途事务与遗嘱跨连接保留，
  重连 CONNACK 的 Session Present=1，并按原订阅恢复、按原顺序排空离线队列。
- CleanSession=1：连接结束即销毁全部会话状态。
- 遗嘱仅在**异常断开**（TCP 中断、keep-alive 1.5 倍宽限超时等）时发布；
  正常 DISCONNECT 删除遗嘱、绝不误发；服务端主动关停也不发遗嘱。
- 同一 ClientID 新连接接管旧连接（§3.1.4），旧 TCP 被关闭且不触发遗嘱。

### 6. 并发与顺序边界

- 单把异步锁保护会话表/在线表/订阅/保留索引，发布、订阅、重连、保留更新
  对共享状态的变更串行化，匹配集合在每次发布时确定。
- **同一连接内**：所有下行字节经连接写锁串行，消息顺序严格保持。
- **不同连接之间**：不承诺跨连接的全局先后顺序（各自独立 TCP 通道），
  但每个连接内部有序、消息不撕裂。

### 7. 崩溃恢复

- 所有改变持久状态的事件（会话/订阅/入队/在途/确认/保留/入站 QoS2 事务/遗嘱）
  **先写 WAL 并 fsync，再发送协议确认**，因此“已确认即已持久”，未完成事务
  不会被标记为已确认。
- 一次发布对多个订阅者产生的事件以**单批次单次 write+fsync** 落盘，
  崩溃时整批可见或整批不可见，不出现“只缓存了部分订阅”的中间状态。
- 快照写入临时文件后 `os.replace` 原子替换；重启先载快照再按序列号重放 WAL，
  且服务在 `start()` 恢复完成前不接受连接，不暴露中间状态。
- 恢复后：保留消息与订阅恢复；已确认消息不重放；未确认的下行消息回到离线队列，
  重连后以 DUP 重传；未完成的入站 QoS2 事务重建，发布者重传不重复投递。

## 代码结构

| 模块 | 职责 |
| --- | --- |
| `mqtt_broker/packet.py` | MQTT 3.1.1 报文编解码与协议校验 |
| `mqtt_broker/topic.py` | 主题校验、`+/#` 通配符匹配 |
| `mqtt_broker/config.py` | 配置与环境变量 |
| `mqtt_broker/audit.py` | JSONL 接入审计 |
| `mqtt_broker/storage.py` | WAL（批量 fsync）+ 原子快照 |
| `mqtt_broker/session.py` | 会话、订阅、离线队列、QoS 事务、包 ID 分配 |
| `mqtt_broker/broker.py` | 鉴权、路由匹配、转发、保留、遗嘱、恢复状态机 |
| `mqtt_broker/server.py` | TCP 接入、连接生命周期、keep-alive |
| `mqtt_broker/testclient.py` | 仅用于本地回环测试的异步 MQTT 客户端 |
| `main.py` | FastAPI 进程入口，lifespan 内启停 broker |

## 测试

```bash
uv run pytest                      # 全部用例
uv run pytest tests/test_qos.py    # 单个文件
```

覆盖：通配符与报文边界、鉴权/审计、QoS0/1/2 重复与丢失、保留消息（含清除与
重启）、持久会话与遗嘱（正常/异常/关停/接管）、并发顺序边界、keep-alive、
以及崩溃恢复（已确认不重放、未完成事务重传、批量原子性、恢复后不暴露中间状态）。
所有网络验证均使用 `127.0.0.1` 回环连接。
