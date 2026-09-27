# mqtt-broker-python

在本仓库的 FastAPI 服务进程内嵌了一个兼容 MQTT 3.1.1 基本特性的本地消息代理。
服务启动时（FastAPI lifespan）在同一事件循环中拉起 MQTT TCP 监听，仅绑定本地回环
地址，供本地客户端建立连接并完成发布/订阅消息交换；不依赖任何外部服务。

## 运行

```bash
uv run fastapi dev main.py        # 开发模式启动 HTTP + MQTT
uv run uvicorn main:app           # 或直接 uvicorn
```

- HTTP 接口：`GET /`（原有示例）、`GET /mqtt/status`（查看 MQTT 监听地址）。
- MQTT 监听：默认 `127.0.0.1:1883`（TCP，MQTT 3.1.1，协议名 `MQTT`、级别 4）。

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `MQTT_HOST` | `127.0.0.1` | MQTT 监听地址（仅回环） |
| `MQTT_PORT` | `1883` | MQTT 监听端口，`0` 表示随机端口 |
| `MQTT_USERS` | `admin:secret` | 逗号分隔的 `用户名:口令` 列表 |
| `MQTT_ALLOW_ANONYMOUS` | `0` | `1` 时允许无用户名匿名接入 |
| `MQTT_DB_PATH` | `broker_state.db` | SQLite 持久化文件（会话/保留消息/QoS 状态） |
| `MQTT_AUDIT_LOG` | `broker_audit.log` | 审计日志文件（JSON Lines） |

## 协议语义说明

### 1. 接入鉴权与审计
- CONNECT 携带的用户名/口令与 `MQTT_USERS` 比对（常量时间比较）。
- 鉴权失败立即返回 CONNACK 返回码 4（Bad username or password）并关闭连接；
  空客户端 ID 且 `clean_session=0` 返回码 2。
- 接入成功（`connect_ok`）、拒绝（`connect_rejected`）、断开（`disconnect`）、
  会话接管（`takeover`）、遗嘱发布（`will_published`）均写入审计日志
  （JSON Lines，同时保留在 `broker.audit.events` 便于测试断言）。

### 2. 主题通配符
- `+` 匹配且仅匹配一个层级；`#` 必须是最后一层，匹配任意剩余层级（含零层，
  即 `a/#` 也匹配 `a` 本身）。
- 以 `+`/`#` 开头的过滤器不匹配 `$` 开头的系统主题（MQTT 3.1.1 §4.7.2）。
- 匹配规则是纯函数，结果确定且可解释（见 `mqtt_broker/topics.py`）。
- 消息只投递给主题匹配且**当前在线**的连接；离线持久会话进入缓存（见第 5 节）。
- 同一客户端多个订阅命中同一主题时，只投递一份，QoS 取各订阅授权的最大值。

### 3. 服务质量（QoS 0/1/2）
- QoS 0：尽力投递，可能丢失，不重复。
- QoS 1：PUBLISH→PUBACK。未确认的出站消息在持久会话中落盘，重连后以
  DUP=1 重发（至少一次，可能重复）。
- QoS 2：PUBLISH→PUBREC→PUBREL→PUBCOMP 四步握手。入站消息在收到 PUBREL
  时才投递给订阅者，且事务状态先落盘再回 PUBREC，保证恰好一次；重复的
  PUBLISH(DUP) 只会重收 PUBREC，不会重复投递。
- 投递 QoS = min(发布 QoS, 订阅 QoS)。
- 网络中断/重连不改变既有确认结果：已确认的不会重发，未确认的按等级重发。

### 4. 保留消息
- PUBLISH 带 RETAIN 且载荷非空：保存为该主题最新保留消息（每主题仅一条）。
- PUBLISH 带 RETAIN 且载荷为空：清除该主题的保留消息。
- 新订阅建立（SUBACK 之后）立即收到所有匹配过滤器的保留消息，RETAIN=1，
  QoS = min(保留 QoS, 订阅 QoS)。保留消息持久化，重启后仍有效。

### 5. 持久会话与遗嘱
- `clean_session=0`：会话（订阅关系、离线期间匹配到的 QoS>0 消息、未确认
  的出站消息）持久化。重连时 CONNACK 的 Session Present=1，缓存消息按原
  订阅恢复投递。离线期间 QoS 0 消息按协议语义丢弃（不缓存）。
- `clean_session=1`：连接断开即删除全部会话状态；以 clean 方式重连同一会话
  ID 也会先清空旧状态。
- 遗嘱：异常断开（TCP 断开、keepalive 超时、协议错误、会话被接管）时按约定
  发布遗嘱；收到 DISCONNECT 的正常断开绝不发布遗嘱。遗嘱可带 RETAIN/QoS。
- 同一 client id 重复连接时，旧连接被关闭（会话接管），新连接接管会话。

### 6. 并发与顺序
- broker 核心运行在单一 asyncio 事件循环上，所有状态变更在同一把锁内完成；
  每个连接一条 FIFO 出站队列，单写者协程保证**同一连接内严格按序**。
- 不同连接之间不承诺全局顺序（仅保证因果顺序）：某连接慢消费不会阻塞他人。
- 持久会话的出站 QoS>0 消息**先落盘再发送**，确认后才删除持久副本。

### 7. 崩溃恢复
- 全部持久状态存于 SQLite（WAL + `synchronous=FULL`）：保留消息、持久会话、
  订阅、离线队列、入站 QoS2 事务。
- 每个变更先提交再对外确认，因此崩溃后：已确认投递不会重发；未确认事务
  （如只完成 PUBREC 的 QoS 2）重启后可由 PUBREL 继续完成，且不会被提前
  标记为已确认；重启过程中不对外提供服务，恢复完成后才接受连接。

## 测试

```bash
uv run pytest tests/ -q
```

覆盖：鉴权与审计、通配符匹配、QoS 0/1/2 握手与重发边界、保留消息、
持久会话与离线缓存、遗嘱（正常/异常断开）、并发与顺序、崩溃恢复、
FastAPI 进程内集成。测试仅使用本地回环连接。

## 代码结构

```
main.py                 FastAPI 应用，lifespan 中启动/停止 MQTT 服务
mqtt_broker/
  config.py             BrokerConfig 配置
  packet.py             MQTT 3.1.1 报文编解码
  topics.py             主题校验与通配符匹配
  auth.py               用户名/口令鉴权
  audit.py              JSON Lines 审计日志
  storage.py            SQLite 持久化（保留消息/会话/队列/QoS2 事务）
  session.py            会话状态
  broker.py             核心：会话、订阅、路由、QoS 流程、遗嘱
  server.py             asyncio TCP 服务与连接读写循环
tests/                  pytest 测试与同步 MQTT 测试客户端
```
