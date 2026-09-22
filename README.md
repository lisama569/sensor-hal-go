# sensor-hal-go

一个可完全在本地运行的传感器驱动与硬件抽象层（HAL）。真实硬件由
内存模拟的寄存器总线 `SimBus` 替代，不依赖任何真实设备或外部服务。

## 总线模型

- 驱动只通过 `Bus` 接口（`ReadReg` / `WriteReg`，8 位寄存器、16 位地址）
  访问设备。
- `SimBus` 用内存数组模拟寄存器空间，支持故障注入（`Faults`）：
  - `TimeoutAddrs`：访问指定地址时总线不应答（超时）；
  - `NackAddrs`：设备主动拒绝访问；
  - `CorruptReads`：指定地址读回被 XOR 污染的数据（模拟回读不一致）；
  - `Clear()`：清空全部寄存器，模拟掉电丢配置。
- 每次读写都记录到 `AccessLog`（定长环形缓冲），测试日志中可打印
  全部读写激励；容量有界，长时间采样内存占用保持稳定。

## 寄存器映射与版本兼容

固定布局（见 `hal/regs.go`）：`RegVersion`(0x00)、`RegDeviceID`(0x01)、
`RegMagic`(0x02)、`RegCtrl`(0x03)、`RegRate`(0x04)、`RegGain`(0x05)、
`RegDataHi/Lo`(0x06/0x07)、`RegStatus`(0x08)、`RegLowPower`(0x09，v2 新增)。

兼容判定是确定性的（`CheckCompatibility`）：

| 设备报告版本 | 判定 | 原因 |
|---|---|---|
| 1..2（已知） | 接受 | 完全支持 |
| 0 / 未定义 | 拒绝 | `rejected-unknown-version` |
| > 2（更新） | 拒绝 | `rejected-too-new` |

规则：新版本只允许**追加**寄存器，既有配置字段（采样率、增益）的语义
在任何版本下保持一致；v2 新增的 `LowPower` 字段在 v1 设备上会被
`KindInvalidConfig` 显式拒绝，而不是被静默忽略或重新解释。

## 错误分类

所有失败在对外接口前归一化为 `*hal.Error`，用 `hal.KindOf(err)` 区分：

| Kind | 含义 |
|---|---|
| `KindTimeout` | 总线超时（读会重试，重试仍失败则如实上报） |
| `KindNack` | 设备拒绝（确定性的拒绝，绝不重试） |
| `KindReadbackMismatch` | 写后回读不一致 |
| `KindInvalidConfig` | 配置非法（在任何总线访问之前拒绝） |
| `KindIncompatibleVersion` | 版本未知或过新（原因可在消息中区分） |
| `KindSuspended` | 挂起期间访问（快速失败，不阻塞、不返回陈旧数据） |
| `KindNotInitialized` | 未初始化即使用 |

重试策略：只有**读**在超时时重试（上限 `maxRetries`）；**写**只执行一次
并立即回读校验，失败绝不重试，因此重试既不会掩盖失败，也不会产生
重复副作用。经历过重试的采样会被标记为 `QualityDegraded`。

## 电源状态迁移

```
uninitialized --Init--> active <--Suspend/Resume--> suspended
```

- `Suspend`：关闭转换（`RegCtrl=0`，回读校验）后进入挂起；幂等。
- 挂起期间的 `Sample` 立即返回 `KindSuspended`，不阻塞、不提供旧数据。
- `Resume`：先读 `RegMagic`。magic 丢失说明挂起期间掉电、配置已丢，
  此时**完整重新初始化**（重写全部配置寄存器），而不是沿用旧状态；
  magic 仍在则只重新使能转换。
- 所有状态迁移记录在定长的迁移日志中（`Device.Transitions()`）。

## 并发与资源稳定性

- `Device` 全部方法可并发调用；互斥锁覆盖采样的高低字节两次读，
  杜绝撕裂读；无忙等、无每采样 goroutine。
- 访问日志与迁移日志均为定长结构，长时间运行资源占用稳定
  （`TestLongRunResourceStability` 验证）。

## 本地验证

```sh
go build ./...
go vet ./...
go test ./...        # 功能测试
go test -race ./...  # 并发数据竞争检测
go test -v -run TestPowerLossDuringSuspendForcesReinit ./hal
# 单测日志（-v）会打印每条读写激励与每次电源状态迁移
```

测试覆盖：正常探测/初始化/采样、总线超时、设备拒绝、回读不一致、
非法配置、版本不兼容（未知/过新可区分）、挂起期间访问、挂起中掉电
后恢复重初始化、并发采样与长时运行资源稳定性。
