/**
 * DyHub CLI —— 命令行直接监听单个直播间
 *
 * 前台常驻：默认只输出弹幕（chat），不需要礼物事件、不需要登录、
 * 也不需要打开管理后台。同时默认起一个独立的 WebSocket 服务
 * （ws://host:port/ws?roomId=&types=，事件协议与 `npm run dev` 控制台模式一致），
 * 方便弹幕墙 / 弹幕游戏等消费端直接接入；`--no-ws` 可关闭。
 *
 * 进程退出（Ctrl-C / kill / 关闭终端）时会先断开房间、关闭 WS 与 Chromium，
 * 再退出，不留后台浏览器进程。
 *
 * 用法：
 *   npm run listen -- 708764876300
 *   npx tsx src/cli.ts 708764876300          # 开发模式
 *   node dist/cli.js 708764876300            # 生产模式（先 npm run build）
 *
 * 采集内核与账号复用服务端同一套逻辑：
 *   DYHUB_COLLECTOR=browser|lightweight、DYHUB_CHROME、DYHUB_HEADED、DYHUB_COOKIE
 */

import { BrowserManager } from './collector/browser.js';
import { Collector } from './collector/collector.js';
import * as accountStore from './collector/accountStore.js';
import { EventBus } from './pipeline/eventBus.js';
import { EventDeduplicator } from './pipeline/dedupe.js';
import { normalize } from './pipeline/normalizer.js';
import { startStandaloneWs, type StandaloneWsHandle } from './dispatch/standaloneWs.js';
import { normalizeRoomIdInput } from './util/roomId.js';
import type { RawProtoMessage } from './proto/douyin.proto.js';
import type { ChatEvent, DanmakuEvent, DanmakuEventType } from './types/events.js';

const HELP = `DyHub 命令行监听 —— 前台常驻，默认只输出弹幕（chat）

用法：
  npm run listen -- <直播间号> [选项]
  npx tsx src/cli.ts <直播间号> [选项]     # 开发模式
  node dist/cli.js <直播间号> [选项]        # 生产模式（先 npm run build）

示例：
  npm run listen -- 708764876300
  npm run listen -- 'https://live.douyin.com/708764876300' --json
  DYHUB_COLLECTOR=lightweight npm run listen -- 708764876300

选项：
  --collector=browser|lightweight  采集内核（默认取 DYHUB_COLLECTOR，否则 browser）
  --types=chat,gift                输出的事件类型，逗号分隔（默认 chat，只要弹幕）
  --json                           每行输出完整事件 JSON，便于管道 / 落盘处理
  --ws-port <端口>                 WS 监听端口（默认取 DYHUB_PORT，否则 8757；0 = 随机空闲端口）
  --ws-host <地址>                 WS 监听地址（默认取 DYHUB_HOST，否则 0.0.0.0）
  --no-ws                          不启动 WebSocket 服务，只输出到终端
  -h, --help                       显示本帮助

WebSocket：
  启动后 stderr 会打印可连接的 ws://<host>:<端口>/ws?roomId=<房间号>&types=<类型>。
  客户端按自己连接的 ?roomId= / ?types= 过滤事件，参数与 npm run dev 控制台模式一致。
  端口被占用时只告警、不影响终端输出，可用 --ws-port 换端口。

退出：
  Ctrl-C / kill / 关闭终端 → 先断开房间、关闭 WS 与 Chromium，再退出进程。
  退出码：0 正常（含直播结束），1 连接失败 / 运行错误，2 参数错误。
`;

/** 外部允许的事件类型（与服务端协议一致） */
const ALL_TYPES: DanmakuEventType[] = ['chat', 'gift', 'member', 'like', 'follow', 'room', 'roomStatus', 'unknown'];

/** 首次连接等待弹幕通道建立的上限；超时视为未开播 / 网络不通 */
const INITIAL_WAIT_MS = 45_000;
/** 已进入直播态后掉线，等待抖音自动重连的宽限时间 */
const RECONNECT_GRACE_MS = 30_000;

interface Options {
  roomId: string;
  collector: 'browser' | 'lightweight';
  types: DanmakuEventType[];
  json: boolean;
  help: boolean;
  /** 是否启动独立 WS 服务（默认开，--no-ws 关） */
  ws: boolean;
  /** WS 监听端口；0 = 系统分配 */
  wsPort: number;
  wsHost: string;
}

// ---- 终端着色（仅 TTY，便于重定向到文件） ----

const COLOR = Boolean(process.stdout.isTTY) && !process.env.NO_COLOR;
const dim = (s: string) => (COLOR ? `\x1b[2m${s}\x1b[0m` : s);

// ---- 参数解析 ----

/** 取 --flag=value 或 --flag value 的值；返回下一个待处理下标 */
function parseFlagValue(argv: string[], index: number, arg: string): { value?: string; next: number } {
  const eq = arg.indexOf('=');
  if (eq >= 0) return { value: arg.slice(eq + 1), next: index };
  const following = argv[index + 1];
  if (following && !following.startsWith('-')) return { value: following, next: index + 1 };
  return { value: undefined, next: index };
}

/** 端口校验：0-65535 的整数（0 = 由系统分配空闲端口） */
function parsePort(raw: string, source: string): number {
  const text = raw.trim();
  const n = Number(text);
  if (text === '' || !Number.isInteger(n) || n < 0 || n > 65535) {
    throw new Error(`${source} 必须是 0-65535 的整数，收到「${raw}」`);
  }
  return n;
}

function parseArgs(argv: string[]): Options {
  const envCollector = (process.env.DYHUB_COLLECTOR ?? 'browser').trim().toLowerCase();
  const envPort = (process.env.DYHUB_PORT ?? '').trim();
  const envHost = (process.env.DYHUB_HOST ?? '').trim();
  const opts: Options = {
    roomId: '',
    collector: envCollector === 'lightweight' ? 'lightweight' : 'browser',
    types: ['chat'],
    json: false,
    help: false,
    ws: true,
    wsPort: envPort ? parsePort(envPort, 'DYHUB_PORT') : 8757,
    wsHost: envHost || '0.0.0.0',
  };
  const positional: string[] = [];

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '-h' || arg === '--help') {
      opts.help = true;
      continue;
    }
    if (arg === '--json') {
      opts.json = true;
      continue;
    }
    if (arg === '--collector' || arg.startsWith('--collector=')) {
      const { value, next } = parseFlagValue(argv, i, arg);
      i = next;
      const kernel = (value ?? '').trim().toLowerCase();
      if (kernel !== 'browser' && kernel !== 'lightweight') {
        throw new Error(`--collector 只支持 browser / lightweight，收到「${value ?? ''}」`);
      }
      opts.collector = kernel;
      continue;
    }
    if (arg === '--types' || arg.startsWith('--types=')) {
      const { value, next } = parseFlagValue(argv, i, arg);
      i = next;
      const list = (value ?? '').split(',').map((s) => s.trim()).filter(Boolean);
      if (list.length === 0) throw new Error('--types 不能为空，例如 --types=chat');
      const bad = list.filter((t) => !ALL_TYPES.includes(t as DanmakuEventType));
      if (bad.length > 0) {
        throw new Error(`未知事件类型：${bad.join(', ')}（可选：${ALL_TYPES.join(', ')}）`);
      }
      opts.types = list as DanmakuEventType[];
      continue;
    }
    if (arg === '--no-ws') {
      opts.ws = false;
      continue;
    }
    if (arg === '--ws-port' || arg.startsWith('--ws-port=')) {
      const { value, next } = parseFlagValue(argv, i, arg);
      i = next;
      opts.wsPort = parsePort(value ?? '', '--ws-port');
      continue;
    }
    if (arg === '--ws-host' || arg.startsWith('--ws-host=')) {
      const { value, next } = parseFlagValue(argv, i, arg);
      i = next;
      const host = (value ?? '').trim();
      if (!host) throw new Error('--ws-host 不能为空，例如 --ws-host=127.0.0.1');
      opts.wsHost = host;
      continue;
    }
    if (arg.startsWith('-')) throw new Error(`未知选项：${arg}`);
    positional.push(arg);
  }

  if (positional.length > 1) throw new Error(`只支持监听一个直播间，收到多个参数：${positional.join(' ')}`);
  if (positional.length === 1) {
    opts.roomId = normalizeRoomIdInput(positional[0]);
    if (!opts.roomId) throw new Error(`无法从「${positional[0]}」解析出直播间号（应为 live.douyin.com/<数字> 中的数字）`);
  }
  return opts;
}

// ---- 输出 ----

/** 弹幕富文本 → 单行文本；表情用 [名称] 占位，便于终端阅读 */
function chatText(ev: ChatEvent): string {
  const parts = ev.data.parts;
  if (parts && parts.length > 0) {
    return parts
      .map((p) => {
        if (p.type === 'text') return p.text;
        if (p.type === 'mention') return p.text;
        return `[${p.name ?? '表情'}]`;
      })
      .join('');
  }
  return ev.data.content ?? '';
}

function printEvent(ev: DanmakuEvent, json: boolean): void {
  if (json || ev.type !== 'chat') {
    process.stdout.write(JSON.stringify(ev) + '\n');
    return;
  }
  const at = dim(new Date(ev.ts).toLocaleTimeString('zh-CN', { hour12: false }));
  const nick = ev.user?.nickname?.trim() || '匿名';
  process.stdout.write(`${at} ${nick}: ${chatText(ev)}\n`);
}

// ---- 生命周期 ----

let collectorRef: Collector | null = null;
let browserRef: BrowserManager | null = null;
let wsRef: StandaloneWsHandle | null = null;
let shuttingDown = false;

/** 断开采集、关闭 WS 与 Chromium 后退出；清理最多等 10s，避免卡死 */
async function shutdown(code: number, message?: string): Promise<void> {
  if (shuttingDown) {
    // 用户二次中断（信号触发的 code 0）：不再等待清理，直接退出；
    // 其余情况（如连接失败与退出同时发生）交给正在进行中的清理收尾，避免覆盖原有退出码
    if (code === 0) process.exit(130);
    return;
  }
  shuttingDown = true;
  if (message) process.stderr.write(`${message}\n`);
  process.stderr.write('[dyhub] 正在断开采集…\n');
  const cleanup = (async () => {
    try {
      await collectorRef?.disconnectAll();
    } catch {
      /* 会话可能已被页面关闭，忽略 */
    }
    try {
      await wsRef?.close();
    } catch {
      /* ignore */
    }
    try {
      await browserRef?.close();
    } catch {
      /* ignore */
    }
  })();
  await Promise.race([cleanup, new Promise<void>((resolve) => setTimeout(resolve, 10_000))]);
  process.stderr.write('[dyhub] 已断开，退出\n');
  // 给 stdout/stderr 一点冲刷时间，再真正退出
  await new Promise<void>((resolve) => setTimeout(resolve, 30));
  process.exit(code);
}

/** 轮询采集状态，打印连接 / 掉线 / 结束提示，并在失败或结束时退出 */
function startWatchdog(roomId: string, collector: Collector): void {
  const startedAt = Date.now();
  let everLive = false;
  let lostAt: number | null = null;

  setInterval(() => {
    if (shuttingDown) return;
    const info = collector.getRoomInfo(roomId);
    if (!info) return;
    const status = info.status;

    if (status === 'live') {
      if (!everLive) {
        const who = info.meta?.nickname ? `主播「${info.meta.nickname}」` : '直播间';
        process.stderr.write(`✓ 已接入${who}的弹幕流，开始监听\n`);
        everLive = true;
      } else if (lostAt !== null) {
        process.stderr.write('✓ 弹幕通道已恢复\n');
      }
      lostAt = null;
      return;
    }

    const errText = info.error ?? (info.progress?.step === 'error' ? info.progress.detail : undefined);

    if (!everLive) {
      // 从未进入直播态：明确失败立即退出，否则等 INITIAL_WAIT_MS
      if (status === 'closed' || status === 'error' || errText) {
        void shutdown(1, `✗ 连接失败：${errText ?? status}`);
      } else if (Date.now() - startedAt > INITIAL_WAIT_MS) {
        void shutdown(1, '✗ 等待弹幕通道超时（直播间可能未开播，或网络 / 风控导致 wss 未建立）');
      }
      return;
    }

    // 进入过直播态后掉线：先等抖音自动重连（浏览器内核会自行恢复）
    if (lostAt === null) {
      lostAt = Date.now();
      const detail = info.error ? `：${info.error}` : '';
      process.stderr.write(`! 弹幕通道中断（${status}${detail}），等待重连…\n`);
      return;
    }
    if (Date.now() - lostAt > RECONNECT_GRACE_MS) {
      const ended = status === 'closed';
      void shutdown(ended ? 0 : 1, ended ? '· 直播已结束，退出' : '✗ 弹幕通道无法恢复，退出');
    }
  }, 1000);
}

async function main(opts: Options): Promise<void> {
  // 弹幕为游客可见，没有账号时创建一个空的默认账号（连接接口要求账号绑定）
  if (!accountStore.getDefaultAccountId()) {
    accountStore.createAccount('默认');
    process.stderr.write('[dyhub] 首次运行：已创建游客默认账号（弹幕无需登录）\n');
  }

  // 采集 → 标准化 → 去重 → 总线；终端输出与 WS 分发是总线的两个订阅者
  const bus = new EventBus();
  const dedupe = new EventDeduplicator(50_000);
  const onMessage = (msg: RawProtoMessage, meta: { roomId: string }) => {
    const ev = normalize(msg, meta);
    if (!ev) return;
    if (dedupe.isDuplicate(ev.id)) return;
    bus.publish(ev);
  };
  // 终端只输出 --types 指定的事件；WS 客户端按各自连接的 ?types= 过滤
  bus.subscribe({ type: opts.types }, (ev) => printEvent(ev, opts.json));

  // 房间状态变更 → roomStatus 事件（与服务端模式一致），WS 客户端可见
  const onStatusChange = (roomId: string, status: string, error?: string) => {
    bus.publish({
      id: `rs_${roomId}_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
      roomId,
      platform: 'douyin',
      type: 'roomStatus',
      ts: Date.now(),
      receivedAt: Date.now(),
      data: { status, message: error },
    });
  };

  const onError = (roomId: string, err: Error) => process.stderr.write(`[dyhub] 房间 ${roomId} 采集错误：${err.message}\n`);

  let collector: Collector;
  if (opts.collector === 'lightweight') {
    const { LightweightSession } = await import('./collector/lightweightSession.js');
    collector = new Collector({
      createSession: (roomId, cookie) =>
        new LightweightSession(roomId, { onMessage, onError: (err) => onError(roomId, err), cookie }),
      onMessage: () => {},
      onStatusChange,
    });
  } else {
    // DYHUB_HEADED 语义与 index.ts 一致：仅显式 1/true 时开有头
    const headed = ['1', 'true'].includes((process.env.DYHUB_HEADED ?? '').trim().toLowerCase());
    browserRef = new BrowserManager({
      executablePath: process.env.DYHUB_CHROME || undefined,
      headless: !headed,
      // 由本进程负责退出清理（Playwright 默认处理器会抢先 close 并 exit 130）
      handleSignals: false,
    });
    await browserRef.init();
    collector = new Collector({ browser: browserRef, onMessage, onStatusChange });
  }
  collectorRef = collector;

  // WS 服务先起：客户端可以在采集连接期间就接进来，事件到达后自动推送
  if (opts.ws) {
    try {
      wsRef = await startStandaloneWs({
        bus,
        collector,
        host: opts.wsHost,
        port: opts.wsPort,
        // 命令行只监听启动时指定的房间，不允许 WS 客户端再触发别的房间采集
        autoConnect: false,
      });
      process.stderr.write(`· WS 事件流：${wsRef.url}?roomId=${opts.roomId}&types=${opts.types.join(',')}\n`);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      process.stderr.write(`! WS 服务启动失败（${msg}），本次只输出到终端；可用 --ws-port 换端口\n`);
    }
  } else {
    process.stderr.write('· WS 服务已关闭（--no-ws）\n');
  }

  process.stderr.write(`· 采集内核：${opts.collector}\n`);
  process.stderr.write(`· 事件类型：${opts.types.join(',')}（Ctrl-C 退出）\n`);
  process.stderr.write(`· 正在连接直播间 ${opts.roomId} …\n`);

  try {
    await collector.connect(opts.roomId);
  } catch (e) {
    await shutdown(1, `✗ 连接失败：${e instanceof Error ? e.message : String(e)}`);
    return;
  }
  process.stderr.write(
    opts.collector === 'browser' ? '· 直播间页面已打开，等待弹幕通道…\n' : '· 已建立采集会话，等待弹幕通道…\n',
  );
  startWatchdog(opts.roomId, collector);

  // 常驻：不主动结束，直到信号或 watchdog 触发退出
  await new Promise<void>(() => {});
}

async function run(): Promise<void> {
  let opts: Options;
  try {
    opts = parseArgs(process.argv.slice(2));
  } catch (e) {
    process.stderr.write(`[dyhub] ${e instanceof Error ? e.message : String(e)}\n\n${HELP}`);
    process.exit(2);
  }
  if (opts.help) {
    process.stdout.write(HELP);
    return;
  }
  if (!opts.roomId) {
    process.stderr.write(`[dyhub] 缺少直播间号（例如 708764876300）\n\n${HELP}`);
    process.exit(2);
  }

  for (const sig of ['SIGINT', 'SIGTERM', 'SIGHUP'] as const) {
    process.on(sig, () => {
      void shutdown(0);
    });
  }
  process.on('uncaughtException', (e) => {
    process.stderr.write(`[dyhub] 未捕获异常：${e?.stack ?? String(e)}\n`);
    void shutdown(1);
  });
  process.on('unhandledRejection', (e) => {
    process.stderr.write(`[dyhub] 未处理的 Promise 拒绝：${e instanceof Error ? e.stack : String(e)}\n`);
    void shutdown(1);
  });

  try {
    await main(opts);
  } catch (e) {
    // 退出过程中（浏览器被主动关闭）connect 会失败，此时忽略，由正在进行的清理决定退出码
    if (shuttingDown) return;
    await shutdown(1, `✗ 监听失败：${e instanceof Error ? e.message : String(e)}`);
  }
}

void run();
