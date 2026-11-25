/**
 * 独立 WebSocket 分发服务 —— 命令行模式专用
 *
 * 与服务端模式（src/index.ts）复用同一个 WsDispatcher，但不启动 Fastify、
 * 管理 API 与控制台，只开一个裸 http server 承载 /ws（另带一个 /health）。
 * 命令行进程本身仍把事件打到 stdout，WS 是并行的额外通道。
 */

import { createServer } from 'node:http';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { EventBus } from '../pipeline/eventBus.js';
import type { Collector } from '../collector/collector.js';
import { WsDispatcher } from './wsServer.js';

export interface StandaloneWsOptions {
  /** 监听地址（与 DYHUB_HOST 一致） */
  host: string;
  /** 监听端口；0 表示由系统分配空闲端口 */
  port: number;
  bus: EventBus;
  collector: Collector;
  /**
   * 是否允许 WS 客户端用 ?roomId= 触发自动连接（服务端模式默认行为）。
   * 命令行只监听一个房间，传 false 时未知房间回 __error 并断开。
   */
  autoConnect?: boolean;
  /** WS 运行期错误回调（启动失败走 reject，不走这里）；默认写 stderr */
  onError?: (err: Error) => void;
}

export interface StandaloneWsHandle {
  /** 实际可连接的地址；端口为 0 时是系统分配的真实端口 */
  url: string;
  clientCount(): number;
  close(): Promise<void>;
}

export async function startStandaloneWs(opts: StandaloneWsOptions): Promise<StandaloneWsHandle> {
  let dispatcher: WsDispatcher | null = null;
  let listening = false;

  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    // 只做健康检查；管理 API 属于服务端模式，这里不提供
    if (req.method === 'GET' && (req.url === '/' || req.url === '/health')) {
      res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: true, service: 'dyhub-cli-ws', ws: '/ws', clients: dispatcher?.clientCount() ?? 0 }));
      return;
    }
    res.writeHead(404, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ error: 'not found', hint: 'WS 事件流请连接 /ws?roomId=...&types=...' }));
  });

  dispatcher = new WsDispatcher(server, opts.bus, opts.collector, {
    autoConnect: opts.autoConnect,
    onError: (err) => {
      // 启动阶段的错误由下面的 listen reject 统一报，避免一条错误打两遍
      if (!listening) return;
      if (opts.onError) opts.onError(err);
      else process.stderr.write(`[dyhub] WS 服务错误：${err.message}\n`);
    },
  });

  try {
    await new Promise<void>((resolve, reject) => {
      const onError = (err: Error) => reject(err);
      server.once('error', onError);
      server.listen(opts.port, opts.host, () => {
        server.off('error', onError);
        resolve();
      });
    });
  } catch (e) {
    // 端口占用等启动失败：撤掉总线订阅，避免留下悬挂引用
    dispatcher.close();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    throw e;
  }
  listening = true;

  const port = (server.address() as AddressInfo).port;
  const shownHost = opts.host === '0.0.0.0' || opts.host === '::' ? 'localhost' : opts.host;

  return {
    url: `ws://${shownHost}:${port}/ws`,
    clientCount: () => dispatcher?.clientCount() ?? 0,
    close: async () => {
      dispatcher?.close();
      await new Promise<void>((resolve) => {
        // 客户端可能还在长连接上：直接清掉 TCP 连接，避免 close 一直等
        server.closeAllConnections?.();
        server.close(() => resolve());
        // 兜底：极端情况下 close 回调不来，也不能卡住退出
        setTimeout(resolve, 1000).unref();
      });
    },
  };
}
