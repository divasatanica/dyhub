/**
 * WebSocket 事件分发 —— 面向实时消费端（网页弹幕墙 / 弹幕游戏 / 数据看板）
 *
 * 客户端连接：ws://host:port/ws?roomId=xxx&types=chat,gift,member
 * 服务端将匹配的标准化事件以 JSON 推送。
 */

import { WebSocketServer, WebSocket } from 'ws';
import type { Server as HttpServer } from 'node:http';
import { EventBus } from '../pipeline/eventBus.js';
import { eventMatches, type DanmakuEvent, type EventFilter } from '../types/events.js';
import type { Collector, RoomInfo } from '../collector/collector.js';

export interface WsDispatcherOptions {
  /**
   * 订阅者带 roomId 接入、且该房间未活跃时，是否自动触发采集。
   * 服务端模式（src/index.ts）默认 true；命令行模式只监听启动时指定的房间，
   * 传 false 后未知房间直接回 __error 并断开，不会偷偷再开房间。
   */
  autoConnect?: boolean;
  /**
   * 底层 http server / wss 的错误回调（ws 会把 http server 的 'error' 转发到 wss，
   * 例如端口被占用）。不传时静默忽略——但必须挂上监听，否则 Node 会因未处理的
   * 'error' 事件直接退出进程。
   */
  onError?: (err: Error) => void;
}

export class WsDispatcher {
  private wss: WebSocketServer;
  private bus: EventBus;
  private collector: Collector;
  private clients = new Map<WebSocket, EventFilter>();
  private busSub: number;
  private readonly autoConnect: boolean;
  private readonly onError?: (err: Error) => void;

  constructor(httpServer: HttpServer, bus: EventBus, collector: Collector, options: WsDispatcherOptions = {}) {
    this.bus = bus;
    this.collector = collector;
    this.autoConnect = options.autoConnect !== false;
    this.onError = options.onError;
    this.wss = new WebSocketServer({ server: httpServer, path: '/ws' });
    // ws 会把 http server 的 'error' 转发到 wss；没有监听器时 Node 会直接崩进程
    this.wss.on('error', (err: Error) => this.onError?.(err));
    this.wss.on('connection', (ws, req) => {
      const url = new URL(req.url ?? '/', 'http://localhost');
      const roomId = url.searchParams.get('roomId') ?? undefined;
      const types = (url.searchParams.get('types') ?? '')
        .split(',')
        .filter(Boolean) as EventFilter['type'] extends any[] ? any[] : any[];
      const filter: EventFilter = { roomId, type: types.length ? types : undefined };
      this.clients.set(ws, filter);
      ws.on('close', () => this.clients.delete(ws));
      ws.on('error', () => this.clients.delete(ws));
      ws.send(JSON.stringify({ type: '__hello', roomId, types, ts: Date.now() }));

      // 订阅者接入即连接：房间未活跃时自动触发采集，并向客户端推送连接进度
      if (roomId) this.autoConnectRoom(ws, roomId);
    });
    // 心跳保活
    const hb = setInterval(() => {
      for (const ws of this.clients.keys()) {
        if (ws.readyState === WebSocket.OPEN) ws.ping();
      }
    }, 20000);
    this.wss.on('close', () => clearInterval(hb));

    this.busSub = this.bus.subscribe(undefined, (ev) => this.broadcast(ev));
  }

  /**
   * 房间未活跃时自动触发采集，向客户端推送连接进度帧：
   *   __connecting → __connected（成功）/ __error（失败，随后关闭连接）
   * 已活跃房间直接回 __connected。自动连接的房间不会随订阅者断开而自动断开
   * （连接成本高，保留至手动 disconnect）。
   * autoConnect=false（命令行模式）时不触发采集：房间未监听则回 __error 并断开。
   */
  private autoConnectRoom(ws: WebSocket, roomId: string): void {
    const send = (obj: Record<string, unknown>) => {
      if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
    };
    const info = this.collector.getRoomInfo(roomId);
    if (info && info.status !== 'stopped') {
      send({ type: '__connected', roomId, room: info, ts: Date.now() });
      return;
    }
    if (!this.autoConnect) {
      send({ type: '__error', roomId, error: `房间 ${roomId} 未在监听，本进程只监听启动时指定的房间`, ts: Date.now() });
      ws.close();
      return;
    }
    send({ type: '__connecting', roomId, ts: Date.now() });
    this.collector
      .connect(roomId)
      .then((room: RoomInfo) => send({ type: '__connected', roomId, room, ts: Date.now() }))
      .catch((e: unknown) => {
        const msg = e instanceof Error ? e.message : String(e);
        send({ type: '__error', roomId, error: `房间自动连接失败: ${msg}`, ts: Date.now() });
        ws.close();
      });
  }

  private broadcast(ev: DanmakuEvent): void {
    const text = JSON.stringify(ev);
    for (const [ws, filter] of this.clients) {
      if (ws.readyState !== WebSocket.OPEN) continue;
      if (eventMatches(ev, filter)) {
        try {
          ws.send(text);
        } catch {
          /* ignore */
        }
      }
    }
  }

  clientCount(): number {
    return this.clients.size;
  }

  close(): void {
    this.bus.unsubscribe(this.busSub);
    this.wss.close();
  }
}
