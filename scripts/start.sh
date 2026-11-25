#!/usr/bin/env bash
#
# DyHub 一键启动：管理服务 + 固定直播间的弹幕打印
#
# 用法：
#   ./scripts/start.sh                  # 起服务 + 监听下面写死的 ROOM_ID
#   ./scripts/start.sh --no-server      # 只跑弹幕打印（listen 自带采集内核，不依赖服务端）
#   ./scripts/start.sh --dry-run        # 只打印将要执行的命令，不真正启动
#   ./scripts/start.sh --prod           # 用 dist/ 构建产物（需先 npm run build）
#   ./scripts/start.sh 708764876300     # 临时覆盖房间号（纯数字）
#   ./scripts/start.sh --types=chat,gift --json    # 其余 --xxx 原样透传给 listen
#
# 行为：
#   - 端口上已有 DyHub 服务时直接复用，不重复启动；
#   - listen 在后台启动并由本脚本转发 SIGTERM，退出（Ctrl-C / 直播结束 / 出错）
#     时先断开房间、关闭 Chromium，再停掉本脚本启动的服务端；
#   - 服务端日志写入 logs/dyhub-server.log，终端只留弹幕输出。
#
# 注意：房间号是「web_rid」，即 live.douyin.com/<这串纯数字>，
#       不是抖音号，也不是 v.douyin.com 分享短链。
#
# 退出码：listen 的退出码原样返回（0 正常，1 连接失败，2 参数错误）。
#
set -uo pipefail

# ======== 改这里：你的固定直播间号（纯数字） ========
ROOM_ID="708764876300"
# ===================================================

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || exit 1

PORT="${DYHUB_PORT:-8757}"
LOG_DIR="$REPO_ROOT/logs"
SERVER_LOG="$LOG_DIR/dyhub-server.log"

MODE="dev"
START_SERVER=1
DRY_RUN=0
ROOM_OVERRIDE=""
PASSTHROUGH=()

info() { printf '[start] %s\n' "$*" >&2; }
die() { printf '[start] ✗ %s\n' "$*" >&2; exit 1; }

HELP="$(cat <<'EOF'
DyHub 一键启动：管理服务 + 固定直播间的弹幕打印

用法：
  ./scripts/start.sh [房间号] [选项] [透传给 listen 的选项]

选项：
  --no-server       只跑弹幕打印，不启动管理服务
  --prod            使用 dist/ 构建产物（需先 npm run build）
  --dry-run         只打印将要执行的命令
  --types=chat,gift 指定事件类型（透传给 listen，默认 chat）
  --json            输出完整事件 JSON（透传给 listen）
  -h, --help        显示本帮助

示例：
  ./scripts/start.sh
  ./scripts/start.sh --no-server
  ./scripts/start.sh 708764876300 --types=chat,gift
EOF
)"

# ---- 参数解析：第一个非选项参数是房间号，其余 --xxx 透传给 listen ----

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)   printf '%s\n' "$HELP"; exit 0 ;;
    --no-server) START_SERVER=0 ;;
    --prod)      MODE="prod" ;;
    --dry-run)   DRY_RUN=1 ;;
    --)
      shift
      while [[ $# -gt 0 ]]; do PASSTHROUGH+=("$1"); shift; done
      break
      ;;
    --*)
      # 形如 --types chat,gift 的写法：把值一起收进来，避免被误认成房间号
      PASSTHROUGH+=("$1")
      if [[ "$1" != *=* && $# -gt 1 && "$2" != -* ]]; then
        PASSTHROUGH+=("$2")
        shift
      fi
      ;;
    -*) die "未知选项：$1" ;;
    *)
      [[ -z "$ROOM_OVERRIDE" ]] || die "只支持监听一个直播间，多余参数：$1"
      ROOM_OVERRIDE="$1"
      ;;
  esac
  shift
done

[[ -n "$ROOM_OVERRIDE" ]] && ROOM_ID="$ROOM_OVERRIDE"
ROOM_ID="${ROOM_ID//[[:space:]]/}"          # 粘贴时常见的空白
[[ "$ROOM_ID" =~ ^[0-9]+$ ]] || die "房间号必须是纯数字（live.douyin.com/<数字>），收到「${ROOM_ID}」"

# ---- 运行方式：默认 tsx 直跑源码；--prod 用 dist ----

if [[ "$MODE" == "prod" ]]; then
  [[ -f dist/index.js && -f dist/cli.js ]] || die "dist/ 不完整，请先 npm run build（或去掉 --prod）"
  SERVER_CMD=(node dist/index.js)
  LISTEN_CMD=(node dist/cli.js "$ROOM_ID")
else
  [[ -x node_modules/.bin/tsx ]] || die "未安装依赖，请先 npm install"
  SERVER_CMD=(node_modules/.bin/tsx src/index.ts)
  LISTEN_CMD=(node_modules/.bin/tsx src/cli.ts "$ROOM_ID")
fi
if [[ ${#PASSTHROUGH[@]} -gt 0 ]]; then
  LISTEN_CMD+=("${PASSTHROUGH[@]}")
fi

# ---- 服务端进程管理 ----

SERVER_PID=""
LISTEN_PID=""

server_up() { curl -fsS --max-time 2 "http://127.0.0.1:$PORT/api/stats" >/dev/null 2>&1; }

stop_server() {
  [[ -n "$SERVER_PID" ]] || return 0
  kill -0 "$SERVER_PID" 2>/dev/null || return 0
  info "正在停止 DyHub 服务（pid ${SERVER_PID}）…"
  kill -TERM "$SERVER_PID" 2>/dev/null
  for _ in $(seq 1 50); do
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 0.1
  done
  kill -0 "$SERVER_PID" 2>/dev/null && kill -KILL "$SERVER_PID" 2>/dev/null
  wait "$SERVER_PID" 2>/dev/null
  return 0
}

on_exit() {
  local code=$?
  trap - EXIT INT TERM
  stop_server
  exit "$code"
}

on_term() {
  # 让 listen 走自己的优雅退出（断开房间 + 关 Chromium）
  [[ -n "$LISTEN_PID" ]] && kill -TERM "$LISTEN_PID" 2>/dev/null
  for _ in $(seq 1 100); do
    [[ -z "$LISTEN_PID" ]] || kill -0 "$LISTEN_PID" 2>/dev/null || break
    sleep 0.1
  done
  exit 143
}

trap on_exit EXIT
trap on_term TERM
trap 'exit 130' INT

# ---- 启动服务端 ----

if [[ "$START_SERVER" == "1" ]]; then
  if server_up; then
    info "端口 $PORT 上已有 DyHub 服务，直接复用"
  elif [[ "$DRY_RUN" == "1" ]]; then
    printf '[start] 将启动服务端：%s  (日志 %s)\n' "${SERVER_CMD[*]}" "$SERVER_LOG" >&2
  else
    mkdir -p "$LOG_DIR"
    "${SERVER_CMD[@]}" >>"$SERVER_LOG" 2>&1 &
    SERVER_PID=$!
    ready=0
    for _ in $(seq 1 60); do
      if server_up; then ready=1; break; fi
      if ! kill -0 "$SERVER_PID" 2>/dev/null; then break; fi
      sleep 0.5
    done
    if [[ "$ready" != "1" ]]; then
      printf '[start] ✗ 服务端未就绪，日志末尾：\n' >&2
      tail -n 20 "$SERVER_LOG" >&2
      exit 1
    fi
    info "服务端已就绪：http://127.0.0.1:$PORT （pid ${SERVER_PID}，日志 ${SERVER_LOG}）"
  fi
else
  info "跳过服务端（listen 自带采集内核，无需服务端）"
fi

# ---- 启动弹幕打印（前台转后台，便于转发信号） ----

info "弹幕打印：${LISTEN_CMD[*]}"
if [[ "$DRY_RUN" == "1" ]]; then
  exit 0
fi

"${LISTEN_CMD[@]}" &
LISTEN_PID=$!
wait "$LISTEN_PID"
status=$?
info "listen 已退出（code ${status}）"
exit "$status"
