/**
 * 房间号归一化 —— API 与 CLI 共用
 *
 * 用户可识别的直播间短号（web_rid，如 708764876300），需要容忍：
 *  - 全角数字（中文输入法）
 *  - 直接粘贴的直播间链接（live.douyin.com/<数字>）
 *  - 空格 / 零宽字符等噪声
 */

export function normalizeRoomIdInput(raw: unknown): string {
  if (raw === null || raw === undefined) return '';
  let s = String(raw);
  // 全角数字 ０-９ → 半角
  s = s.replace(/[０-９]/g, (c) => String.fromCharCode(c.charCodeAt(0) - 0xfee0));
  // 粘贴了直播间链接：取 live.douyin.com/<数字> 尾号
  const fromUrl = s.match(/live\.douyin\.com\/(\d+)/);
  if (fromUrl) return fromUrl[1];
  // 其余只保留数字（顺带去掉空格 / 零宽字符 / 标点）
  return s.replace(/\D/g, '');
}
