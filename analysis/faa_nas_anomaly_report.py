"""
FAA NAS 异常延误检测 & 中文摘要生成
=====================================
每日由 Airflow / daily_pipeline.py 调用，步骤：
  1. 从 Snowflake RAW.FAA_NAS_STATUS_RAW 查询当日数据
  2. 判断异常机场（延误 ≥ 60 分钟 或 地面停飞）
  3. 调用 Claude API，生成结构化中文摘要
  4. 将摘要写入文件，并可选通过邮件/Slack 发送

用法：
    python analysis/faa_nas_anomaly_report.py                # 分析昨天的数据
    python analysis/faa_nas_anomaly_report.py --date 2025-04-10
    python analysis/faa_nas_anomaly_report.py --dry-run      # 不调用 Claude，只打印数据
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import anthropic

# ── 路径设置，使 config/ 可被导入 ──────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.snowflake_conn import get_conn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── 异常阈值配置 ──────────────────────────────────────────────────────────
DELAY_THRESHOLD_MIN = 45       # 平均延误超过 45 分钟视为异常
ANOMALY_DELAY_COUNT = 3        # 延误航班数 ≥ 3 视为有影响
GROUND_STOP_TYPES = {"Ground Stop", "Ground Delay", "Airspace Flow Program"}


# ────────────────────────────────────────────────────────────────────────────
# Step 1: 从 Snowflake 查询当日 FAA NAS 数据
# ────────────────────────────────────────────────────────────────────────────
def query_faa_nas(target_date: date) -> list[dict]:
    """
    查询 RAW.FAA_NAS_STATUS_RAW 中指定日期的全部机场状态记录。
    返回：dict 列表，每条记录代表一个机场当日的 NAS 状态快照。
    """
    sql = """
        SELECT
            IATA_CODE,
            HAS_DELAY,
            DELAY_COUNT,
            DELAY_TYPE,
            REASON,
            AVG_DELAY_MIN,
            MAX_DELAY_MIN,
            TREND,
            END_TIME,
            WEATHER_CONDITION,
            WEATHER_TEMP_F,
            WEATHER_WIND,
            FETCH_DATE
        FROM FLIGHT_DB.RAW.FAA_NAS_STATUS_RAW
        WHERE FETCH_DATE = %(fetch_date)s
        ORDER BY COALESCE(AVG_DELAY_MIN, 0) DESC NULLS LAST
    """
    conn = get_conn(schema="RAW")
    cur = conn.cursor()
    try:
        cur.execute(sql, {"fetch_date": target_date.isoformat()})
        cols = [desc[0] for desc in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        logger.info("从 Snowflake 查询到 %d 条 FAA NAS 记录（日期：%s）", len(rows), target_date)
        return rows
    finally:
        cur.close()
        conn.close()


# ────────────────────────────────────────────────────────────────────────────
# Step 2: 识别异常机场
# ────────────────────────────────────────────────────────────────────────────
def detect_anomalies(records: list[dict]) -> dict:
    """
    从查询结果中提取：
    - 高延误机场（AVG_DELAY_MIN ≥ DELAY_THRESHOLD_MIN）
    - 地面停飞 / 地面延误机场（Ground Stop / Ground Delay）
    - 整体统计摘要

    返回结构：
    {
        "total_airports": int,
        "airports_with_delay": int,
        "severe_delay_airports": [{"iata": ..., "avg_delay": ..., ...}],
        "ground_stops": [{"iata": ..., "delay_type": ..., ...}],
        "summary_stats": {...}
    }
    """
    total = len(records)
    with_delay = [r for r in records if r.get("HAS_DELAY")]

    severe = [
        r for r in with_delay
        if (r.get("AVG_DELAY_MIN") or 0) >= DELAY_THRESHOLD_MIN
        or (r.get("DELAY_COUNT") or 0) >= ANOMALY_DELAY_COUNT
    ]

    ground_stops = [
        r for r in with_delay
        if r.get("DELAY_TYPE") in GROUND_STOP_TYPES
    ]

    avg_delay_all = (
        sum(r.get("AVG_DELAY_MIN") or 0 for r in with_delay) / len(with_delay)
        if with_delay else 0
    )

    return {
        "total_airports": total,
        "airports_with_delay": len(with_delay),
        "severe_delay_airports": severe,
        "ground_stops": ground_stops,
        "avg_delay_across_delayed": round(avg_delay_all, 1),
        "all_delayed_records": with_delay,
    }


# ────────────────────────────────────────────────────────────────────────────
# Step 3: 格式化 prompt 数据（发给 Claude）
# ────────────────────────────────────────────────────────────────────────────
def format_data_for_claude(target_date: date, anomalies: dict) -> str:
    """
    将异常分析结果序列化为结构化文本，作为用户消息发给 Claude。
    使用文本而非 JSON，因为中文语境下更易于 Claude 理解和生成自然语言。
    """
    lines = [
        f"【数据日期】{target_date.strftime('%Y年%m月%d日')}",
        f"【监控机场总数】{anomalies['total_airports']} 个",
        f"【出现延误的机场数】{anomalies['airports_with_delay']} 个",
        f"【有延误机场的平均延误时长】{anomalies['avg_delay_across_delayed']} 分钟",
        "",
    ]

    # 地面停飞
    if anomalies["ground_stops"]:
        lines.append("━━ 地面停飞 / 地面延误项目（最严重）━━")
        for r in anomalies["ground_stops"]:
            lines.append(
                f"  • {r['IATA_CODE']}  "
                f"类型：{r.get('DELAY_TYPE', 'N/A')}  "
                f"平均延误：{r.get('AVG_DELAY_MIN', 'N/A')} 分钟  "
                f"原因：{r.get('REASON', '未知')}"
            )
        lines.append("")

    # 高延误机场（排除已在上面列出的）
    gs_codes = {r["IATA_CODE"] for r in anomalies["ground_stops"]}
    other_severe = [r for r in anomalies["severe_delay_airports"] if r["IATA_CODE"] not in gs_codes]
    if other_severe:
        lines.append("━━ 高延误机场（平均延误 ≥ 45 分钟）━━")
        for r in other_severe[:10]:   # 最多列出前 10 个
            weather = r.get("WEATHER_CONDITION") or "N/A"
            lines.append(
                f"  • {r['IATA_CODE']}  "
                f"平均延误：{r.get('AVG_DELAY_MIN', 'N/A')} 分钟  "
                f"最大延误：{r.get('MAX_DELAY_MIN', 'N/A')} 分钟  "
                f"天气：{weather}  "
                f"原因：{r.get('REASON', '未知')[:80]}"
            )
        lines.append("")

    # 正常 / 无数据情况
    if not anomalies["ground_stops"] and not other_severe:
        lines.append("【异常情况】当日未发现明显延误异常，整体运行正常。")

    return "\n".join(lines)


# ────────────────────────────────────────────────────────────────────────────
# Step 4: 调用 Claude API 生成中文摘要
# ────────────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """\
你是一位专业的美国国家空域系统（NAS）运行分析师，负责每日撰写航班运行状况报告。

报告要求：
1. 语言：简体中文，专业但通俗易懂
2. 结构：固定三段格式
   【当日总览】：1-2 句话概括当日整体情况
   【重点异常】：按严重程度列出异常机场，每条包含机场代码、延误原因、预计影响
   【运营建议】：针对旅客和航空公司的简短建议（1-3 条）
3. 若无异常，也需完整输出三段，表明运行正常
4. 数字保留，不要省略；机场代码使用括号补充中文名（如已知）
5. 不要输出 Markdown 标题（# 等），使用【】作为段落标识

常见机场中文名参考（仅用已知的）：
ORD=芝加哥奥黑尔, ATL=亚特兰大哈茨菲尔德, LAX=洛杉矶, JFK=纽约肯尼迪,
EWR=纽瓦克, SFO=旧金山, DFW=达拉斯沃思堡, DEN=丹佛, SEA=西雅图,
BOS=波士顿洛根, MIA=迈阿密, LGA=纽约拉瓜迪亚, MSP=明尼阿波利斯
"""

def generate_summary(target_date: date, data_text: str, dry_run: bool = False) -> str:
    """
    调用 Claude API，输入当日 FAA NAS 数据，生成中文运行摘要。
    使用 Prompt Caching 缓存系统提示（每日多次调用时节省成本）。
    """
    if dry_run:
        logger.info("[DRY RUN] 跳过 Claude API 调用，返回占位摘要")
        return f"[DRY RUN] {target_date} FAA NAS 摘要（未实际调用 Claude）\n\n{data_text}"

    # 初始化客户端（读取 ANTHROPIC_API_KEY 环境变量）
    client = anthropic.Anthropic()

    user_message = (
        f"请根据以下 {target_date.strftime('%Y年%m月%d日')} FAA NAS 实时数据，"
        f"生成当日航班延误运行摘要报告：\n\n{data_text}"
    )

    logger.info("正在调用 Claude API 生成摘要（字符数：%d）...", len(user_message))

    # 使用 streaming + 缓存系统提示
    summary_parts = []
    with client.messages.stream(
        model="claude-opus-4-7",
        max_tokens=1024,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},  # 系统提示跨请求缓存，5 分钟 TTL
            }
        ],
        messages=[
            {"role": "user", "content": user_message}
        ],
    ) as stream:
        for text_chunk in stream.text_stream:
            summary_parts.append(text_chunk)
            print(text_chunk, end="", flush=True)   # 实时打印到控制台

        final = stream.get_final_message()
        logger.info(
            "Claude 调用完成 | 输入 tokens: %d（缓存命中: %d）| 输出 tokens: %d",
            final.usage.input_tokens,
            final.usage.cache_read_input_tokens or 0,
            final.usage.output_tokens,
        )

    print()  # 换行
    return "".join(summary_parts)


# ────────────────────────────────────────────────────────────────────────────
# Step 5: 保存摘要到文件
# ────────────────────────────────────────────────────────────────────────────
def save_report(target_date: date, summary: str, anomalies: dict) -> Path:
    """
    将摘要写入 reports/faa_nas/ 目录，文件名按日期命名。
    返回文件路径。
    """
    report_dir = Path(__file__).parent.parent / "reports" / "faa_nas"
    report_dir.mkdir(parents=True, exist_ok=True)

    report_path = report_dir / f"{target_date.strftime('%Y-%m-%d')}_nas_report.txt"

    header = (
        f"{'=' * 60}\n"
        f"FAA NAS 每日运行报告\n"
        f"日期：{target_date.strftime('%Y年%m月%d日')}\n"
        f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"监控机场：{anomalies['total_airports']} 个 | "
        f"延误机场：{anomalies['airports_with_delay']} 个\n"
        f"{'=' * 60}\n\n"
    )

    report_path.write_text(header + summary, encoding="utf-8")
    logger.info("报告已保存：%s", report_path)
    return report_path


# ────────────────────────────────────────────────────────────────────────────
# 主流程
# ────────────────────────────────────────────────────────────────────────────
def run_report(target_date: date, dry_run: bool = False) -> str:
    """
    完整执行一次 FAA NAS 异常分析 + 摘要生成。
    可由 Airflow task 或命令行直接调用。
    返回生成的摘要文本。
    """
    logger.info("开始 FAA NAS 异常检测（%s）...", target_date)

    # 1. 查询数据
    records = query_faa_nas(target_date)
    if not records:
        logger.warning("当日无 FAA NAS 数据（%s），可能尚未入库", target_date)
        return f"[无数据] {target_date} 的 FAA NAS 数据尚未入库，无法生成报告。"

    # 2. 识别异常
    anomalies = detect_anomalies(records)
    logger.info(
        "检测到：%d 个延误机场，%d 个严重延误，%d 个地面停飞",
        anomalies["airports_with_delay"],
        len(anomalies["severe_delay_airports"]),
        len(anomalies["ground_stops"]),
    )

    # 3. 格式化发给 Claude 的数据
    data_text = format_data_for_claude(target_date, anomalies)
    logger.debug("发送给 Claude 的数据：\n%s", data_text)

    # 4. 生成摘要
    summary = generate_summary(target_date, data_text, dry_run=dry_run)

    # 5. 保存报告
    save_report(target_date, summary, anomalies)

    return summary


def main():
    parser = argparse.ArgumentParser(description="FAA NAS 每日异常检测 & 中文摘要")
    parser.add_argument("--date", help="目标日期 YYYY-MM-DD（默认昨天）")
    parser.add_argument("--dry-run", action="store_true", help="跳过 Claude API 调用")
    args = parser.parse_args()

    # 加载 .env（本地开发用）
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv(env_file)

    target = (
        datetime.strptime(args.date, "%Y-%m-%d").date()
        if args.date
        else date.today() - timedelta(days=1)
    )

    summary = run_report(target, dry_run=args.dry_run)
    print("\n" + "=" * 60)
    print(summary)


if __name__ == "__main__":
    main()
