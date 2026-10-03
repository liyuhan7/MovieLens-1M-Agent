# -*- coding: utf-8 -*-
"""迭代一 · 原始数据探查脚本(本地 Python 预扫描)

目的:在编写 Hadoop 清洗/评分作业之前,先摸清 ml-1m 工作区数据中
实际存在的质量问题模式。探查结果仅用于设计清洗规则与评分口径,
最终清洗与评分仍由 Hadoop MapReduce 作业执行。

输出:JSON 汇总 + 控制台摘要。
"""
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))   # profiling/
BASE = os.path.dirname(SCRIPT_DIR)                        # 仓库根(ml-1m)
DATA = os.path.join(BASE, "data")
SEP = "::"
ENC = "ISO-8859-1"

# 数据集官方口径(data/README-MovieLens.txt)
OFFICIAL = {
    "users": 6040,
    "movies": 3883,
    "ratings": 1000209,
    "user_id_max": 6040,
    "movie_id_max": 3952,
}
VALID_AGES = {"1", "18", "25", "35", "45", "50", "56"}
VALID_OCC = {str(i) for i in range(21)}
VALID_GENRES = {
    "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
    "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical",
    "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western",
}
# 数据集发布:2003-02;评分产生于 2000-2003
TS_MIN = int(datetime(1995, 1, 1, tzinfo=timezone.utc).timestamp())
TS_MAX = int(datetime(2005, 1, 1, tzinfo=timezone.utc).timestamp())

MOJIBAKE = re.compile(r"[ÃÂâ€¢ð]")


def read_lines(path):
    with open(path, "r", encoding=ENC, errors="replace") as f:
        for ln, line in enumerate(f, 1):
            yield ln, line.rstrip("\r\n")


def profile():
    rep = {}

    # ---------- users.dat ----------
    u = Counter()
    users_seen = {}
    u_bad_zip = Counter()
    for ln, line in read_lines(os.path.join(DATA, "users.dat")):
        parts = line.split(SEP)
        u["total"] += 1
        if len(parts) != 5:
            u[f"fieldcount_{len(parts)}"] += 1
            continue
        uid, gender, age, occ, zipc = parts
        if uid in users_seen:
            u["dup_userid"] += 1
            if users_seen[uid] != line:
                u["dup_userid_conflict"] += 1
        users_seen[uid] = line
        if not uid.isdigit():
            u["bad_userid"] += 1
        if gender not in ("M", "F"):
            u[f"bad_gender:{gender[:12]}"] += 1
        if age not in VALID_AGES:
            u[f"bad_age:{age[:12]}"] += 1
        if occ not in VALID_OCC:
            u[f"bad_occ:{occ[:12]}"] += 1
        if not re.fullmatch(r"\d{5}(-\d{4})?", zipc):
            u[f"bad_zip:{zipc[:10]}"] += 1
    u["distinct_users"] = len(users_seen)
    rep["users"] = dict(u)

    # ---------- movies.dat ----------
    m = Counter()
    movies_seen = {}
    for ln, line in read_lines(os.path.join(DATA, "movies.dat")):
        m["total"] += 1
        parts = line.split(SEP)
        if len(parts) != 3:
            m[f"fieldcount_{len(parts)}"] += 1
            continue
        mid, title, genres = parts
        if mid in movies_seen:
            m["dup_movieid"] += 1
            if movies_seen[mid] != line:
                m["dup_movieid_conflict"] += 1
        movies_seen[mid] = line
        if not mid.isdigit():
            m["bad_movieid"] += 1
        if not title or not title.strip():
            m["empty_title"] += 1
        if MOJIBAKE.search(title):
            m["title_mojibake"] += 1
        if not re.search(r"\((\d{4})\)\s*$", title):
            m["title_no_year"] += 1
        if not genres or not genres.strip():
            m["empty_genres"] += 1
        else:
            gs = genres.split("|")
            if len(set(gs)) != len(gs):
                m["genre_duplicated_in_row"] += 1
            for g in gs:
                if g not in VALID_GENRES:
                    m[f"bad_genre:{g[:16]}"] += 1
    m["distinct_movies"] = len(movies_seen)
    rep["movies"] = dict(m)

    # ---------- ratings.dat ----------
    r = Counter()
    ratings_keys = Counter()
    ts_list = []
    ts_minmax = [None, None]
    for ln, line in read_lines(os.path.join(DATA, "ratings.dat")):
        r["total"] += 1
        parts = line.split(SEP)
        if len(parts) != 4:
            r[f"fieldcount_{len(parts)}"] += 1
            continue
        uid, mid, rating, ts = parts
        if not uid.isdigit():
            r["bad_userid"] += 1
        if not mid.isdigit():
            r["bad_movieid"] += 1
        if not re.fullmatch(r"[1-5]", rating):
            r[f"bad_rating:{rating[:8]}"] += 1
        if not re.fullmatch(r"\d+", ts):
            r["bad_ts"] += 1
        else:
            tsv = int(ts)
            if tsv < TS_MIN or tsv > TS_MAX:
                r["ts_out_of_range"] += 1
            else:
                ts_list.append(tsv)
                if ts_minmax[0] is None or tsv < ts_minmax[0]:
                    ts_minmax[0] = tsv
                if ts_minmax[1] is None or tsv > ts_minmax[1]:
                    ts_minmax[1] = tsv
        ratings_keys[(uid, mid)] += 1
        if uid.isdigit() and uid not in users_seen:
            r["uid_not_in_users"] += 1
        if mid.isdigit() and mid not in movies_seen:
            r["mid_not_in_movies"] += 1
    dup_pairs = {k: v for k, v in ratings_keys.items() if v > 1}
    r["distinct_user_movie_pairs"] = len(ratings_keys)
    r["dup_user_movie_pairs"] = len(dup_pairs)
    r["dup_extra_rows"] = sum(v - 1 for v in dup_pairs.values())
    # 同一 user+movie 不同评分(冲突)
    r["ts_range"] = [ts_minmax[0], ts_minmax[1]]
    rep["ratings"] = dict(r)

    # 抽样:重复对中是否存在评分不同
    conflicts = 0
    seen_pair_rating = {}
    for ln, line in read_lines(os.path.join(DATA, "ratings.dat")):
        parts = line.split(SEP)
        if len(parts) != 4:
            continue
        uid, mid, rating, ts = parts
        k = (uid, mid)
        if k in seen_pair_rating and seen_pair_rating[k] != rating:
            conflicts += 1
        else:
            seen_pair_rating[k] = rating
    r["dup_pair_rating_conflict"] = conflicts
    rep["ratings"] = dict(r)

    rep["official"] = OFFICIAL
    rep["generated_at"] = datetime.now().isoformat(timespec="seconds")
    out = os.path.join(SCRIPT_DIR, "profile_raw_report.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(rep, f, ensure_ascii=False, indent=2)
    print(json.dumps(rep, ensure_ascii=False, indent=1))
    print(f"\nsaved -> {out}", file=sys.stderr)


if __name__ == "__main__":
    profile()
