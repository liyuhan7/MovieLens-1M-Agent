# -*- coding: utf-8 -*-
"""ml-1m 数据探查脚本：检查 ratings.dat / movies.dat / users.dat 的实际质量问题。

运行: python explore_data.py
输出: 控制台摘要 + 数据探查报告.txt
"""
import os
import re
import sys
from collections import Counter, defaultdict

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:
    pass

BASE = os.path.dirname(os.path.abspath(__file__))
SEP = "::"
REPORT_LINES = []


def out(s=""):
    print(s)
    REPORT_LINES.append(str(s))


def load_lines(filename):
    """按数据集说明使用 ISO-8859-1 读取；同时检测是否为合法 UTF-8。"""
    path = os.path.join(BASE, filename)
    raw = open(path, "rb").read()
    utf8_ok = True
    try:
        raw.decode("utf-8")
    except UnicodeDecodeError:
        utf8_ok = False
    text = raw.decode("iso-8859-1")
    lines = [ln for ln in text.split("\n") if ln != ""]
    # 末尾是否残留 \r（Windows 行尾）
    crlf = raw.count(b"\r\n")
    return lines, utf8_ok, crlf


def split_fields(line):
    return line.split(SEP)


# ---------------------------------------------------------------- ratings
def explore_ratings(users_ids, movies_ids):
    out("=" * 70)
    out("一、ratings.dat")
    lines, utf8_ok, crlf = load_lines("ratings.dat")
    out(f"行数: {len(lines)}   可整体按UTF-8解码: {utf8_ok}   含CRLF行尾: {crlf > 0}")

    field_bad, rating_bad, ts_bad, int_bad = [], [], [], []
    ratings_counter = Counter()          # (user, movie) -> 次数
    um_ratings = defaultdict(set)        # (user, movie) -> 出现过的评分值
    per_user = Counter()
    ts_min, ts_max = None, None
    rating_values = Counter()

    for i, ln in enumerate(lines, 1):
        f = split_fields(ln)
        if len(f) != 4:
            field_bad.append((i, ln))
            continue
        uid, mid, r, ts = f
        if not (uid and mid and r and ts):
            field_bad.append((i, ln))
            continue
        per_user[uid] += 1
        ratings_counter[(uid, mid)] += 1
        if r.isdigit():
            rv = int(r)
            rating_values[rv] += 1
            if not (1 <= rv <= 5):
                rating_bad.append((i, ln))
            um_ratings[(uid, mid)].add(rv)
        else:
            int_bad.append((i, ln))
        if ts.isdigit():
            t = int(ts)
            ts_min = t if ts_min is None or t < ts_min else ts_min
            ts_max = t if ts_max is None or t > ts_max else ts_max
        else:
            ts_bad.append((i, ln))

    out(f"字段数不为4/字段为空的行: {len(field_bad)}" + (f"  示例: {field_bad[:3]}" if field_bad else ""))
    out(f"评分非整数的行: {len(int_bad)}" + (f"  示例: {int_bad[:3]}" if int_bad else ""))
    out(f"评分超出1-5的行: {len(rating_bad)}" + (f"  示例: {rating_bad[:3]}" if rating_bad else ""))
    out(f"时间戳非数字的行: {len(ts_bad)}" + (f"  示例: {ts_bad[:3]}" if ts_bad else ""))
    out(f"评分值分布: {dict(sorted(rating_values.items()))}")

    import datetime
    if ts_min and ts_max:
        def f(t):
            try:
                return datetime.datetime.fromtimestamp(t, datetime.UTC).strftime("%Y-%m-%d")
            except (OSError, OverflowError, ValueError):
                return "无效时间戳"
        out(f"时间戳范围: {ts_min} ({f(ts_min)}) ~ {ts_max} ({f(ts_max)})")
        # 统计无法解释的时间戳(如负数或远超数据集年代的值)
        weird = 0
        for i, ln in enumerate(lines, 1):
            parts = split_fields(ln)
            if len(parts) == 4 and parts[3].isdigit():
                t = int(parts[3])
                if not (631123200 <= t <= 1047791999):  # 1990-01-01 ~ 2003-03-15
                    weird += 1
        out(f"时间戳超出数据集合理年代(1990-2003)的行: {weird}")

    dup_pairs = {k: v for k, v in ratings_counter.items() if v > 1}
    out(f"重复的 (用户,电影) 对: {len(dup_pairs)} 对, 共 {sum(dup_pairs.values())} 条记录")
    conflict = {k for k, v in um_ratings.items() if len(v) > 1}
    out(f"同一(用户,电影)出现不同评分值: {len(conflict)} 对" +
        (f"  示例: {sorted(conflict)[:3]}" if conflict else ""))
    if dup_pairs:
        sample = sorted(dup_pairs.items(), key=lambda x: -x[1])[:5]
        out(f"  重复最多的对示例: {sample}")

    sizes = sorted(per_user.values())
    n = len(sizes)
    out(f"用户数(出现在评分中): {n};  人均评分数: 最小{sizes[0]} / 中位{sizes[n//2]} / 最大{sizes[-1]}")
    top = per_user.most_common(5)
    out(f"评分最多的5个用户: {top}")
    heavy = [u for u, c in per_user.items() if c > 2000]
    out(f"评分数>2000 的用户: {len(heavy)} 个 (是否异常需结合业务判断)")

    unknown_users = sorted({u for (u, _m) in ratings_counter if u not in users_ids})
    unknown_movies = sorted({m for (_u, m) in ratings_counter if m not in movies_ids})
    out(f"引用了 users.dat 中不存在的用户: {len(unknown_users)} 个" +
        (f"  示例: {unknown_users[:5]}" if unknown_users else ""))
    out(f"引用了 movies.dat 中不存在的电影: {len(unknown_movies)} 个" +
        (f"  示例: {unknown_movies[:5]}" if unknown_movies else ""))

    return unknown_users, unknown_movies


# ---------------------------------------------------------------- movies
KNOWN_GENRES = {
    "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
    "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical",
    "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western",
}


def explore_movies():
    out("=" * 70)
    out("二、movies.dat")
    lines, utf8_ok, crlf = load_lines("movies.dat")
    out(f"行数: {len(lines)}   可整体按UTF-8解码: {utf8_ok}   含CRLF行尾: {crlf > 0}")

    field_bad = []
    titles = {}          # id -> set(title)
    genres_map = {}      # id -> set(genres)
    id_seen = Counter()
    year_missing, genre_bad, genre_empty, title_ws = [], [], [], []
    nonascii = 0
    title_by_name = defaultdict(list)

    year_re = re.compile(r"\((\d{4})\)\s*$")

    for i, ln in enumerate(lines, 1):
        f = split_fields(ln)
        if len(f) != 3 or not all(x for x in f):
            field_bad.append((i, ln))
            continue
        mid, title, genres = f
        id_seen[mid] += 1
        titles.setdefault(mid, set()).add(title)
        genres_map.setdefault(mid, set()).add(genres)
        if any(ord(c) > 127 for c in title):
            nonascii += 1
        if title != title.strip():
            title_ws.append((i, ln))
        if not year_re.search(title):
            year_missing.append((i, ln))
        glist = [g.strip() for g in genres.split("|")]
        if genres.strip() == "" or glist == [""]:
            genre_empty.append((i, ln))
        bad = [g for g in glist if g not in KNOWN_GENRES]
        if bad:
            genre_bad.append((i, ln, bad))
        name = year_re.sub("", title).strip()
        title_by_name[name].append((mid, title))

    out(f"字段数不为3/字段为空的行: {len(field_bad)}" + (f"  示例: {field_bad[:3]}" if field_bad else ""))
    out(f"含非ASCII字符(乱码/外文标题)的电影数: {nonascii} (ISO-8859-1正常现象, 但易在UTF-8管线中变乱码)")
    out(f"标题含多余空白: {len(title_ws)}" + (f"  示例: {title_ws[:3]}" if title_ws else ""))
    out(f"标题末尾无 (年份): {len(year_missing)}" + (f"  示例: {year_missing[:5]}" if year_missing else ""))
    out(f"类型为空: {len(genre_empty)}" + (f"  示例: {genre_empty[:3]}" if genre_empty else ""))
    out(f"含无法识别类型的行: {len(genre_bad)}" +
        (f"  示例: {genre_bad[:5]}" if genre_bad else ""))

    diff_title = {k: v for k, v in titles.items() if len(v) > 1}
    diff_genre = {k: v for k, v in genres_map.items() if len(v) > 1}
    dup_id = {k: v for k, v in id_seen.items() if v > 1}
    out(f"MovieID 出现多行的: {len(dup_id)}" + (f"  示例: {list(dup_id.items())[:3]}" if dup_id else ""))
    out(f"同一MovieID对应不同标题: {len(diff_title)}" + (f"  示例: {list(diff_title.items())[:3]}" if diff_title else ""))
    out(f"同一MovieID对应不同类型: {len(diff_genre)}" + (f"  示例: {list(diff_genre.items())[:3]}" if diff_genre else ""))

    dup_names = {k: v for k, v in title_by_name.items() if len({m for m, _ in v}) > 1}
    out(f"不同MovieID同名电影: {len(dup_names)} 组" +
        (f"  示例: {list(dup_names.items())[:5]}" if dup_names else ""))

    ids = sorted(int(k) for k in id_seen)
    out(f"MovieID 范围: {ids[0]} ~ {ids[-1]};  连续性: 缺口数 = {ids[-1] - ids[0] + 1 - len(ids)}")

    return titles


# ---------------------------------------------------------------- users
def explore_users():
    out("=" * 70)
    out("三、users.dat")
    lines, utf8_ok, crlf = load_lines("users.dat")
    out(f"行数: {len(lines)}   可整体按UTF-8解码: {utf8_ok}   含CRLF行尾: {crlf > 0}")

    VALID_GENDER = {"F", "M"}
    VALID_AGE = {1, 18, 25, 35, 45, 50, 56}

    field_bad, gender_bad, age_bad, occ_bad, zip_bad = [], [], [], [], []
    ids = Counter()
    zip_formats = Counter()
    attrs = defaultdict(set)

    for i, ln in enumerate(lines, 1):
        f = split_fields(ln)
        if len(f) != 5 or not all(x for x in f):
            field_bad.append((i, ln))
            continue
        uid, gender, age, occ, zc = f
        ids[uid] += 1
        attrs[uid].add((gender, age, occ, zc))
        if gender not in VALID_GENDER:
            gender_bad.append((i, ln))
        if not age.isdigit() or int(age) not in VALID_AGE:
            age_bad.append((i, ln))
        if not occ.isdigit() or not (0 <= int(occ) <= 20):
            occ_bad.append((i, ln))
        zip_formats[re.sub(r"\d", "9", zc)] += 1
        if not (zc.isdigit() and len(zc) == 5):
            zip_bad.append((i, ln, zc))

    out(f"字段数不为5/字段为空的行: {len(field_bad)}" + (f"  示例: {field_bad[:3]}" if field_bad else ""))
    out(f"性别非法(非F/M): {len(gender_bad)}" + (f"  示例: {gender_bad[:3]}" if gender_bad else ""))
    out(f"年龄段编码非法: {len(age_bad)}" + (f"  示例: {age_bad[:3]}" if age_bad else ""))
    out(f"职业编码非法(非0-20): {len(occ_bad)}" + (f"  示例: {occ_bad[:3]}" if occ_bad else ""))
    out(f"邮编非5位纯数字: {len(zip_bad)}" + (f"  示例: {zip_bad[:5]}" if zip_bad else ""))
    out(f"邮编格式分布: {dict(zip_formats.most_common(10))}")

    dup_id = {k: v for k, v in ids.items() if v > 1}
    conflict = {k: v for k, v in attrs.items() if len(v) > 1}
    out(f"UserID 重复行: {len(dup_id)}" + (f"  示例: {list(dup_id.items())[:3]}" if dup_id else ""))
    out(f"同一UserID属性冲突: {len(conflict)}" + (f"  示例: {list(conflict.items())[:3]}" if conflict else ""))

    g = Counter(next(iter(a))[0] for a in attrs.values())
    out(f"性别分布: {dict(g)}")
    return set(ids)


def main():
    out("#" * 70)
    out("# ml-1m 数据探查报告")
    out("#" * 70)
    users_ids = explore_users()
    movies = explore_movies()
    explore_ratings(users_ids, set(movies))

    out("=" * 70)
    out("结论提示: 以上均为实际统计结果; '异常高/低'类现象(如评分数量、年代久远)")
    out("属于分布特征而非错误, 是否纳入清洗需结合业务判断并在报告中说明。")

    report_path = os.path.join(BASE, "数据探查报告.txt")
    with open(report_path, "w", encoding="utf-8") as fp:
        fp.write("\n".join(REPORT_LINES))
    print(f"\n报告已保存: {report_path}")


if __name__ == "__main__":
    main()
