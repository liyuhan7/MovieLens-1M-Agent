package mliter1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 迭代一 Hadoop MapReduce 作业集(单 jar)。
 *
 * 作业清单(见 迭代一_正式设计文档.md §5.1):
 *   J1 extractUsers / extractMovies   —— 参照 ID 提取
 *   J2 scoreRatings / scoreUsers / scoreMovies —— 五维评分计数(清洗前后共用)
 *   J3 cleanUsers                     —— U1–U6
 *   J4 cleanMovies(含 M5b 经 distributed cache 的 midcount)
 *   J7 cleanRatings                   —— R1–R9
 *
 * 运行方式(容器内):
 *   hadoop jar iter1.jar mliter1.IterationOne <job> <input> <output> [cachePath#linkName ...]
 * 作业名见 main() 分发表。
 */
public class IterationOne {

    // ================= 共享常量(rules-v1.0)=================
    static final Charset LATIN1 = Charset.forName("ISO-8859-1");
    static final String SEP = "::";
    static final Set<String> AGES = new HashSet<>(Arrays.asList("1", "18", "25", "35", "45", "50", "56"));
    static final Set<String> GENRES = new HashSet<>(Arrays.asList(
            "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime", "Documentary", "Drama",
            "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"));
    /** 1990-01-01 UTC ~ 2003-03-15 23:59:59 UTC(数据集 2003-02 发布,宽限至发布后一个月) */
    static final long TS_MIN = 631123200L, TS_MAX = 1047791999L;
    /** T1/T2 时间边界(登记用,不参与打分) */
    static final long T1 = 1009843200L; // 2002-01-01 00:00:00 UTC
    static final long T2 = 1025481600L; // 2002-07-01 00:00:00 UTC

    static String[] fields(String s) { return s.split(SEP, -1); }
    static boolean digits(String s) { return s != null && s.matches("\\d+"); }
    static boolean blank(String s) { return s == null || s.trim().isEmpty(); }

    /** 评分表输出的指标键(逐行计数,ScoreReducer 汇总)。 */
    enum Metric { N, ACC, COMP, CONS, UD, TRAIN, VALID, TEST, UNIQ_EXCESS }

    // ================= J1 参照 ID 提取 =================
    public static class ExtractIdsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = decodeLatin1(value);
            String[] f = fields(line);
            if (f.length >= 1 && digits(f[0].trim())) {
                ctx.write(new Text(f[0].trim()), new Text(""));
            }
        }
    }

    public static class ExtractIdsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            ctx.write(key, new Text(""));
        }
    }

    // ================= J2 五维评分(三表) =================

    /**
     * 五维评分 Mapper。清洗前后共用同一实现:差异仅在输入数据(原始 vs 清洗后)与
     * 参照 ID(经 -files 广播的 ids_users.txt / ids_movies.txt)。
     * 参照集口径见设计文档 §4.3:评原始数据用原始表 ID,评清洗后数据用清洗后表 ID。
     */
    public static class ScoreMapper extends Mapper<LongWritable, Text, Text, Text> {
        String mode;
        Set<String> refUsers = new HashSet<>(), refMovies = new HashSet<>();

        @Override
        protected void setup(Context ctx) throws IOException {
            mode = ctx.getConfiguration().get("mode");
            java.net.URI[] cache = ctx.getCacheFiles();
            if (cache != null) for (java.net.URI uri : cache) {
                String name = new Path(uri.getPath()).getName();
                if (name.contains("users")) loadIds(uri, refUsers);
                else if (name.contains("movies")) loadIds(uri, refMovies);
            }
        }

        void loadIds(java.net.URI uri, Set<String> set) throws IOException {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(
                    FileSystem.get(uri, new Configuration()).open(new Path(uri)), "UTF-8"))) {
                String l;
                while ((l = br.readLine()) != null) if (!l.trim().isEmpty()) set.add(l.trim());
            }
        }

        void metric(Context ctx, Metric m) throws IOException, InterruptedException {
            ctx.write(new Text(m.name()), new Text("1"));
        }
        void metric(Context ctx, Metric m, long n) throws IOException, InterruptedException {
            ctx.write(new Text(m.name()), new Text(Long.toString(n)));
        }
        void dupKey(Context ctx, String key) throws IOException, InterruptedException {
            ctx.write(new Text("PAIR|" + key), new Text("1"));
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = decodeLatin1(value);
            if ("scoreRatings".equals(mode)) scoreRatings(line, ctx);
            else if ("scoreUsers".equals(mode)) scoreUsers(line, ctx);
            else if ("scoreMovies".equals(mode)) scoreMovies(line, ctx);
        }

        // ---- ratings ----
        void scoreRatings(String s, Context ctx) throws IOException, InterruptedException {
            String[] f = fields(s);
            metric(ctx, Metric.N);
            int nonBlank = 0;
            for (int i = 0; i < Math.min(4, f.length); i++) if (!blank(f[i])) nonBlank++;
            metric(ctx, Metric.COMP, nonBlank);
            if (f.length >= 2 && digits(f[0].trim()) && digits(f[1].trim())) dupKey(ctx, f[0].trim() + "|" + f[1].trim());
            else dupKey(ctx, "\u0000" + keyOf(s));
            if (f.length != 4) return;
            String u = f[0].trim(), m = f[1].trim(), r = f[2].trim(), t = f[3].trim();
            boolean idsOk = digits(u) && digits(m);
            boolean ratingOk = r.matches("[1-5]");
            boolean tsOk = digits(t);
            if (tsOk) {
                long ts = Long.parseLong(t);
                boolean inRange = ts >= TS_MIN && ts <= TS_MAX;
                if (inRange) metric(ctx, Metric.UD);
                if (ts <= T1) metric(ctx, Metric.TRAIN);
                else if (ts <= T2) metric(ctx, Metric.VALID);
                else metric(ctx, Metric.TEST);
            }
            if (idsOk && ratingOk && tsOk) {
                long ts = Long.parseLong(t);
                if (ts >= TS_MIN && ts <= TS_MAX && refUsers.contains(u) && refMovies.contains(m))
                    metric(ctx, Metric.ACC);
            }
            if (idsOk && ratingOk && (t.matches("\\d{9}") || t.matches("\\d{10}"))) metric(ctx, Metric.CONS);
        }

        // ---- users ----
        void scoreUsers(String s, Context ctx) throws IOException, InterruptedException {
            String[] f = fields(s);
            metric(ctx, Metric.N);
            int nonBlank = 0;
            for (String x : f) if (!blank(x)) nonBlank++;
            metric(ctx, Metric.COMP, nonBlank);
            if (f.length == 5 && digits(f[0])) dupKey(ctx, f[0]);
            else dupKey(ctx, "\u0000" + keyOf(s));
            boolean fmt = f.length == 5 && digits(f[0]) && f[1].matches("[FM]")
                    && AGES.contains(f[2]) && f[3].matches("\\d+") && (Integer.parseInt(f[3]) <= 20)
                    && f[4].matches("\\d{5}(-\\d{4})?");
            if (fmt) metric(ctx, Metric.CONS);
            boolean val = f.length == 5 && digits(f[0]) && f[1].matches("[FM]")
                    && AGES.contains(f[2]) && f[3].matches("\\d+") && (Integer.parseInt(f[3]) <= 20)
                    && f[4].matches("\\d{5}");
            if (val) metric(ctx, Metric.ACC);
        }

        // ---- movies ----
        void scoreMovies(String s, Context ctx) throws IOException, InterruptedException {
            String[] f = fields(s);
            metric(ctx, Metric.N);
            int nonBlank = 0;
            for (int i = 1; i < Math.min(3, f.length); i++) if (!blank(f[i])) nonBlank++;
            metric(ctx, Metric.COMP, nonBlank);
            // 唯一性业务键:(归一标题, 年份);年份不可解析时按行唯一(不参与同名分组)
            String title = f.length >= 2 ? f[1].trim() : "";
            String titleNorm = title.replaceAll("\\s*\\(\\d{4}\\)\\s*$", "").trim();
            String year = null;
            if (title.matches(".*\\(\\d{4}\\)\\s*")) {
                int i = title.lastIndexOf('(');
                year = title.substring(i + 1, i + 5);
            }
            if (!titleNorm.isEmpty() && year != null) dupKey(ctx, titleNorm + "|" + year);
            else dupKey(ctx, "\u0000" + keyOf(s));
            boolean yearOk = year != null && Integer.parseInt(year) <= 2003;
            boolean genresOk = f.length == 3 && !blank(f[2]);
            if (genresOk) for (String g : f[2].split("\\|")) if (!GENRES.contains(g.trim())) { genresOk = false; break; }
            if (f.length == 3 && !blank(f[1]) && yearOk && genresOk) metric(ctx, Metric.ACC);
            boolean cons = f.length == 3 && digits(f[0]) && !blank(f[1]) && year != null && genresOk;
            if (cons) metric(ctx, Metric.CONS);
        }

        String keyOf(String s) { return Integer.toHexString(s.hashCode()); } // 仅用于"非法行唯一性"近似键
    }

    /** 汇总:指标求和;PAIR| 前缀输出该业务键的超额数。 */
    public static class ScoreReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            String k = key.toString();
            if (k.startsWith("PAIR|")) {
                long n = 0;
                for (Text v : values) n += Long.parseLong(v.toString());
                if (n > 1) ctx.write(new Text("UNIQ_EXCESS"), new Text(Long.toString(n - 1)));
                return;
            }
            long n = 0;
            for (Text v : values) n += Long.parseLong(v.toString());
            ctx.write(key, new Text(Long.toString(n)));
        }
    }

    // ================= 清洗 Mapper/Reducer(三表) =================

    /** 行级判定(Mapper)。输出 key:分组键;value:"seq|action|rule|payload"。 */
    public static class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {
        String mode;
        Set<String> userIds = new HashSet<>(), movieIds = new HashSet<>();

        @Override
        protected void setup(Context ctx) throws IOException {
            mode = ctx.getConfiguration().get("mode");
            // DistributedCache 符号链接落在任务工作目录;本地模式即提交进程 cwd
            java.nio.file.Path dir = java.nio.file.Paths.get("").toAbsolutePath();
            java.net.URI[] cache = ctx.getCacheFiles();
            int loaded = 0;
            if (cache != null) for (java.net.URI uri : cache) {
                String link = new Path(uri.getPath()).getName();
                File f = new File(link);
                if (!f.exists()) f = new File(dir.toString(), link);
                if (!f.exists()) {
                    System.err.println("[setup] cache file MISSING: " + link + " (uri=" + uri + ")");
                    continue;
                }
                Set<String> target = link.contains("users") ? userIds : movieIds;
                try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f), "UTF-8"))) {
                    String s;
                    while ((s = br.readLine()) != null) if (!s.trim().isEmpty()) target.add(s.trim());
                }
                loaded++;
            }
            System.err.println("[setup] mode=" + mode + " loaded=" + loaded + " users=" + userIds.size() + " movies=" + movieIds.size());
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = decodeLatin1(value);
            long seq = key.get(); // 文件字节偏移,作为"文件序"判定依据
            if ("cleanUsers".equals(mode)) cleanUsers(line, seq, ctx);
            else if ("cleanMovies".equals(mode)) cleanMovies(line, seq, ctx);
            else if ("cleanMoviesTitle".equals(mode)) cleanMoviesTitle(line, seq, ctx);
            else cleanRatings(line, seq, ctx);
        }

        // ---------- users: U1–U6 ----------
        void cleanUsers(String line, long seq, Context ctx) throws IOException, InterruptedException {
            // U1c: 已知的冒号分隔行恢复为 MovieLens 标准 :: 分隔;仅在精确五字段且无歧义时修复。
            String[] f = fields(line);
            boolean colonFixed = false;
            if (f.length == 1 && line.indexOf(':') >= 0) {
                String[] colon = line.split(":", -1);
                if (colon.length == 5 && Arrays.stream(colon).noneMatch(IterationOne::blank)) {
                    f = colon;
                    colonFixed = true;
                }
            }
            // U1a: 6 字段且前 5 合法 → 截断(修复)
            if (f.length == 6 && validUserPrefix(f)) {
                String fixed = String.join(SEP, Arrays.copyOf(f, 5));
                out(ctx, "U|" + f[0].trim(), seq, "repair", "U1a", fixed);
                return;
            }
            // U1b: 字段数<5 或 UserID 非法 → 隔离
            if (f.length < 5 || blank(f[0]) || !digits(f[0].trim())) {
                out(ctx, "ISO", seq, "isolate", "U1b", line);
                return;
            }
            String uid = f[0].trim(), gender = f[1].trim(), age = f[2].trim(), occ = f[3].trim(), zip = f[4].trim();
            // U2–U4: 非法枚举置 NULL(登记)
            if (!gender.matches("[FM]")) gender = "NULL";
            if (!AGES.contains(age)) age = "NULL";
            if (!digits(occ) || Integer.parseInt(occ) > 20) occ = "NULL";
            // U5: 邮编
            if (zip.matches("\\d{5}-\\d{4}")) { zip = zip.substring(0, 5); out(ctx, "U|" + uid, seq, "repair", "U5a", join(uid, gender, age, occ, zip)); return; }
            if (zip.matches("\\d{9}")) { zip = zip.substring(0, 5); out(ctx, "U|" + uid, seq, "repair", "U5b", join(uid, gender, age, occ, zip)); return; }
            if (!zip.matches("\\d{5}")) zip = "NULL";
            out(ctx, "U|" + uid, seq, colonFixed ? "repair" : "log",
                    colonFixed ? "U1c" : "U2-U5c", join(uid, gender, age, occ, zip));
        }

        boolean validUserPrefix(String[] f) {
            return digits(f[0].trim()) && f[1].trim().matches("[FM]") && AGES.contains(f[2].trim())
                    && digits(f[3].trim()) && Integer.parseInt(f[3].trim()) <= 20;
        }

        // ---------- movies: M1–M8(行级) ----------
        void cleanMoviesTitle(String line, long seq, Context ctx) throws IOException, InterruptedException {
            String[] f = fields(line);
            if (f.length != 3) { out(ctx, "ISO", seq, "isolate", "M5b-invalid", line); return; }
            String title = f[1].trim();
            String norm = title.replaceAll("\\s*\\(\\d{4}\\)\\s*$", "").trim().toLowerCase(Locale.ROOT);
            String year = title.matches(".*\\(\\d{4}\\)\\s*$")
                    ? title.substring(title.lastIndexOf('(') + 1, title.lastIndexOf(')')) : "unknown";
            String k = year.equals("unknown") ? "M|ROW|" + seq : "M|TITLE|" + norm + "|" + year;
            out(ctx, k, seq, "clean", "M5b", line);
        }

        void cleanMovies(String line, long seq, Context ctx) throws IOException, InterruptedException {
            String[] f = fields(line);
            if (f.length != 3 || blank(f[0]) || !digits(f[0].trim())) { out(ctx, "ISO", seq, "isolate", "M2", line); return; }
            String id = f[0].trim();
            String title = f[1].trim();
            String genres = f[2].trim();
            // M8 mojibake 逆转(确定性可逆)
            String repaired = mojibakeReverse(title);
            if (!repaired.equals(title)) { title = repaired; }
            // M1 trim 在上面完成
            if (title.isEmpty()) { out(ctx, "ISO", seq, "isolate", "M2", line); return; }
            // M3 年份不可解析 → 登记保留
            boolean yearOk = title.matches(".*\\(\\d{4}\\)\\s*") && parseYear(title) <= 2003;
            // M4 剥离词表外类型
            List<String> gs = new ArrayList<>();
            boolean stripped = false;
            for (String g : genres.split("\\|", -1)) {
                String gt = g.trim();
                if (GENRES.contains(gt)) gs.add(gt);
                else if (!gt.isEmpty()) stripped = true;
            }
            String ng = gs.isEmpty() ? "NULL" : String.join("|", gs);
            String action = stripped ? "log" : (yearOk ? "repair" : "log");
            String rule = stripped ? "M4" : (yearOk ? "M7/M8/M1" : "M3");
            out(ctx, "M|" + id, seq, action, rule, id + SEP + title + SEP + ng);
        }

        String mojibakeReverse(String s) {
            try {
                if (!s.contains("\u00C3") && !s.contains("\u00C2")) return s;
                byte[] b = s.getBytes("ISO-8859-1");
                String x = new String(b, "UTF-8");
                return x.indexOf('\uFFFD') >= 0 ? s : x;
            } catch (Exception e) { return s; }
        }

        int parseYear(String title) {
            int i = title.lastIndexOf('(');
            if (i < 0 || i + 4 >= title.length()) return 9999;
            try { return Integer.parseInt(title.substring(i + 1, i + 5)); } catch (NumberFormatException e) { return 9999; }
        }

        // ---------- ratings: R1–R9(R7/R8/R9 在 Reducer/driver 二阶段) ----------
        void cleanRatings(String line, long seq, Context ctx) throws IOException, InterruptedException {
            // R1b: 逗号分隔行还原
            String[] f = fields(line);
            if (f.length == 1 && line.contains(",")) {
                String[] q = line.split(",", -1);
                if (q.length == 4) f = q;
            }
            // R1a: 5 字段截断
            if (f.length == 5) f = Arrays.copyOf(f, 4);
            // R1c: 隔离
            if (f.length != 4 || Arrays.stream(f).anyMatch(IterationOne::blank)) {
                out(ctx, "ISO", seq, "isolate", "R1c", line);
                return;
            }
            String u = f[0].trim(), m = f[1].trim(), r = f[2].trim(), t = f[3].trim();
            if (!digits(u) || !digits(m)) { out(ctx, "ISO", seq, "isolate", "R1c", line); return; }
            // R2/R3:评分非 [1-5](整数)
            if (!r.matches("[1-5]")) { out(ctx, "ISO", seq, "isolate", "R2", line); return; }
            // R4 时间戳非数字
            if (!digits(t)) { out(ctx, "ISO", seq, "isolate", "R4", line); return; }
            long ts = Long.parseLong(t);
            // R5 毫秒→秒
            boolean msFixed = false;
            if (ts > 1_000_000_000_000L) { ts /= 1000L; msFixed = true; }
            // R6 年代超界
            if (ts < TS_MIN || ts > TS_MAX) { out(ctx, "ISO", seq, "isolate", "R6", line); return; }
            // R8 ID 偏移修复(需参照 ID)
            String rule = msFixed ? "R5" : "R1";
            String uu = u, mm = m;
            if (!userIds.contains(u) && u.length() >= 7) {
                String cand = offset(u);
                if (cand != null && userIds.contains(cand)) { uu = cand; rule = "R8a"; }
            }
            if (!movieIds.contains(m) && m.length() >= 7) {
                String cand = offset(m);
                if (cand != null && movieIds.contains(cand)) { mm = cand; rule = "R8b"; }
            }
            // R9 悬空引用
            if (!userIds.contains(uu) || !movieIds.contains(mm)) { out(ctx, "ISO", seq, "isolate", "R9", line); return; }
            String payload = join(uu, mm, r, Long.toString(ts));
            out(ctx, "R|" + uu + "|" + mm, seq, "repair", rule, payload);
        }

        void out(Context ctx, String groupKey, long seq, String action, String rule, String payload)
                throws IOException, InterruptedException {
            ctx.write(new Text(groupKey), new Text(seq + "|" + action + "|" + rule + "|" + payload));
        }

        /** Reducer 侧字段串(同构):"seq|action|rule|payload" → parts[4]。 */
        static String[] payloadOf(Text v) {
            String s = v.toString();
            String[] p = s.split("\\|", 4);
            return p.length == 4 ? p : null;
        }
    }

    /**
     * 清洗 Reducer。分组去重:
     *   U|uid  → U6 保留合法字段最多,并列取文件序靠前
     *   M|mid  → M5a 保留合法字段最多(含 M6 归并后的冲突),并列文件序靠前
     *   M|titleNorm → M5b(由 driver 以 title 为 key 的第二次 cleanMovies 执行)
     *   R|u|m  → R7 保留时间戳最新,并列文件序靠前
     * ISO 组直通隔离区。
     */
    public static class CleanReducer extends Reducer<Text, Text, Text, Text> {
        String mode;
        @Override
        protected void setup(Context ctx) { mode = ctx.getConfiguration().get("mode"); }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            String k = key.toString();
            List<String> rows = new ArrayList<>();
            for (Text v : values) rows.add(v.toString());
            if (k.equals("ISO")) {
                for (String v : rows) ctx.write(new Text("isolate"), new Text(v));
                return;
            }
            if (k.startsWith("R|")) { // R7: 保留最新评分
                String best = null; long bestTs = Long.MIN_VALUE, bestSeq = Long.MAX_VALUE;
                List<String> dups = new ArrayList<>();
                for (String v : rows) {
                    String[] p = splitPayload(v);
                    long seq = Long.parseLong(p[0]);
                    String[] f = fields(p[3]);
                    long ts = f.length == 4 && digits(f[3]) ? Long.parseLong(f[3]) : Long.MIN_VALUE;
                    if (best == null || ts > bestTs || (ts == bestTs && seq < bestSeq)) {
                        if (best != null) dups.add(best);
                        best = v; bestTs = ts; bestSeq = seq;
                    } else dups.add(v);
                }
                if (best != null) ctx.write(new Text("clean"), new Text(best));
                for (String d : dups) ctx.write(new Text("log"), new Text("R7|dedup|" + d));
                return;
            }
            // U6 / M5a / M5b: 保留合法字段最多的一行
            String best = null; int bestScore = -1; long bestSeq = Long.MAX_VALUE;
            List<String> dups = new ArrayList<>();
            for (String v : rows) {
                String[] p = splitPayload(v);
                long seq = Long.parseLong(p[0]);
                int score = validFieldCount(mode, p[3]);
                if (best == null || score > bestScore || (score == bestScore && seq < bestSeq)) {
                    if (best != null) dups.add(best);
                    best = v; bestScore = score; bestSeq = seq;
                } else dups.add(v);
            }
            if (best != null) ctx.write(new Text("clean"), new Text(best));
            String rule = k.startsWith("U|") ? "U6" : (k.startsWith("M|TITLE|") ? "M5b" : (k.startsWith("M|") ? "M5a" : "X"));
            for (String d : dups) ctx.write(new Text("log"), new Text(rule + "|dedup|" + d));
        }

        int validFieldCount(String mode, String payload) {
            String[] f = fields(payload);
            int n = 0;
            if (mode.contains("Users")) {
                if (f.length == 5) {
                    if (digits(f[0])) n++;
                    if (f[1].matches("[FM]")) n++;
                    if (AGES.contains(f[2])) n++;
                    if (digits(f[3]) && Integer.parseInt(f[3]) <= 20) n++;
                    if (f[4].matches("\\d{5}")) n++;
                }
            } else {
                if (f.length == 3) {
                    if (digits(f[0])) n++;
                    if (!blank(f[1]) && f[1].matches(".*\\(\\d{4}\\)\\s*")) n++;
                    if (!blank(f[2]) && !"NULL".equals(f[2])) n++;
                }
            }
            return n;
        }

        String[] splitPayload(String v) {
            List<String> parts = new ArrayList<>();
            int i1 = v.indexOf('|'), i2 = v.indexOf('|', i1 + 1), i3 = v.indexOf('|', i2 + 1);
            parts.add(v.substring(0, i1));
            parts.add(v.substring(i1 + 1, i2));
            parts.add(v.substring(i2 + 1, i3));
            parts.add(v.substring(i3 + 1));
            return parts.toArray(new String[0]);
        }
    }

    // ================= 工具 =================
    static String decodeLatin1(Text value) {
        return new String(value.getBytes(), 0, value.getLength(), LATIN1);
    }
    static String join(String... parts) { return String.join(SEP, parts); }

    /** R8 偏移还原:减 1,000,000 或 2,000,000 后返回候选 ID,否则 null。 */
    static String offset(String id) {
        try {
            long v = Long.parseLong(id);
            if (v > 1_000_000) {
                long d1 = v - 1_000_000L, d2 = v - 2_000_000L;
                if (d2 > 0) return Long.toString(d2);
                if (d1 > 0) return Long.toString(d1);
            }
        } catch (NumberFormatException ignored) {}
        return null;
    }

    // ================= Driver =================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // hadoop jar 的 generic 参数在不同 Hadoop 发行版中的解析位置略有差异;
        // 显式复制 -files 到作业配置,确保 local runner 也建立 DistributedCache。
        for (int i = 0; i + 1 < args.length; i++) {
            if ("-files".equals(args[i]) || "--files".equals(args[i])) {
                conf.set("mapreduce.job.cache.files", args[i + 1]);
                conf.set("mapreduce.job.cache.symlink.create", "true");
            }
        }
        org.apache.hadoop.util.GenericOptionsParser generic = new org.apache.hadoop.util.GenericOptionsParser(conf, args);
        String[] remaining = generic.getRemainingArgs();
        if (remaining.length < 3) {
            System.err.println("usage: hadoop jar iter1.jar mliter1.IterationOne <job> <input> <output> [-files ref...]");
            System.err.println("  jobs: extractUsers extractMovies scoreRatings scoreUsers scoreMovies cleanUsers cleanMovies cleanRatings");
            System.exit(2);
        }
        String jobName = remaining[0];
        String in = remaining[1], out = remaining[2];
        conf.set("mode", jobName);
        Job job = Job.getInstance(conf, "iter1-" + jobName);
        job.setJarByClass(IterationOne.class);

        boolean score = jobName.startsWith("score");
        if (score) {
            job.setMapperClass(ScoreMapper.class);
            job.setReducerClass(ScoreReducer.class);
        } else if (jobName.startsWith("extract")) {
            job.setMapperClass(ExtractIdsMapper.class);
            job.setReducerClass(ExtractIdsReducer.class);
        } else {
            job.setMapperClass(CleanMapper.class);
            job.setReducerClass(CleanReducer.class);
        }
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
