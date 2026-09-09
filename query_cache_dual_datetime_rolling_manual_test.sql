-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- 双 DATETIME LIST 表达式分区：手工正确性及 Query Cache 复用测试
--
-- 执行：先选一个测试数据库，在同一个连接里按节执行。
-- mysql -h <FE_HOST> -P <QUERY_PORT> -u <USER> -p <TEST_DB> \
--       --comments --verbose < query_cache_dual_datetime_rolling_manual_test.sql
-- 脚本仅重建 qc_list_dt1_dt2_rolling、qc_list_dt1_dt2_multi 两张测试表；
-- 最后保留数据。所有 SET 均为会话设置，执行完可关闭测试连接。
-- 建表语法依据当前开发分支，不代表已发布版本支持此功能。
--
-- 观察步骤（优先分节执行，及时保存 Profile，避免历史数量限制）：
--   1. 第 3 节 EXPLAIN 应有 DIGEST：代表已生成可缓存的计划，不代表命中。
--   2. 每条待观察查询执行后立即 SELECT LAST_QUERY_ID(); 记录查询 ID。
--      等待约 10 秒后 SHOW QUERY PROFILE; 找到对应的 QC2D_ 标签/查询 ID，
--      在执行该查询的 FE Web UI 的 QueryProfile 页面查看或下载实例 Profile。
--      当前分支 SHOW QUERY PROFILE '<id>' 不会展开实例明细。
--   3. 查看 CACHE_SOURCE / CacheSource 的 HitCache（1 命中，0 未命中），
--      用 CacheTabletId 对应 SHOW TABLETS 的 TabletId。按分区查询即可定位：
--      SHOW TABLETS FROM qc_list_dt1_dt2_rolling PARTITION(p10_10);
--      SHOW TABLETS 的结果没有 PartitionId 列，勿依赖该列进行关联。
--      按不同 CacheTabletId 统计，勿把不同 pipeline task 当作不同分区。
--   4. 窗口滚动后允许同时出现 1 和 0。不要只找任意一个 1 就宣称全部命中。
--      HitCache 才是直接证据；耗时下降、DIGEST 相同都不能单独证明复用。
--
-- 前提：缓存容量可用；两次查询间无数据写入、BE 重启、迁移或缓存淘汰。
-- 每个日期二元组一个分区、每分区一个 bucket、单副本，方便映射缓存单位。
-- 关闭 SQL Cache 排除整条 SQL 缓存干扰；关闭增量 Query Cache，
-- 让第 6 节写入后的行为明确为版本失效后重新计算。
-- 预期命中数量是这些条件下的验收目标，需要在你的运行集群上验证。
--
-- 性能放大：第 1 节 CROSS JOIN numbers 的 "1" 是唯一的造数倍率 R。
-- 首次用 R=1 验正确性；改成 "4096" 后从第 0 节重跑，共 9,437,184 行。
-- 第 2～5 节所有非 NULL 的 SUM / COUNT 预期均乘 R（不包括多 tuple 小表）。
-- 第 6 节增量固定为 SUM +1,000,000,000、COUNT +1，不乘 R。
-- 2304 行的小数据只适合验证正确性及命中，不保证端到端耗时明显下降。
-- 放大后在写入测试前，用第 2/3 节 A、D 的原始 SELECT 比较：
--   enable_query_cache=false：重复执行数次，记录稳定的扫描量和耗时；
--   enable_query_cache=true ：先执行 A，再 D，再重复 D，记录 Profile。
-- 保持查询/并发/设置一致；不要把冷磁盘与热磁盘的差异算作 Query Cache 收益。
-- 多次性能轮次可 query_cache_force_refresh=true 执行 A、D 来强制重算；
-- 之后必须 SET query_cache_force_refresh=false 才会读取缓存。
-- force_refresh 仍会写缓存，耗时包含写回开销，不能代替关闭缓存的性能基线；
-- 同时刷新 A、D 后再查询 D 会全命中，也不会复现首次滚动的部分命中。
-- 关闭 enable_query_cache 并不会清空已存缓存；要复现第 3 节首次 A→D，
-- 应从建表重新执行，不能靠开关假装缓存为空。

-- 0. 会话设置
SET enable_sql_cache = false;
SET enable_nereids_distribute_planner = true;
SET parallel_pipeline_task_num = 2;
SET enable_profile = true;
SET profile_level = 2;
SET enable_query_cache_incremental = false;
SET query_cache_force_refresh = false;
SET enable_query_cache = false;

-- 1. 64 个日期二元组，覆盖非对角组合，而非只让 dt1/dt2 同步变化。
DROP TABLE IF EXISTS qc_list_dt1_dt2_rolling;
CREATE TABLE qc_list_dt1_dt2_rolling (
    id BIGINT NOT NULL,
    dt1 DATETIMEV2(0) NOT NULL,
    dt2 DATETIMEV2(0) NOT NULL,
    col1 BIGINT NOT NULL
)
DUPLICATE KEY(id, dt1, dt2)
PARTITION BY LIST(date_trunc(dt1, 'day'), date_trunc(dt2, 'day')) (
    PARTITION p07_07 VALUES IN (('2026-08-07 00:00:00', '2016-08-07 00:00:00')),
    PARTITION p07_08 VALUES IN (('2026-08-07 00:00:00', '2016-08-08 00:00:00')),
    PARTITION p07_09 VALUES IN (('2026-08-07 00:00:00', '2016-08-09 00:00:00')),
    PARTITION p07_10 VALUES IN (('2026-08-07 00:00:00', '2016-08-10 00:00:00')),
    PARTITION p07_11 VALUES IN (('2026-08-07 00:00:00', '2016-08-11 00:00:00')),
    PARTITION p07_12 VALUES IN (('2026-08-07 00:00:00', '2016-08-12 00:00:00')),
    PARTITION p07_13 VALUES IN (('2026-08-07 00:00:00', '2016-08-13 00:00:00')),
    PARTITION p07_14 VALUES IN (('2026-08-07 00:00:00', '2016-08-14 00:00:00')),
    PARTITION p08_07 VALUES IN (('2026-08-08 00:00:00', '2016-08-07 00:00:00')),
    PARTITION p08_08 VALUES IN (('2026-08-08 00:00:00', '2016-08-08 00:00:00')),
    PARTITION p08_09 VALUES IN (('2026-08-08 00:00:00', '2016-08-09 00:00:00')),
    PARTITION p08_10 VALUES IN (('2026-08-08 00:00:00', '2016-08-10 00:00:00')),
    PARTITION p08_11 VALUES IN (('2026-08-08 00:00:00', '2016-08-11 00:00:00')),
    PARTITION p08_12 VALUES IN (('2026-08-08 00:00:00', '2016-08-12 00:00:00')),
    PARTITION p08_13 VALUES IN (('2026-08-08 00:00:00', '2016-08-13 00:00:00')),
    PARTITION p08_14 VALUES IN (('2026-08-08 00:00:00', '2016-08-14 00:00:00')),
    PARTITION p09_07 VALUES IN (('2026-08-09 00:00:00', '2016-08-07 00:00:00')),
    PARTITION p09_08 VALUES IN (('2026-08-09 00:00:00', '2016-08-08 00:00:00')),
    PARTITION p09_09 VALUES IN (('2026-08-09 00:00:00', '2016-08-09 00:00:00')),
    PARTITION p09_10 VALUES IN (('2026-08-09 00:00:00', '2016-08-10 00:00:00')),
    PARTITION p09_11 VALUES IN (('2026-08-09 00:00:00', '2016-08-11 00:00:00')),
    PARTITION p09_12 VALUES IN (('2026-08-09 00:00:00', '2016-08-12 00:00:00')),
    PARTITION p09_13 VALUES IN (('2026-08-09 00:00:00', '2016-08-13 00:00:00')),
    PARTITION p09_14 VALUES IN (('2026-08-09 00:00:00', '2016-08-14 00:00:00')),
    PARTITION p10_07 VALUES IN (('2026-08-10 00:00:00', '2016-08-07 00:00:00')),
    PARTITION p10_08 VALUES IN (('2026-08-10 00:00:00', '2016-08-08 00:00:00')),
    PARTITION p10_09 VALUES IN (('2026-08-10 00:00:00', '2016-08-09 00:00:00')),
    PARTITION p10_10 VALUES IN (('2026-08-10 00:00:00', '2016-08-10 00:00:00')),
    PARTITION p10_11 VALUES IN (('2026-08-10 00:00:00', '2016-08-11 00:00:00')),
    PARTITION p10_12 VALUES IN (('2026-08-10 00:00:00', '2016-08-12 00:00:00')),
    PARTITION p10_13 VALUES IN (('2026-08-10 00:00:00', '2016-08-13 00:00:00')),
    PARTITION p10_14 VALUES IN (('2026-08-10 00:00:00', '2016-08-14 00:00:00')),
    PARTITION p11_07 VALUES IN (('2026-08-11 00:00:00', '2016-08-07 00:00:00')),
    PARTITION p11_08 VALUES IN (('2026-08-11 00:00:00', '2016-08-08 00:00:00')),
    PARTITION p11_09 VALUES IN (('2026-08-11 00:00:00', '2016-08-09 00:00:00')),
    PARTITION p11_10 VALUES IN (('2026-08-11 00:00:00', '2016-08-10 00:00:00')),
    PARTITION p11_11 VALUES IN (('2026-08-11 00:00:00', '2016-08-11 00:00:00')),
    PARTITION p11_12 VALUES IN (('2026-08-11 00:00:00', '2016-08-12 00:00:00')),
    PARTITION p11_13 VALUES IN (('2026-08-11 00:00:00', '2016-08-13 00:00:00')),
    PARTITION p11_14 VALUES IN (('2026-08-11 00:00:00', '2016-08-14 00:00:00')),
    PARTITION p12_07 VALUES IN (('2026-08-12 00:00:00', '2016-08-07 00:00:00')),
    PARTITION p12_08 VALUES IN (('2026-08-12 00:00:00', '2016-08-08 00:00:00')),
    PARTITION p12_09 VALUES IN (('2026-08-12 00:00:00', '2016-08-09 00:00:00')),
    PARTITION p12_10 VALUES IN (('2026-08-12 00:00:00', '2016-08-10 00:00:00')),
    PARTITION p12_11 VALUES IN (('2026-08-12 00:00:00', '2016-08-11 00:00:00')),
    PARTITION p12_12 VALUES IN (('2026-08-12 00:00:00', '2016-08-12 00:00:00')),
    PARTITION p12_13 VALUES IN (('2026-08-12 00:00:00', '2016-08-13 00:00:00')),
    PARTITION p12_14 VALUES IN (('2026-08-12 00:00:00', '2016-08-14 00:00:00')),
    PARTITION p13_07 VALUES IN (('2026-08-13 00:00:00', '2016-08-07 00:00:00')),
    PARTITION p13_08 VALUES IN (('2026-08-13 00:00:00', '2016-08-08 00:00:00')),
    PARTITION p13_09 VALUES IN (('2026-08-13 00:00:00', '2016-08-09 00:00:00')),
    PARTITION p13_10 VALUES IN (('2026-08-13 00:00:00', '2016-08-10 00:00:00')),
    PARTITION p13_11 VALUES IN (('2026-08-13 00:00:00', '2016-08-11 00:00:00')),
    PARTITION p13_12 VALUES IN (('2026-08-13 00:00:00', '2016-08-12 00:00:00')),
    PARTITION p13_13 VALUES IN (('2026-08-13 00:00:00', '2016-08-13 00:00:00')),
    PARTITION p13_14 VALUES IN (('2026-08-13 00:00:00', '2016-08-14 00:00:00')),
    PARTITION p14_07 VALUES IN (('2026-08-14 00:00:00', '2016-08-07 00:00:00')),
    PARTITION p14_08 VALUES IN (('2026-08-14 00:00:00', '2016-08-08 00:00:00')),
    PARTITION p14_09 VALUES IN (('2026-08-14 00:00:00', '2016-08-09 00:00:00')),
    PARTITION p14_10 VALUES IN (('2026-08-14 00:00:00', '2016-08-10 00:00:00')),
    PARTITION p14_11 VALUES IN (('2026-08-14 00:00:00', '2016-08-11 00:00:00')),
    PARTITION p14_12 VALUES IN (('2026-08-14 00:00:00', '2016-08-12 00:00:00')),
    PARTITION p14_13 VALUES IN (('2026-08-14 00:00:00', '2016-08-13 00:00:00')),
    PARTITION p14_14 VALUES IN (('2026-08-14 00:00:00', '2016-08-14 00:00:00'))
)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- 每个分区内 dt1 和 dt2 独立各取六个时间点（共 36 行）：
-- 00:00:00、12:12:11、12:12:12、12:12:13、18:00:00、23:59:59。
-- i/j 分别是从 08-07 开始的日期偏移；k/l 分别是两个时间点编号。
-- col1 = (i+1)*10000 + (j+1)*100 + (k+1)*10 + (l+1)，
-- 刻意使用不对称权重，避免错用/漏掉某一列条件但 SUM 恰好相同。
INSERT INTO qc_list_dt1_dt2_rolling
SELECT
    n.number + copies.number * 2304,
    seconds_add(days_add(CAST('2026-08-07 00:00:00' AS DATETIMEV2(0)), n.i),
        CASE n.k WHEN 0 THEN 0 WHEN 1 THEN 43931 WHEN 2 THEN 43932
                 WHEN 3 THEN 43933 WHEN 4 THEN 64800 ELSE 86399 END),
    seconds_add(days_add(CAST('2016-08-07 00:00:00' AS DATETIMEV2(0)), n.j),
        CASE n.l WHEN 0 THEN 0 WHEN 1 THEN 43931 WHEN 2 THEN 43932
                 WHEN 3 THEN 43933 WHEN 4 THEN 64800 ELSE 86399 END),
    (n.i + 1) * 10000 + (n.j + 1) * 100 + (n.k + 1) * 10 + (n.l + 1)
FROM (
    SELECT number,
           CAST(FLOOR(number / 288) AS INT) AS i,
           CAST(FLOOR(number / 36) AS INT) % 8 AS j,
           CAST(FLOOR(number / 6) AS INT) % 6 AS k,
           CAST(number % 6 AS INT) AS l
    FROM numbers("number" = "2304")
) n
CROSS JOIN numbers("number" = "1") copies; -- 性能造数倍率 R，只改这里

-- R=1：2304 行，SUM=104805504；务必确认 INSERT 已成功且数据已可见。
SELECT COUNT(*) AS row_count, SUM(col1) AS sum_col1 FROM qc_list_dt1_dt2_rolling;
SHOW PARTITIONS FROM qc_list_dt1_dt2_rolling;
SHOW TABLETS FROM qc_list_dt1_dt2_rolling;

-- 2. 关闭缓存，保存全部基准。每个结果为 SUM / COUNT；M 返回 NULL / 0。
-- 后续相同编号必须与这里一致，且还应与独立计算的注释预期一致。

-- BASE_A: 原始双窗口；预期 SUM / COUNT = 37523738 / 841 (R=1)
SELECT /* QC2D_BASE_A */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- BASE_B: 只滚动 dt1 一天；预期 SUM / COUNT = 45933738 / 841 (R=1)
SELECT /* QC2D_BASE_B */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-09 12:12:12' AND dt1 < '2026-08-14 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- BASE_C: 只滚动 dt2 一天；预期 SUM / COUNT = 37607838 / 841 (R=1)
SELECT /* QC2D_BASE_C */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-09 12:12:12' AND dt2 < '2016-08-14 12:12:12';

-- BASE_D: 同时滚动 dt1、dt2 一天；预期 SUM / COUNT = 46017838 / 841 (R=1)
SELECT /* QC2D_BASE_D */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-09 12:12:12' AND dt1 < '2026-08-14 12:12:12'
  AND dt2 > '2016-08-09 12:12:12' AND dt2 < '2016-08-14 12:12:12';

-- BASE_E: 日期不变，两维日内边界改成 18:00:00；预期 SUM / COUNT = 40452100 / 841 (R=1)
SELECT /* QC2D_BASE_E */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 18:00:00' AND dt1 < '2026-08-13 18:00:00'
  AND dt2 > '2016-08-08 18:00:00' AND dt2 < '2016-08-13 18:00:00';

-- BASE_F: 原始窗口的两个下界改为 >=；预期 SUM / COUNT = 39424650 / 900 (R=1)
SELECT /* QC2D_BASE_F */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 >= '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 >= '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- BASE_G: 原始窗口的两个上界改为 <=；预期 SUM / COUNT = 40939650 / 900 (R=1)
SELECT /* QC2D_BASE_G */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 <= '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 <= '2016-08-13 12:12:12';

-- BASE_H: 两维均为整天半开区间 [08,13)；预期 SUM / COUNT = 36394650 / 900 (R=1)
SELECT /* QC2D_BASE_H */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 >= '2026-08-08 00:00:00' AND dt1 < '2026-08-13 00:00:00'
  AND dt2 >= '2016-08-08 00:00:00' AND dt2 < '2016-08-13 00:00:00';

-- BASE_I: 两维均为整天半开区间 [09,14)；预期 SUM / COUNT = 45484650 / 900 (R=1)
SELECT /* QC2D_BASE_I */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 >= '2026-08-09 00:00:00' AND dt1 < '2026-08-14 00:00:00'
  AND dt2 >= '2016-08-09 00:00:00' AND dt2 < '2016-08-14 00:00:00';

-- BASE_J: 同一天的小窗口 (12:12:11,18:00:00)；预期 SUM / COUNT = 161754 / 4 (R=1)
SELECT /* QC2D_BASE_J */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-10 12:12:11' AND dt1 < '2026-08-10 18:00:00'
  AND dt2 > '2016-08-10 12:12:11' AND dt2 < '2016-08-10 18:00:00';

-- BASE_K: 同一天的小窗口 (12:12:12,18:00:00)；预期 SUM / COUNT = 40444 / 1 (R=1)
SELECT /* QC2D_BASE_K */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-10 12:12:12' AND dt1 < '2026-08-10 18:00:00'
  AND dt2 > '2016-08-10 12:12:12' AND dt2 < '2016-08-10 18:00:00';

-- BASE_L: 两维均为 07 日，与之前的窗口不重叠；预期 SUM / COUNT = 364986 / 36 (R=1)
SELECT /* QC2D_BASE_L */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 >= '2026-08-07 00:00:00' AND dt1 < '2026-08-08 00:00:00'
  AND dt2 >= '2016-08-07 00:00:00' AND dt2 < '2016-08-08 00:00:00';

-- BASE_M: dt2 超出全部分区，空结果；预期 SUM / COUNT = NULL / 0 (R=1)
SELECT /* QC2D_BASE_M */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-20 12:12:12' AND dt2 < '2016-08-21 12:12:12';

-- BASE_N: 原始窗口增加非分区列过滤 col1 % 2 = 0；预期 SUM / COUNT = 19405540 / 435 (R=1)
SELECT /* QC2D_BASE_N */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12'
  AND col1 % 2 = 0;

-- 3. 最重要的复用链：首次 A → 重复 A → 首次 D → 重复 D。
-- 前面的基准全部关闭缓存，不会预先填充缓存。此段应在新建表后首次运行。
SET enable_query_cache = true;
EXPLAIN
SELECT /* QC2D_EXPLAIN_A */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- A、D 均选中 36 个分区。A 首次全部未命中，重复 A 应全部命中。
-- A_FILL: 原始双窗口；预期 SUM / COUNT = 37523738 / 841 (R=1)
SELECT /* QC2D_A_FILL */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- A_HIT: 原始双窗口；预期 SUM / COUNT = 37523738 / 841 (R=1)
SELECT /* QC2D_A_HIT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- A 和 D 重叠 25 个日期分区，但边界分区内的范围不同。
-- 首次 D 仅 p10_10..p12_12 组成的 3×3=9 个分区应复用；
-- 其余 27 个分区需计算（11 个新分区 + 16 个边界语义变化的重叠分区）。
-- 比如 p09_10：A 覆盖 dt1 全天，D 仅 dt1>12:12:12，不能混用缓存。
-- D_ROLL_FIRST: 同时滚动 dt1、dt2 一天；预期 SUM / COUNT = 46017838 / 841 (R=1)
SELECT /* QC2D_D_ROLL_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-09 12:12:12' AND dt1 < '2026-08-14 12:12:12'
  AND dt2 > '2016-08-09 12:12:12' AND dt2 < '2016-08-14 12:12:12';

-- D_HIT: 同时滚动 dt1、dt2 一天；预期 SUM / COUNT = 46017838 / 841 (R=1)
SELECT /* QC2D_D_HIT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-09 12:12:12' AND dt1 < '2026-08-14 12:12:12'
  AND dt2 > '2016-08-09 12:12:12' AND dt2 < '2016-08-14 12:12:12';

-- 立即记录上面最后一个 SELECT 的查询 ID；其他查询也可照此操作。
SELECT LAST_QUERY_ID();
SHOW QUERY PROFILE;

-- 4. 独立维度滚动、边界条件、同一天切片、空结果、非分区谓词。
-- B/C/E/F/G/H/I 会复用已有等价分区并重算变化的分区；
-- 因为前面已经执行多种窗口，不为本节首查假定固定命中数。
-- 每个非空查询紧接的重复执行应命中该查询刚填充的缓存。

-- B_FIRST: 只滚动 dt1 一天；预期 SUM / COUNT = 45933738 / 841 (R=1)
SELECT /* QC2D_B_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-09 12:12:12' AND dt1 < '2026-08-14 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- B_REPEAT: 只滚动 dt1 一天；预期 SUM / COUNT = 45933738 / 841 (R=1)
SELECT /* QC2D_B_REPEAT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-09 12:12:12' AND dt1 < '2026-08-14 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- C_FIRST: 只滚动 dt2 一天；预期 SUM / COUNT = 37607838 / 841 (R=1)
SELECT /* QC2D_C_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-09 12:12:12' AND dt2 < '2016-08-14 12:12:12';

-- C_REPEAT: 只滚动 dt2 一天；预期 SUM / COUNT = 37607838 / 841 (R=1)
SELECT /* QC2D_C_REPEAT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-09 12:12:12' AND dt2 < '2016-08-14 12:12:12';

-- E_FIRST: 日期不变，两维日内边界改成 18:00:00；预期 SUM / COUNT = 40452100 / 841 (R=1)
SELECT /* QC2D_E_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 18:00:00' AND dt1 < '2026-08-13 18:00:00'
  AND dt2 > '2016-08-08 18:00:00' AND dt2 < '2016-08-13 18:00:00';

-- E_REPEAT: 日期不变，两维日内边界改成 18:00:00；预期 SUM / COUNT = 40452100 / 841 (R=1)
SELECT /* QC2D_E_REPEAT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 18:00:00' AND dt1 < '2026-08-13 18:00:00'
  AND dt2 > '2016-08-08 18:00:00' AND dt2 < '2016-08-13 18:00:00';

-- F_FIRST: 原始窗口的两个下界改为 >=；预期 SUM / COUNT = 39424650 / 900 (R=1)
SELECT /* QC2D_F_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 >= '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 >= '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- F_REPEAT: 原始窗口的两个下界改为 >=；预期 SUM / COUNT = 39424650 / 900 (R=1)
SELECT /* QC2D_F_REPEAT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 >= '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 >= '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- G_FIRST: 原始窗口的两个上界改为 <=；预期 SUM / COUNT = 40939650 / 900 (R=1)
SELECT /* QC2D_G_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 <= '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 <= '2016-08-13 12:12:12';

-- G_REPEAT: 原始窗口的两个上界改为 <=；预期 SUM / COUNT = 40939650 / 900 (R=1)
SELECT /* QC2D_G_REPEAT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 <= '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 <= '2016-08-13 12:12:12';

-- H_FIRST: 两维均为整天半开区间 [08,13)；预期 SUM / COUNT = 36394650 / 900 (R=1)
SELECT /* QC2D_H_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 >= '2026-08-08 00:00:00' AND dt1 < '2026-08-13 00:00:00'
  AND dt2 >= '2016-08-08 00:00:00' AND dt2 < '2016-08-13 00:00:00';

-- H_REPEAT: 两维均为整天半开区间 [08,13)；预期 SUM / COUNT = 36394650 / 900 (R=1)
SELECT /* QC2D_H_REPEAT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 >= '2026-08-08 00:00:00' AND dt1 < '2026-08-13 00:00:00'
  AND dt2 >= '2016-08-08 00:00:00' AND dt2 < '2016-08-13 00:00:00';

-- I_FIRST: 两维均为整天半开区间 [09,14)；预期 SUM / COUNT = 45484650 / 900 (R=1)
SELECT /* QC2D_I_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 >= '2026-08-09 00:00:00' AND dt1 < '2026-08-14 00:00:00'
  AND dt2 >= '2016-08-09 00:00:00' AND dt2 < '2016-08-14 00:00:00';

-- I_REPEAT: 两维均为整天半开区间 [09,14)；预期 SUM / COUNT = 45484650 / 900 (R=1)
SELECT /* QC2D_I_REPEAT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 >= '2026-08-09 00:00:00' AND dt1 < '2026-08-14 00:00:00'
  AND dt2 >= '2016-08-09 00:00:00' AND dt2 < '2016-08-14 00:00:00';

-- J 与 K 都只读 p10_10，同一 tablet 上不同日内范围不能交叉误命中。

-- J_FIRST: 同一天的小窗口 (12:12:11,18:00:00)；预期 SUM / COUNT = 161754 / 4 (R=1)
SELECT /* QC2D_J_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-10 12:12:11' AND dt1 < '2026-08-10 18:00:00'
  AND dt2 > '2016-08-10 12:12:11' AND dt2 < '2016-08-10 18:00:00';

-- J_REPEAT: 同一天的小窗口 (12:12:11,18:00:00)；预期 SUM / COUNT = 161754 / 4 (R=1)
SELECT /* QC2D_J_REPEAT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-10 12:12:11' AND dt1 < '2026-08-10 18:00:00'
  AND dt2 > '2016-08-10 12:12:11' AND dt2 < '2016-08-10 18:00:00';

-- K 首查必须重新计算：J 是 4 行 / 161754，K 是 1 行 / 40444。

-- K_FIRST: 同一天的小窗口 (12:12:12,18:00:00)；预期 SUM / COUNT = 40444 / 1 (R=1)
SELECT /* QC2D_K_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-10 12:12:12' AND dt1 < '2026-08-10 18:00:00'
  AND dt2 > '2016-08-10 12:12:12' AND dt2 < '2016-08-10 18:00:00';

-- K_REPEAT: 同一天的小窗口 (12:12:12,18:00:00)；预期 SUM / COUNT = 40444 / 1 (R=1)
SELECT /* QC2D_K_REPEAT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-10 12:12:12' AND dt1 < '2026-08-10 18:00:00'
  AND dt2 > '2016-08-10 12:12:12' AND dt2 < '2016-08-10 18:00:00';

-- L_FIRST: 两维均为 07 日，与之前的窗口不重叠；预期 SUM / COUNT = 364986 / 36 (R=1)
SELECT /* QC2D_L_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 >= '2026-08-07 00:00:00' AND dt1 < '2026-08-08 00:00:00'
  AND dt2 >= '2016-08-07 00:00:00' AND dt2 < '2016-08-08 00:00:00';

-- L_REPEAT: 两维均为 07 日，与之前的窗口不重叠；预期 SUM / COUNT = 364986 / 36 (R=1)
SELECT /* QC2D_L_REPEAT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 >= '2026-08-07 00:00:00' AND dt1 < '2026-08-08 00:00:00'
  AND dt2 >= '2016-08-07 00:00:00' AND dt2 < '2016-08-08 00:00:00';

-- M 应始终 NULL / 0，可能直接生成 EMPTYSET；此项不要求有缓存算子。

-- M_FIRST: dt2 超出全部分区，空结果；预期 SUM / COUNT = NULL / 0 (R=1)
SELECT /* QC2D_M_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-20 12:12:12' AND dt2 < '2016-08-21 12:12:12';

-- M_REPEAT: dt2 超出全部分区，空结果；预期 SUM / COUNT = NULL / 0 (R=1)
SELECT /* QC2D_M_REPEAT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-20 12:12:12' AND dt2 < '2016-08-21 12:12:12';

-- N 新增 col1 过滤，不能复用未过滤的 A 聚合；它的 SUM/COUNT 都不同。

-- N_FIRST: 原始窗口增加非分区列过滤 col1 % 2 = 0；预期 SUM / COUNT = 19405540 / 435 (R=1)
SELECT /* QC2D_N_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12'
  AND col1 % 2 = 0;

-- N_REPEAT: 原始窗口增加非分区列过滤 col1 % 2 = 0；预期 SUM / COUNT = 19405540 / 435 (R=1)
SELECT /* QC2D_N_REPEAT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12'
  AND col1 % 2 = 0;

-- 5. 直接覆盖用户的 SUM-only 形态。
-- 增减聚合项会改变缓存计划，因此它自成一组；不要与 SUM+COUNT 的命中混算。
SET enable_query_cache = false;
-- SUM_ONLY_BASE: 原始双窗口；预期 SUM = 37523738 (R=1)
SELECT /* QC2D_SUM_ONLY_BASE */ SUM(col1) AS sum_col1
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

SET enable_query_cache = true;
-- SUM_ONLY_FILL: 原始双窗口；预期 SUM = 37523738 (R=1)
SELECT /* QC2D_SUM_ONLY_FILL */ SUM(col1) AS sum_col1
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- SUM_ONLY_HIT: 原始双窗口；预期 SUM = 37523738 (R=1)
SELECT /* QC2D_SUM_ONLY_HIT */ SUM(col1) AS sum_col1
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- SUM_ONLY_ROLL: 同时滚动 dt1、dt2 一天；预期 SUM = 46017838 (R=1)
SELECT /* QC2D_SUM_ONLY_ROLL */ SUM(col1) AS sum_col1
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-09 12:12:12' AND dt1 < '2026-08-14 12:12:12'
  AND dt2 > '2016-08-09 12:12:12' AND dt2 < '2016-08-14 12:12:12';

-- SUM_ONLY_ROLL_HIT: 同时滚动 dt1、dt2 一天；预期 SUM = 46017838 (R=1)
SELECT /* QC2D_SUM_ONLY_ROLL_HIT */ SUM(col1) AS sum_col1
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-09 12:12:12' AND dt1 < '2026-08-14 12:12:12'
  AND dt2 > '2016-08-09 12:12:12' AND dt2 < '2016-08-14 12:12:12';


-- 6. 分区版本变化：先重新热好 A，再只向 p10_10 插入一行。
-- 只执行 INSERT 一次；重跑整套时会在第 1 节重建表。
-- BEFORE_WRITE_WARM: 原始双窗口；预期 SUM / COUNT = 37523738 / 841 (R=1)
SELECT /* QC2D_BEFORE_WRITE_WARM */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- BEFORE_WRITE_HIT: 原始双窗口；预期 SUM / COUNT = 37523738 / 841 (R=1)
SELECT /* QC2D_BEFORE_WRITE_HIT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

INSERT INTO qc_list_dt1_dt2_rolling VALUES
    (9000000000, '2026-08-10 13:00:00', '2016-08-10 13:00:00', 1000000000);

-- 确认 INSERT 成功且已可见后执行。R=1：SUM=1037523738，COUNT=842。
-- 一般 R：SUM=37523738*R+1000000000，COUNT=841*R+1。
-- 增量缓存已关闭：p10_10 应版本失效并重算，其余 35 个分区可命中。
SELECT /* QC2D_AFTER_WRITE_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- 重复应命中新版本，结果仍为 1037523738 / 842（R=1）。
SELECT /* QC2D_AFTER_WRITE_HIT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

SET enable_query_cache = false;
SELECT /* QC2D_AFTER_WRITE_BASE */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_rolling
WHERE dt1 > '2026-08-08 12:12:12' AND dt1 < '2026-08-13 12:12:12'
  AND dt2 > '2016-08-08 12:12:12' AND dt2 < '2016-08-13 12:12:12';

-- 7. 补充：一个 LIST 分区含多个日期 tuple，防止同一 tablet 内错误复用。
DROP TABLE IF EXISTS qc_list_dt1_dt2_multi;
CREATE TABLE qc_list_dt1_dt2_multi (
    id BIGINT NOT NULL,
    dt1 DATETIMEV2(0) NOT NULL,
    dt2 DATETIMEV2(0) NOT NULL,
    col1 BIGINT NOT NULL
)
DUPLICATE KEY(id, dt1, dt2)
PARTITION BY LIST(date_trunc(dt1, 'day'), date_trunc(dt2, 'day')) (
    PARTITION p_cross VALUES IN (
        ('2026-08-10 00:00:00', '2016-08-11 00:00:00'),
        ('2026-08-11 00:00:00', '2016-08-10 00:00:00')
    ),
    PARTITION p_same VALUES IN (('2026-08-10 00:00:00', '2016-08-10 00:00:00'))
)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO qc_list_dt1_dt2_multi VALUES
    (1, '2026-08-10 09:00:00', '2016-08-11 09:00:00', 11),
    (2, '2026-08-10 18:00:00', '2016-08-11 18:00:00', 13),
    (3, '2026-08-11 09:00:00', '2016-08-10 09:00:00', 101),
    (4, '2026-08-11 18:00:00', '2016-08-10 18:00:00', 103),
    (5, '2026-08-10 09:00:00', '2016-08-10 09:00:00', 1001);

-- T1_BASE：预期 24 / 2，与 R 无关。
SELECT /* QC2D_T1_BASE */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_multi
WHERE dt1 >= '2026-08-10 00:00:00' AND dt1 < '2026-08-11 00:00:00'
  AND dt2 >= '2016-08-11 00:00:00' AND dt2 < '2016-08-12 00:00:00';

-- T2_BASE：预期 204 / 2，与 R 无关。
SELECT /* QC2D_T2_BASE */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_multi
WHERE dt1 >= '2026-08-11 00:00:00' AND dt1 < '2026-08-12 00:00:00'
  AND dt2 >= '2016-08-10 00:00:00' AND dt2 < '2016-08-11 00:00:00';

-- T3_BASE：预期 1001 / 1，与 R 无关。
SELECT /* QC2D_T3_BASE */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_multi
WHERE dt1 >= '2026-08-10 00:00:00' AND dt1 < '2026-08-11 00:00:00'
  AND dt2 >= '2016-08-10 00:00:00' AND dt2 < '2016-08-11 00:00:00';

SET enable_query_cache = true;

-- T1_FIRST：预期 24 / 2，与 R 无关。
SELECT /* QC2D_T1_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_multi
WHERE dt1 >= '2026-08-10 00:00:00' AND dt1 < '2026-08-11 00:00:00'
  AND dt2 >= '2016-08-11 00:00:00' AND dt2 < '2016-08-12 00:00:00';

-- T1_HIT：预期 24 / 2，与 R 无关。
SELECT /* QC2D_T1_HIT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_multi
WHERE dt1 >= '2026-08-10 00:00:00' AND dt1 < '2026-08-11 00:00:00'
  AND dt2 >= '2016-08-11 00:00:00' AND dt2 < '2016-08-12 00:00:00';

-- T2 与 T1 共用 p_cross/tablet，但选择的是另一个 tuple，首查不能命中 T1。

-- T2_FIRST：预期 204 / 2，与 R 无关。
SELECT /* QC2D_T2_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_multi
WHERE dt1 >= '2026-08-11 00:00:00' AND dt1 < '2026-08-12 00:00:00'
  AND dt2 >= '2016-08-10 00:00:00' AND dt2 < '2016-08-11 00:00:00';

-- T2_HIT：预期 204 / 2，与 R 无关。
SELECT /* QC2D_T2_HIT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_multi
WHERE dt1 >= '2026-08-11 00:00:00' AND dt1 < '2026-08-12 00:00:00'
  AND dt2 >= '2016-08-10 00:00:00' AND dt2 < '2016-08-11 00:00:00';

-- T3 只选择 p_same，不应把 p_cross 的 (10,11)/(11,10) 拼成虚假的 (10,10)。

-- T3_FIRST：预期 1001 / 1，与 R 无关。
SELECT /* QC2D_T3_FIRST */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_multi
WHERE dt1 >= '2026-08-10 00:00:00' AND dt1 < '2026-08-11 00:00:00'
  AND dt2 >= '2016-08-10 00:00:00' AND dt2 < '2016-08-11 00:00:00';

-- T3_HIT：预期 1001 / 1，与 R 无关。
SELECT /* QC2D_T3_HIT */ SUM(col1) AS sum_col1, COUNT(*) AS row_count
FROM qc_list_dt1_dt2_multi
WHERE dt1 >= '2026-08-10 00:00:00' AND dt1 < '2026-08-11 00:00:00'
  AND dt2 >= '2016-08-10 00:00:00' AND dt2 < '2016-08-11 00:00:00';

-- 测试结束。保留两张表供排查；关闭本测试连接即可结束会话级设置。
