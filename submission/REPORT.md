# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Dương Hà Đức Anh — _(MSSV: điền)_
**Repo:** _(điền URL repo bài nộp `K4-Track02-Day17-HoVaTen-MSSV-DataPipelineEngineering`)_
**Commit bài nộp:** _(cập nhật sau khi commit)_
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code — đọc code, chỉ ra 3 lỗi và đề
xuất cách sửa; tôi tự chạy verify/pytest/rerun/dbt trên máy và kiểm chứng từng thay đổi.
**Nguồn tham khảo khác (nếu có):** slide Day 17, tài liệu DuckDB `MERGE INTO` / `microbatch`.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `verify` báo *"silver_tickets has exactly one row per ticket_id (24 rows for 12 tickets)"*; T-91 hiện 3 hàng `low/open`, `high/open`, `high/closed`. | `gold_feature_daily` lệch full recompute (`c50b8851 != 8630e04a`); u05 ngày 08-12 chỉ còn `(2,0)` thay vì `(5,1)`; check *LOOKBACK_DAYS covers P99* fail vì `0 < 3`. | T-97 vẫn là hàng "sống" ở `silver_tickets` (`is_deleted=false`, còn tên/email/sđt); còn 1 hàng trong snapshot `v2026-08-16` và 2 chunk trong RAG index. |
| **Nguyên nhân gốc** | `upsert_silver_tickets` ghi bằng `INSERT` (append) thay vì upsert theo khoá: mỗi batch thêm hàng mới, không dedup giữa các batch và không có chốt thứ tự. | `config.LOOKBACK_DAYS = 0` — giả định event tới ngay trong ngày, nhưng thực tế có event trễ 3 ngày; cửa sổ recompute không phủ partition mà event trễ sẽ rơi vào. | `staging.ticket_changes_sql` lấy `ticket_id` **chỉ từ `after`**; bản ghi `op='d'` có `after = null` nên khoá thành NULL, bị `WHERE ticket_id IS NOT NULL` loại → delete biến mất. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: thay `INSERT` bằng `MERGE INTO silver_tickets ... ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`. | `pipeline/config.py`: đặt `LOOKBACK_DAYS = 3` (đo, không đoán: `python main.py --lateness` → p99 = 3.00 ngày). | `pipeline/staging.py`: `coalesce(after->>'ticket_id', before->>'ticket_id')` để delete lấy được khoá từ `before`. |
| **Khái niệm trên slide** | *Silver — Có khoá*: một hàng = một thực thể; bốn cách ghi idempotent (keyed MERGE + LSN guard). | *Data về muộn*: event time ≠ ingest time; lookback = ceil(P99) đo từ Bronze, recompute partition để nhận dữ liệu trễ. | *CDC log-based* + *Xoá phải lan*: delete gửi `after = null`, phải giữ khoá/LSN và lan tombstone xuống Gold/RAG. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (p50=0.00, p95=2.90, max=3) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: **PASS** — Gold checksum (combined): `39e115c510ecdf526800eac227158a4f`
- `make parity`: **PARITY** — `silver_tickets` và `gold_feature_daily` cùng checksum ở cả hai bản

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- **MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`:** Silver
  giữ *trạng thái hiện tại* của từng ticket nên phải upsert theo khoá + chốt LSN để batch cũ
  không ghi đè batch mới; Gold là *aggregate theo partition* nên chỉ cần `DELETE` rồi `INSERT`
  lại cửa sổ `[day-lookback, day]` — rẻ hơn và vẫn idempotent.
- **Tombstone thay vì xoá hẳn hàng trong Silver:** xoá cứng sẽ làm replay batch cũ "hồi sinh"
  ticket; giữ hàng với `is_deleted=true`, `_lsn` cao nhất và PII đã clear vừa lan được thao tác
  xoá xuống Gold/RAG, vừa để LSN chặn các thay đổi cũ hơn.
- **Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ:** tái lập được
  vĩnh viễn và point-in-time (T-91 vẫn `priority_at_creation='low'`); feedback trễ tạo version
  mới (`v2026-08-15`) thay vì sửa `v2026-08-14` — chống leakage khi train.
- **DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark:** dữ liệu vài
  chục dòng, DuckDB chạy in-process zero-key, `MERGE`/`microbatch` có sẵn; Spark chỉ đáng khi
  dữ liệu vượt một máy.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot `v2026-08-12..14` vẫn chứa văn bản T-97 vs "quyền được xoá".** Snapshot bất biến
   là hợp đồng với *training* (tái lập được, chống leakage), còn quyền xoá là hợp đồng với
   *khách hàng*. Tôi tách hai mặt: dữ liệu đã bị xoá thì không được **phục vụ** (Silver tombstone,
   snapshot mới nhất, RAG index đều sạch — đúng như lab yêu cầu), nhưng các snapshot lịch sử vẫn
   giữ text. Với yêu cầu xoá thật, hướng xử lý là **crypto-shredding**: mã hoá PII bằng key theo
   chủ thể, "xoá" = huỷ key (bản ghi vẫn còn nhưng vô nghĩa), kèm một sổ đăng ký xoá để rebuild
   hoặc redact snapshot cũ khi cần tuân thủ.
2. **Regex chỉ che email/sđt, tên "Nguyễn Văn An" vẫn còn.** Tôi đặt chốt PII **ngay tại biên
   Silver** (nơi dữ liệu rời Bronze) và bổ sung một lớp **kiểm tra hậu kiểm trên mọi cột text đi
   ra Gold** (`verify` đang làm đúng kiểu này). Cách đo: chạy một bộ nhận diện rộng hơn (NER cho
   tên người/địa chỉ + regex cho định danh), rồi **test contract khẳng định 0 hit** trên
   `silver_*`, `gold_training_set`, `gold_doc_chunks`; đếm số hit/quarantine theo ngày làm chỉ số
   observability. Không coi regex hiện tại là đủ.

## 5. Output (dán nguyên văn)

```text
$ python -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS

$ python -m pytest
..................................                                       [100%]
34 passed in 6.54s

$ python -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ python main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ python -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Bonus B1 — `python -m scripts.bonus_llm`:

```text
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```
