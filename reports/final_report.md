# Báo cáo Lab Day 25: Reliability Engineering cho Production Agents

Sinh viên: Nguyễn Cao Quang Anh, Track 3

Cách chạy lại toàn bộ:

```bash
pip install -e ".[dev]"
docker compose up -d
make test
make run-chaos
make report
```

## 1. Kiến trúc hệ thống

Ý tưởng chính của em là gateway không được phép hỏng đột ngột, mà phải xuống cấp theo từng
nấc. Mọi request đều đi qua cùng một đường ống: cache ngữ nghĩa, rồi tới từng provider được
bọc trong circuit breaker riêng, cuối cùng là một câu trả lời tĩnh. Trong toàn bộ code em
không đặt vòng lặp retry nào cả. Khi một provider lỗi thì bỏ luôn provider đó và chuyển sang
provider kế tiếp, nhờ vậy một sự cố provider không thể biến thành bão retry.

```
Request của người dùng ("Chính sách hoàn tiền là gì?")
    |
    v
+------------------------------------------------------------------+
| ReliabilityGateway.complete(prompt)                               |
+------------------------------------------------------------------+
    |
    v
[1] Tầng cache  (ResponseCache in-memory | SharedRedisCache)
    |  - chặn privacy: _is_uncacheable(query) thì bỏ qua cache hoàn toàn
    |  - cosine similarity trên n-gram >= 0.92
    |  - chặn false-hit: số 4 chữ số khác nhau thì từ chối và ghi log
    |
    +--- HIT ---> trả về route="cache_hit:<score>", cost=0, latency=0
    |
    v MISS
[2] Cổng single-flight (gộp các request trùng prompt đang chạy song song)
    |  - request đầu tiên làm leader và đi tiếp; các request sau chờ event
    |    của leader rồi đọc lại cache
    |  - nếu chờ xong mà cache vẫn rỗng thì tự đi gọi provider
    |
    v
[3] Cổng ngân sách (BudgetTracker)
    |  - từ 80% hạn mức: sắp xếp lại provider theo giá rẻ trước
    |    (route="cost_degraded")
    |  - từ 100% hạn mức: không gọi provider nào nữa
    |    (route="budget_exhausted")
    |
    v
[4] Chuỗi provider (theo thứ tự, không retry)
    |
    +--> [CircuitBreaker "primary"]  --CLOSED/HALF_OPEN--> FakeLLMProvider(primary)
    |         |  OPEN thì ném CircuitOpenError, fail fast (0 ms)     |
    |         |                                        thành công ---+--> cache.set(...)
    |         |                                                           route="primary"
    |         v lỗi provider hoặc circuit đang mở
    +--> [CircuitBreaker "backup"]   --CLOSED/HALF_OPEN--> FakeLLMProvider(backup)
    |         |                                    thành công ------> route="fallback"
    |         v hết provider
    v
[5] Fallback tĩnh
    "The service is temporarily degraded. Please try again soon."
    route="static_fallback", error=<lỗi cuối cùng>
```

Máy trạng thái của circuit breaker, mỗi provider một instance:

```
        failure_count >= failure_threshold                hết reset_timeout
CLOSED ----------------------------------> OPEN -------------------------------> HALF_OPEN
  ^        reason="failure_threshold_reached"   (allow_request mở đường cho probe)    |
  |                                                                                   |
  |  success_count >= success_threshold, reason="probe_success"                       |
  +-----------------------------------------------------------------------------------+
                                                                                      |
                          probe thất bại, reason="probe_failure"                      |
                          OPEN <-----------------------------------------------------+
```

Mỗi lần chuyển trạng thái em đều ghi vào `transition_log` dưới dạng
`{"from", "to", "reason", "ts"}`. Tầng metrics đọc log này để tính `circuit_open_count` và
`recovery_time_ms`, nên hai con số đó là bằng chứng lấy từ hệ thống chứ không phải em tự đếm
tay.

## 2. Cấu hình và lý do chọn

| Tham số | Giá trị | Lý do em chọn |
|---|---:|---|
| `failure_threshold` | 3 | Để 1 thì chỉ cần một request xui là circuit đã mở, mà primary vốn có fail rate 0.25 nên lỗi lẻ tẻ là chuyện bình thường. Để 5 trở lên thì circuit đóng quá lâu, tốn latency vào một provider đã chết. Xác suất 3 lần lỗi liên tiếp ở mức 0.25 chỉ khoảng 1.6%, đủ hiếm để circuit chỉ mở khi provider thực sự có vấn đề. |
| `reset_timeout_seconds` | 2 | Đủ dài để probe không bị bắn vào giữa lúc sự cố còn đang diễn ra, nhưng đủ ngắn để thời gian phục hồi đo được chỉ khoảng 2.3 giây, thấp hơn nhiều so với mục tiêu 5 giây. |
| `success_threshold` | 1 | Provider ở đây không giữ trạng thái nên một probe thành công là đủ bằng chứng. Nếu để 2 thì thời gian circuit nằm ở HALF_OPEN sẽ dài gấp đôi, trong khi lưu lượng vẫn đang bị chặn bớt. |
| `cache ttl_seconds` | 300 | Các câu hỏi FAQ và chính sách trong `sample_queries.jsonl` ổn định trong vài phút chứ không phải vài giờ. 300 giây bắt được đợt traffic dồn dập, đó là nguồn của tỉ lệ hit gần 60%, mà vẫn giới hạn được độ cũ của dữ liệu cho các câu hỏi có mốc thời gian. |
| `cache similarity_threshold` | 0.92 | Em có thử 0.85 và gặp lỗi thật: câu "What is the tuition fee for the 2024 academic year?" khớp với biến thể 2026 ở mức 0.90, tức là một false hit đúng nghĩa, chỉ bị chặn lại nhờ bộ lọc số. Ở 0.92 thì các cặp diễn đạt lại vẫn hit, còn các cặp gần giống nhưng khác ý thì rớt xuống dưới ngưỡng. Em cũng thử 0.97 thì tỉ lệ hit gần như bằng 0, cache chỉ còn khớp chính xác. |
| `load_test.requests` | 100 mỗi kịch bản | 100 request nhân 5 kịch bản là 500 mẫu, đủ để P95 và P99 ổn định mà không phải chạy tới vài phút. |
| `load_test.concurrency` | 1 (mặc định) | Để mặc định chạy tuần tự cho số liệu nền dễ lặp lại. Kịch bản `concurrent_load_8` sẽ ghi đè lên 8, trình bày ở mục 10.1. |
| `budget.limit` | null (mặc định) | Em tắt định tuyến theo chi phí ở lần chạy nền để nó không làm nhiễu các chỉ số về độ tin cậy. Kịch bản `cost_budget_squeeze` đặt 0.008 để kiểm tra riêng phần này. |
| `budget.degrade_ratio` | 0.8 | Chừa lại 20% ngân sách để chạy trên provider rẻ sau khi đã hạ cấp provider đắt, thay vì rơi thẳng từ phục vụ đầy đủ xuống chỉ còn cache. |
| providers | primary (fail 0.25, 180 ms, $0.010/1k) rồi backup (fail 0.05, 260 ms, $0.006/1k) | Primary nhanh nhưng hay lỗi, backup chậm hơn nhưng rẻ và ổn định hơn. Thứ tự này làm cho đường fallback được kích hoạt và quan sát được ở mọi kịch bản. |

## 3. Định nghĩa SLO

Em đo dựa trên lần chạy tổng hợp trong `reports/metrics.json` (500 request qua 5 kịch bản,
`--seed 7` là mặc định) và bảng chi tiết từng kịch bản trong `reports/scenario_metrics.json`.

Về tính lặp lại, em phải nói rõ một điều mà lúc đầu em tưởng đã làm được nhưng đo kỹ thì
không đúng. Ban đầu em nghĩ chỉ cần cố định seed là mọi chỉ số đếm sẽ giống hệt nhau giữa các
lần chạy. Em chạy thử bốn lần liên tiếp cùng `--seed 7` và kết quả như sau:

| Chỉ số | Lần 1 | Lần 2 | Lần 3 | Lần 4 | Ổn định? |
|---|---:|---:|---:|---:|---|
| total_requests | 500 | 500 | 500 | 500 | Có |
| concurrency | 8 | 8 | 8 | 8 | Có |
| availability | 0.918 | 0.918 | 0.914 | 0.914 | Không |
| cache_hit_rate | 0.596 | 0.596 | 0.594 | 0.594 | Không |
| fallback_success_rate | 0.7192 | 0.7192 | 0.7034 | 0.6972 | Không |
| circuit_open_count | 11 | 11 | 11 | 10 | Không |
| estimated_cost | 0.070634 | 0.070634 | 0.070346 | 0.071254 | Không |
| latency_p95_ms | 315.93 | 314.52 | 313.65 | 313.56 | Không |

Nguyên nhân nằm ở kịch bản `concurrent_load_8`. Em đã rút sẵn danh sách prompt trên luồng
chính nên tập câu hỏi là cố định, nhưng bên trong `FakeLLMProvider` vẫn còn `random.random()`
để quyết định lỗi và `random.randint()` để sinh số output token, và tám luồng worker tranh
nhau bộ sinh số ngẫu nhiên dùng chung theo thứ tự không xác định. Vì vậy vài request đổi kết
quả giữa các lần chạy. Biên độ dao động là khoảng 2 request trên 500, tức 0.4% availability,
và `circuit_open_count` lệch 1 lần mở giữa các lần chạy.

Em đo tiếp các kịch bản chạy tuần tự, mỗi kịch bản hai lần với cùng seed 7, và kết quả cũng
không hoàn toàn như em nghĩ:

| Kịch bản | Lần 1 | Lần 2 | Giống nhau? |
|---|---|---|---|
| `primary_timeout_100` | 100 req, 100 thành công, 61 hit, 6 lần mở circuit | y hệt | Có |
| `all_healthy` | 100 req, 99 thành công, 69 hit, 1 lần mở circuit | y hệt | Có |
| `cost_budget_squeeze` | 100 req, 62 thành công, 46 hit, 0 lần mở circuit | y hệt | Có |
| `primary_flaky_50` | 100 req, 98 thành công, 58 hit, 2 lần mở circuit | 2 lần mở thành 3 | Không |

Ba trong bốn kịch bản tuần tự lặp lại chính xác từng con số. Riêng `primary_flaky_50` thì
không, và lý do khác hẳn với chuyện tranh chấp luồng ở trên: việc chuyển từ OPEN sang
HALF_OPEN phụ thuộc `time.monotonic()` so với `reset_timeout_seconds`. Ở fail rate 0.5,
circuit nằm rất sát ranh giới hết thời gian chờ, nên chỉ cần máy chạy nhanh hay chậm hơn vài
mili giây là số lần mở circuit lệch đi một đơn vị. Đây là đặc tính vốn có của circuit breaker
dựa trên đồng hồ chứ không phải lỗi của em, nhưng nó có nghĩa là không thể tất định hoá hoàn
toàn nếu vẫn dùng đồng hồ thật.

Nếu bắt buộc phải có số liệu tất định tuyệt đối thì cần hai thay đổi mà em chưa làm: cấp cho
mỗi worker một `random.Random(seed + worker_id)` riêng thay vì dùng module `random` toàn cục,
và tiêm một nguồn thời gian giả vào circuit breaker thay cho `time.monotonic()`. Các phân vị
latency thì trong mọi trường hợp đều không tất định được vì chúng đo bằng đồng hồ thực.

Về ý nghĩa của availability: con số tổng hợp có tính cả `cost_budget_squeeze`, mà mục đích
của kịch bản này là từ chối request khi đã hết hạn mức chi phí. Những lần từ chối đó bị tính
là thất bại nên kéo con số chung xuống 0.914. Vì vậy em đánh giá SLO trên bốn kịch bản đại
diện cho lưu lượng người dùng thật, còn kịch bản chặn chi phí em báo cáo riêng ở mục 10.2.

| SLI | Mục tiêu | Đo được (các kịch bản người dùng thật) | Đạt? |
|---|---|---:|---|
| Availability | >= 99% | 1.00 ở `primary_flaky_50` và `all_healthy`, 0.99 ở `concurrent_load_8`, 0.98 khi primary chết hoàn toàn | Một phần |
| Latency P95 | < 2500 ms | 313.56 ms tổng hợp, 311 tới 320 ms theo từng kịch bản | Đạt |
| Fallback success rate | >= 95% | 1.00 ở hai kịch bản, 0.9524 khi primary chết hoàn toàn, 0.95 khi chạy song song | Đạt |
| Cache hit rate | >= 10% | 59.40% tổng hợp | Đạt |
| Recovery time | < 5000 ms | 2226.52 ms | Đạt |

Chỗ chưa đạt trọn vẹn là `primary_timeout_100` với 0.98. Khi primary lỗi 100%, 2 request thất
bại trên 100 là những request mà backup cũng lỗi theo (fail rate 0.05 của chính nó) trong khi
cache chưa có sẵn câu trả lời nào. Em nghĩ thứ duy nhất bịt được khe này là thêm một provider
độc lập thứ ba, chứ không có logic nào ở gateway cứu được tình huống tất cả provider cùng lỗi
một lúc.

## 4. Số liệu đo được

Chép nguyên từ `reports/metrics.json`, bản CSV nằm ở `reports/metrics.csv`:

| Chỉ số | Giá trị |
|---|---:|
| total_requests | 500 |
| availability | 0.9140 |
| error_rate | 0.0860 |
| latency_p50_ms | 269.50 |
| latency_p95_ms | 313.56 |
| latency_p99_ms | 319.83 |
| fallback_success_rate | 0.6972 |
| cache_hit_rate | 0.5940 |
| estimated_cost | 0.071254 |
| estimated_cost_saved | 0.297000 |
| circuit_open_count | 10 |
| recovery_time_ms | 2226.52 |
| concurrency | 8 |
| coalesced_waits | 22 |
| coalesced_hits | 12 |
| budget_spent | 0.071254 |

Có một điểm về latency em muốn nói rõ. Các lần hit cache được ghi là 0 ms và bị loại khỏi
phân phối latency (điều kiện `if result.latency_ms > 0`), nên P50, P95, P99 chỉ mô tả phần
đuôi của những request thực sự đi tới provider. Đó mới là con số dùng để tính toán năng lực
hệ thống. Nếu gộp cả cache hit vào thì các phân vị sẽ đẹp lên nhưng không còn nói được gì về
sức khỏe của provider nữa.

Về hai tỉ lệ tổng hợp: `availability` 0.914 và `fallback_success_rate` 0.6972 đều bị kéo
xuống bởi riêng `cost_budget_squeeze` với 41 lần từ chối có chủ đích. Nếu bỏ kịch bản đó ra
thì bốn kịch bản còn lại chạy ở mức availability 0.98 tới 1.00 và fallback success 0.95 tới
1.00. Bảng chi tiết ở mục 7 mới là góc nhìn có ý nghĩa, bảng này chỉ là số liệu thô xuất ra
nguyên trạng.

## 5. So sánh có cache và không cache

Hai dòng dưới đây lấy từ `reports/scenario_metrics.json`, seed 42, cùng cấu hình provider của
`all_healthy`, mỗi bên 100 request, khác biệt duy nhất là `cache.enabled`.

| Chỉ số | Không cache | Có cache | Chênh lệch |
|---|---:|---:|---|
| availability | 1.00 | 1.00 | không đổi |
| latency_p50_ms | 219.34 | 277.42 | +58.08 ms, em giải thích bên dưới |
| latency_p95_ms | 310.48 | 313.96 | +3.48 ms |
| estimated_cost | 0.052268 | 0.017950 | giảm 0.034318, tức 65.7% |
| cache_hit_rate | 0.0 | 0.64 | +0.64 |
| estimated_cost_saved | 0.0 | 0.064 | +0.064 |

Con số P50 tăng 58 ms nhìn qua giống như cache làm chậm hệ thống, nhưng thực ra đó là hiệu
ứng của cách đo chứ không phải hồi quy, và em nghĩ cần nói chính xác chỗ này. Vì cache hit bị
loại khỏi phân phối latency, nên phân vị của lần chạy có cache chỉ được tính trên 36 request
thực sự chạm tới provider. Mà 36 request đó lại lệch về phía chậm: chúng là những câu cache
không phục vụ được, trong đó vài câu rơi xuống backup vốn chậm hơn. Lần chạy không cache thì
tính trung bình trên cả 100 request. Đem hai P50 này so với nhau là đang so hai tập khác
nhau.

Cái mà cache thực sự mang lại, nói bằng những chỉ số so sánh được với nhau:

- Chi phí giảm 65.7%, từ 0.0523 xuống 0.0180, ở tỉ lệ hit 64%.
- 64 trên 100 request được trả lời trong 0 ms, không chạm tới provider nên cũng không thể
  lỗi. Latency trung bình mà người dùng cảm nhận trên cả 100 request rơi vào khoảng
  0.36 x 278, tức gần 100 ms khi có cache, so với 219 ms khi không có cache. Đó là mức cải
  thiện thật khoảng 54% mà cột P50 đã che mất.

## 6. Cache dùng chung qua Redis

Vì sao cache in-memory không đủ cho môi trường thật: `ResponseCache` nằm trong heap của một
tiến trình. Khi có N bản sao gateway đứng sau load balancer, một câu đã được trả lời ở bản
sao số 1 vẫn là miss ở các bản sao còn lại, nên tỉ lệ hit thực tế tụt xuống còn khoảng
`hit_rate / N`, kéo theo phần tiết kiệm chi phí cũng giảm. Tệ hơn nữa, mỗi lần deploy cuốn
chiếu là toàn bộ cache bị vứt đi, tạo ra một đợt dồn request vào provider đúng vào lúc hệ
thống đang yếu nhất.

`SharedRedisCache` giải quyết bằng cách đưa cache ra khỏi tiến trình. Key là
`rl:cache:<md5(query)[:12]>`, value là một Redis Hash chứa `query` và `response`, còn hạn
dùng thì giao cho `EXPIRE` của Redis lo, nên không cần quét dọn thủ công và TTL sống sót qua
cả lần khởi động lại. Khi tra cứu, em thử khớp chính xác bằng hash trước vì đó là đường O(1)
và cho điểm 1.0, sau đó mới `SCAN` và chấm điểm tương đồng cục bộ. Điểm quan trọng là em dùng
lại đúng hàm `ResponseCache.similarity()`, nhờ vậy hai backend xếp hạng kết quả giống hệt
nhau. Bộ chặn privacy và bộ chặn false-hit đều được áp cho cả `set` lẫn `get`, nên một câu
hỏi nhạy cảm không bao giờ bị ghi vào kho dùng chung, nơi mà nó sẽ sống lâu hơn cả tiến trình
đã nhìn thấy nó.

### Bằng chứng dùng chung trạng thái

Hai đối tượng `SharedRedisCache` độc lập, hai kết nối riêng, cùng prefix. Ghi ở instance A,
đọc ở instance B:

```
instance B get     -> ('Refund within 30 days.', 1.0)        # hit chính xác do A ghi
instance B similar -> (None, 0.8997354108424374)             # 0.90 < ngưỡng 0.92 nên miss, đúng như mong đợi
privacy stored?    -> ['rl:proof:17b9f1398a8d']              # chỉ 1 key, câu "account balance for user 123" đã KHÔNG được ghi
```

Test `tests/test_redis_cache.py::test_shared_state_across_instances` kiểm tra đúng tính chất
này trong CI.

### Output từ Redis CLI

```bash
$ docker compose exec redis redis-cli KEYS "rl:cache:*"
rl:cache:0bc3b1acf73d
rl:cache:095946136fea
rl:cache:fff10da1c72c
rl:cache:da61fb49b4f6
rl:cache:d354658dc020
...

$ docker compose exec redis redis-cli DBSIZE
(integer) 15

$ docker compose exec redis redis-cli HGETALL rl:cache:0bc3b1acf73d
1) "response"
2) "[primary] reliable answer for: What is the tuition fee for the 2024 academic year?"
3) "query"
4) "What is the tuition fee for the 2024 academic year?"

$ docker compose exec redis redis-cli TTL rl:cache:0bc3b1acf73d
(integer) 272        # đang đếm ngược từ ttl_seconds=300
```

15 key cho 20 câu hỏi mẫu, nghĩa là các câu bị gắn cờ nhạy cảm chưa từng được ghi vào. Đây là
bộ chặn hoạt động xuyên suốt từ gateway xuống tới Redis chứ không chỉ đúng trong unit test.

### So sánh latency giữa in-memory và Redis

Cùng cấu hình `all_healthy`, 100 request, seed 42, chỉ đổi `cache.backend`:

| Chỉ số | Cache in-memory | Cache Redis | Ghi chú |
|---|---:|---:|---|
| availability | 1.00 | 1.00 | đổi backend không ảnh hưởng gì tới độ tin cậy |
| latency_p50_ms | 277.42 | 214.67 | hai tập miss khác nhau, giống lý do ở mục 5, không phải Redis nhanh hơn |
| latency_p95_ms | 313.96 | 304.55 | cả hai đều bị chi phối bởi latency provider 180 tới 320 ms |
| cache_hit_rate | 0.64 | 0.62 | lần chạy Redis bắt đầu từ cache đã xóa sạch nên mất nhiều thời gian làm nóng hơn |
| estimated_cost | 0.017950 | 0.019630 | bám sát chênh lệch tỉ lệ hit |

Kết luận trung thực là ở quy mô này Redis gần như không tốn thêm gì đo được. Một vòng đi về
qua mạng là không đáng kể so với một lần gọi provider mất 180 ms, và với 15 key thì việc
`SCAN` cũng không tệ hơn việc quét tuyến tính danh sách `_entries` trong bộ nhớ. Redis chỉ
bắt đầu tốn kém khi không gian key đủ lớn để `SCAN` trở thành phần chiếm ưu thế, đây chính là
điểm yếu về khả năng mở rộng em nêu ở mục 8. Còn thứ Redis mang lại thì nằm ở nửa đầu mục
này: trạng thái dùng chung giữa các bản sao và sống sót qua khởi động lại.

## 7. Các kịch bản chaos

Số liệu từng kịch bản lấy từ `reports/scenario_metrics.json`, seed 42, mỗi kịch bản 100
request. Điều kiện đạt hay không đạt em viết thẳng trong code, ở `SCENARIO_CRITERIA` trong
`src/reliability_lab/chaos.py`, để việc chấm là máy móc chứ không phải nhìn bằng mắt.

| Kịch bản | Kỳ vọng | Quan sát được | Kết quả |
|---|---|---|---|
| `primary_timeout_100` (primary fail_rate=1.0) | Circuit của primary mở nhanh và hầu như giữ nguyên trạng thái mở, gần như toàn bộ lưu lượng còn sống do backup phục vụ | availability 0.98, fallback_success_rate 0.9524, circuit_open_count 6, cache_hits 58, 40 lần fallback thành công, 2 lần fallback tĩnh, chi phí 0.0139 so với 0.0523 của lần chạy không cache. Circuit breaker đã biến 100 lần lỗi primary thành các lần bỏ qua tức thì | Đạt (`availability >= 0.95 and fallback_success_rate >= 0.9`) |
| `primary_flaky_50` (primary fail_rate=0.5) | Circuit dao động qua lại giữa OPEN, HALF_OPEN và CLOSED, lưu lượng chia cho cả primary lẫn backup | availability 1.00, circuit_open_count 4, recovery_time_ms 2306, fallback_success_rate 1.00, 33 lần fallback thành công, 0 lần fallback tĩnh. Trong transition log có đủ bốn chu kỳ mở rồi phục hồi | Đạt (`availability >= 0.95 and circuit_open_count >= 1`) |
| `all_healthy` (mặc định: primary 0.25, backup 0.05) | Đa số lưu lượng đi qua primary, circuit hiếm khi mở, fallback tĩnh hiếm khi xảy ra | availability 1.00, circuit_open_count 2, recovery_time_ms 2284, cache_hits 64, 21 lần fallback thành công, 0 lần fallback tĩnh | Đạt (`availability >= 0.95 and static_fallbacks <= 5% số request`) |
| `no_cache_baseline` (kịch bản em tự thêm: tắt cache, provider khỏe mạnh) | Availability tương đương, không có cache hit nào, chi phí cao hơn hẳn, để tách bạch phần đóng góp của cache | availability 1.00, cache_hits 0, estimated_cost 0.0523 so với 0.0180 khi có cache, tức đắt gấp 2.9 lần, circuit_open_count 1 | Đạt (`availability >= 0.95 and cache_hits == 0`) |
| `cost_budget_squeeze` (kịch bản em tự thêm: `budget_limit: 0.008`) | Gateway hạ cấp xuống provider rẻ khi qua 80% ngân sách, rồi chỉ còn phục vụ bằng cache hoặc câu trả lời tĩnh khi chạm 100% | availability 0.59, estimated_cost 0.008442 so với hạn mức 0.008, tức vượt 5.5% đúng bằng một request đang bay, cache_hits 43, static_fallbacks 41. Hệ thống vẫn trả lời được 43% lưu lượng bằng cache rất lâu sau khi đã ngừng trả tiền cho provider | Đạt (`budget_spent <= limit * 1.2 and cache_hits > 0`) |
| `concurrent_load_8` (kịch bản em tự thêm: 8 luồng worker) | Availability ngang với chạy tuần tự, các bộ đếm vẫn chính xác khi có tranh chấp | availability 0.99, cache_hits 63, circuit_open_count 1, chi phí 0.0168 (trước khi có single-flight là 0.0251), total_requests đúng 100 và `successful + failed == 100` | Đạt (`availability >= 0.95 and concurrency > 1`) |

Bằng chứng về thời gian phục hồi: hàm `calculate_recovery_time_ms()` ghép mỗi lần chuyển
`to="open"` với lần chuyển `to="closed"` kế tiếp của cùng breaker rồi lấy trung bình các
khoảng cách. Lần chạy tổng hợp phục hồi trong 2227 ms so với `reset_timeout_seconds` là 2
giây. Phần dôi ra khoảng 227 ms chính là latency của bản thân request probe, đúng như mô hình
dự đoán, và điều này xác nhận probe ở trạng thái HALF_OPEN là một request thật chứ không phải
một hiệu ứng của bộ đếm giờ.

Kịch bản `primary_timeout_100` báo `recovery_time_ms: null`, và như vậy là đúng. Primary có
fail rate 1.0 nên không bao giờ phục hồi, do đó không tồn tại cặp `open` rồi `closed` nào cả.
Nếu chỗ này ra một con số khác null thì mới là dấu hiệu có lỗi.

## 8. Phân tích điểm yếu còn lại

Điểm yếu em thấy rõ nhất là trường hợp một sự cố provider kéo dài hơn TTL của cache.

Phần Redis và phần breaker dùng chung ở mục 10.3 và 10.4 đã xử lý xong chuyện cache và trạng
thái circuit bị bó hẹp trong một tiến trình. Thứ còn lại là tình huống gateway thật sự không
hấp thụ nổi: cả hai provider cùng lỗi trên cùng một request. Với fail rate đang cấu hình
(0.25 cho primary và 0.05 cho backup) thì xác suất đó rơi vào khoảng 1.25% lưu lượng, và đúng
bằng 2 lần fallback tĩnh trên 100 request quan sát được ở `all_healthy`. Cache che được phần
lớn vì gần 60% request không hề chạm tới provider, nhưng chỉ che được cho tới khi các entry
hết hạn theo `ttl_seconds`. Một sự cố provider kéo dài hơn 300 giây sẽ làm cạn cache, và tỉ
lệ fallback tĩnh sẽ leo dần về đúng xác suất hai provider cùng lỗi.

Hướng sửa trước khi đưa lên production: thêm provider thứ ba vào chuỗi, và phải là nhà cung
cấp độc lập chứ không phải một region khác của cùng nhà cung cấp đó. Song song là làm cho TTL
biết thích nghi: khi `circuit_open_count` khác 0 thì kéo dài TTL hiệu dụng để cache giữ lại
những câu trả lời tuy cũ nhưng vẫn dùng được xuyên suốt sự cố, thay vì để chúng hết hạn đúng
vào thời điểm tệ nhất. Trả về chính sách hoàn tiền của 10 phút trước vẫn tốt hơn nhiều so với
trả về câu "dịch vụ đang tạm thời suy giảm". Điều làm cho việc phục vụ dữ liệu cũ này an toàn
chính là hai bộ chặn đã có sẵn: bộ chặn privacy và bộ kiểm tra số 4 chữ số, vì các câu hỏi
nhạy cảm và các câu gắn mốc thời gian đúng là những câu bị loại khỏi cơ chế này.

Điểm yếu thứ hai nhẹ hơn: `SharedRedisCache.get()` quét toàn bộ prefix bằng `SCAN` mỗi lần
miss, tức là O(kích thước cache) cho mỗi request. Với 15 key thì không nhìn thấy gì, nhưng
với 100 nghìn key thì nó sẽ chi phối toàn bộ latency. Cách sửa là bỏ hẳn việc tính tương đồng
theo kiểu vét cạn ở gateway và đưa embedding vào một vector index, cụ thể là dùng `FT.SEARCH`
của Redis Search với truy vấn KNN, đồng thời giữ nguyên kiểu trả về `(text, score)` để không
có chỗ nào phía trên phải sửa theo.

## 9. Các bước tiếp theo

1. Trạng thái circuit lưu trên Redis với cơ chế bầu probe bằng `SET NX`: em đã làm, trình bày
   ở mục 10.4. Bước tiếp theo là chuyển việc bầu probe sang một key lease riêng thay vì dùng
   chính cờ open, để một bản sao chết giữa lúc probe sẽ nhả slot sớm chứ không làm cả cụm
   phải chờ hết một chu kỳ reset.
2. Cache ngữ nghĩa có vector index: thay việc `SCAN` cộng chấm điểm cosine trên n-gram bằng
   một index KNN của Redis Search trên embedding. Cách này đưa đường xử lý miss từ O(n) về
   O(log n) và cải thiện khả năng nhận ra các câu diễn đạt lại mà n-gram bỏ sót, ví dụ "how
   do I get my money back" so với "refund policy" hiện đang chấm thấp hơn 0.92 rất nhiều dù ý
   định giống hệt nhau.
3. Định tuyến theo chi phí có ngắt ngân sách: em đã làm, trình bày ở mục 10.2. Bước tiếp theo
   là đưa `BudgetTracker.spent` vào Redis bằng `INCRBYFLOAT` trên một key theo cửa sổ thời
   gian, để hạn mức có hiệu lực trên toàn cụm thay vì trên từng tiến trình, vì hiện tại N bản
   sao đều có thể tiêu hết trọn vẹn hạn mức.
4. Single-flight cho các lần cache miss: em đã làm, trình bày ở mục 10.1, và nó đã khôi phục
   tỉ lệ hit khi chạy song song (từ 44 lên 62 hit ở 16 worker) cùng 4 điểm availability ở 8
   worker. Bước tiếp theo là gộp theo prompt đã chuẩn hóa thay vì chuỗi khớp chính xác, để
   những cách diễn đạt khác nhau của cùng một ý cũng chia chung một lần gọi provider.

## 10. Các phần nâng cao (điểm cộng)

Cả sáu mục nâng cao trong đề em đều đã làm và đều có test đi kèm. Ngoài ra em làm thêm một
phần không nằm trong đề, là single-flight, vì lúc đo phần chạy song song em phát hiện ra một
lỗi thật.

| Mục nâng cao | Nằm ở đâu | Bằng chứng |
|---|---|---|
| Bảng SLO | mục 3 | 5 SLI đo với mục tiêu cụ thể |
| Chạy song song (`ThreadPoolExecutor`) | `chaos.run_scenario`, `CircuitBreaker.lock` | mục 10.1, kịch bản `concurrent_load_8` |
| Single-flight (ngoài đề bài) | `ReliabilityGateway._call_providers_single_flight` | mục 10.1, sửa lỗi mà phần đo song song phát hiện ra |
| Định tuyến theo chi phí | `gateway.BudgetTracker` | mục 10.2, kịch bản `cost_budget_squeeze` |
| Redis xuống cấp mềm | `cache.SharedRedisCache._degrade` | mục 10.3 |
| Trạng thái circuit trên Redis | `shared_circuit.SharedCircuitBreaker` | mục 10.4 |
| Test theo tính chất | `tests/test_properties.py` | mục 10.5 |

### 10.1 Chạy song song và single-flight

Hàm `run_scenario()` phân tải ra `ThreadPoolExecutor` khi `concurrency > 1`. Việc này biến
breaker thành trạng thái dùng chung có thể bị sửa đồng thời, nên em cho `CircuitBreaker` bảo
vệ các bộ đếm và transition log bằng một `RLock`. Nếu không có khóa thì hai worker có thể
cùng đọc `failure_count == 2`, cùng ghi 3, rồi hoặc là ghi log chuyển trạng thái OPEN hai lần
làm `circuit_open_count` bị thổi phồng, hoặc là bỏ lỡ ngưỡng hoàn toàn. Danh sách prompt được
rút sẵn trên luồng chính nên số worker không thể làm đổi tập câu hỏi được chạy, mà chỉ đổi
cách chúng đan xen nhau.

Lần đo đầu tiên đã lộ ra một khiếm khuyết thật. Đây là kết quả khi chưa có cơ chế chống dồn,
tức `single_flight=False`, 100 request, seed 42:

| Số worker | Thông lượng | Availability | Cache hits | Chi phí |
|---:|---:|---:|---:|---:|
| 1 | 8.25 req/s | 0.99 | 62 | 0.017766 |
| 4 | 28.91 req/s | 0.96 | 56 | 0.017162 |
| 8 | 52.92 req/s | 0.94 | 52 | 0.017552 |
| 16 | 74.81 req/s | 0.97 | 44 | 0.021560 |

Thông lượng tăng khoảng 9 lần và P95 gần như không đổi, nhưng cache hit giảm 29% và
availability rơi từ 0.99 xuống 0.94. Hai hiện tượng này thực chất là một vấn đề. Nhiều worker
cùng miss một câu hỏi vào cùng một thời điểm, tất cả cùng gọi provider, rồi tất cả cùng ghi
một entry giống nhau. Mỗi lần gọi thừa như vậy đều mang theo fail rate 0.25 của primary, nên
tỉ lệ hit sụp xuống đồng nghĩa với việc nhiều request hơn bị phơi ra trước rủi ro lỗi
provider.

Cách sửa của em là single-flight ở nhánh miss. `ReliabilityGateway` giữ một
`dict[str, threading.Event]` cho các prompt đang chạy. Người gọi đầu tiên của một prompt trở
thành leader và đi hết chuỗi provider; những người gọi cùng prompt sau đó chờ trên event của
leader rồi đọc lại cache. Nếu một follower tỉnh dậy mà cache vẫn rỗng, tức leader đã lỗi hoặc
đã hết thời gian chờ, thì follower tự đi gọi provider. Nhờ vậy việc gộp request không bao giờ
biến một lỗi provider thành nhiều request bị rớt. Cơ chế này tự động không hoạt động khi
`concurrency == 1` hoặc khi không cấu hình cache.

Cùng phép đo đó với `single_flight=True`, đây là mặc định:

| Số worker | Thông lượng | Availability | Cache hits | Chi phí | Số lần chờ gộp |
|---:|---:|---:|---:|---:|---:|
| 1 | 8.26 req/s | 0.99 | 62 | 0.017766 | 0 |
| 4 | 29.14 req/s | 0.97 | 62 | 0.015326 | 9 |
| 8 | 46.20 req/s | 0.98 | 59 | 0.015070 | 16 |
| 16 | 80.15 req/s | 0.97 | 62 | 0.016268 | 30 |

So sánh trực tiếp tại điểm suy giảm nặng nhất:

| Chỉ số | 8 worker, không single-flight | 8 worker, có single-flight | Thay đổi |
|---|---:|---:|---|
| Availability | 0.94 | 0.98 | tăng 4 điểm |
| Cache hits | 52 | 59 | tăng 13% |
| Chi phí | 0.017552 | 0.015070 | giảm 14% |
| Thông lượng | 52.92 req/s | 46.20 req/s | giảm 13% |

Ở mức 16 worker, tỉ lệ hit được khôi phục hoàn toàn về mức chạy tuần tự, 62 trên 100, đúng
bằng lúc chạy 1 worker, chi phí giảm 25% so với lần chạy không có bảo vệ, còn thông lượng thì
thậm chí nhỉnh hơn (80.15 so với 74.81 req/s) vì bớt được các request làm việc thừa. Mức giảm
13% thông lượng ở 8 worker là cái giá em chấp nhận và cũng xin nói thẳng: follower phải chờ
leader thay vì lao lên chạy trước, đổi một chút thời gian thực lấy tỉ lệ hit tốt hơn hẳn, chi
phí thấp hơn và availability cao hơn.

Kịch bản `concurrent_load_8` ở mục 7 phản ánh đúng điều này: chi phí giảm từ 0.0251 xuống
0.0168 và cache hit tăng từ 55 lên 63 sau khi có single-flight.

Giới hạn còn lại em xin nói rõ: việc gộp đang lấy khóa theo đúng chuỗi prompt. Hai câu hỏi
giống nhau về ý nhưng khác nhau về chữ thì vẫn dồn vào provider như cũ. Muốn xử lý thì phải
lấy khóa theo dạng đã chuẩn hóa, hoặc theo entry gần nhất trong cache vượt ngưỡng, đổi lại là
chấp nhận rủi ro gộp nhầm những request chỉ trông giống nhau thôi.

### 10.2 Định tuyến theo chi phí

`BudgetTracker` cho gateway hai ngưỡng thay vì một:

- Dưới `degrade_ratio` (0.8): định tuyến bình thường theo đúng thứ tự provider đã cấu hình.
- Từ 80% hạn mức trở lên: sắp xếp lại provider theo giá rẻ trước, hạ cấp provider đắt nhưng
  vẫn giữ nó làm phương án cuối, route được ghi là `cost_degraded`.
- Từ 100% hạn mức: không gọi provider nào nữa, route là `budget_exhausted`, và chỉ còn cache
  có thể trả lời.

Kết quả quét qua các mức ngân sách, mỗi mức 100 request, seed 42:

| Hạn mức | Chi tiêu cuối | Vượt | Availability | Cache hits |
|---:|---:|---:|---:|---:|
| 0.004 | 0.004108 | +2.7% | 0.44 | 36 |
| 0.008 | 0.008252 | +3.2% | 0.65 | 50 |
| 0.020 | 0.020426 | +2.1% | 0.97 | 59 |

Hạn mức được giữ trong khoảng sai lệch chừng 3% ở cả ba trường hợp. Phần vượt là những request
đã bay dở khi ngưỡng bị chạm: em ghi nhận chi tiêu sau khi provider trả về, nên bất kỳ lời gọi
nào bắt đầu trước ngưỡng thì vẫn hạ cánh. Muốn triệt tiêu hẳn phần vượt thì phải đặt trước
ngân sách rồi hoàn lại phần chênh, việc này đáng làm nếu hạn mức là ràng buộc cứng theo hợp
đồng, còn nếu chỉ là một lan can bảo vệ thì không cần thiết.

Em muốn lưu ý dáng của sự suy giảm ở đây. Với hạn mức 0.004, hệ thống vẫn trả lời được 36%
lưu lượng bằng cache rất lâu sau khi đã ngừng trả tiền cho provider. Đó đúng là hành vi em
mong muốn: một hạn mức chi phí nên xuống cấp thành câu trả lời từ cache, chứ không phải thành
một trang lỗi trắng.

### 10.3 Redis xuống cấp mềm

`SharedRedisCache` nhận thêm cờ `local_fallback`, mặc định là bật. Mọi lời gọi Redis đều được
bọc lại. Khi gặp `ConnectionError`, `TimeoutError`, `RedisError` hoặc `OSError`, cache sẽ tăng
`degraded_operations`, ghi vào `degradation_log`, rồi phục vụ bằng một `ResponseCache` nội bộ.
Các lần ghi được nhân bản xuống bộ nhớ cục bộ trước khi ghi lên Redis, nhờ vậy một cache bị
mất Redis giữa chừng vẫn còn trạng thái nóng ở cục bộ để dựa vào.

Em kiểm chứng bằng cách trỏ vào một cổng không có ai lắng nghe (`redis://localhost:6399/0`):

```
cache.ping()              -> False
cache.set("hello world", "local answer")
cache.get("hello world")  -> ('local answer', 1.0)     # do fallback cục bộ phục vụ
cache.degraded_operations -> 2
cache.degradation_log[0]  -> {'reason': 'redis_unavailable', 'operation': 'set', ...}
```

Với `local_fallback=False` thì cùng chuỗi thao tác đó trả về `(None, 0.0)`, tức là một lần
miss sạch sẽ, không có exception nào lọt lên tới gateway. Cả hai nhánh đều có test trong
`tests/test_stretch_goals.py`.

### 10.4 Trạng thái circuit lưu trên Redis

Đây chính là hướng sửa em đề xuất ở mục 8, và lần này em làm thật, đặt tên là
`SharedCircuitBreaker`:

```
rl:cb:<name>:failures   INCR + EXPIRE 60s    bộ đếm lỗi trượt, dùng chung toàn cụm
rl:cb:<name>:open       SET NX EX <reset>    cờ open, đồng thời là cơ chế bầu probe
```

Phần chịu lực chính là `SET ... NX`. Khi cờ open hết hạn, mọi bản sao cùng đua nhau tạo lại
nó; đúng một bản sao thắng và được phép gửi probe ở trạng thái HALF_OPEN, những bản sao thua
tiếp tục fail fast. Nhờ vậy lưu lượng probe của cả cụm chỉ là một request cho mỗi chu kỳ
reset, bất kể có bao nhiêu bản sao.

Em đo với 3 rồi 5 bản sao trong cùng tiến trình dùng chung một Redis:

```
# bộ đếm lỗi dùng chung: 2 lỗi ở bản sao A cộng 1 lỗi ở bản sao B là đủ ngắt circuit
sau 2 lỗi ở A:      A cho phép True  | B cho phép True     # vẫn dưới ngưỡng
sau lỗi thứ 3 ở B:  A trạng thái closed | B trạng thái open
A cho phép False | B cho phép False                        # A tiếp nhận sự cố dùng chung

# bầu probe giữa 5 bản sao, sau khi cờ open hết hạn
sau chu kỳ reset, kết quả bầu -> [True, False, False, False, False] | số bản sao thắng: 1
```

Khi Redis chết thì mọi nhánh ở trên đều lùi về logic in-memory của lớp cha, nên cơ chế bảo vệ
chỉ yếu đi chứ không biến mất. Có ba test trong `tests/test_stretch_goals.py` phủ các tình
huống: trạng thái open dùng chung, bầu ra đúng một bản sao, và phục hồi toàn cụm khi probe
thành công.

### 10.5 Test theo tính chất

File `tests/test_properties.py` dùng `hypothesis` để dò những trường hợp mà test viết tay
không liệt kê hết được. Một `RuleBasedStateMachine` điều khiển breaker bằng các chuỗi ngẫu
nhiên gồm success, failure và allow_request (200 ví dụ, mỗi ví dụ 40 bước), rồi kiểm tra các
bất biến phải đúng sau mọi bước:

- các bộ đếm không bao giờ âm
- `state == OPEN` kéo theo `opened_at is not None`, nếu không thì bộ đếm giờ reset trở nên vô
  nghĩa
- một circuit đang OPEN với timeout dài thì luôn từ chối request
- một circuit đang CLOSED không bao giờ nằm trên một bộ đếm lỗi đã vượt ngưỡng
- không có lần chuyển trạng thái nào là tự trỏ về chính nó (`from != to`), vì kiểu chuyển đó
  sẽ thổi phồng `circuit_open_count`
- mọi lần chuyển sang OPEN đều mang một trong hai lý do hợp lệ

Ngoài ra em viết thêm bốn tính chất độc lập: circuit mở đúng tại ngưỡng với mọi cặp (số lỗi,
ngưỡng); một lần thành công luôn đưa bộ đếm lỗi về 0; N lần lỗi liên tiếp chỉ ghi đúng một lần
chuyển sang OPEN; và hàm similarity luôn nằm trong đoạn [0, 1] và đối xứng với chuỗi bất kỳ.

Hai bất biến về tự trỏ và về một lần OPEN duy nhất là hai cái em thấy đáng giá nhất, vì cả hai
đều bắt được đúng lỗi kinh điển ở `record_failure()`, khi một provider chết làm hàm này ghi
thêm một entry OPEN cho mỗi lần gọi lỗi và con số đếm sự cố trong báo cáo mất hết ý nghĩa.
