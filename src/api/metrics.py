from prometheus_client import Counter, Histogram

CACHE_REQUESTS = Counter(
    "capstone_cache_requests_total",
    "API cache requests",
    ["result"],
)

HTTP_REQUESTS = Counter(
    "capstone_http_requests_total",
    "HTTP requests",
    ["route", "status"],
)

HTTP_DURATION = Histogram(
    "capstone_http_duration_seconds",
    "HTTP request duration in seconds",
    ["route"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)

REDIS_ERRORS = Counter(
    "capstone_redis_errors_total",
    "Failed Redis cache operations",
    ["operation"],
)
