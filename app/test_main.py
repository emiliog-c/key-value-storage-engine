import random
import time

import requests
from requests.adapters import HTTPAdapter

BASE_URL = "http://localhost:8000"
CHAOS_RATE = 0.05  # 5% of requests will simulate a disconnect

latencies = []
acknowledged_writes: dict[str, str] = {}


# --- Chaos adapter ---


class ChaosAdapter(HTTPAdapter):
    """
    Wraps a real HTTP adapter and randomly raises ConnectionError
    before the request completes, simulating a network disconnect.
    """

    def __init__(self, chaos_rate: float = 0.05, **kwargs):
        self.chaos_rate = chaos_rate
        super().__init__(**kwargs)

    def send(self, request, **kwargs):
        if random.random() < self.chaos_rate:
            print(f"[CHAOS] Simulating disconnect on {request.method} {request.url}")
            raise requests.ConnectionError("Simulated network disconnect")
        return super().send(request, **kwargs)


def make_session() -> requests.Session:
    session = requests.Session()
    adapter = ChaosAdapter(chaos_rate=CHAOS_RATE)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# --- Helpers ---


def calculate_percentile(sorted_data, p):
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[-1]
    d0 = sorted_data[f]
    d1 = sorted_data[c]
    return d0 + (d1 - d0) * (k - f)


def request_with_retry(session, method, url, **kwargs):
    """
    Retry indefinitely with exponential backoff on any connection-level
    failure (real server down OR simulated disconnect from ChaosAdapter).
    """
    delay = 0.5
    attempt = 0
    while True:
        try:
            response = session.request(method, url, timeout=5, **kwargs)
            return response
        except (requests.ConnectionError, requests.Timeout):
            attempt += 1
            print(
                f"[RETRY] Connection lost, attempt {attempt}. Retrying in {delay:.1f}s..."
            )
            time.sleep(delay)
            delay = min(delay * 2, 30)


def verify_durability(session):
    print(f"\n[DURABILITY] Verifying {len(acknowledged_writes)} acknowledged writes...")
    lost = []
    for key, expected_value in acknowledged_writes.items():
        response = request_with_retry(session, "GET", f"{BASE_URL}/{key}")
        if response.status_code != 200:
            lost.append((key, expected_value, f"HTTP {response.status_code}"))
            continue
        actual = response.json().get("value")
        if str(actual) != expected_value:
            lost.append((key, expected_value, actual))

    if lost:
        print(f"[DURABILITY] FAILED — {len(lost)} lost writes:")
        for key, expected, actual in lost:
            print(f"  key='{key}'  expected='{expected}'  got='{actual}'")
        assert False, f"{len(lost)} acknowledged writes were lost after restart!"
    else:
        print(
            f"[DURABILITY] PASSED — all {len(acknowledged_writes)} writes survived.\n"
        )


# --- Main test ---


def test_api_sequence_from_file():
    session = make_session()

    with open("put.txt", "r") as file:
        lines = [line.strip() for line in file if line.strip()]

    for line_number, line in enumerate(lines, start=1):
        parts = line.split()
        method = parts[0]
        key = parts[1]
        value_or_expected = parts[2]

        start_time = time.perf_counter()

        if method == "PUT":
            response = request_with_retry(
                session, "PUT", f"{BASE_URL}/{key}", json={"value": value_or_expected}
            )
            assert response.status_code == 200, (
                f"Line {line_number}: PUT failed for key '{key}'"
            )
            acknowledged_writes[key] = value_or_expected

        elif method == "GET":
            response = request_with_retry(session, "GET", f"{BASE_URL}/{key}")

            if value_or_expected == "NOT_FOUND":
                assert response.status_code == 404, (
                    f"Line {line_number}: Expected 404 for '{key}', got {response.status_code}"
                )
            else:
                assert response.status_code == 200, (
                    f"Line {line_number}: Expected 200 for GET '{key}', got {response.status_code}"
                )
                actual_value = response.json().get("value")
                assert str(actual_value) == value_or_expected, (
                    f"Line {line_number}: Mismatch on '{key}'. "
                    f"Expected '{value_or_expected}', got '{actual_value}'"
                )

        end_time = time.perf_counter()
        latencies.append((end_time - start_time) * 1000)

    verify_durability(session)

    if latencies:
        latencies.sort()
        p50 = calculate_percentile(latencies, 50)
        p95 = calculate_percentile(latencies, 95)
        p99 = calculate_percentile(latencies, 99)

        print("\n=== Latency Metrics ===")
        print(f"Total Requests:   {len(latencies)}")
        print(f"Fastest:          {latencies[0]:.2f} ms")
        print(f"Slowest:          {latencies[-1]:.2f} ms")
        print(f"p50 (Median):     {p50:.2f} ms")
        print(f"p95:              {p95:.2f} ms")
        print(f"p99:              {p99:.2f} ms")
        print("=======================\n")
