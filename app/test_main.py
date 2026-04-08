import time

from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

latencies = []


def calculate_percentile(sorted_data, p):
    """Simple percentile calculation for a sorted list."""
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[-1]
    # Linear interpolation
    d0 = sorted_data[f]
    d1 = sorted_data[c]
    return d0 + (d1 - d0) * (k - f)


def test_api_sequence_from_file():
    with open("put.txt", "r") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()
            parts = line.split()
            method = parts[0]
            key = parts[1]
            value_or_expected = parts[2]
            start_time = time.perf_counter()
            if method == "PUT":
                response = client.put(f"/{key}", json={"value": value_or_expected})
                assert response.status_code == 200, (
                    f"Line {line_number}: PUT request failed for key '{key}'"
                )

            elif method == "GET":
                response = client.get(f"/{key}")

                if value_or_expected == "NOT_FOUND":
                    assert response.status_code == 404, (
                        f"Line {line_number}: Expected 404 for missing key '{key}', but got {response.status_code}"
                    )
                else:
                    assert response.status_code == 200, (
                        f"Line {line_number}: Expected 200 for GET '{key}', but got {response.status_code}"
                    )

                    data = response.json()
                    actual_value = data.get("value")

                    assert str(actual_value) == value_or_expected, (
                        f"Line {line_number}: Mismatch on '{key}'. Expected '{value_or_expected}', got '{actual_value}'"
                    )

            # End timer and record latency in milliseconds
            end_time = time.perf_counter()
            latency_ms = (end_time - start_time) * 1000
            latencies.append(latency_ms)

            # --- Process and Print Metrics ---

    if latencies:
        latencies.sort()
        p50 = calculate_percentile(latencies, 50)
        p95 = calculate_percentile(latencies, 95)
        p99 = calculate_percentile(latencies, 99)

        print("\n\n=== Latency Metrics ===")
        print(f"Total Requests Executed: {len(latencies)}")
        print(f"Fastest Request:         {latencies[0]:.2f} ms")
        print(f"Slowest Request:         {latencies[-1]:.2f} ms")
        print(f"p50 (Median) Latency:    {p50:.2f} ms")
        print(f"p95 Latency:             {p95:.2f} ms")
        print(f"p99 Latency:             {p99:.2f} ms")
        print("=======================\n")
