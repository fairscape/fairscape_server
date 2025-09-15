import time
from fastapi import Request

def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()
    response = call_next(request)
    process_time = time.perf_counter() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response