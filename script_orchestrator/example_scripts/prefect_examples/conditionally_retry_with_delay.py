# /// script
# dependencies = ["prefect"]
# ///

"""
Demonstrates using built-in retry functionality to retry a task after a delay
when specific HTTP status codes are encountered.
"""

from typing import Any

import httpx
from prefect import Task, task
from prefect.client.schemas.objects import TaskRun
from prefect.states import State


def retry_on_503(task: Task[..., Any], task_run: TaskRun, state: State[Any]) -> bool:
    try:
        state.result()
    except Exception as e:
        if isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 503:
            return True
    return False


@task(
    retries=2,
    retry_delay_seconds=[3, 9],
    retry_condition_fn=retry_on_503,
)
def make_api_call():
    from prefect.context import TaskRunContext

    # Get the current retry count from context
    context = TaskRunContext.get()
    run_count = context.task_run.run_count if context and context.task_run else 1

    # Succeed on the final retry (run_count 3 = initial + 2 retries)
    if run_count >= 3:
        print(f"✅ Attempt {run_count}: Success! (demonstrating successful retry)")
        response = httpx.get("https://httpbin.org/status/200")
    else:
        print(f"⚠️  Attempt {run_count}: Simulating 503 error (will retry)")
        response = httpx.get("https://httpbin.org/status/503")

    response.raise_for_status()
    return response.text


if __name__ == "__main__":
    result = make_api_call()
    print(f"\n🎉 Task completed successfully!")
    print(f"Response preview: {result[:100]}...")
