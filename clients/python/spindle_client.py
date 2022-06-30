"""
Spindle Python Client SDK
High-performance asynchronous activity worker client for the Spindle durable execution kernel.
"""

from __future__ import annotations
import json
import time
import urllib.request
import urllib.error
from typing import Any, Callable, Dict, List, Optional


class SpindleClient:
    """HTTP client communicating with the Spindle kernel management endpoints."""

    def __init__(self, endpoint: str = "http://localhost:5000", timeout_sec: float = 10.0):
        self.endpoint = endpoint.rstrip("/")
        self.timeout_sec = timeout_sec

    def _request(self, method: str, path: str, data: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.endpoint}{path}"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        payload = json.dumps(data).encode("utf-8") if data is not None else None

        req = urllib.request.Request(url, data=payload, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                status_code = resp.getcode()
                raw = resp.read().decode("utf-8")
                if not raw:
                    return None
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            raw_err = e.read().decode("utf-8")
            raise RuntimeError(f"Spindle API error HTTP {e.code}: {raw_err}") from e
        except urllib.error.URLError as e:
            raise ConnectionError(f"Failed to connect to Spindle kernel at {url}: {e.reason}") from e

    def start_workflow(
        self,
        workflow_type: str,
        workflow_id: str,
        tenant_id: str = "default",
        input_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Start a new durable workflow execution."""
        body = {
            "WorkflowType": workflow_type,
            "WorkflowId": workflow_id,
            "TenantId": tenant_id,
            "InputJson": json.dumps(input_data or {})
        }
        return self._request("POST", "/api/v1/workflows/start", body)

    def get_status(self, workflow_id: str) -> Dict[str, Any]:
        """Query snapshot and execution status of a workflow instance."""
        return self._request("GET", f"/api/v1/workflows/{workflow_id}/status")

    def get_history(self, workflow_id: str) -> List[Dict[str, Any]]:
        """Fetch complete cryptographic event sourcing journal history."""
        return self._request("GET", f"/api/v1/workflows/{workflow_id}/history")

    def signal_workflow(self, workflow_id: str, signal_name: str, payload: Dict[str, Any]) -> None:
        """Deliver an external signal to a waiting workflow instance."""
        body = {
            "SignalName": signal_name,
            "PayloadJson": json.dumps(payload)
        }
        self._request("POST", f"/api/v1/workflows/{workflow_id}/signal", body)

    def get_cluster_topology(self) -> Dict[str, Any]:
        """Retrieve SWIM cluster membership table and shard partitioning."""
        return self._request("GET", "/api/v1/cluster/topology")

    def get_metrics(self) -> Dict[str, Any]:
        """Retrieve telemetry and runtime engine statistics."""
        return self._request("GET", "/api/v1/metrics")


class ActivityWorker:
    """Asynchronous activity worker hosting polyglot activity handlers."""

    def __init__(self, client: SpindleClient, task_queue: str = "default-queue"):
        self.client = client
        self.task_queue = task_queue
        self.handlers: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {}

    def activity(self, name: str):
        """Decorator to register a Python function as a durable activity handler."""
        def decorator(func: Callable[[Dict[str, Any]], Dict[str, Any]]):
            self.handlers[name] = func
            return func
        return decorator

    def execute_activity(self, activity_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Directly invoke a registered activity handler with input arguments."""
        if activity_type not in self.handlers:
            raise KeyError(f"No registered handler for activity type '{activity_type}'")
        handler = self.handlers[activity_type]
        return handler(payload)
