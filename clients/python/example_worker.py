#!/usr/bin/env python3
"""
Example polyglot Python activity worker for Spindle.
Demonstrates registering handlers for e-commerce and financial saga operations.
"""

import sys
import time
from spindle_client import SpindleClient, ActivityWorker


def main():
    print("=" * 60)
    print("Spindle Python Activity Worker Process")
    print("=" * 60)

    client = SpindleClient(endpoint="http://localhost:5000")
    worker = ActivityWorker(client, task_queue="ecommerce-queue")

    @worker.activity("AuthorizePayment")
    def handle_authorize_payment(order_data):
        print(f"[Worker] Processing AuthorizePayment for order: {order_data.get('order_id')}")
        time.sleep(0.05)
        return {
            "auth_code": "AUTH-PY-7789",
            "authorized_amount": order_data.get("total_amount", 100.0),
            "status": "APPROVED"
        }

    @worker.activity("RefundPayment")
    def handle_refund_payment(auth_data):
        print(f"[Worker] Compacting RefundPayment for auth_code: {auth_data.get('auth_code')}")
        time.sleep(0.02)
        return {"refund_id": "REF-9901", "status": "REFUNDED"}

    @worker.activity("ReserveInventory")
    def handle_reserve_inventory(order_data):
        print(f"[Worker] Reserving inventory items: {order_data.get('items')}")
        time.sleep(0.04)
        return {
            "reservation_token": "RES-TOK-445",
            "warehouse_id": "WH-CENTRAL"
        }

    @worker.activity("ReleaseInventory")
    def handle_release_inventory(res_data):
        print(f"[Worker] Compacting ReleaseInventory for token: {res_data.get('reservation_token')}")
        return {"status": "RELEASED"}

    print("[Worker] Registered 4 activities:")
    for act_name in worker.handlers.keys():
        print(f"  * {act_name}")

    print("\n[Worker] Simulating sample activity execution:")
    sample_order = {"order_id": "ord-8812", "total_amount": 349.50, "items": ["gpu-cooler", "pcie-riser"]}
    auth_res = worker.execute_activity("AuthorizePayment", sample_order)
    print(f"  Result -> {auth_res}")

    inv_res = worker.execute_activity("ReserveInventory", sample_order)
    print(f"  Result -> {inv_res}")

    print("\n[Worker] Activity worker initialized and ready.")


if __name__ == "__main__":
    main()
