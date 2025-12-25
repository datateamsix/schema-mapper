"""
Example 7: Connection Pooling for High-Concurrency Workloads

Demonstrates using connection pools for efficient resource management:
1. Create connection pool
2. Handle concurrent requests
3. Monitor pool statistics
4. Optimize pool configuration

Time: ~10 minutes
Prerequisites: Database credentials in config/connections.yaml
"""

import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from schema_mapper.connections import ConnectionFactory, ConnectionConfig


def worker_task(pool, worker_id: int, num_queries: int):
    """Simulate worker performing database operations."""
    results = []

    for i in range(num_queries):
        try:
            # Get connection from pool
            with pool.get_connection() as conn:
                # Simulate database work
                if conn.test_connection():
                    result = f"Worker {worker_id} - Query {i+1}: SUCCESS"
                else:
                    result = f"Worker {worker_id} - Query {i+1}: FAILED"

                results.append(result)

                # Simulate query execution time
                time.sleep(0.1)

        except Exception as e:
            results.append(f"Worker {worker_id} - Query {i+1}: ERROR - {e}")

    return results


def demonstrate_pool_statistics(pool):
    """Show pool statistics and metrics."""
    print("\n📊 Pool Statistics:")
    print("=" * 80)

    stats = pool.get_stats()

    print(f"   Configuration:")
    print(f"      Min Size: {stats['min_size']}")
    print(f"      Max Size: {stats['max_size']}")

    print(f"\n   Current State:")
    print(f"      Total Connections: {stats['total_connections']}")
    print(f"      Active: {stats['active_connections']}")
    print(f"      Idle: {stats['idle_connections']}")
    print(f"      Utilization: {stats['utilization']:.1%}")

    print(f"\n   Lifetime Metrics:")
    print(f"      Total Created: {stats['total_created']}")
    print(f"      Total Checkouts: {stats['total_checkouts']}")
    print(f"      Total Checkins: {stats['total_checkins']}")
    print(f"      Validations Failed: {stats['total_validations_failed']}")
    print(f"      Cleanups: {stats['total_cleanups']}")


def main():
    """Run connection pooling example."""

    print("=" * 80)
    print("Example 7: Connection Pooling")
    print("=" * 80)

    # ========================================================================
    # Step 1: Configuration
    # ========================================================================

    print("\n🔧 Step 1: Loading configuration...")

    config_path = Path(__file__).parent.parent / 'config' / 'connections.yaml'

    if not config_path.exists():
        print(f"   ⚠️  Config not found: {config_path}")
        print("\n   📝 Demo Mode:")
        print("      Connection pooling provides:")
        print("      • Reuse connections across requests")
        print("      • Thread-safe operations")
        print("      • Automatic health checks")
        print("      • Resource cleanup")
        print("\n   Create config/connections.yaml to run with real connections")
        return

    config = ConnectionConfig(str(config_path))
    target = config.get_default_target()

    print(f"   ✓ Target: {target}")

    # ========================================================================
    # Step 2: Create Connection Pool
    # ========================================================================

    print("\n🏊 Step 2: Creating connection pool...")

    pool = ConnectionFactory.create_pool(
        target=target,
        config=config,
        min_size=2,           # Minimum connections to maintain
        max_size=10,          # Maximum connections allowed
        max_idle_seconds=300, # Cleanup idle connections after 5 minutes
        max_lifetime_seconds=3600,  # Recycle connections after 1 hour
        validate_on_checkout=True,  # Validate health before use
        wait_timeout=30       # Max wait time for connection
    )

    print(f"   ✓ Pool created:")
    print(f"      Min: 2 | Max: 10")
    print(f"      Validation: Enabled")
    print(f"      Wait timeout: 30s")

    try:
        # Show initial stats
        demonstrate_pool_statistics(pool)

        # ====================================================================
        # Step 3: Single-threaded Usage
        # ====================================================================

        print("\n" + "=" * 80)
        print("Step 3: Single-threaded Usage")
        print("=" * 80)

        print("\n   Executing 3 sequential queries...")

        for i in range(3):
            with pool.get_connection() as conn:
                if conn.test_connection():
                    print(f"   ✓ Query {i+1}: Connection {conn.platform_name()} healthy")
                    time.sleep(0.1)  # Simulate work

        demonstrate_pool_statistics(pool)

        # ====================================================================
        # Step 4: Multi-threaded Usage
        # ====================================================================

        print("\n" + "=" * 80)
        print("Step 4: Multi-threaded Usage (Concurrent Requests)")
        print("=" * 80)

        num_workers = 5
        queries_per_worker = 3

        print(f"\n   Spawning {num_workers} workers...")
        print(f"   Each worker executes {queries_per_worker} queries")
        print(f"   Total: {num_workers * queries_per_worker} queries\n")

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit tasks
            futures = [
                executor.submit(worker_task, pool, worker_id, queries_per_worker)
                for worker_id in range(1, num_workers + 1)
            ]

            # Collect results
            all_results = []
            completed = 0

            for future in as_completed(futures):
                try:
                    results = future.result()
                    all_results.extend(results)
                    completed += 1
                    print(f"   ✓ Worker {completed}/{num_workers} completed")
                except Exception as e:
                    print(f"   ✗ Worker failed: {e}")

        elapsed = time.time() - start_time

        print(f"\n   ✓ All workers complete")
        print(f"   ⏱️  Total time: {elapsed:.2f}s")
        print(f"   📊 Throughput: {len(all_results) / elapsed:.1f} queries/sec")

        # Show final statistics
        demonstrate_pool_statistics(pool)

        # ====================================================================
        # Step 5: Pool Benefits Analysis
        # ====================================================================

        print("\n" + "=" * 80)
        print("Pool Benefits Analysis")
        print("=" * 80)

        stats = pool.get_stats()

        print(f"\n   Connection Reuse:")
        print(f"      Created: {stats['total_created']}")
        print(f"      Checkouts: {stats['total_checkouts']}")
        print(f"      Reuse Ratio: {stats['total_checkouts'] / max(stats['total_created'], 1):.1f}x")

        print(f"\n   Resource Efficiency:")
        print(f"      Peak Active: {stats['active_connections']}")
        print(f"      Pool Size: {stats['total_connections']}")
        print(f"      Saved Connections: {stats['total_checkouts'] - stats['total_created']}")

        print(f"\n   💡 Without pooling:")
        print(f"      Would create {stats['total_checkouts']} connections")
        print(f"      Each with connection overhead")
        print(f"      With pooling:")
        print(f"      Created only {stats['total_created']} connections")
        print(f"      Reused {stats['total_checkouts'] - stats['total_created']} times!")

        # ====================================================================
        # Summary
        # ====================================================================

        print("\n" + "=" * 80)
        print("✅ Connection Pooling Demo Complete")
        print("=" * 80)

        print(f"\n📊 Summary:")
        print(f"   Workers: {num_workers}")
        print(f"   Queries: {len(all_results)}")
        print(f"   Connections Created: {stats['total_created']}")
        print(f"   Reuse Factor: {stats['total_checkouts'] / max(stats['total_created'], 1):.1f}x")
        print(f"   Duration: {elapsed:.2f}s")

        print(f"\n💡 When to Use Pooling:")
        print(f"   ✓ API servers with concurrent requests")
        print(f"   ✓ Multi-threaded data processing")
        print(f"   ✓ Long-running applications")
        print(f"   ✓ Microservices with database access")

        print(f"\n🔧 Pool Tuning Tips:")
        print(f"   • min_size: Based on typical load")
        print(f"   • max_size: Based on peak load + buffer")
        print(f"   • max_idle: Balance resources vs latency")
        print(f"   • validate_on_checkout: Enable for reliability")

    finally:
        # Always close the pool
        print(f"\n🧹 Cleaning up...")
        pool.close()
        print(f"   ✓ Pool closed")


if __name__ == '__main__':
    main()
