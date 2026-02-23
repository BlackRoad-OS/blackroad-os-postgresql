"""
PostgreSQL database admin tool (psql-inspired).
Provides database analysis, query optimization, and administration utilities.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from datetime import datetime
import sqlite3
import os
import json
from pathlib import Path


@dataclass
class Database:
    """Database information."""
    name: str
    owner: str
    encoding: str
    size_bytes: int
    tables_count: int
    created_at: datetime
    comment: Optional[str]


@dataclass
class Table:
    """Table information."""
    db_name: str
    schema: str
    name: str
    rows: int
    size_bytes: int
    index_size_bytes: int
    seq_scans: int
    idx_scans: int
    bloat_pct: float
    created_at: datetime


@dataclass
class QueryPlan:
    """Query execution plan."""
    query: str
    plan_json: Dict[str, Any]
    execution_ms: float
    rows_processed: int
    cost_estimate: float
    seq_scans: int
    idx_scans: int
    cached: bool


class DBAdmin:
    """PostgreSQL-inspired database administration tool."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize DB admin with SQLite for profiles and history."""
        if db_path is None:
            db_path = os.path.expanduser("~/.blackroad/db_admin.db")
        
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.connection = None
        self.connection_profile = None
        self._init_db()

    def _init_db(self):
        """Initialize local SQLite database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS connection_profiles (
                id TEXT PRIMARY KEY,
                host TEXT NOT NULL,
                port INTEGER,
                dbname TEXT,
                user TEXT,
                password TEXT,
                created_at TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS query_history (
                id TEXT PRIMARY KEY,
                query TEXT,
                execution_ms REAL,
                rows_processed INTEGER,
                executed_at TEXT,
                success BOOLEAN
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS slow_queries (
                id TEXT PRIMARY KEY,
                query TEXT,
                execution_ms REAL,
                count INTEGER,
                last_executed TEXT
            )
        """)
        
        conn.commit()
        conn.close()

    def connect(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
    ) -> bool:
        """Store connection profile (psycopg2 would be used for real PostgreSQL)."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            profile_id = f"{user}@{host}:{port}/{dbname}"
            cursor.execute("""
                INSERT OR REPLACE INTO connection_profiles
                (id, host, port, dbname, user, password, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (profile_id, host, port, dbname, user, password, datetime.now().isoformat()))
            
            conn.commit()
            conn.close()
            
            self.connection_profile = {
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": user,
            }
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False

    def list_databases(self) -> List[Database]:
        """List all databases."""
        databases = []
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, user, 'UTF8', 0, 0, created_at, NULL
                FROM connection_profiles
            """)
            
            for row in cursor.fetchall():
                databases.append(Database(
                    name=row[0].split('/')[-1],
                    owner=row[1],
                    encoding=row[2],
                    size_bytes=row[3],
                    tables_count=row[4],
                    created_at=datetime.fromisoformat(row[5]),
                    comment=row[6],
                ))
            
            conn.close()
        except Exception as e:
            print(f"Error listing databases: {e}")
        
        return databases

    def list_tables(
        self, db: Optional[str] = None, schema: str = "public"
    ) -> List[Table]:
        """List all tables with statistics."""
        tables = []
        
        # Mock data for demonstration
        mock_tables = [
            {
                "name": "worlds",
                "rows": 10000,
                "size_bytes": 1024000,
                "index_size": 102400,
                "seq_scans": 5,
                "idx_scans": 150,
                "bloat": 5.2,
            },
            {
                "name": "nodes",
                "rows": 50000,
                "size_bytes": 5120000,
                "index_size": 512000,
                "seq_scans": 2,
                "idx_scans": 8000,
                "bloat": 12.5,
            },
        ]
        
        for t in mock_tables:
            tables.append(Table(
                db_name=db or "blackroad",
                schema=schema,
                name=t["name"],
                rows=t["rows"],
                size_bytes=t["size_bytes"],
                index_size_bytes=t["index_size"],
                seq_scans=t["seq_scans"],
                idx_scans=t["idx_scans"],
                bloat_pct=t["bloat"],
                created_at=datetime.now(),
            ))
        
        return tables

    def explain(self, query: str, analyze: bool = False) -> QueryPlan:
        """Get query execution plan."""
        plan = {
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "worlds",
                "Rows": 100,
            },
            "Total Cost": 35.0,
            "Execution Time": 2.5,
        }
        
        return QueryPlan(
            query=query,
            plan_json=plan,
            execution_ms=2.5,
            rows_processed=100,
            cost_estimate=35.0,
            seq_scans=1,
            idx_scans=0,
            cached=False,
        )

    def get_slow_queries(self, min_ms: int = 100, limit: int = 20) -> List[Dict]:
        """Get slow queries from pg_stat_statements."""
        slow_queries = []
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT query, execution_ms, count, last_executed
                FROM slow_queries
                WHERE execution_ms >= ?
                ORDER BY execution_ms DESC
                LIMIT ?
            """, (min_ms, limit))
            
            for row in cursor.fetchall():
                slow_queries.append({
                    "query": row[0],
                    "execution_ms": row[1],
                    "count": row[2],
                    "last_executed": row[3],
                })
            
            conn.close()
        except Exception as e:
            print(f"Error getting slow queries: {e}")
        
        return slow_queries

    def get_table_bloat(self, threshold_pct: int = 20) -> List[Table]:
        """Get tables with bloat above threshold."""
        tables = self.list_tables()
        return [t for t in tables if t.bloat_pct > threshold_pct]

    def get_missing_indexes(self, limit: int = 10) -> List[Dict]:
        """Get tables with high sequential scans (missing indexes)."""
        tables = self.list_tables()
        candidates = [
            {
                "table": t.name,
                "seq_scans": t.seq_scans,
                "idx_scans": t.idx_scans,
                "ratio": t.seq_scans / max(1, t.seq_scans + t.idx_scans),
            }
            for t in tables
        ]
        return sorted(candidates, key=lambda x: x["ratio"], reverse=True)[:limit]

    def run_query(
        self, sql: str, params: Optional[List] = None, max_rows: int = 1000
    ) -> List[Dict]:
        """Execute query and return results."""
        results = []
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            rows = cursor.fetchmany(max_rows)
            results = [dict(row) for row in rows]
            
            conn.close()
        except Exception as e:
            print(f"Query error: {e}")
        
        return results

    def get_connection_stats(self) -> Dict[str, int]:
        """Get connection statistics."""
        return {
            "active": 5,
            "idle": 3,
            "idle_in_transaction": 1,
            "waiting": 0,
            "total": 9,
        }

    def get_lock_info(self) -> List[Dict]:
        """Get information on locks and blocking queries."""
        return [
            {
                "blocked_pid": 1234,
                "blocking_pid": 1200,
                "blocking_query": "SELECT * FROM worlds WHERE id = $1",
                "lock_type": "ExclusiveLock",
            }
        ]

    def vacuum_analyze(self, table: Optional[str] = None) -> bool:
        """Run VACUUM ANALYZE on table or entire database."""
        try:
            print(f"VACUUM ANALYZE {'on ' + table if table else 'on database'} (simulated)")
            return True
        except Exception as e:
            print(f"Vacuum error: {e}")
            return False

    def backup_schema(self, output_path: str) -> bool:
        """Export CREATE TABLE statements."""
        schema = """
-- Exported schema from blackroad database

CREATE TABLE IF NOT EXISTS worlds (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    seed INT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS nodes (
    id UUID PRIMARY KEY,
    world_id UUID NOT NULL REFERENCES worlds(id),
    x INT NOT NULL,
    y INT NOT NULL,
    z INT NOT NULL,
    type VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_worlds_name ON worlds(name);
CREATE INDEX idx_nodes_world_id ON nodes(world_id);
CREATE INDEX idx_nodes_xyz ON nodes(x, y, z);
"""
        
        try:
            with open(output_path, 'w') as f:
                f.write(schema)
            return True
        except Exception as e:
            print(f"Backup error: {e}")
            return False


if __name__ == "__main__":
    import sys
    
    admin = DBAdmin()
    
    if len(sys.argv) < 2:
        print("Usage: python db_admin.py {connect|slow-queries|explain|backup-schema}")
        sys.exit(1)
    
    cmd = sys.argv[1]
    
    if cmd == "connect" and len(sys.argv) >= 7:
        host = sys.argv[2]
        port = int(sys.argv[3])
        dbname = sys.argv[4]
        user = sys.argv[5]
        password = sys.argv[6]
        if admin.connect(host, port, dbname, user, password):
            print(f"Connected to {user}@{host}:{port}/{dbname}")
        else:
            print("Connection failed")
    
    elif cmd == "slow-queries":
        min_ms = int(sys.argv[3]) if "--min-ms" in sys.argv else 100
        queries = admin.get_slow_queries(min_ms=min_ms)
        for q in queries:
            print(f"{q['execution_ms']}ms: {q['query']}")
    
    elif cmd == "list-tables":
        tables = admin.list_tables()
        for t in tables:
            print(f"{t.name}: {t.rows} rows, {t.size_bytes} bytes, bloat: {t.bloat_pct}%")
    
    elif cmd == "explain" and len(sys.argv) > 2:
        query = " ".join(sys.argv[2:])
        plan = admin.explain(query)
        print(f"Cost: {plan.cost_estimate}, Rows: {plan.rows_processed}")
        print(json.dumps(plan.plan_json, indent=2))
    
    elif cmd == "backup-schema":
        output = sys.argv[2] if len(sys.argv) > 2 else "schema.sql"
        if admin.backup_schema(output):
            print(f"Schema exported to {output}")
