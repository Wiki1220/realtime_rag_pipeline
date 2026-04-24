# -*- coding: utf-8 -*-
import os
import sqlite3
import json
import logging
from typing import List, Dict, Any, Optional, Iterable, Set

from core.context import Context

logger = logging.getLogger("ReviewStore")

class ReviewStore:
    """
    基于本地 SQLite 的超轻量级审核数据持久化层。
    没有额外依赖，保证待审核数据不会因为服务器重启而丢失。
    """
    def __init__(self, db_path: str = "logs/pending_reviews.db"):
        self.db_path = db_path
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self._init_db()
        self._refresh_pending_metric()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pending_reviews (
                    id TEXT PRIMARY KEY,
                    context_json TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()

    def _refresh_pending_metric(self):
        try:
            from utils.metrics import PENDING_REVIEWS
        except Exception:
            return

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM pending_reviews')
            total = int(cursor.fetchone()[0])
        PENDING_REVIEWS.set(total)

    def save_pending_context(self, context_id: str, context: Context):
        """保存被挂起的 Context"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            ctx_data = json.dumps(context.to_dict(), ensure_ascii=False)
            cursor.execute('''
                INSERT OR REPLACE INTO pending_reviews (id, context_json)
                VALUES (?, ?)
            ''', (context_id, ctx_data))
            conn.commit()
            self._refresh_pending_metric()
            logger.info(f"💾 Context {context_id} saved to pending queue.")

    def get_pending_records(self, limit: int = 50) -> List[Dict[str, Any]]:
        """API端获取待审核列表"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id, context_json, created_at FROM pending_reviews ORDER BY created_at ASC LIMIT ?', (limit,))
            rows = cursor.fetchall()
            
        results = []
        for row in rows:
            record_id = row[0]
            ctx_dict = json.loads(row[1])
            created_at = row[2]
            raw_data = ctx_dict.get("raw_data")
            doc_id = raw_data.get("id") if isinstance(raw_data, dict) else None
            # 为了前端展示，只提取关键字段而不是整坨大向量
            results.append({
                "id": record_id,
                "run_id": record_id,
                "doc_id": doc_id,
                "created_at": created_at,
                "raw_data": raw_data,
                "metadata": ctx_dict.get("metadata")
            })
        return results

    def get_context(self, context_id: str) -> Optional[Context]:
        """只读取挂起 Context，不删除记录。"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT context_json FROM pending_reviews WHERE id = ?', (context_id,))
            row = cursor.fetchone()

        if not row:
            return None
        return Context.from_dict(json.loads(row[0]))

    def has_pending_context(self, context_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM pending_reviews WHERE id = ? LIMIT 1', (context_id,))
            return cursor.fetchone() is not None

    def find_existing_context_ids(self, context_ids: Iterable[str]) -> Set[str]:
        context_ids = [context_id for context_id in context_ids if context_id]
        if not context_ids:
            return set()

        placeholders = ",".join("?" for _ in context_ids)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f'SELECT id FROM pending_reviews WHERE id IN ({placeholders})',
                tuple(context_ids),
            )
            return {row[0] for row in cursor.fetchall()}

    def delete_context(self, context_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM pending_reviews WHERE id = ?', (context_id,))
            conn.commit()
            deleted = cursor.rowcount > 0
        if deleted:
            self._refresh_pending_metric()
        return deleted

    def get_and_delete_context(self, context_id: str) -> Optional[Context]:
        """审核完毕后，取出 Context 并从挂起队伍中删除"""
        context = self.get_context(context_id)
        if context is None:
            return None
        self.delete_context(context_id)
        return context
