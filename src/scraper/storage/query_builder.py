# src/scraper/storage/query_builder.py
# For a simple SQLite setup without a heavy ORM, complex query building might be overkill.
# Basic queries can be constructed directly in database.py.
# This file is a placeholder or for more advanced dynamic query needs.

from typing import List, Dict, Any, Tuple, Optional

class SQLQueryBuilder:
    """
    A simple SQL query builder.
    Mainly for constructing INSERT statements with ON CONFLICT for SQLite.
    """

    @staticmethod
    def build_insert_on_conflict_do_nothing(table_name: str, data: Dict[str, Any]) -> Tuple[str, List[Any]]:
        """
        Builds an INSERT INTO ... ON CONFLICT (unique_column) DO NOTHING query.
        Assumes 'hash_id' is the unique column for conflict resolution.
        """
        columns = list(data.keys())
        placeholders = ', '.join(['?'] * len(columns))
        column_names = ', '.join(columns)
        
        # Assuming 'hash_id' is the column that has a UNIQUE constraint.
        # If other columns can cause conflict, this needs to be more generic.
        # For insider_trades, hash_id is UNIQUE.
        # For source_metadata, source_name is PRIMARY KEY (implicitly UNIQUE).
        
        # Determine the conflict target column (must be UNIQUE or PRIMARY KEY)
        if table_name == "insider_trades" and "hash_id" in columns:
            conflict_target = "hash_id"
        elif table_name == "source_metadata" and "source_name" in columns:
            conflict_target = "source_name" # Primary key
        else:
            # Fallback or raise error if no clear conflict target.
            # For DO NOTHING, sometimes specifying the conflict target is not strictly needed if any unique constraint is violated.
            # However, it's better practice.
            # If we don't specify (column), it applies to any UNIQUE constraint violation.
            # For simplicity, let's assume we want general "do nothing" on any unique conflict.
            # For specific "ON CONFLICT (column_name) DO UPDATE", column_name is essential.
            # query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            
            # SQLite syntax: ON CONFLICT DO NOTHING (no need to specify target for DO NOTHING on any unique constraint)
            # If you want to be specific for `hash_id` for `insider_trades`:
            # query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders}) ON CONFLICT(hash_id) DO NOTHING"
            
            # General ON CONFLICT DO NOTHING:
            query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"


        values = [data[col] for col in columns]
        return query, values

    @staticmethod
    def build_upsert_query(table_name: str, data: Dict[str, Any], conflict_target_column: str) -> Tuple[str, List[Any]]:
        """
        Builds an INSERT ... ON CONFLICT(conflict_target_column) DO UPDATE SET ... query.
        """
        columns = list(data.keys())
        placeholders = ', '.join(['?'] * len(columns))
        column_names = ', '.join(columns)

        # Columns to update, excluding the conflict target itself and potentially creation timestamps
        update_columns = [col for col in columns if col != conflict_target_column and col not in ['scraped_at', 'id']]
        
        if not update_columns: # Nothing to update, might as well DO NOTHING
             return SQLQueryBuilder.build_insert_on_conflict_do_nothing(table_name, data)

        set_clauses = ', '.join([f"{col} = excluded.{col}" for col in update_columns])
        
        # Add updated_at if it exists in the table schema definition for this table
        if table_name == "insider_trades": # Assuming 'insider_trades' has 'updated_at'
            set_clauses += ", updated_at = CURRENT_TIMESTAMP" # Ensure this is compatible with schema
        elif table_name == "source_metadata": # Assuming 'source_metadata' might have similar logic
            if "last_successful_scrape_at" in update_columns or "last_checked_at" in update_columns:
                 # No explicit updated_at in source_metadata schema, these fields are updated directly
                 pass


        query = (
            f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders}) "
            f"ON CONFLICT({conflict_target_column}) DO UPDATE SET {set_clauses}"
        )
        
        values = [data[col] for col in columns]
        return query, values

    # Add other query building methods as needed, e.g., for SELECT, UPDATE, DELETE
    @staticmethod
    def build_select_query(table_name: str, columns: Optional[List[str]] = None,
                           conditions: Optional[Dict[str, Any]] = None,
                           order_by: Optional[str] = None, limit: Optional[int] = None) -> Tuple[str, List[Any]]:
        """Builds a SELECT query."""
        select_cols = "*" if not columns else ", ".join(columns)
        query = f"SELECT {select_cols} FROM {table_name}"
        
        values = []
        if conditions:
            where_clauses = []
            for col, val in conditions.items():
                if isinstance(val, tuple) and len(val) == 2: # e.g. ('operator', value) -> "col > ?"
                    operator, actual_val = val
                    where_clauses.append(f"{col} {operator} ?")
                    values.append(actual_val)
                else: # e.g. "col = ?"
                    where_clauses.append(f"{col} = ?")
                    values.append(val)
            query += " WHERE " + " AND ".join(where_clauses)
            
        if order_by:
            query += f" ORDER BY {order_by}"
            
        if limit is not None:
            query += f" LIMIT ?"
            values.append(limit)
            
        return query, values