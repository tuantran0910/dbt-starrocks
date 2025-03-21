#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from concurrent.futures import Future
from typing import Callable, Dict, List, Optional, Set, FrozenSet, Tuple

import agate
import dbt.exceptions
from dbt.adapters.base import available
from dbt.adapters.base.impl import _expect_row_value, catch_as_completed
from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.protocol import AdapterConfig
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.sql.impl import LIST_RELATIONS_MACRO_NAME, LIST_SCHEMAS_MACRO_NAME
from dbt_common.clients.agate_helper import table_from_rows
from dbt_common.utils import executor
from typing_extensions import override

from dbt.adapters.starrocks.column import StarRocksColumn
from dbt.adapters.starrocks.connections import StarRocksConnectionManager
from dbt.adapters.starrocks.relation import StarRocksRelation


class StarRocksConfig(AdapterConfig):
    engine: Optional[str] = None
    table_type: Optional[str] = None  # DUPLICATE/PRIMARY/UNIQUE/AGGREGATE
    keys: Optional[List[str]] = None
    partition_by: Optional[List[str]] = None
    partition_by_init: Optional[List[str]] = None
    distributed_by: Optional[List[str]] = None
    buckets: Optional[int] = None
    properties: Optional[Dict[str, str]] = None
    microbatch_use_dynamic_overwrite: Optional[bool] = None


class StarRocksAdapter(SQLAdapter):
    ConnectionManager = StarRocksConnectionManager
    Relation = StarRocksRelation
    AdapterSpecificConfigs = StarRocksConfig
    Column = StarRocksColumn

    @classmethod
    def date_function(cls) -> str:
        return "current_date()"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "datetime"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    def quote(self, identifier):
        return "`{}`".format(identifier)

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database})

        exists = True if schema in [row[0] for row in results] else False
        return exists

    def get_relation(self, database: Optional[str], schema: str, identifier: str):
        if not self.Relation.get_default_include_policy().database:
            database = None

        return super().get_relation(database, schema, identifier)

    def list_relations_without_caching(
        self, schema_relation: StarRocksRelation
    ) -> List[StarRocksRelation]:
        kwargs = {"schema_relation": schema_relation}
        results = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)

        relations = []
        for row in results:
            if len(row) != 4:
                raise dbt.exceptions.DbtRuntimeError(
                    f"Invalid value from 'show table extended ...', "
                    f"got {len(row)} values, expected 4"
                )
            _database, name, schema, type_info = row
            relation = self.Relation.create(
                database=None,
                schema=schema,
                identifier=name,
                type=self.Relation.get_relation_type(type_info),
            )
            relations.append(relation)

        return relations

    def get_catalog(self, manifest, used_schemas):
        schema_map = self._get_catalog_schemas(manifest)
        if len(schema_map) > 1:
            dbt.exceptions.CompilationError(
                f"Expected only one database in get_catalog, found " f"{list(schema_map)}"
            )

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    for d, s in used_schemas:
                        if schema.lower() == s.lower():
                            schema = s
                            break
                    futures.append(
                        tpe.submit_connected(
                            self,
                            schema,
                            self._get_one_catalog,
                            info,
                            [schema],
                            used_schemas,
                        )
                    )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    @classmethod
    def _catalog_filter_table(
        cls, table: "agate.Table", used_schemas: FrozenSet[Tuple[str, str]]
    ) -> agate.Table:
        table = table_from_rows(
            table.rows,
            table.column_names,
            text_only_columns=["table_schema", "table_name"],
        )
        return table.where(_catalog_filter_schemas(used_schemas))

    @available
    def is_before_version(self, version: str) -> bool:
        conn = self.connections.get_if_exists()
        if conn:
            server_version = conn.handle.server_version
            server_version_tuple = tuple(server_version)
            version_detail_tuple = tuple(
                int(part) for part in version.split(".") if part.isdigit()
            )
            if version_detail_tuple > server_version_tuple:
                return True
        return False

    @available
    def current_version(self):
        conn = self.connections.get_if_exists()
        if conn:
            server_version = conn.handle.server_version
            if server_version != (999, 999, 999):
                return "{}.{}.{}".format(server_version[0], server_version[1], server_version[2])
        return "UNKNOWN"

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> agate.Table:
        if len(schemas) != 1:
            dbt.exceptions.CompilationError(
                f"Expected only one schema in StarRocks _get_one_catalog, found " f"{schemas}"
            )

        return super()._get_one_catalog(information_schema, schemas, used_schemas)

    @override
    def valid_incremental_strategies(self):
        return ["default", "insert_overwrite", "dynamic_overwrite", "microbatch"]


def _catalog_filter_schemas(
    used_schemas: FrozenSet[Tuple[str, str]]
) -> Callable[[agate.Row], bool]:
    schemas = frozenset((None, s.lower()) for d, s in used_schemas)

    def test(row: agate.Row) -> bool:
        table_database = _expect_row_value("table_database", row)
        table_schema = _expect_row_value("table_schema", row)
        if table_schema is None:
            return False
        return (table_database, table_schema.lower()) in schemas

    return test
