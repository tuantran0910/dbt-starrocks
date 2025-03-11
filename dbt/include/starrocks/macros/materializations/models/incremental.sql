/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https:*www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

{% macro get_incremental_insert_overwrite_sql(arg_dict) %}
      {% do return(get_insert_overwrite_into_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"])) %}
{% endmacro %}

{% macro get_incremental_dynamic_overwrite_sql(arg_dict) %}
      {% do return(get_dynamic_overwrite_into_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"])) %}
{% endmacro %}

{% macro _get_strategy_sql(target_relation, temp_relation, dest_cols_csv, is_insert_overwrite=false, is_dynamic_overwrite=false) %}
    {%- if not is_insert_overwrite and is_dynamic_overwrite %}
        {%- set msg -%}
            [is_insert_overwrite] and [is_dynamic_overwrite] cannot be set to True at the same time
        {%- endset %}
        {% do log(msg, warn=True) %}
    {% endif %}

    {%- if not is_insert_overwrite and is_dynamic_overwrite %}
        {%- set overwrite_type = "TRUE" if is_dynamic_overwrite else "FALSE" %}
        insert overwrite /*+SET_VAR(dynamic_overwrite = {{ overwrite_type }})*/ {{ target_relation }}({{ dest_cols_csv }})
        (
            select {{ dest_cols_csv }}
            from {{ temp_relation }}
        )
    {%- else %}
        insert into {{ target_relation }}({{ dest_cols_csv }})
        (
            select {{ dest_cols_csv }}
            from {{ temp_relation }}
        )
    {%- endif %}
{% endmacro %}

{% macro get_insert_overwrite_into_sql(target_relation, temp_relation, dest_columns) %}
    {%- do return(_get_strategy_sql(target_relation, temp_relation, dest_columns, true, false)) -%}
{% endmacro %}

{% macro get_dynamic_overwrite_into_sql(target_relation, temp_relation, dest_columns) %}
    {% if adapter.is_before_version("3.4.0") %}
        {%- set msg -%}
            [dynamic_overwrite] is only available from version 3.4.0 onwards, current version is {{ adapter.current_version() }}
        {%- endset -%}
        {{ exceptions.raise_compiler_error(msg) }}
    {% else %}
        {%- do return(_get_strategy_sql(target_relation, temp_relation, dest_columns, true, true)) -%}
    {% endif %}
{% endmacro %}

{% materialization incremental, adapter='starrocks' %}
    {% set keys = config.get('keys', validator=validation.any[list]) %}
    {% set strategy = starrocks__validate_get_incremental_strategy(config) %}
    {% set microbatch_use_dynamic_overwrite = config.get('microbatch_use_dynamic_overwrite') or False %}
    {% if microbatch_use_dynamic_overwrite and strategy != 'microbatch' %}
        {% do log("The 'microbatch_use_dynamic_overwrite' configuration can only be set when using the 'microbatch' incremental strategy.", warn=True) %}
        {% set microbatch_use_dynamic_overwrite = False %}
    {% endif %}

    {% set full_refresh_mode = (should_full_refresh()) %}
    {% set existing_relation = load_relation(this) %}
    {% set is_target_relation_existed, target_relation = get_or_create_relation(
        database=this.database,
        schema=this.schema,
        identifier=this.identifier,
        type='table',
    ) %}
    {% set tmp_relation = make_temp_relation(this) %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}
    {% set to_drop = [] %}

    {% if not keys or strategy == 'default' %}
        {% if existing_relation is none %}
            {% set build_sql = starrocks__create_table_as(False, target_relation, sql) %}
        {% elif existing_relation.is_view or full_refresh_mode %}
            {% if existing_relation.is_view %}
                {% do starrocks__drop_relation(existing_relation) %}
                {% set build_sql = starrocks__create_table_as(False, target_relation, sql) %}
            {% else %}
                {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
                {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
                {% do adapter.drop_relation(backup_relation) %}
                {% set run_sql = starrocks__create_table_as(False, backup_relation, sql) %}
                {% call statement("run_sql") %}
                    {{ run_sql }}
                {% endcall %}
                {% do starrocks__exchange_relation(target_relation, backup_relation) %}
                {% set build_sql = "select 'hello starrocks'" %}
            {% endif %}
        {% else %}
            {% do to_drop.append(tmp_relation) %}
            {% do run_query(create_table_as(True, tmp_relation, sql)) %}
            {% set dest_columns = adapter.get_columns_in_relation(target_relation) | map(attribute='quoted') | join(', ') %}
            {% set build_sql = _get_strategy_sql(target_relation, tmp_relation, dest_columns, false) %}
        {% endif %}
    {% elif strategy in ['insert_overwrite', 'dynamic_overwrite', 'microbatch'] %}
        {% if not is_target_relation_existed %}
            {% set build_sql = starrocks__create_table_as(False, target_relation, sql) %}
        {% elif existing_relation.is_view or full_refresh_mode %}
            {% if existing_relation.is_view %}
                {% do starrocks__drop_relation(existing_relation) %}
                {% set build_sql = starrocks__create_table_as(False, target_relation, sql) %}
            {% else %}
                {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
                {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
                {% do starrocks__drop_relation(backup_relation) %}
                {% set run_sql = starrocks__create_table_as(False, backup_relation, sql) %}
                {% call statement("run_sql") %}
                    {{ run_sql }}
                {% endcall %}
                {% do starrocks__exchange_relation(target_relation, backup_relation) %}
                {% set build_sql = "select 'hello starrocks'" %}
            {% endif %}
        {% else %}
            {% do run_query(starrocks__create_table_as(True, tmp_relation, sql)) %}
            {% do to_drop.append(tmp_relation) %}
            {% do adapter.expand_target_column_types(
                  from_relation=tmp_relation,
                  to_relation=target_relation
            ) %}
            {% set dest_columns = adapter.get_columns_in_relation(target_relation) | map(attribute='quoted') | join(', ') %}
            {% if strategy == 'insert_overwrite' or (strategy == 'microbatch' and not microbatch_use_dynamic_overwrite) %}
                {% set build_sql = get_incremental_insert_overwrite_sql(({
                    "target_relation": target_relation,
                    "temp_relation": tmp_relation,
                    "dest_columns": dest_columns
                })) %}
            {% elif strategy == 'dynamic_overwrite' or (strategy == 'microbatch' and microbatch_use_dynamic_overwrite) %}
                {% set build_sql = get_incremental_dynamic_overwrite_sql(({
                    "target_relation": target_relation,
                    "temp_relation": tmp_relation,
                    "dest_columns": dest_columns
                })) %}
            {% endif %}
        {% endif %}
    {% endif %}

    {% call statement("main") %}
        {{ build_sql }}
    {% endcall %}

    {% do persist_docs(target_relation, model) %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {% do adapter.commit() %}
    {% for rel in to_drop %}
        {% do starrocks__drop_relation(rel) %}
    {% endfor %}
    {{ run_hooks(post_hooks, inside_transaction=False) }}
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
