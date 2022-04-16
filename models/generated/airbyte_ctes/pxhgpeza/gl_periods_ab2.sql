{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_pxhgpeza",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('gl_periods_ab1') }}
select
    cast({{ empty_string_to_null('end_date') }} as {{ type_date() }}) as end_date,
    cast(attribute1 as {{ dbt_utils.type_string() }}) as attribute1,
    cast(attribute2 as {{ dbt_utils.type_string() }}) as attribute2,
    cast(attribute3 as {{ dbt_utils.type_string() }}) as attribute3,
    cast(attribute4 as {{ dbt_utils.type_string() }}) as attribute4,
    cast(attribute5 as {{ dbt_utils.type_string() }}) as attribute5,
    cast(attribute6 as {{ dbt_utils.type_string() }}) as attribute6,
    cast(attribute7 as {{ dbt_utils.type_string() }}) as attribute7,
    cast(attribute8 as {{ dbt_utils.type_string() }}) as attribute8,
    cast(created_by as {{ dbt_utils.type_string() }}) as created_by,
    cast(period_num as {{ dbt_utils.type_float() }}) as period_num,
    cast({{ empty_string_to_null('start_date') }} as {{ type_date() }}) as start_date,
    cast(description as {{ dbt_utils.type_string() }}) as description,
    cast(fiscal_year as {{ dbt_utils.type_string() }}) as fiscal_year,
    cast(period_name as {{ dbt_utils.type_string() }}) as period_name,
    cast(period_type as {{ dbt_utils.type_string() }}) as period_type,
    cast(period_year as {{ dbt_utils.type_float() }}) as period_year,
    cast(quarter_num as {{ dbt_utils.type_float() }}) as quarter_num,
    cast({{ empty_string_to_null('creation_date') }} as {{ type_timestamp_with_timezone() }}) as creation_date,
    cast(enterprise_id as {{ dbt_utils.type_float() }}) as enterprise_id,
    cast({{ empty_string_to_null('attribute_date1') }} as {{ type_date() }}) as attribute_date1,
    cast({{ empty_string_to_null('attribute_date2') }} as {{ type_date() }}) as attribute_date2,
    cast({{ empty_string_to_null('attribute_date3') }} as {{ type_date() }}) as attribute_date3,
    cast({{ empty_string_to_null('attribute_date4') }} as {{ type_date() }}) as attribute_date4,
    cast({{ empty_string_to_null('attribute_date5') }} as {{ type_date() }}) as attribute_date5,
    cast(last_updated_by as {{ dbt_utils.type_string() }}) as last_updated_by,
    cast(period_set_name as {{ dbt_utils.type_string() }}) as period_set_name,
    cast({{ empty_string_to_null('year_start_date') }} as {{ type_date() }}) as year_start_date,
    cast({{ empty_string_to_null('last_update_date') }} as {{ type_timestamp_with_timezone() }}) as last_update_date,
    cast(attribute_number1 as {{ dbt_utils.type_float() }}) as attribute_number1,
    cast(attribute_number2 as {{ dbt_utils.type_float() }}) as attribute_number2,
    cast(attribute_number3 as {{ dbt_utils.type_float() }}) as attribute_number3,
    cast(attribute_number4 as {{ dbt_utils.type_float() }}) as attribute_number4,
    cast(attribute_number5 as {{ dbt_utils.type_float() }}) as attribute_number5,
    cast(global_attribute1 as {{ dbt_utils.type_string() }}) as global_attribute1,
    cast(global_attribute2 as {{ dbt_utils.type_string() }}) as global_attribute2,
    cast(global_attribute3 as {{ dbt_utils.type_string() }}) as global_attribute3,
    cast(global_attribute4 as {{ dbt_utils.type_string() }}) as global_attribute4,
    cast(global_attribute5 as {{ dbt_utils.type_string() }}) as global_attribute5,
    cast(global_attribute6 as {{ dbt_utils.type_string() }}) as global_attribute6,
    cast(global_attribute7 as {{ dbt_utils.type_string() }}) as global_attribute7,
    cast(global_attribute8 as {{ dbt_utils.type_string() }}) as global_attribute8,
    cast(global_attribute9 as {{ dbt_utils.type_string() }}) as global_attribute9,
    cast(last_update_login as {{ dbt_utils.type_string() }}) as last_update_login,
    cast(attribute_category as {{ dbt_utils.type_string() }}) as attribute_category,
    cast(global_attribute10 as {{ dbt_utils.type_string() }}) as global_attribute10,
    cast(global_attribute11 as {{ dbt_utils.type_string() }}) as global_attribute11,
    cast(global_attribute12 as {{ dbt_utils.type_string() }}) as global_attribute12,
    cast(global_attribute13 as {{ dbt_utils.type_string() }}) as global_attribute13,
    cast(global_attribute14 as {{ dbt_utils.type_string() }}) as global_attribute14,
    cast(global_attribute15 as {{ dbt_utils.type_string() }}) as global_attribute15,
    cast(global_attribute16 as {{ dbt_utils.type_string() }}) as global_attribute16,
    cast(global_attribute17 as {{ dbt_utils.type_string() }}) as global_attribute17,
    cast(global_attribute18 as {{ dbt_utils.type_string() }}) as global_attribute18,
    cast(global_attribute19 as {{ dbt_utils.type_string() }}) as global_attribute19,
    cast(global_attribute20 as {{ dbt_utils.type_string() }}) as global_attribute20,
    cast({{ empty_string_to_null('quarter_start_date') }} as {{ type_date() }}) as quarter_start_date,
    cast(confirmation_status as {{ dbt_utils.type_string() }}) as confirmation_status,
    cast(entered_period_name as {{ dbt_utils.type_string() }}) as entered_period_name,
    cast(object_version_number as {{ dbt_utils.type_float() }}) as object_version_number,
    cast(adjustment_period_flag as {{ dbt_utils.type_string() }}) as adjustment_period_flag,
    cast({{ empty_string_to_null('global_attribute_date1') }} as {{ type_date() }}) as global_attribute_date1,
    cast({{ empty_string_to_null('global_attribute_date2') }} as {{ type_date() }}) as global_attribute_date2,
    cast({{ empty_string_to_null('global_attribute_date3') }} as {{ type_date() }}) as global_attribute_date3,
    cast({{ empty_string_to_null('global_attribute_date4') }} as {{ type_date() }}) as global_attribute_date4,
    cast({{ empty_string_to_null('global_attribute_date5') }} as {{ type_date() }}) as global_attribute_date5,
    cast(global_attribute_number1 as {{ dbt_utils.type_float() }}) as global_attribute_number1,
    cast(global_attribute_number2 as {{ dbt_utils.type_float() }}) as global_attribute_number2,
    cast(global_attribute_number3 as {{ dbt_utils.type_float() }}) as global_attribute_number3,
    cast(global_attribute_number4 as {{ dbt_utils.type_float() }}) as global_attribute_number4,
    cast(global_attribute_number5 as {{ dbt_utils.type_float() }}) as global_attribute_number5,
    cast(global_attribute_category as {{ dbt_utils.type_string() }}) as global_attribute_category,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('gl_periods_ab1') }}
-- gl_periods
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

