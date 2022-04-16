{{ config(
    indexes = [{'columns':['_airbyte_active_row','_airbyte_unique_key_scd','_airbyte_emitted_at'],'type': 'btree'}],
    unique_key = "_airbyte_unique_key_scd",
    schema = "pxhgpeza",
    post_hook = ["delete from _airbyte_pxhgpeza.gl_periods_stg where _airbyte_emitted_at != (select max(_airbyte_emitted_at) from _airbyte_pxhgpeza.gl_periods_stg)"],
    tags = [ "top-level" ]
) }}
-- depends_on: ref('gl_periods_stg')
with
{% if is_incremental() %}
new_data as (
    -- retrieve incremental "new" data
    select
        *
    from {{ ref('gl_periods_stg')  }}
    -- gl_periods from {{ source('pxhgpeza', '_airbyte_raw_gl_periods') }}
    where 1 = 1
    {{ incremental_clause('_airbyte_emitted_at') }}
),
new_data_ids as (
    -- build a subset of _airbyte_unique_key from rows that are new
    select distinct
        {{ dbt_utils.surrogate_key([
            'period_name',
            'period_set_name',
            'period_type',
        ]) }} as _airbyte_unique_key
    from new_data
),
empty_new_data as (
    -- build an empty table to only keep the table's column types
    select * from new_data where 1 = 0
),
previous_active_scd_data as (
    -- retrieve "incomplete old" data that needs to be updated with an end date because of new changes
    select
        {{ star_intersect(ref('gl_periods_stg'), this, from_alias='inc_data', intersect_alias='this_data') }}
    from {{ this }} as this_data
    -- make a join with new_data using primary key to filter active data that need to be updated only
    join new_data_ids on this_data._airbyte_unique_key = new_data_ids._airbyte_unique_key
    -- force left join to NULL values (we just need to transfer column types only for the star_intersect macro on schema changes)
    left join empty_new_data as inc_data on this_data._airbyte_ab_id = inc_data._airbyte_ab_id
    where _airbyte_active_row = 1
),
input_data as (
    select {{ dbt_utils.star(ref('gl_periods_stg')) }} from new_data
    union all
    select {{ dbt_utils.star(ref('gl_periods_stg')) }} from previous_active_scd_data
),
{% else %}
input_data as (
    select *
    from {{ ref('gl_periods_stg')  }}
    -- gl_periods from {{ source('pxhgpeza', '_airbyte_raw_gl_periods') }}
),
{% endif %}
scd_data as (
    -- SQL model to build a Type 2 Slowly Changing Dimension (SCD) table for each record identified by their primary key
    select
      {{ dbt_utils.surrogate_key([
      'period_name',
      'period_set_name',
      'period_type',
      ]) }} as _airbyte_unique_key,
      end_date,
      attribute1,
      attribute2,
      attribute3,
      attribute4,
      attribute5,
      attribute6,
      attribute7,
      attribute8,
      created_by,
      period_num,
      start_date,
      description,
      fiscal_year,
      period_name,
      period_type,
      period_year,
      quarter_num,
      creation_date,
      enterprise_id,
      attribute_date1,
      attribute_date2,
      attribute_date3,
      attribute_date4,
      attribute_date5,
      last_updated_by,
      period_set_name,
      year_start_date,
      last_update_date,
      attribute_number1,
      attribute_number2,
      attribute_number3,
      attribute_number4,
      attribute_number5,
      global_attribute1,
      global_attribute2,
      global_attribute3,
      global_attribute4,
      global_attribute5,
      global_attribute6,
      global_attribute7,
      global_attribute8,
      global_attribute9,
      last_update_login,
      attribute_category,
      global_attribute10,
      global_attribute11,
      global_attribute12,
      global_attribute13,
      global_attribute14,
      global_attribute15,
      global_attribute16,
      global_attribute17,
      global_attribute18,
      global_attribute19,
      global_attribute20,
      quarter_start_date,
      confirmation_status,
      entered_period_name,
      object_version_number,
      adjustment_period_flag,
      global_attribute_date1,
      global_attribute_date2,
      global_attribute_date3,
      global_attribute_date4,
      global_attribute_date5,
      global_attribute_number1,
      global_attribute_number2,
      global_attribute_number3,
      global_attribute_number4,
      global_attribute_number5,
      global_attribute_category,
      last_update_date as _airbyte_start_at,
      lag(last_update_date) over (
        partition by period_name, period_set_name, period_type
        order by
            last_update_date is null asc,
            last_update_date desc,
            _airbyte_emitted_at desc
      ) as _airbyte_end_at,
      case when row_number() over (
        partition by period_name, period_set_name, period_type
        order by
            last_update_date is null asc,
            last_update_date desc,
            _airbyte_emitted_at desc
      ) = 1 then 1 else 0 end as _airbyte_active_row,
      _airbyte_ab_id,
      _airbyte_emitted_at,
      _airbyte_gl_periods_hashid
    from input_data
),
dedup_data as (
    select
        -- we need to ensure de-duplicated rows for merge/update queries
        -- additionally, we generate a unique key for the scd table
        row_number() over (
            partition by
                _airbyte_unique_key,
                _airbyte_start_at,
                _airbyte_emitted_at
            order by _airbyte_active_row desc, _airbyte_ab_id
        ) as _airbyte_row_num,
        {{ dbt_utils.surrogate_key([
          '_airbyte_unique_key',
          '_airbyte_start_at',
          '_airbyte_emitted_at'
        ]) }} as _airbyte_unique_key_scd,
        scd_data.*
    from scd_data
)
select
    _airbyte_unique_key,
    _airbyte_unique_key_scd,
    end_date,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    created_by,
    period_num,
    start_date,
    description,
    fiscal_year,
    period_name,
    period_type,
    period_year,
    quarter_num,
    creation_date,
    enterprise_id,
    attribute_date1,
    attribute_date2,
    attribute_date3,
    attribute_date4,
    attribute_date5,
    last_updated_by,
    period_set_name,
    year_start_date,
    last_update_date,
    attribute_number1,
    attribute_number2,
    attribute_number3,
    attribute_number4,
    attribute_number5,
    global_attribute1,
    global_attribute2,
    global_attribute3,
    global_attribute4,
    global_attribute5,
    global_attribute6,
    global_attribute7,
    global_attribute8,
    global_attribute9,
    last_update_login,
    attribute_category,
    global_attribute10,
    global_attribute11,
    global_attribute12,
    global_attribute13,
    global_attribute14,
    global_attribute15,
    global_attribute16,
    global_attribute17,
    global_attribute18,
    global_attribute19,
    global_attribute20,
    quarter_start_date,
    confirmation_status,
    entered_period_name,
    object_version_number,
    adjustment_period_flag,
    global_attribute_date1,
    global_attribute_date2,
    global_attribute_date3,
    global_attribute_date4,
    global_attribute_date5,
    global_attribute_number1,
    global_attribute_number2,
    global_attribute_number3,
    global_attribute_number4,
    global_attribute_number5,
    global_attribute_category,
    _airbyte_start_at,
    _airbyte_end_at,
    _airbyte_active_row,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_gl_periods_hashid
from dedup_data where _airbyte_row_num = 1

