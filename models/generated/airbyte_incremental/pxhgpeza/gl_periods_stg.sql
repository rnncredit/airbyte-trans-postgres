{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_pxhgpeza",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('gl_periods_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'end_date',
        'attribute1',
        'attribute2',
        'attribute3',
        'attribute4',
        'attribute5',
        'attribute6',
        'attribute7',
        'attribute8',
        'created_by',
        'period_num',
        'start_date',
        'description',
        'fiscal_year',
        'period_name',
        'period_type',
        'period_year',
        'quarter_num',
        'creation_date',
        'attribute_date1',
        'attribute_date2',
        'attribute_date3',
        'attribute_date4',
        'attribute_date5',
        'last_updated_by',
        'period_set_name',
        'year_start_date',
        'last_update_date',
        'global_attribute1',
        'global_attribute2',
        'global_attribute3',
        'global_attribute4',
        'global_attribute5',
        'global_attribute6',
        'global_attribute7',
        'global_attribute8',
        'global_attribute9',
        'last_update_login',
        'attribute_category',
        'global_attribute10',
        'global_attribute11',
        'global_attribute12',
        'global_attribute13',
        'global_attribute14',
        'global_attribute15',
        'global_attribute16',
        'global_attribute17',
        'global_attribute18',
        'global_attribute19',
        'global_attribute20',
        'quarter_start_date',
        'confirmation_status',
        'entered_period_name',
        'adjustment_period_flag',
        'global_attribute_date1',
        'global_attribute_date2',
        'global_attribute_date3',
        'global_attribute_date4',
        'global_attribute_date5',
        'global_attribute_category',
    ]) }} as _airbyte_gl_periods_hashid,
    tmp.*
from {{ ref('gl_periods_ab2') }} tmp
-- gl_periods
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

